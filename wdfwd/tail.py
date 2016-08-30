import os
import glob
import time
import threading
import socket
import logging
from StringIO import StringIO
import traceback
import json
import uuid
import msgpack
from collections import namedtuple

import win32file
import pywintypes
from fluent.sender import FluentSender, MAX_SEND_FAIL
from aws_kinesis_agg import aggregator

from wdfwd.util import OpenNoLock, get_fileid, escape_path, validate_format as\
    _validate_format, validate_order_ptrn as _validate_order_ptrn,\
    query_aws_client

MAX_READ_BUF = 1024 * 1024
MAX_SEND_RETRY = 5
SEND_TERM = 1       # 1 second
UPDATE_TERM = 5     # 5 seconds
MAX_BETWEEN_DATA = 1024 * 1024
FMT_NO_BODY = 0
FMT_JSON_BODY = 1
FMT_TEXT_BODY = 2
BULK_SEND_SIZE = 200

FluentCfg = namedtuple('FluentCfg', ['host', 'port'])
KinesisCfg = namedtuple('KinesisCfg', ['stream_name', 'region', 'access_key', 'secret_key'])


class NoTargetFile(Exception):
    pass


class LatestFileChanged(Exception):
    pass


class TailThread(threading.Thread):
    def __init__(self, name, tailer):
        threading.Thread.__init__(self)
        self.name = name
        self.tailer = tailer
        self.linfo("TailThread::__init__ - {}".format(self.name))
        self._exit = False

    def ldebug(self, tabfunc, msg=""):
        _log(self.tailer, 'debug', tabfunc, msg)

    def lwarning(self, tabfunc, msg=""):
        _log(self.tailer, 'warning', tabfunc, msg)

    def lerror(self, tabfunc, msg=""):
        _log(self.tailer, 'error', tabfunc, msg)

    def linfo(self, tabfunc, msg=""):
        _log(self.tailer, 'info', tabfunc, msg)

    def run(self):
        self.linfo("start run")
        self.tailer.update_target(True)

        sltime = 1
        while True:
            try:
                time.sleep(sltime)

                st = time.time()
                self.linfo("TAIL START: sltime {}".format(sltime))
                self.tailer.tmain()
                elapsed = time.time() - st
                self.linfo("TAIL END IN {}".format(elapsed))
                sltime = 0 if elapsed > 1 else 1

                if self._exit:
                    break
            except NoTargetFile:
                self.lwarning("run", "NoTargetfile")
            except Exception, e:
                self.linfo("run", str(e))
                tb = traceback.format_exc()
                for line in tb.splitlines():
                    self.lerror(line)

    def exit(self):
        self.linfo("exit")
        self._exit = True


def get_file_lineinfo(path, post_lines=None):
    pos_tot = 0
    line_tot = 0
    if post_lines:
        with OpenNoLock(path) as fh:
            while True:
                res, data = win32file.ReadFile(fh, MAX_READ_BUF, None)
                nbyte = len(data)
                pos_tot += nbyte
                line_tot += len(data.splitlines())
                if nbyte < MAX_READ_BUF:
                    break

    pos = 0
    nlines = 0
    with OpenNoLock(path) as fh:
        while True:
            res, data = win32file.ReadFile(fh, MAX_READ_BUF, None)
            nbyte = len(data)
            for line in data.splitlines():
                if post_lines and line_tot - nlines <= post_lines:
                    break
                pos += len(line) + 2  # for \r\n
                if pos_tot and pos > pos_tot:
                    pos = pos_tot
                nlines += 1
            if nbyte < MAX_READ_BUF:
                break
    return nlines, pos


def _log(tail, level, tabfunc, _msg):
    if logging.getLogger().getEffectiveLevel() > getattr(logging,
                                                         level.upper()):
        return

    lfun = getattr(logging, level)
    if type(tabfunc) == int:
        msg = "  " * tabfunc + "{}".format(_msg)
    elif len(_msg) > 0:
        msg = "{} - {}".format(tabfunc, _msg)
    else:
        msg = "{}".format(tabfunc)

    lfun(msg)
    ts = int(time.time())
    if tail.fsender:
        try:
            tail.fsender.emit_with_time("{}".format(level), ts, {"message": msg})
        except Exception, e:
            logging.warning("_log", "send fail '{}'".format(e))


class FileTailer(object):
    def __init__(self, bdir, ptrn, tag, pdir,
                 stream_cfg,
                 send_term=SEND_TERM, update_term=UPDATE_TERM,
                 max_send_fail=None, elatest=None, echo=False, encoding=None,
                 lines_on_start=None, max_between_data=None, format=None,
                 parser=None, order_ptrn=None, reverse_order=False):

        self.fsender = self.kclient = None
        self.ksent_seqn = self.ksent_shid = None
        self.linfo("__init__", "max_send_fail: '{}'".format(max_send_fail))
        self.bdir = bdir
        self.ptrn = ptrn
        self.sname = socket.gethostname()
        self.saddr = socket.gethostbyname(self.sname)
        tag = "{}.{}".format(self.sname.lower(), tag)
        self.linfo(1, "tag: '{}'".format(tag))
        self.tag = tag
        self.target_path = self.target_fid = None
        self.send_term = send_term
        self.last_send_try = 0
        self.update_term = update_term
        self.last_update = 0
        self.kpk_cnt = 0  # count for kinesis partition key
        self.reverse_order = reverse_order
        max_send_fail = max_send_fail if max_send_fail else MAX_SEND_FAIL

        tstc = type(stream_cfg)
        if tstc == FluentCfg:
            host, port = stream_cfg
            self.fsender = FluentSender(tag, host, port, max_send_fail=max_send_fail)
        elif tstc == KinesisCfg:
            stream_name, region, access_key, secret_key = stream_cfg
            self.kstream_name = stream_name
            self.ldebug('query_aws_client kinesis {}'.format(region))
            self.kclient = query_aws_client('kinesis', region, access_key,
                                            secret_key)
            self.kagg = aggregator.RecordAggregator()

        # AWS Kinesis
        self.pdir = pdir
        self.send_retry = 0
        self.elatest = elatest
        self.echo_file = StringIO() if echo else None
        self.elatest_fid = None
        self.cache_sent_pos = {}
        self.encoding = encoding
        self.lines_on_start = lines_on_start if lines_on_start else 0
        self.max_between_data = max_between_data if max_between_data else\
            MAX_BETWEEN_DATA
        self._reset_ml_msg()
        self.linfo("effective format: '{}'".format(format))
        self.format = self.validate_format(format)
        self.fmt_body = self.format_body_type(format)
        self.parser = parser
        self.pending_mlmsg = None
        self.order_ptrn = self.validate_order_ptrn(order_ptrn)
        self.parser_compl = 0
        self.no_format = False

    def format_body_type(self, format):
        if format:
            if '_json_' in format:
                return FMT_JSON_BODY
            elif '_text_' in format:
                return FMT_TEXT_BODY
        return FMT_NO_BODY

    def _reset_ml_msg(self):
        self.ml_msg = dict(message=[])

    def ldebug(self, tabfunc, msg=""):
        _log(self, 'debug', tabfunc, msg)

    def linfo(self, tabfunc, msg=""):
        _log(self, 'info', tabfunc, msg)

    def lwarning(self, tabfunc, msg=""):
        _log(self, 'warning', tabfunc, msg)

    def lerror(self, tabfunc, msg=""):
        _log(self, 'error', tabfunc, msg)

    def raise_if_notarget(self):
        if self.target_path is None:
            raise NoTargetFile()

    def _tmain_may_update_target(self, cur):
        self.ldebug("_tmain_may_update_target")
        if cur - self.last_update >= self.update_term:
            self.ldebug(1, "{} > {}".format(cur - self.last_update,
                                            self.update_term))
            self.last_update = cur
            self.update_target()

    def _tmain_may_send_newlines(self, cur, scnt, netok):
        self.ldebug("_tmain_may_send_newlines")
        if cur - self.last_send_try >= self.send_term:
            self.ldebug(1, "{} >= {}".format(cur - self.last_send_try,
                                             self.send_term))
            try:
                self.last_send_try = cur
                scnt = self.may_send_newlines()
            except pywintypes.error, e:
                if e[0] == 2 and e[1] == 'CreateFile':
                    # file has been deleted
                    self.lerror("_run", "file '{}' might have been deleted. "
                                "will find new "
                                "target..".format(self.target_path))
                    self.set_target(None)
                else:
                    self.lerror("send error", str(e))
                    netok = False
        return scnt, netok

    def tmain(self):
        cur = time.time()
        self.ldebug("tmain {}".format(cur))

        # send new lines when need
        sent_line = 0
        netok = True
        # handle if elatest file has been rotated
        latest_rot = False
        if not self.elatest:
            latest_rot = self.handle_file_recreate(cur)
        else:
            try:
                epath = os.path.join(self.bdir, self.elatest)
                latest_rot = self.handle_elatest_rotation(epath, cur)
            except pywintypes.error, e:
                self.lerror("File '{}' open error - {}".format(epath, e))
                self.lwarning("Skip to next turn")
                return

        psent_pos = self.get_sent_pos() if self.target_path else None

        if self.target_path:
            try:
                sent_line, netok = self._tmain_may_send_newlines(cur,
                                                                 sent_line,
                                                                 netok)
            except LatestFileChanged:
                # delegate to handle_elatest_rotation
                self.lwarning("Possibly latest file has been rotated")
                return

        # if nothing sent with net ok, try update target timely
        if not latest_rot and (sent_line == 0 and netok):
            self._tmain_may_update_target(cur)
        return latest_rot, psent_pos, sent_line

    def set_target(self, target):
        changed = self.target_path != target
        if changed:
            self.linfo("set_target from '{}' to '{}'".format(self.target_path,
                                                             target))
            self.target_path = target
            self.target_fid = None
            if target:
                if os.path.isfile(target):
                    with OpenNoLock(target) as fh:
                        self.target_fid = get_fileid(fh)
                else:
                    self.lerror("set_target", "target file '{}' not "
                                "exists".format(target))
        return changed

    def _update_elatest_target(self, files):
        self.linfo("_update_elatest_target")
        epath, efid = self.get_elatest_info()

        if efid:
            # elatest handle updated only after handle_elatest_rotation
            if self.elatest_fid is None:
                # new elatest
                self.elatest_fid = efid
            files.append(epath)

    def handle_file_recreate(self, cur=None):
        ret = 0
        if self.target_path:
            if not os.path.isfile(self.target_path):
                self.lwarning("handle_file_recreate",
                              "target file '{}' has been removed, but new"
                              " file not created yet".format(self.target_path))
                ret = 1
            else:
                with OpenNoLock(self.target_path) as fh:
                    fid = get_fileid(fh)
                if self.target_fid and fid != self.target_fid:
                    self.lwarning("handle_file_recreate",
                                  "target file '{}' has been "
                                  "recreated".format(self.target_path))
                    ret = 2

        if ret > 0:
            self.linfo(1, "reset target to delegate update_target")
            self.save_sent_pos(0)
            self.set_target(None)
        return ret

    def handle_elatest_rotation(self, epath=None, cur=None):
        cur = cur if cur else int(time.time())
        if not epath:
            epath = os.path.join(self.bdir, self.elatest)
        efid = None
        if os.path.isfile(epath):
            with OpenNoLock(epath) as fh:
                efid = get_fileid(fh)

        #  if elatest file has been changed
        if self.elatest_fid and efid != self.elatest_fid:
            if not efid:
                self.lwarning("handle_elatest_rotation",
                              "elatest file has been rotated, but new"
                              " elatest file not created yed.")
            else:
                self.lwarning("handle_elatest_rotation",
                              "elatest file has been rotated({} -> {})."
                              " update_target "
                              "immediately.".format(self.elatest_fid, efid))

            self.elatest_fid = None
            # get pre-elatest, which should exist
            files = self.get_sorted_target_files()
            if len(files) == 0:
                self.lerror(1, "pre-elatest files are not exist!")
                return True
            pre_elatest = files[-1]
            self.linfo("pre-elatest is '{}'".format(pre_elatest))

            self.linfo(1, "move sent pos & clear elatest sent pos")
            # even elatest file not exist, there might be pos file
            self._save_sent_pos(pre_elatest, self.get_sent_pos(epath))
            # reset elatest sent_pos
            self._save_sent_pos(epath, 0)
            self.last_update = cur
            # pre-elatest is target for now
            self.set_target(pre_elatest)
            return True
        return False

    def get_sorted_target_files(self):
        """
            Returns sorted target files.
            If elatest is in target files, remove it.
            Default sort order is ascending alphanumerical order.
            Oldest file appears first, and newest file supposed to be at the
            end.
        """
        reverse = self.reverse_order
        self.linfo("get_sorted_target_files - reverse {}".format(reverse))

        # remove elatest if it is in this candidates
        files = glob.glob(os.path.join(self.bdir, self.ptrn))
        if self.elatest:
            epath = os.path.join(self.bdir, self.elatest)
            if epath in files:
                files.remove(epath)

        if self.order_ptrn:
            self.linfo("order_ptrn {}".format(self.order_ptrn))
            order_key = {}
            for afile in files:
                match = self.order_ptrn.search(afile)
                if not match:
                    self.lwarning(1, "file order pattern mismatch - "
                                  "'{} - {}'".format(afile))
                    continue
                gd = match.groupdict()
                order_key[afile] = gd['date'] +\
                    ".{:06d}".format(int(gd['order']))
                self.ldebug("order_key - {}".format(order_key[afile]))
            return sorted(files, key=lambda f: order_key[f], reverse=reverse)
        else:
            if len(files) > 0 and reverse:
                files = sorted(files, reverse=reverse)
            self.ldebug("files {}".format(files))
            return files

    def update_target(self, start=False):
        self.linfo("update_target")

        files = self.get_sorted_target_files()
        self.linfo(1, "{} target files".format(len(files)))

        if self.elatest is not None:
            self._update_elatest_target(files)

        cnt = len(files)
        newt = False
        if cnt == 0:
            self.set_target(None)
            self.lwarning(1, "NoCandidateFile")
        else:
            if cnt == 1:
                tpath = os.path.join(self.bdir, files[0])
            else:
                tpath = os.path.join(self.bdir, files[-1])
            newt = self.set_target(tpath)

        if newt:
            self.linfo(1, "new target {}".format(self.target_path))
            if start:
                self.start_sent_pos(self.target_path)
            else:
                self.update_sent_pos(self.target_path)
        else:
            self.linfo(1, "cur target {}".format(self.target_path))

    def get_file_pos(self, path=None):
        if path is None:
            self.raise_if_notarget()
            tpath = self.target_path
        else:
            tpath = path
        self.ldebug("get_file_pos", "for {}".format(tpath))
        try:
            with OpenNoLock(tpath) as fh:
                return win32file.GetFileSize(fh)
        except pywintypes.error, e:
            err = e[2]
            if self.encoding:
                err = err.decode(self.encoding).encode('utf8')
            self.lerror("get_file_pos", "{} - {}".format(err, tpath))
            raise

    def validate_format(self, fmt):
        return _validate_format(self.linfo, self.lerror, fmt)

    def validate_order_ptrn(self, fmt):
        return _validate_order_ptrn(self.linfo, self.lerror, fmt)

    def _convert_matched_msg(self, match):
        has_body = False
        gd = match.groupdict()

        if self.fmt_body == FMT_JSON_BODY:
            if '_json_' in gd:
                self.linfo(1, "json data found")
                has_body = True
                try:
                    msg = json.loads(gd['_json_'])
                except ValueError:
                    self.lerror("can't parse json data "
                                "'{}'".format(gd['_json_'][:50]))
                    msg = {'message': gd['_json_']}
            else:
                self.lwarning(1, "no json body found: {}'".format(msg[:50]))
        elif self.fmt_body == FMT_TEXT_BODY:
            if '_text_' in gd:
                self.linfo(1, "text data found")
                has_body = True
                msg = {}
                msg['message'] = gd['_text_']
            else:
                self.lwarning(1, "no text body found: '{}'".format(msg[:50]))
        else:
            msg = gd

        if has_body:
            for key in gd.keys():
                if key[0] == '_' and key[-1] == '_':
                    continue
                if key in msg.keys():
                    self.lwarning(1, "'{}' is overwritten from '{}' to "
                                  "'{}'".format(key, msg[key], gd[key]))
                msg[key] = gd[key]
        return msg

    def convert_msg(self, msg):
        self.ldebug("convert_msg")
        if self.encoding:
            msg = msg.decode(self.encoding).encode('utf8')

        parsed = None
        self.ldebug(1, "try match format")
        match = self.format.search(msg)
        if match:
            parsed = self._convert_matched_msg(match)
            self.ldebug(1, "parsed: {}".format(parsed))
            return parsed
        else:
            self.lwarning(1, "can't parse line '{}'".format(msg[:50]))
            return None

    def _read_target_to_end(self, fh):
        self.raise_if_notarget()

        res, lines = win32file.ReadFile(fh, MAX_READ_BUF, None)
        nbyte = len(lines)
        if res != 0:
            self.lerror(1, "ReadFile Error! {}".format(res))
            return '', 0
        if nbyte == MAX_READ_BUF:
            self.lwarning(1, "Read Buffer Full! Possibly corrupted last line.")

        return lines, nbyte

    def get_elatest_info(self):
        self.linfo("get_elatst_info")
        if self.elatest:
            epath = os.path.join(self.bdir, self.elatest)
            efid = None
            if os.path.isfile(epath):
                with OpenNoLock(epath) as fh:
                    efid = get_fileid(fh)
                    return epath, efid
            else:
                self.lwarning(1, "can not find elatest '{}'".format(epath))
        return None, None

    def may_send_newlines(self):
        self.raise_if_notarget()

        self.ldebug("may_send_newlines")

        # skip if no newlines
        file_pos = self.get_file_pos()
        sent_pos = self.get_sent_pos()
        self.ldebug("file_pos {}, sent_pos {}".format(file_pos, sent_pos))
        if sent_pos >= file_pos:
            if sent_pos > file_pos:
                self.lerror("sent_pos {} > file_pos {}. pos"
                            " mismatch".format(sent_pos, file_pos))
                if self.elatest:
                    raise LatestFileChanged()
            return 0

        # move to last sent pos
        with OpenNoLock(self.target_path) as fh:
            win32file.SetFilePointer(fh, sent_pos, win32file.FILE_BEGIN)
            # read file to the end
            lines, rbytes = self._read_target_to_end(fh)

        scnt = 0
        self.ldebug(1, "sent_pos {} file_pos {} rbytes "
                       "{}".format(sent_pos, file_pos, rbytes))
        if rbytes > 0:
            scnt = self._may_send_newlines(lines, rbytes, scnt,
                                           file_path=self.target_path)

        # save sent pos
        self.save_sent_pos(sent_pos + rbytes)
        return scnt

    def attach_msg_extra(self, msg):
        if type(msg) is dict:
            msg['sname_'] = self.sname
            msg['saddr_'] = self.saddr
            return msg
        else:
            return msg

    def _iterate_lines(self, lines, file_path):
        self.linfo("_iterate_lines")
        if self.parser:
            self.parser.set_file_path(file_path)

        for line in lines.splitlines():
            if len(line) > 0:
                parsed = None
                if self.format:
                    parsed = self.convert_msg(line)
                    if not parsed:
                        self.lwarning("can't convert '{}'".format(line))
                elif self.parser:
                    if self.parser.parse_line(line):
                        if self.parser.completed > self.parser_compl:
                            parsed = self.parser.parsed
                            self.parser_compl = self.parser.completed
                    else:
                        self.lwarning("can't parse '{}'".format(line))
                else:
                    self.lwarning("no format / parser exists. send raw "
                                  "message")
                    yield line

                if parsed:
                    yield self.attach_msg_extra(parsed)

    def _send_newline(self, msg, msgs):
        self.ldebug("_send_newline {}".format(msg))
        ts = int(time.time())
        self.may_echo(ts, msg)

        msgs.append((ts, msg))
        if len(msgs) >= BULK_SEND_SIZE:
            if self.fsender:
                bytes_ = self._make_fluent_bulk(msgs)
                self.fsender._send(bytes_)
            elif self.kclient:
                self._kinesis_put(msgs)
            msgs[:] = []

    def _kinesis_put(self, msgs):
        self.linfo('_kinesis_put {} messages'.format(len(msgs)))
        self.kpk_cnt += 1  # round robin shards
        for aggd in self._iter_kinesis_aggrec(msgs):
            pk, ehk, data = aggd.get_contents()
            self.linfo("  kinesis aggregated put_record: {} bytes".format(len(data)))
            st = time.time()
            ret = self.kclient.put_record(
                StreamName=self.kstream_name,
                Data=data,
                PartitionKey=pk,
                ExplicitHashKey=ehk
            )
            stat = ret['ResponseMetadata']['HTTPStatusCode']
            shid = ret['ShardId']
            seqn = ret['SequenceNumber']
            self.ksent_seqn = seqn
            self.ksent_shid = shid
            elp = time.time() - st
            if stat == 200:
                self.linfo("Kinesis put success in {}: ShardId: {}, "
                            "SequenceNumber: {}".format(elp, shid, seqn))
            else:
                self.error("Kineis put failed in {}!: "
                        "{}".format(elp, ret['ResponseMetadata']))

    def _iter_kinesis_aggrec(self, msgs):
        for msg in msgs:
            data = {'tag_': self.tag + '.data', 'ts_': msg[0]}
            if type(msg[1]) == dict:
                data.update(msg[1])
            else:
                data['value_'] = msg[1]

            pk = str(uuid.uuid4())
            res = self.kagg.add_user_record(pk, str(data))
            # if payload fits max send size, send it
            if res:
                yield self.kagg.clear_and_get()

        # send remain payload
        yield self.kagg.clear_and_get()

    def _may_send_newlines(self, lines, rbytes=None, scnt=0, file_path=None):
        self.ldebug("_may_send_newlines", "sending {} bytes..".format(rbytes))
        if not rbytes:
            rbytes = len(lines)
        try:
            itr = self._iterate_lines(lines, file_path)
            msgs = []
            for msg in itr:
                if not msg:
                    # skip bad form message (can't parse)
                    self.linfo("skip bad form message")
                    continue
                self._send_newline(msg, msgs)
                scnt += 1

            # send remain bulk msgs
            if len(msgs) > 0:
                if self.fsender:
                    bytes_ = self._make_fluent_bulk(msgs)
                    self.fsender._send(bytes_)
                elif self.kclient:
                    self._kinesis_put(msgs)

        except Exception, e:
            self.lwarning(1, "send fail '{}'".format(e))
            self.send_retry += 1
            if self.send_retry < MAX_SEND_RETRY:
                self.lerror(1, "Not exceed max retry({} < {}), will try "
                            "again".format(self.send_retry,
                                           MAX_SEND_RETRY))
                raise
            else:
                self.lerror(1, "Exceed max retry, Giving up this change({}"
                            " Bytes)!!".format(rbytes))
        self.send_retry = 0
        return scnt

    def _make_fluent_bulk(self, msgs):
        tag = '.'.join((self.tag, "data"))
        bulk = [msgpack.packb((tag, ts, data)) for ts, data in msgs]
        return ''.join(bulk)

    def may_echo(self, ts, line):
        if self.echo_file:
            self.echo_file.write("{} {}\n".format(ts, line))
            self.echo_file.flush()

    def get_sent_pos(self, epath=None):
        tpath = epath if epath else self.target_path
        if tpath in self.cache_sent_pos:
            return self.cache_sent_pos[tpath]

        pos = self.update_sent_pos(tpath)
        self.cache_sent_pos[tpath] = pos
        return pos

    def start_sent_pos(self, tpath):
        """
          calculate sent pos for new service start
        """
        self.ldebug("start_sent_pos", "for '{}'..".format(tpath))
        tname = escape_path(tpath)
        ppath = os.path.join(self.pdir, tname + '.pos')
        file_pos = self.get_file_pos(tpath)
        if os.path.isfile(ppath):
            # between data
            # previous pos file means continuation after restart
            with open(ppath, 'r') as f:
                line = f.readline()
                elm = line.split(',')
                spos = int(elm[0])
            self.ldebug(1, "found pos file - {}: {}".format(ppath, spos))
            if spos > file_pos:
                self.lwarning(1, "sent pos ({}) > file pos ({}). possible file"
                              " change. trim..".format(spos, file_pos))
                spos = file_pos
        else:
            spos = 0

        send_bytes = file_pos - spos
        if send_bytes > self.max_between_data:
            self.lwarning(1, "between data: start from current file pos"
                             " since {} > {}".format(send_bytes,
                                                     self.max_between_data))
            spos = file_pos

        if self.lines_on_start:
            lines, pos = get_file_lineinfo(tpath, self.lines_on_start)
            self.lwarning("get_file_lineinfo for lines_on_start "
                          "{} - {} {}".format(self.lines_on_start, lines, pos))
            spos = pos

        self._save_sent_pos(tpath, spos)

    def update_sent_pos(self, tpath):
        """
            update sent pos for for new target
        """
        self.linfo("update_sent_pos", "updating for '{}'..".format(tpath))
        # try to read position file
        # which is needed to continue send after temporal restart
        tname = escape_path(tpath)
        ppath = os.path.join(self.pdir, tname + '.pos')
        pos = 0
        if os.path.isfile(ppath):
            with open(ppath, 'r') as f:
                line = f.readline()
                elm = line.split(',')
                pos = int(elm[0])
            self.linfo(1, "found pos file - {}: {}".format(ppath, pos))
        else:
            self.linfo(1, "can't find pos file for {}, save as "
                          "0".format(tpath))
            self._save_sent_pos(tpath, 0)
        return pos

    def save_sent_pos(self, pos):
        self.linfo("save_sent_pos")
        self.raise_if_notarget()

        # save pos file
        self._save_sent_pos(self.target_path, pos)

    def _save_sent_pos(self, tpath, pos):
        self.linfo(1, "_save_sent_pos for {} - {}".format(tpath, pos))
        tname = escape_path(tpath)
        path = os.path.join(self.pdir, tname + '.pos')
        try:
            with open(path, 'w') as f:
                f.write("{}\n".format(pos))
        except Exception, e:
            self.lerror("Fail to write pos file: {} {}".format(e, path))
        self.cache_sent_pos[tpath] = pos
