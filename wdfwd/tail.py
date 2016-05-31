import os
import glob
import time
import threading
import socket
import logging
from StringIO import StringIO
import traceback
import json

import win32file
import pywintypes
from fluent.sender import FluentSender, MAX_SEND_FAIL

from wdfwd.util import OpenNoLock, get_fileid, escape_path, validate_format as\
    _validate_format, validate_order_ptrn as _validate_order_ptrn

MAX_READ_BUF = 1000 * 1000
MAX_SEND_RETRY = 5
SEND_TERM = 1       # 1 second
UPDATE_TERM = 5     # 5 seconds
MAX_BETWEEN_DATA = 1000 * 1000
FMT_NO_BODY = 0
FMT_JSON_BODY = 1
FMT_TEXT_BODY = 2


class NoTargetFile(Exception):
    pass


class LatestFileChanged(Exception):
    pass


class TailThread(threading.Thread):
    def __init__(self, name, tailer):
        threading.Thread.__init__(self)
        self.name = name
        tailer.trd_name = name
        self.tailer = tailer
        self.ldebug("TailThread::__init__ - {}".format(self.name))
        self._exit = False

    def ldebug(self, tabfunc, msg=""):
        _log(self.tailer.sender, 'debug', tabfunc, msg)

    def lwarning(self, tabfunc, msg=""):
        _log(self.tailer.sender, 'warning', tabfunc, msg)

    def lerror(self, tabfunc, msg=""):
        _log(self.tailer.sender, 'error', tabfunc, msg)

    def run(self):
        self.ldebug("start run")
        self.tailer.update_target(True)

        while True:
            try:
                time.sleep(1)

                self.tailer.tmain()

                if self._exit:
                    break
            except NoTargetFile:
                self.lwarning("run", "NoTargetfile")
            except Exception, e:
                self.ldebug("run", str(e))
                tb = traceback.format_exc()
                for line in tb.splitlines():
                    self.lerror(line)

    def exit(self):
        self.ldebug("exit")
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


def _log(fsender, level, tabfunc, _msg):
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
    if fsender:
        ts = int(time.time())
        try:
            fsender.emit_with_time("{}".format(level), ts, {"message": msg})
        except Exception, e:
            logging.warning("_log", "send fail '{}'".format(e))


class FileTailer(object):
    def __init__(self, bdir, ptrn, tag, pdir, fhost, fport,
                 send_term=SEND_TERM, update_term=UPDATE_TERM,
                 max_send_fail=None, elatest=None, echo=False, encoding=None,
                 lines_on_start=None, max_between_data=None, format=None,
                 multiline=None, order_ptrn=None):

        self.trd_name = ""
        self.sender = None
        self.ldebug("__init__", "max_send_fail: '{}'".format(max_send_fail))
        self.bdir = bdir
        self.ptrn = ptrn
        self.sname = socket.gethostname()
        self.saddr = socket.gethostbyname(self.sname)
        tag = "{}.{}".format(self.sname.lower(), tag)
        self.ldebug(1, "tag: '{}'".format(tag))
        self.tag = tag
        self.target_path = self.target_fid = None
        self.send_term = send_term
        self.last_send_try = 0
        self.update_term = update_term
        self.last_update = 0
        max_send_fail = max_send_fail if max_send_fail else MAX_SEND_FAIL
        self.sender = FluentSender(tag, fhost, fport,
                                   max_send_fail=max_send_fail)
        self.fhost = fhost
        self.fport = fport
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
        self.multiline = multiline if multiline else None
        self._reset_ml_msg()
        self.ldebug("effective format: '{}'".format(format))
        self.format = self.validate_format(format, multiline)
        self.fmt_body = self.format_body_type(format)
        self.pending_mlmsg = None
        self.order_ptrn = self.validate_order_ptrn(order_ptrn)

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
        _log(self.sender, 'debug', tabfunc, msg)

    def lwarning(self, tabfunc, msg=""):
        _log(self.sender, 'warning', tabfunc, msg)

    def lerror(self, tabfunc, msg=""):
        _log(self.sender, 'error', tabfunc, msg)

    def raise_if_notarget(self):
        if self.target_path is None:
            raise NoTargetFile()

    def _tmain_may_update_target(self, cur):
        # self.ldebug("_tmain_may_update_target")
        if cur - self.last_update >= self.update_term:
            # self.ldebug(1, "{} > {}".format(cur - self.last_update,
            #                                 self.update_term))
            self.last_update = cur
            self.update_target()

    def _tmain_may_send_newlines(self, cur, scnt, netok):
        # self.ldebug("_tmain_may_send_newlines")
        if cur - self.last_send_try >= self.send_term:
            # self.ldebug(1, "{} >= {}".format(cur - self.last_send_try,
            #                                 self.send_term))
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
        # self.ldebug("tmain {}".format(cur))

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
            self.ldebug("set_target from '{}' to '{}'".format(self.target_path,
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
        self.ldebug("_update_elatest_target")
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
            self.ldebug(1, "reset target to delegate update_target")
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
            self.ldebug("pre-elatest is '{}'".format(pre_elatest))

            self.ldebug(1, "move sent pos & clear elatest sent pos")
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
        self.ldebug("get_sorted_target_files")
        files = glob.glob(os.path.join(self.bdir, self.ptrn))
        if self.order_ptrn:
            order_key = {}
            for afile in files:
                match = self.order_ptrn.search(afile)
                if not match:
                    self.lwarning(1, "file order pattern mismatch - "
                                  "'{}'".format(afile))
                    continue
                gd = match.groupdict()
                order_key[afile] = gd['date'] +\
                    ".{:06d}".format(int(gd['order']))
                # self.ldebug("order_key - {}".format(order_key[afile]))
            return sorted(files, key=lambda f: order_key[f])
        else:
            return files

    def update_target(self, start=False):
        self.ldebug("update_target")

        files = self.get_sorted_target_files()
        self.ldebug(1, "{} target files".format(len(files)))

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
            self.ldebug(1, "new target {}".format(self.target_path))
            if start:
                self.start_sent_pos(self.target_path)
            else:
                self.update_sent_pos(self.target_path)
        else:
            self.ldebug(1, "cur target {}".format(self.target_path))

    def get_file_pos(self, path=None):
        if path is None:
            self.raise_if_notarget()
            tpath = self.target_path
        else:
            tpath = path
        # self.ldebug("get_file_pos", "for {}".format(tpath))
        try:
            with OpenNoLock(tpath) as fh:
                return win32file.GetFileSize(fh)
        except pywintypes.error, e:
            err = e[2]
            if self.encoding:
                err = err.decode(self.encoding).encode('utf8')
            self.lerror("get_file_pos", "{} - {}".format(err, tpath))
            raise

    def validate_format(self, fmt, multiline):
        return _validate_format(self.ldebug, self.lerror, fmt, multiline)

    def validate_order_ptrn(self, fmt):
        return _validate_order_ptrn(self.ldebug, self.lerror, fmt)

    def _convert_matched_msg(self, match):
        parsed = True
        has_body = False
        gd = match.groupdict()

        if self.fmt_body == FMT_JSON_BODY:
            if '_json_' in gd:
                self.ldebug(1, "json data found")
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
                self.ldebug(1, "text data found")
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
        return msg, parsed

    def convert_msg(self, msg, return_unparsed=False):
        self.ldebug("convert_msg")
        if self.encoding:
            msg = msg.decode(self.encoding).encode('utf8')

        parsed = False
        if self.format:
            # self.ldebug(1, "try match format")
            match = self.format.search(msg)
            if match:
                msg, parsed = self._convert_matched_msg(match)
            else:
                self.lwarning(1, "can't parse line '{}'".format(msg[:50]))
                if return_unparsed:
                    return msg, False
                else:
                    msg = None
        else:
            self.ldebug(1, "no format")

        self.ldebug(1, "parsed: {}".format(parsed))
        return msg, parsed

    def _read_target_to_end(self, fh):
        self.raise_if_notarget()
        lines = []
        rbytes = 0
        while True:
            res, _lines = win32file.ReadFile(fh, MAX_READ_BUF, None)
            nbyte = len(_lines)
            if res != 0:
                self.lerror(1, "ReadFile Error! {}".format(res))
                break
            if nbyte == MAX_READ_BUF:
                self.lwarning(1, "Read Buffer Full!")

            rbytes += nbyte
            lines.append(_lines)
            if nbyte < MAX_READ_BUF:
                break
        return ''.join(lines), rbytes

    def get_elatest_info(self):
        self.ldebug("get_elatst_info")
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

        # self.ldebug("may_send_newlines")

        # skip if no newlines
        file_pos = self.get_file_pos()
        sent_pos = self.get_sent_pos()
        # self.ldebug("file_pos {}, sent_pos {}".format(file_pos, sent_pos))
        if sent_pos >= file_pos:
            if sent_pos > file_pos:
                self.lerror("sent_pos {} > file_pos {}. cache"
                            " bug?".format(sent_pos, file_pos))
                if self.elatest:
                    raise LatestFileChanged()
            return 0

        # sent_pos = self._clamp_sent_pos(file_pos, sent_pos)

        # move to last sent pos
        with OpenNoLock(self.target_path) as fh:
            win32file.SetFilePointer(fh, sent_pos, win32file.FILE_BEGIN)
            # read file to the end
            lines, rbytes = self._read_target_to_end(fh)

        # TODO: optimize by bulk sending
        scnt = 0
        # self.ldebug(1, "sent_pos {} file_pos {} rbytes "
        #            "{}".format(sent_pos, file_pos, rbytes))
        if rbytes > 0:
            scnt = self._may_send_newlines(lines, rbytes, scnt)

        # save sent pos
        self.save_sent_pos(sent_pos + rbytes)
        return scnt

    def _iterate_multiline(self, lines):
        self.ldebug("_iterate_multiline")

        for line in lines.splitlines():
            if self.encoding:
                line = line.decode(self.encoding).encode('utf8')

            def _match_body_or_tail(line, fmt, parsed, lbody=False):
                if hasattr(fmt, '__iter__'):
                    assert len(fmt) == 2
                    start_f = fmt[0]
                    match = start_f.search(line)
                    if match:
                        parsed = True
                        rv = match.groupdict()
                        rs = match.end()
                        repeat_f = fmt[1]
                        match = repeat_f.findall(line[rs:])
                        if match:
                            for m in match:
                                k, v = m[1], m[2]
                                rv[k] = v

                        if lbody:
                            if 'lbody_' not in self.pending_mlmsg:
                                self.pending_mlmsg['lbody_'] = []
                            self.pending_mlmsg['lbody_'].append(rv)
                        else:
                            self.pending_mlmsg.update(rv)
                else:
                    match = fmt.search(line)
                    if match:
                        parsed = True
                        self.pending_mlmsg.update(match.groupdict())
                return parsed

            parsed = False
            # dictionary format
            if type(self.format) is dict:
                # head
                match = self.format['head'].search(line)
                if match:
                    parsed = True
                    # send pending message
                    if self.pending_mlmsg:
                        yield self.pending_mlmsg
                    self.pending_mlmsg = match.groupdict()
                else:
                    # tail
                    if 'tail' in self.format:
                        parsed = _match_body_or_tail(line, self.format['tail'],
                                                     parsed)
                    # body
                    if not parsed and 'lbody' in self.format:
                        parsed = _match_body_or_tail(line,
                                                     self.format['lbody'],
                                                     parsed, True)
            # line format
            else:
                msg, parsed = self.convert_msg(line, True)
                # head
                if parsed:
                    # send pending message
                    if self.pending_mlmsg:
                        if 'tail_' in self.pending_mlmsg:
                            self.pending_mlmsg['tail_'] =\
                                '\n'.join(self.pending_mlmsg['tail_'])
                        yield self.pending_mlmsg

                    self.pending_mlmsg = msg
                elif msg:
                    # no format
                    if 'tail_' not in self.pending_mlmsg:
                        self.pending_mlmsg['tail_'] = []
                    # append raw message
                    self.pending_mlmsg['tail_'].append(msg)

    def attach_msg_extra(self, msg):
        if type(msg) is dict:
            msg['sname_'] = self.sname
            msg['saddr_'] = self.saddr
            return msg
        else:
            return msg

    def _iterate_line(self, lines):
        self.ldebug("_iterate_line")
        for line in lines.splitlines():
            if len(line) > 0:
                yield self.attach_msg_extra(self.convert_msg(line)[0])

    def _may_send_newlines(self, lines, rbytes=None, scnt=0):
        self.ldebug("_may_send_newlines", "sending {} bytes..".format(rbytes))
        if not rbytes:
            rbytes = len(lines)
        try:
            itr = self._iterate_multiline(lines) if self.multiline else\
                self._iterate_line(lines)
            for msg in itr:
                if not msg:
                    # skip bad form message (can't parse)
                    self.ldebug("skip bad form message")
                    continue
                ts = int(time.time())
                self.sender.emit_with_time("data", ts, msg)
                self.may_echo(ts, msg)
                scnt += 1
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
        self.ldebug("update_sent_pos", "updating for '{}'..".format(tpath))
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
            self.ldebug(1, "found pos file - {}: {}".format(ppath, pos))
        else:
            self.ldebug(1, "can't find pos file for {}, save as "
                        "0".format(tpath))
            self._save_sent_pos(tpath, 0)
        return pos

    def save_sent_pos(self, pos):
        self.ldebug("save_sent_pos")
        self.raise_if_notarget()

        # save pos file
        self._save_sent_pos(self.target_path, pos)

    def _save_sent_pos(self, tpath, pos):
        self.ldebug(1, "_save_sent_pos for {} - {}".format(tpath, pos))
        tname = escape_path(tpath)
        path = os.path.join(self.pdir, tname + '.pos')
        with open(path, 'w') as f:
            f.write("{}\n".format(pos))
        self.cache_sent_pos[tpath] = pos
