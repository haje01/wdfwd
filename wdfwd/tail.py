import os
import glob
import time
import threading
import logging
from StringIO import StringIO

import win32file
from fluent.sender import FluentSender, MAX_SEND_FAIL

from wdfwd.util import OpenNoLock

MAX_READ_BUF = 100 * 100
MAX_SEND_RETRY = 5
SEND_TERM = 1       # 1 second
UPDATE_TERM = 30    # 30 seconds
MAX_PREV_DATA = 100 * 100


class NoTargetFile(Exception):
    pass


class NoCandidateFile(Exception):
    pass


class TailThread(threading.Thread):
    def __init__(self, name, tailer, send_term=SEND_TERM,
                 update_term=UPDATE_TERM):
        threading.Thread.__init__(self)
        self.name = name
        tailer.trd_name = name
        logging.debug("TailThread::__init__ - {}".format(self.name))
        self.tailer = tailer
        self.send_term = send_term
        self.last_send = 0
        self.update_term = update_term
        self.last_update = 0
        self._exit = False

    def ldebug(self, tabfunc, msg=""):
        _log(self.name, 'TailThread', 'debug', tabfunc, msg)

    def lwarning(self, tabfunc, msg=""):
        _log(self.name, 'TailThread', 'warning', tabfunc, msg)

    def lerror(self, tabfunc, msg=""):
        _log(self.name, 'TailThread', 'error', tabfunc, msg)

    def update_target(self):
        try:
            return self.tailer.update_target()
        except NoCandidateFile:
            self.ldebug(1, "NoCandidateFile for {}".format(self.name))

    def run(self):
        self.ldebug("run", "starts")
        self.update_target()

        while True:
            time.sleep(0.5)

            cur = time.time()

            # send new lines when need
            scnt = 0
            netok = True
            # self.ldebug("check send - {} {}".format(self.tailer.target,
            #                                        cur-self.last_send))
            if self.tailer.target:
                if cur - self.last_send > self.send_term:
                    try:
                        scnt = self.tailer.chk_send_newlines()
                    except Exception, e:
                        # do not update last_send
                        self.lerror("send error", str(e))
                        netok = False
                    else:
                        self.last_send = cur

            # if nothing sent with net ok, try update target timely
            if scnt == 0 and netok:
                if cur - self.last_update > self.update_term:
                    self.last_update = cur
                    self.update_target()

            if self._exit:
                break

    def exit(self):
        self.ldebug("exit")
        self._exit = True


def _log(tname, cname, level, tabfunc, msg):
    lfun = getattr(logging, level)
    if type(tabfunc) == int:
        lfun("  " * tabfunc + "<{}> {}".format(tname, msg))
    else:
        lfun("<{}> {}::{} - {}".format(tname, cname, tabfunc, msg))


class FileTailer(object):
    def __init__(self, bdir, ptrn, tag, pdir, fhost, fport,
                 max_send_fail=MAX_SEND_FAIL, elatest=None, echo=False):

        self.trd_name = ""
        self.ldebug("__init__", "max_send_fail: '{}'".format(max_send_fail))
        self.bdir = bdir
        self.ptrn = ptrn
        # self.rx = re.compile(ptrn)
        self.tag = tag
        self.target = None
        self.sender = FluentSender(tag, fhost, fport,
                                   max_send_fail=max_send_fail)
        self.fhost = fhost
        self.fport = fport
        self.pdir = pdir
        self.send_retry = 0
        self.elatest = elatest
        self._sent_pos = None
        if echo:
            self.echo_file = StringIO()

    def ldebug(self, tabfunc, msg=""):
        _log(self.trd_name, 'FileTailer', 'debug', tabfunc, msg)

    def lwarning(self, tabfunc, msg=""):
        _log(self.trd_name, 'FileTailer', 'warning', tabfunc, msg)

    def lerror(self, tabfunc, msg=""):
        _log(self.trd_name, 'FileTailer', 'error', tabfunc, msg)

    def raise_if_notarget(self):
        if self.target is None or self.target.handle is None:
            raise NoTargetFile()

    def fdebug(self, msg):
        ts = time.time()
        tag = ("debug." + self.trd_name) if self.trd_name else "debug"
        self.sender.emit_with_time(tag, ts, msg)

    def get_fileid(self, fh):
        info = win32file.GetFileInformationByHandle(fh)
        return info[8:]

    def _chk_open_target(self, path):
        self.ldebug("_chk_open_target - {}".format(path))
        if self.target is None or self.target.path != path:
            # target is changed, reset per file cache
            self._reset_per_file_cache()
            self.target = OpenNoLock(path)
            self.target.open()
            return True
        return False

    def _reset_per_file_cache(self):
        self.ldebug("_reset_per_file_cache")
        self._sent_pos = None

    def update_target(self):
        self.ldebug("update_target")

        files = glob.glob(os.path.join(self.bdir, self.ptrn))

        cnt = len(files)
        newt = False
        if cnt == 0:
            raise NoCandidateFile()
        elif cnt == 1:
            tpath = os.path.join(self.bdir, files[0])
            newt = self._chk_open_target(tpath)
        else:
            tpath = files[-1]
            newt = self._chk_open_target(tpath)

        if newt:
            self.ldebug(1, "new target {}".format(self.target.path))
            # update sent pos
            self.sent_pos
        else:
            self.ldebug(1, "cur target {}".format(self.target.path))

    @property
    def file_pos(self):
        # logging.debug("FileTailer::file_pos")
        self.raise_if_notarget()
        return win32file.GetFileSize(self.target.handle)

    def _read_target_to_end(self):
        self.raise_if_notarget()
        lines = []
        rbytes = 0
        while True:
            res, _lines = win32file.ReadFile(self.target.handle, MAX_READ_BUF,
                                             None)
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
        # if len(lines) > 1:
        #    logging.debug(''.join(lines))
        return ''.join(lines), rbytes

    def chk_send_newlines(self):
        self.raise_if_notarget()
        # skip if no newlines
        if self.sent_pos >= self.file_pos:
            return 0

        self.ldebug("chk_send_newlines", "start")
        # move to last sent pos
        win32file.SetFilePointer(self.target.handle, self.sent_pos,
                                 win32file.FILE_BEGIN)
        # read file to the end
        lines, rbytes = self._read_target_to_end()

        # TODO: optimize by bulk sending
        scnt = 0
        self.ldebug(1, "_sent_pos {} file_pos {} rbytes {}".format(self.sent_pos,
                                                       self.file_pos, rbytes))
        if rbytes > 0:
            self.ldebug("chk_send_newlines", "sending {} bytes..".format(rbytes))
            try:
                for line in lines.splitlines():
                    if len(line) == 0:
                        continue
                    ts = int(time.time())
                    self.sender.emit_with_time(None, ts, line)
                    self.chk_echo(ts, line)
                    scnt += 1
            except Exception, e:
                self.ldebug(1, "send fail '{}'".format(e))
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

            # save sent pos
            self.save_sent_pos(self.sent_pos + rbytes)
            # self.ldebug("chk_send_newlines", "done")
        return scnt

    def chk_echo(self, ts, line):
        if self.echo_file:
            self.echo_file.write("{} {}\n".format(ts, line))
            self.echo_file.flush()

    @property
    def sent_pos(self):
        # use cache if exists (= not first call)
        if self._sent_pos is not None:
            return self._sent_pos

        ## this is new start
        self.ldebug("sent_pos", "updating..")
        # try to read position file
        # which is needed to continue send after temporal restart
        tname = os.path.basename(self.target.path)
        path = os.path.join(self.pdir, tname + '.pos')
        pos = 0
        if os.path.isfile(path):
            with open(path, 'r') as f:
                line = f.readline()
                elm = line.split(',')
                pos = int(elm[0])
            self.ldebug(1, "found pos file - {}: {}".format(path, pos))
        else:
            self.ldebug(1, "can't find pos file for {}, start from 0".format(path))

        # skip previous data, if data to send is too large
        file_pos = self.file_pos
        bytes_to_send = file_pos - pos
        if bytes_to_send > MAX_PREV_DATA:
            self.ldebug(1, "skip previous data since {} > "
                        "{}".format(bytes_to_send, MAX_PREV_DATA))
            pos = file_pos
        else:
            self.ldebug(1, "will send previous data since {} < "
                        "{}".format(bytes_to_send, MAX_PREV_DATA))
        # logging.debug("  FileTailer::sent_pos: {}".format(pos))
        self._sent_pos = pos
        return pos

    def save_sent_pos(self, pos):
        self.ldebug("save_sent_pos", "{}".format(pos))
        self.raise_if_notarget()

        # update cache
        self._sent_pos = pos
        # save pos file
        tname = os.path.basename(self.target.path)
        path = os.path.join(self.pdir, tname + '.pos')
        with open(path, 'w') as f:
            f.write("{}\n".format(pos))
