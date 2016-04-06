import os
import glob
import time
import threading
import logging

import win32file
from fluent.sender import FluentSender, MAX_SEND_FAIL

MAX_READ_BUF = 100 * 100
MAX_SEND_RETRY = 3
SEND_TERM = 1  # 1 second
UPDATE_TERM = 30  # 30 seconds


class NoTargetFile(Exception):
    pass


class NoCandidateFile(Exception):
    pass


class TailThread(threading.Thread):
    def __init__(self, name, tailer, send_term=SEND_TERM,
                 update_term=UPDATE_TERM):
        threading.Thread.__init__(self)
        self.name = name
        logging.debug("TailThread::__init__ - {}".format(self.name))
        tailer.trd_name = name
        self.tailer = tailer
        self.send_term = send_term
        self.last_send = 0
        self.update_term = update_term
        self.last_update = 0
        self._exit = False

    def ldebug(self, tabfunc, msg=""):
        if type(tabfunc) == int:
            logging.debug("  " * tabfunc + "<{}> - {}".format(self.name, msg))
        else:
            logging.debug("<{}> TailThread::{} - {}".format(self.name, tabfunc,
                                                            msg))

    def update_target(self, first=None):
        try:
            return self.tailer.update_target(first)
        except NoCandidateFile:
            self.ldebug(1, "NoCandidateFile for {}".format(self.name))

    def run(self):
        self.ldebug("run", "starts")
        self.update_target(True)

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
                        scnt = self.tailer.send_newlines()
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


class FileTailer(object):
    def __init__(self, bdir, ptrn, tag, pdir, fhost, fport,
                 max_send_fail=MAX_SEND_FAIL):

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

    def _log(self, level, tabfunc, msg):
        lfun = getattr(logging, level)
        if type(tabfunc) == int:
            lfun("  " * tabfunc + "<{}> {}".format(self.trd_name, msg))
        else:
            lfun("<{}> FileTailer::{} - {}".format(self.trd_name, tabfunc,
                                                   msg))

    def ldebug(self, tabfunc, msg=""):
        self._log('debug', tabfunc, msg)

    def lwarning(self, tabfunc, msg=""):
        self._log('warning', tabfunc, msg)

    def lerror(self, tabfunc, msg=""):
        self._log('error', tabfunc, msg)

    def raise_if_notarget(self):
        if self.target is None:
            raise NoTargetFile()

    def fdebug(self, msg):
        ts = time.time()
        tag = ("debug." + self.trd_name) if self.trd_name else "debug"
        self.sender.emit_with_time(tag, ts, msg)

    def update_target(self, first=None):
        self.ldebug("update_target")
        files = glob.glob(os.path.join(self.bdir, self.ptrn))
        cnt = len(files)
        newt = False
        if cnt == 0:
            raise NoCandidateFile()
        elif cnt == 1:
            target = os.path.join(self.bdir, files[0])
            newt = self.target != target
            self.target = target
        else:
            target = files[-1]
            newt = self.target != target
            self.target = target

        if newt:
            self.ldebug(1, "new target {}".format(self.target))
        else:
            self.ldebug(1, "cur target {}".format(self.target))

        if first:
            if self.target:
                # save sent pos as file pos to skip existing lines
                self.save_sent_pos(self.file_pos)
        else:
            if newt:
                # reset sent pos if not first and found new target
                self.save_sent_pos(0)

    def open_nolock(self, fname, moveto=None):
        fh = win32file.CreateFile(fname, win32file.GENERIC_READ,
                                  win32file.FILE_SHARE_DELETE |
                                  win32file.FILE_SHARE_READ |
                                  win32file.FILE_SHARE_WRITE, None,
                                  win32file.OPEN_EXISTING,
                                  win32file.FILE_ATTRIBUTE_NORMAL, None)
        if moveto:
            win32file.SetFilePointer(fh, moveto, win32file.FILE_BEGIN)
        return fh

    @property
    def file_pos(self):
        # logging.debug("FileTailer::file_pos")
        self.raise_if_notarget()
        fh = self.open_nolock(self.target)
        return win32file.GetFileSize(fh)

    def read_file(self, fh):
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
        # if len(lines) > 1:
        #    logging.debug(''.join(lines))
        return ''.join(lines), rbytes

    def send_newlines(self):
        # self.ldebug("send_newlines", "start")
        self.raise_if_notarget()

        self._sent_pos = self.sent_pos
        fh = self.open_nolock(self.target, self._sent_pos)
        lines, rbytes = self.read_file(fh)
        win32file.CloseHandle(fh)

        # TODO: optimize by bulk sending
        scnt = 0
        # self.ldebug(1, "_sent_pos {} rbytes {}".format(self._sent_pos,
        #                                               self.file_pos, rbytes))
        if rbytes > 0:
            self.ldebug("send_newlines", "sending {} bytes..".format(rbytes))
            try:
                for line in lines.splitlines():
                    if len(line) == 0:
                        continue
                    ts = int(time.time())
                    self.sender.emit_with_time(None, ts, line)
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
            self._sent_pos += rbytes
            self.save_sent_pos(self._sent_pos)
            # self.ldebug("send_newlines", "done")
        return scnt

    @property
    def sent_pos(self):
        tname = os.path.basename(self.target)
        path = os.path.join(self.pdir, tname + '.pos')
        pos = 0
        if os.path.isfile(path):
            with open(path, 'r') as f:
                line = f.readline()
                elm = line.split(',')
                pos = int(elm[0])
        # logging.debug("  FileTailer::sent_pos: {}".format(pos))
        return pos

    def _new_lines(self):
        fh = self.open_nolock(self.target, self._sent_pos)
        # TODO: Improve by not alloc every time
        res, lines = win32file.ReadFile(fh, MAX_READ_BUF, None)
        win32file.CloseHandle(fh)
        if len(lines) > 0:
            for line in lines.splitlines():
                yield lines

    def save_sent_pos(self, pos):
        self.ldebug("save_sent_pos", "{}".format(pos))
        self.raise_if_notarget()
        tname = os.path.basename(self.target)
        path = os.path.join(self.pdir, tname + '.pos')
        with open(path, 'w') as f:
            f.write("{}\n".format(pos))
