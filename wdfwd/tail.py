import os
import glob
import time
import threading
import logging
from StringIO import StringIO

import win32file, pywintypes
from fluent.sender import FluentSender, MAX_SEND_FAIL

from wdfwd.util import OpenNoLock, get_fileid

MAX_READ_BUF = 100 * 100
MAX_SEND_RETRY = 5
SEND_TERM = 1       # 1 second
UPDATE_TERM = 10    # 30 seconds
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
        _log(self.name, self.tailer.sender, 'TailThread', 'debug', tabfunc, msg)

    def lwarning(self, tabfunc, msg=""):
        _log(self.name, self.tailer.sender, 'TailThread', 'warning', tabfunc, msg)

    def lerror(self, tabfunc, msg=""):
        _log(self.name, self.tailer.sender, 'TailThread', 'error', tabfunc, msg)

    def update_target(self):
        try:
            return self.tailer.update_target()
        except NoCandidateFile:
            self.ldebug(1, "NoCandidateFile for {}".format(self.name))
        except pywintypes.error, e:
            self.lerror(1, "update_target error: {} - will find new "
                        "target..".format(str(e)))
            self.tailer.set_target(None)

    def run(self):
        self.ldebug("run", "starts")
        self.update_target()

        while True:
            try:
                if not self._run():
                    break
            except Exception, e:
                self.lerror("run", str(e))

    def _run(self):
        time.sleep(0.5)

        cur = time.time()

        # send new lines when need
        scnt = 0
        netok = True
        #self.ldebug("check send", "{} {}".format(self.tailer.target.path if
                                                    #self.tailer.target else
                                                    #"NoTarget",
                                                    #cur-self.last_send))
        if self.tailer.target_path:
            if cur - self.last_send > self.send_term:
                try:
                    scnt = self.tailer.chk_send_newlines()
                except pywintypes.error, e:
                    if e[0] == 2 and e[1] == 'CreateFile':
                        # file has been deleted
                        self.lerror("_run", "file '{}' might have been "
                                    "deleted. will find new "
                                    "target..".format(self.tailer.target_path))
                        self.tailer.target_path = None
                    else:
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
            return False
        return True

    def exit(self):
        self.ldebug("exit")
        self._exit = True


def _log(tname, fsender, cname, level, tabfunc, _msg):
    lfun = getattr(logging, level)
    if type(tabfunc) == int:
        msg = "  " * tabfunc + "<{}> {}".format(tname, _msg)
    else:
        msg = "<{}> {}::{} - {}".format(tname, cname, tabfunc, _msg)

    lfun(msg)
    if fsender:
        ts = time.time()
        fsender.emit_with_time("wdfwd._inner_.{}".format(level), ts, msg)


class FileTailer(object):
    def __init__(self, bdir, ptrn, tag, pdir, fhost, fport,
                 max_send_fail=MAX_SEND_FAIL, elatest=None, echo=False):

        self.trd_name = ""
        self.sender = None
        self.ldebug("__init__", "max_send_fail: '{}'".format(max_send_fail))
        self.bdir = bdir
        self.ptrn = ptrn
        # self.rx = re.compile(ptrn)
        self.tag = tag
        self.target_path = None
        self.sender = FluentSender(tag, fhost, fport,
                                   max_send_fail=max_send_fail)
        self.fhost = fhost
        self.fport = fport
        self.pdir = pdir
        self.send_retry = 0
        self.elatest = elatest
        self._sent_pos = None
        self.echo_file = StringIO() if echo else None
        self.elatest_fid = None

    def ldebug(self, tabfunc, msg=""):
        _log(self.trd_name, self.sender, 'FileTailer', 'debug', tabfunc, msg)

    def lwarning(self, tabfunc, msg=""):
        _log(self.trd_name, self.sender, 'FileTailer', 'warning', tabfunc, msg)

    def lerror(self, tabfunc, msg=""):
        _log(self.trd_name, self.sender, 'FileTailer', 'error', tabfunc, msg)

    def raise_if_notarget(self):
        if self.target_path is None:
            raise NoTargetFile()

    def fdebug(self, msg):
        ts = time.time()
        tag = ("debug." + self.trd_name) if self.trd_name else "debug"
        self.sender.emit_with_time(tag, ts, msg)

    def set_target(self, target):
        changed = self.target_path != target
        if changed:
            self.target_path = target
            self._sent_pos = None
        return changed

    def _update_elatest_target(self, files):
        self.ldebug("_update_elatest_target")
        epath, efid = self.get_elatest_info()

        if self.elatest_fid is None:
            if efid:
                ## new elatest
                self.elatest_fid = efid
                # reset sent_pos of elatest
                self._save_sent_pos(epath, 0)
                files.append(epath)
        elif self.elatest_fid != efid:
            ## elatest file has been rotated
            oefid = self.elatest_fid
            self.elatest_fid = None
            # pre-elatest file should exist
            if len(files) == 0:
                self.lerror(1, "pre-elatest files are not exist!")
                return

            pre_elatest = files[-1]
            # save elatest sent pos as pre-latest's pos file
            self._save_sent_pos(pre_elatest, self.get_sent_pos(epath))

            with OpenNoLock(pre_elatest) as fh:
                pefid = get_fileid(fh)
            # and, should equal to old elatest fid
            if pefid != oefid:
                self.lerror(1, "pre-elatest fileid not equal to old elatest"
                            " fileid!")


    def update_target(self):
        self.ldebug("update_target")

        files = glob.glob(os.path.join(self.bdir, self.ptrn))

        if self.elatest is not None:
            self._update_elatest_target(files)

        cnt = len(files)
        newt = False
        if cnt == 0:
            self.set_target(None)
            raise NoCandidateFile()
        else:
            if cnt == 1:
                tpath = os.path.join(self.bdir, files[0])
            else:
                tpath = files[-1]
            newt = self.set_target(tpath)

        if newt:
            self.ldebug(1, "new target {}".format(self.target_path))
            # update sent pos
            self.get_sent_pos()
        else:
            self.ldebug(1, "cur target {}".format(self.target_path))

    def get_file_pos(self, path=None):
        # logging.debug("FileTailer::file_pos")
        if path is None:
            self.raise_if_notarget()
            tpath = self.target_path
        else:
            tpath = path
        with OpenNoLock(tpath) as fh:
            return win32file.GetFileSize(fh)

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
        # if len(lines) > 1:
        #    logging.debug(''.join(lines))
        return ''.join(lines), rbytes

    def get_elatest_info(self):
        if self.elatest:
            epath = os.path.join(self.bdir, self.elatest)
            efid = None
            if os.path.isfile(epath):
                with OpenNoLock(epath) as fh:
                    efid = get_fileid(fh)
                    return epath, efid
        return None, None

    def chk_send_newlines(self):
        self.raise_if_notarget()

        self.ldebug("chk_send_newlines")

        # if elatest file has been rotated
        _, efid = self.get_elatest_info()
        if self.elatest_fid and efid != self.elatest_fid:
            # resort to update_target
            self.ldebug(1, "elatest file has been rotated. resort to "
                        "update_target")
            self.set_target(None)
            return 0

        # skip if no newlines
        file_pos = self.get_file_pos()
        if self.get_sent_pos() >= file_pos:
            return 0

        # move to last sent pos
        with OpenNoLock(self.target_path) as fh:
            win32file.SetFilePointer(fh, self.get_sent_pos(), win32file.FILE_BEGIN)
            # read file to the end
            lines, rbytes = self._read_target_to_end(fh)

        # TODO: optimize by bulk sending
        scnt = 0
        self.ldebug(1, "_sent_pos {} file_pos {} rbytes "
                    "{}".format(self.get_sent_pos(), file_pos, rbytes))
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
            self.save_sent_pos(self.get_sent_pos() + rbytes)
            # self.ldebug("chk_send_newlines", "done")
        return scnt

    def chk_echo(self, ts, line):
        if self.echo_file:
            self.echo_file.write("{} {}\n".format(ts, line))
            self.echo_file.flush()

    def get_sent_pos(self, path=None):
        # use cache if exists (= not first call)
        if self._sent_pos is not None:
            return self._sent_pos

        tpath = path if path else self.target_path

        ## this is new start
        self.ldebug("get_sent_pos", "updating for {}..".format(tpath))
        # try to read position file
        # which is needed to continue send after temporal restart
        tname = os.path.basename(tpath)
        ppath = os.path.join(self.pdir, tname + '.pos')
        pos = 0
        if os.path.isfile(ppath):
            with open(ppath, 'r') as f:
                line = f.readline()
                elm = line.split(',')
                pos = int(elm[0])
            self.ldebug(1, "found pos file - {}: {}".format(ppath, pos))
        else:
            self.ldebug(1, "can't find pos file for {}, start from 0".format(tpath))

        # skip previous data, if data to send is too large
        file_pos = self.get_file_pos(path)
        bytes_to_send = file_pos - pos
        if bytes_to_send > MAX_PREV_DATA:
            self.ldebug(1, "skip previous data since {} > "
                        "{}".format(bytes_to_send, MAX_PREV_DATA))
            pos = file_pos
        else:
            self.ldebug(1, "will send previous data since {} < "
                        "{}".format(bytes_to_send, MAX_PREV_DATA))
        self.ldebug("  FileTailer::get_sent_pos: {}".format(pos))
        self._sent_pos = pos
        return pos

    def save_sent_pos(self, pos):
        self.ldebug("save_sent_pos", "{}".format(pos))
        self.raise_if_notarget()

        # update cache
        self._sent_pos = pos
        # save pos file
        self._save_sent_pos(self.target_path, pos)

    def _save_sent_pos(self, tpath, pos):
        tname = os.path.basename(tpath)
        path = os.path.join(self.pdir, tname + '.pos')
        with open(path, 'w') as f:
            f.write("{}\n".format(pos))
