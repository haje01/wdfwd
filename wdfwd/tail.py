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
        self.ldebug("run", "starts")
        try:
            self.tailer.update_target()
        except NoCandidateFile:
            pass

        while True:
            try:
                time.sleep(0.5)

                self.tailer.tmain()

                if self._exit:
                    break
            except Exception, e:
                self.lerror("run", str(e))

    def exit(self):
        self.ldebug("exit")
        self._exit = True


def _log(fsender, level, tabfunc, _msg):
    lfun = getattr(logging, level)
    if type(tabfunc) == int:
        msg = "  " * tabfunc + "{}".format(_msg)
    else:
        msg = "{} - {}".format(tabfunc, _msg)

    lfun(msg)
    if fsender:
        ts = int(time.time())
        fsender.emit_with_time("{}".format(level), ts, msg)


class FileTailer(object):
    def __init__(self, bdir, ptrn, tag, pdir, fhost, fport,
                 send_term=SEND_TERM, update_term=UPDATE_TERM,
                 max_send_fail=MAX_SEND_FAIL, elatest=None,
                 echo=False):

        self.trd_name = ""
        self.sender = None
        self.ldebug("__init__", "max_send_fail: '{}'".format(max_send_fail))
        self.bdir = bdir
        self.ptrn = ptrn
        # self.rx = re.compile(ptrn)
        self.tag = tag
        self.target_path = None
        self.send_term = send_term
        self.last_send_try = 0
        self.update_term = update_term
        self.last_update = 0
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
        if cur - self.last_update >= self.update_term:
            self.last_update = cur
            self.update_target()

    def _tmain_may_send_newlines(self, cur, scnt, netok):
        if cur - self.last_send_try >= self.send_term:
            try:
                self.last_send_try = cur
                scnt = self.may_send_newlines()
            except pywintypes.error, e:
                if e[0] == 2 and e[1] == 'CreateFile':
                    # file has been deleted
                    self.lerror("_run", "file '{}' might have been "
                                "deleted. will find new "
                                "target..".format(self.target_path))
                    self.target_path = None
                else:
                    self.lerror("send error", str(e))
                    netok = False
        return scnt, netok

    def tmain(self):
        cur = time.time()

        # send new lines when need
        sent_line = 0
        netok = True
        #self.ldebug("check send", "{} {}".format(self.target.path if
                                                    #self.target else
                                                    #"NoTarget",
                                                    #cur-self.last_send_try))
        # handle if elatest file has been rotated
        latest_rot = False
        if self.elatest:
            latest_rot = self.handle_elatest_rotation(cur)
        psent_pos = self.get_sent_pos() if self.target_path else None

        if self.target_path:
            sent_line, netok = self._tmain_may_send_newlines(cur, sent_line,
                                                             netok)

        # if nothing sent with net ok, try update target timely
        if not latest_rot and (sent_line == 0 and netok):
            self._tmain_may_update_target(cur)
        return latest_rot, psent_pos, sent_line

    def set_target(self, target):
        changed = self.target_path != target
        self.target_path = target
        return changed

    def _update_elatest_target(self, files):
        self.ldebug("_update_elatest_target")
        epath, efid = self.get_elatest_info()

        if self.elatest_fid is None:
            if efid:
                ## new elatest
                self.elatest_fid = efid
                files.append(epath)

        #elif self.elatest_fid != efid:
            #self.ldebug("elatest file rotation found")
            #oefid = self.elatest_fid
            #self.elatest_fid = None
            ## pre-elatest file should exist
            #if len(files) == 0:
                #self.lerror(1, "pre-elatest files are not exist!")
                #return

            #pre_elatest = files[-1]
            ## save old elatest sent pos as pre-latest's pos file
            #self.ldebug("'{}' rotated as '{}'".format(epath, pre_elatest))
            #self._save_sent_pos(pre_elatest, self.get_sent_pos(epath, oefid))

            #with OpenNoLock(pre_elatest) as fh:
                #pefid = get_fileid(fh)
            ## and, should equal to old elatest fid
            #if pefid != oefid:
                #self.lerror(1, "pre-elatest fileid not equal to old elatest"
                            #" fileid!")

    def handle_elatest_rotation(self, cur=None):
        cur = cur if cur else int(time.time())
        epath = os.path.join(self.bdir, self.elatest)
        efid = None
        if os.path.isfile(epath):
            with OpenNoLock(epath) as fh:
                efid = get_fileid(fh)

        #  if elatest file has been changed
        if self.elatest_fid and efid != self.elatest_fid:
            oefid = self.elatest_fid
            self.elatest_fid = None
            if not epath:
                self.ldebug(1, "elatest file has been rotated, but new elatest"
                            " file not created yed.")
            else:
                self.ldebug(1, "elatest file has been rotated({} -> {})."
                            " update_target "
                            "immediately.".format(self.elatest_fid, efid))
            files = glob.glob(os.path.join(self.bdir, self.ptrn))
            # pre-elatest file should exist
            if len(files) == 0:
                self.lerror(1, "pre-elatest files are not exist!")
                return True

            pre_elatest = files[-1]
            if epath:
                # even elatest file not exist, there might be pos file
                self._save_sent_pos(pre_elatest, self.get_sent_pos(epath))
                self._save_sent_pos(epath, 0)
            self.last_update = cur
            self.set_target(pre_elatest)
            return True
        return False


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
            self.lerror("get_file_pos", "{} - {}".\
                        format(tpath, e[2].decode('cp949').encode('utf8')))

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
        if self.elatest:
            epath = os.path.join(self.bdir, self.elatest)
            efid = None
            if os.path.isfile(epath):
                with OpenNoLock(epath) as fh:
                    efid = get_fileid(fh)
                    return epath, efid
        return None, None

    def _clamp_sent_pos(self, file_pos, sent_pos):
        # skip previous data, if data to send is too large
        bytes_to_send = file_pos - sent_pos
        if bytes_to_send > MAX_PREV_DATA:
            self.ldebug(1, "skip previous data since {} > "
                        "{}".format(bytes_to_send, MAX_PREV_DATA))
            sent_pos = file_pos
        else:
            self.ldebug(1, "will send previous data since {} < "
                        "{}".format(bytes_to_send, MAX_PREV_DATA))
        self.ldebug(1, "sent_pos: {}".format(sent_pos))
        return sent_pos

    def may_send_newlines(self):
        self.raise_if_notarget()

        self.ldebug("may_send_newlines")

        # skip if no newlines
        file_pos = self.get_file_pos()
        sent_pos = self.get_sent_pos()
        if sent_pos >= file_pos:
            return 0

        sent_pos = self._clamp_sent_pos(file_pos, sent_pos)

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
        # self.ldebug("may_send_newlines", "done")
        return scnt

    def _may_send_newlines(self, lines, rbytes, scnt):
        self.ldebug("may_send_newlines", "sending {} bytes..".format(rbytes))
        try:
            for line in lines.splitlines():
                if len(line) == 0:
                    continue
                ts = int(time.time())
                self.sender.emit_with_time("data", ts, line)
                self.may_echo(ts, line)
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
        return scnt

    def may_echo(self, ts, line):
        if self.echo_file:
            self.echo_file.write("{} {}\n".format(ts, line))
            self.echo_file.flush()

    def get_sent_pos(self, epath=None):
        tpath = epath if epath else self.target_path
        if tpath in self.cache_sent_pos:
            return self.cache_sent_pos[tpath]

        ## this is new start
        self.ldebug("get_sent_pos", "updating for '{}'..".format(tpath))
        # try to read position file
        # which is needed to continue send after temporal restart
        tname = os.path.basename(tpath)
        ppath = os.path.join(self.pdir, tname + '.pos')
        pos = 0
        if os.path.isfile(ppath):
            with open(ppath, 'r') as f:
                line = f.readline()
                self.ldebug(2, line)
                elm = line.split(',')
                pos = int(elm[0])
            self.ldebug(1, "found pos file - {}: {}".format(ppath, pos))
        else:
            self.ldebug(1, "can't find pos file for {}, save as 0".format(tpath))
            self._save_sent_pos(tpath, 0)

        self.cache_sent_pos[tpath] = pos
        return pos


    def save_sent_pos(self, pos):
        self.ldebug("save_sent_pos")
        self.raise_if_notarget()

        # save pos file
        self._save_sent_pos(self.target_path, pos)

    def _save_sent_pos(self, tpath, pos):
        self.ldebug(1, "_save_sent_pos {} - {}".format(tpath, pos))
        tname = os.path.basename(tpath)
        path = os.path.join(self.pdir, tname + '.pos')
        with open(path, 'w') as f:
            f.write("{}\n".format(pos))
        self.cache_sent_pos[tpath] = pos
