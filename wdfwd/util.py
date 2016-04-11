import os
import logging
import tempfile
import time
from subprocess import check_call as _check_call, CalledProcessError

import win32file


def cap_call(cmd, retry=0, _raise=True, _test=False):
    logging.info('cap_call cmd: {}, retry: {}'.format(str(cmd), _raise))
    if retry > 0:
        for i in range(retry + 1):
            _raise = False if i < retry else True
            if i > 0:
                logging.debug("{} retry".format(i))
            if _cap_call(cmd, retry, _raise, _test):
                logging.debug("success")
                return
            # sleep for a while, then retry
            logging.debug("wait for a while")
            time.sleep(10)
    else:
        _cap_call(cmd, retry, _raise, _test)


def _cap_call(cmd, retry, _raise, _test=False):
    out = tempfile.TemporaryFile()
    err = tempfile.TemporaryFile()
    res = True
    try:
        logging.info('_cap_call: %s', str(cmd))
        _check_call(cmd, shell=True, stdout=out, stderr=err)
    except CalledProcessError, e:
        logging.error(str(e))
        res = False
        if _raise:
            raise
    finally:
        out.flush()
        err.flush()
        out.seek(0)
        err.seek(0)
        _out = out.read()
        _err = err.read()
        if len(_out) > 0:
            logging.debug(_out)
            if _test:
                print(_out)
        if len(_err) > 0:
            logging.error(_err)
            if _test:
                print(_err)
        if not _raise or res:
            return res


def escape_data_path(path):
    if 'library.zip' in path:
        return path.replace('\\library.zip\\wdfwd', '\\files')
    return path


def log_head(msg):
    logging.critical("==================== %s  ====================" % msg)


def safe_fname(fname):
    """
        Returns safe fname by eliminating chances of dir traversing
    """
    return fname.replace('../', '')


class ChangeDir(object):

    def __init__(self, *dirs):
        self.cwd = os.getcwd()
        self.path = os.path.join(*dirs)

    def __enter__(self):
        logging.info('change dir to %s', self.path)
        os.chdir(self.path)

    def __exit__(self, _type, value, tb):
        os.chdir(self.cwd)


def get_fileid(fh):
    info = win32file.GetFileInformationByHandle(fh)
    return sum(info[8:])


class OpenNoLock(object):

    def __init__(self, path, moveto=None):
        self.path = path
        self.moveto = moveto
        self.handle = None
        self.fid = None
        self.ldebug = None

    def __enter__(self):
        return self.open()

    def open(self):
        self.handle = win32file.CreateFile(self.path, win32file.GENERIC_READ,
                                           win32file.FILE_SHARE_DELETE |
                                           win32file.FILE_SHARE_READ |
                                           win32file.FILE_SHARE_WRITE, None,
                                           win32file.OPEN_EXISTING,
                                           win32file.FILE_ATTRIBUTE_NORMAL,
                                           None)
        self.ldebug("OpenNoLock", "open {}".format(self.handle))
        if self.moveto:
            win32file.SetFilePointer(self.handle, self.moveto,
                                     win32file.FILE_BEGIN)
        self.fid = get_fileid(self.handle)
        return self.handle

    def __exit__(self, _type, value, tb):
        self.ldebug("OpenNoLock", "__exit__")
        self.close()

    def __del__(self):
        self.ldebug("OpenNoLock", "__del__")
        self.close()

    def close(self):
        if self.handle:
            self.ldebug("OpenNoLock", "close {}".format(self.handle))
            win32file.CloseHandle(self.handle)
            self.handle = None
            self.fid = None


def get_dump_fname(_tbname, _date=None):
    tbname = _tbname.split('.')[-1]
    if _date is None:
        return "%s.csv" % tbname
    else:
        date = normalize_date_str(_date)
        return "{}_{}.csv".format(tbname, date)


def normalize_date_str(date):
    return date.replace('-', '')


def remove_file(fpath):
    try:
        if os.path.isfile(fpath):
            os.unlink(fpath)
    except OSError:
        logging.error("Failed: _remove_file " + fpath)


def ensure_endsep(path):
    return path if path.endswith('/') else path + '/'
