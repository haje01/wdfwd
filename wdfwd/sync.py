import os
import time
import fnmatch
import logging

from wdfwd.get_config import get_config
from wdfwd.util import cap_call, ChangeDir, ensure_endsep
from wdfwd.const import RSYNC_PASSWD


cfg = get_config()
acfg = cfg['app']
rsync_path = acfg['rsync_path'] if 'rsync_path' in acfg else None
RSYNC_EXCLUDE = "--exclude='_wdfwd_*'"
RSYNC_EXCLUDE2 = "--exclude=.gitignore"
RSYNC_REMOVE = "--remove-source-files"
RSYNC_OPTION = '-azvR'
# decide rsync bandwidth limit
RSYNC_BWLIMIT = '--bwlimit=5120'  # default 5MByte/Sec
if 'rsync_bwlimit' in acfg:
    limit = int(acfg['rsync_bwlimit'])
    if limit > 0:
        RSYNC_BWLIMIT = '--bwlimit={}'.format(acfg['rsync_bwlimit'])
    else:
        RSYNC_BWLIMIT = ''


def sync_file(abspath, to_url):
    to_url = ensure_endsep(to_url)
    st = time.time()
    logging.debug('sync_file: %s to %s' % (abspath, to_url))
    os.environ['RSYNC_PASSWORD'] = RSYNC_PASSWD
    dirn = os.path.dirname(abspath)
    fname = os.path.basename(abspath)
    with ChangeDir(dirn):
        cap_call([rsync_path, RSYNC_OPTION, RSYNC_BWLIMIT, fname, to_url])
    logging.debug('sync elapsed {}'.format(time.time() - st))


def sync_folder(folder, to_url, remove_src=False):
    to_url = ensure_endsep(to_url)
    st = time.time()
    logging.debug('sync_folder')
    os.environ['RSYNC_PASSWORD'] = RSYNC_PASSWD
    # sync dail tables
    with ChangeDir(folder):
        cmd = [rsync_path, RSYNC_OPTION, RSYNC_EXCLUDE, RSYNC_EXCLUDE2,
               RSYNC_BWLIMIT]
        if remove_src:
            cmd.append(RSYNC_REMOVE)
        cmd += ['.', to_url]
        # if fail, retry 5 times
        cap_call(cmd, 5)
    logging.debug('sync elapsed {}'.format(time.time() - st))


def sync_files(folder, files, to_url):
    to_url = ensure_endsep(to_url)
    st = time.time()
    logging.debug('sync_files')
    os.environ['RSYNC_PASSWORD'] = RSYNC_PASSWD
    with ChangeDir(folder):
        for _file in files:
            logging.debug(_file)
            cap_call([rsync_path, RSYNC_OPTION, RSYNC_BWLIMIT, _file, to_url],
                     2)
    logging.debug('sync elapsed {}'.format(time.time() - st))


def find_file_by_ptrn(folder, ptrn, recurse):
    logging.debug('find_file_by_ptrn')
    found = []
    if not folder.endswith(os.path.sep):
        folder += os.path.sep
    for root, dirs, files in os.walk(folder):
        root = root.replace(folder, '')
        for fn in fnmatch.filter(files, ptrn):
            path = os.path.join(root, fn)
            found.append(path.replace(os.path.sep, '/'))
        if not recurse:
            break
    return found
