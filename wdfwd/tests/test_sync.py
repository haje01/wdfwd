import os
from subprocess import CalledProcessError

import pytest

from wdfwd.const import BASE_DIR
from wdfwd.get_config import get_config
from wdfwd.util import cap_call, _cap_call
from wdfwd.sync import sync_folder, find_file_by_ptrn, sync_files, sync_file


cfg_path = os.path.join(BASE_DIR, 'tests', 'cfg_sync.yml')
os.environ['WDFWD_CFG'] = cfg_path
cfg = get_config()
acfg = cfg['app']
rsync_path = acfg['rsync_path'] if 'rsync_path' in acfg else None
tcfg = cfg['tasks'] if 'tasks' in cfg else None

TEST_FILE = "_test_dummy_"


def test_rsync(capsys):
    assert os.path.isfile(rsync_path)
    cap_call([rsync_path], 0, False, True)
    outerr = capsys.readouterr()
    assert "version" in outerr[0]
    assert "" in outerr[1]

    # test return
    assert not _cap_call([rsync_path + '~'], 0, False)

    # test retry
    with pytest.raises(CalledProcessError):
        cap_call([rsync_path+'~'], 2, False, True)
    outerr = capsys.readouterr()
    assert "rsync.exe~" in outerr[0]


def test_sync_folder():
    for task in tcfg:
        cmd = task.keys()[0]
        if cmd == 'sync_folder':
            sync = task['sync_folder']
            folder = sync['folder']
            to_url = sync['to_url']
            sync_folder(folder, to_url)


def test_sync_pattern():
    for task in tcfg:
        cmd = task.keys()[0]
        if cmd == 'sync_files':
            sync = task['sync_files']
            bfolder = sync['base_folder']
            recurse = sync['recurse']
            to_url = sync['to_url']
            ptrn = sync['filename_pattern']
            files = find_file_by_ptrn(bfolder, ptrn, recurse)
            sync_files(bfolder, files, to_url)


def test_sync_single_file():
    for task in tcfg:
        cmd = task.keys()[0]
        if cmd == 'sync_file':
            sync = task['sync_file']
            path = sync['filepath']
            to_url = sync['to_url']
            sync_file(path, to_url)
