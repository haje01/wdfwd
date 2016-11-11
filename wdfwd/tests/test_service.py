import os
import time
import shutil

import wmi

from wdfwd.const import BASE_DIR
from wdfwd.util import cap_call, ChangeDir
from wdfwd.get_config import get_config

TEARDOWN_KILL = False


def get_service():
    c = wmi.WMI()
    services = [s for s in c.Win32_Service() if s.Name == 'WDFwdTest']
    if len(services) > 0:
        return services[0]


def exe_exist():
    path = os.path.join(BASE_DIR, 'test_dist', 'wdfwd_svc.exe')
    return os.path.isfile(path)


def setup_module(mod):
    # if get_service() is not None and exe_exist():
        # os.chdir('wdfwd/test_dist')
        # call('wdfwd_svc.exe stop')
        # call('wdfwd_svc.exe remove')
        # os.chdir('../..')

    # test build
    path = os.path.join(BASE_DIR, 'test_dist')
    if os.path.isdir(path):
        shutil.rmtree(path)
    with ChangeDir('wdfwd'):
        cap_call('python setup.py py2exe -d test_dist')
        assert os.path.isdir(path)


def teardown_module(mod):
    # uninstall service
    svc = get_service()
    if svc is not None and exe_exist():
        with ChangeDir(BASE_DIR, 'test_dist'):
            if not TEARDOWN_KILL:
                cap_call('wdfwd_svc.exe stop')
            else:
                time.sleep(3)
                cap_call('taskkill /F /PID ' + str(svc.ProcessId))
            cap_call('wdfwd_svc.exe remove')


def test_basic():
    # check cfg
    cfg_path = os.path.join(BASE_DIR, 'tests', 'cfg_service.yml')
    os.environ['WDFWD_CFG'] = cfg_path
    cfg = get_config()
    appc = cfg['app']
    assert appc['service']['name'] == 'WDFwdTest'

    # test service isntall
    with ChangeDir('wdfwd', 'test_dist'):
        cap_call('wdfwd_svc.exe install')

        # test start
        cap_call('wdfwd_svc.exe start')
        svc = get_service()
        assert svc is not None
        assert svc.Status == 'OK'
