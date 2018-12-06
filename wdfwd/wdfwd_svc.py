import traceback

import win32service  # NOQA
import win32serviceutil  # NOQA
import win32event  # NOQA
import servicemanager  # NOQA

from wdfwd import app
from wdfwd.get_config import get_config
from wdfwd.util import ldebug, lerror, lheader, init_global_fsender
from wdfwd.const import SVC_SLEEP_SEC

cfg = get_config()
scfg = cfg['app']['service']
lcfg = cfg['log']

if 'fluent' in lcfg:
    fhost = lcfg['fluent'][0]
    fport = lcfg['fluent'][1]
    init_global_fsender('wdfwd.main', fhost, fport)
elif 'kinesis' in lcfg:
    kaccess_key = lcfg['kinesis']['access_key']
    ksecret_key = lcfg['kinesis']['secret_key']
    kregion = lcfg['kinesis']['region']


class WdFwdService(win32serviceutil.ServiceFramework):
    _svc_name_ = scfg['name']
    _svc_display_name_ = scfg['caption']

    def __init__(self, args):
        ldebug("__init__ " + str(args))
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.haltEvent = win32event.CreateEvent(None, 0, 0, None)
        lheader("Service Start")

    def SvcStop(self):
        servicemanager.LogInfoMsg("Service is stopping.")
        lheader("Stopping")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.haltEvent)
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)

    def SvcDoRun(self):
        servicemanager.LogInfoMsg("Service is starting.")
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))

        try:
            app.start_tailing()
        except Exception, e:
            lerror("app.start_tailing error {}".format(str(e)))
            for l in traceback.format_exc().splitlines():
                lerror(l)

        while True:
            app.run_scheduled()
            result = win32event.WaitForSingleObject(self.haltEvent,
                                                    SVC_SLEEP_SEC)
            if result == win32event.WAIT_OBJECT_0:
                break

        app.stop_tailing()

        servicemanager.LogInfoMsg("Service is finished.")
        lheader("Service Finish")


if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(WdFwdService)
