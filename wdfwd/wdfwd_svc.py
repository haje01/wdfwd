import logging

import win32service  # NOQA
import win32serviceutil  # NOQA
import win32event  # NOQA
import servicemanager  # NOQA

from wdfwd import app
from wdfwd.get_config import get_config
from wdfwd import util
from wdfwd.const import SVC_SLEEP_SEC


cfg = get_config()
scfg = cfg['app']['service']


class WdFwdService(win32serviceutil.ServiceFramework):
    _svc_name_ = scfg['name']
    _svc_display_name_ = scfg['caption']

    def __init__(self, args):
        logging.debug("__init__ " + str(args))
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.haltEvent = win32event.CreateEvent(None, 0, 0, None)
        util.log_head("Service Start")

    def SvcStop(self):
        servicemanager.LogInfoMsg("Service is stopping.")
        util.log_head("Stopping")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.haltEvent)
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)

    def SvcDoRun(self):
        servicemanager.LogInfoMsg("Service is starting.")
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))

        app.start_tailing()

        while True:
            app.run_scheduled()
            result = win32event.WaitForSingleObject(self.haltEvent,
                                                    SVC_SLEEP_SEC)
            if result == win32event.WAIT_OBJECT_0:
                break

        app.stop_tailing()

        servicemanager.LogInfoMsg("Service is finished.")
        util.log_head("Service Finish")


if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(WdFwdService)
