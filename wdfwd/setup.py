from distutils.core import setup
import py2exe

import sys
sys.path.append('E:\\works\\wdfwd')


includes = ['wdfwd.get_config', 'wdfwd.const', 'decimal', 'pyodbc']


class Target:

    def __init__(self, **kw):
        self.__dict__.update(kw)
        self.version = "0.0.5.1"
        self.company_name = "Webzen"
        self.copyright = "Copyright (C) 2014 Webzen"
        self.name = "WzDat Forwarder"

target = Target(
    description="A Log / DB Forwarder for WzDat",
    modules=['wdfwd_svc'],
    cmdline_style='pywin32',
)

setup(service=[target],
      options={'py2exe': {
               'includes': includes,
               }},
      data_files=[('files', ['E:\\works\\wdfwd\\wdfwd\\default_config.yml'])])
