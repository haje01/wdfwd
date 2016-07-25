import os

from distutils.core import setup
import py2exe  # NOQA

import sys

assert 'WDFWD_DIR' in os.environ
WDFWD_DIR = os.environ['WDFWD_DIR']

sys.path.append(WDFWD_DIR)


includes = ['wdfwd.get_config', 'wdfwd.const', 'decimal', 'pyodbc', 'bisect',
            'HTMLParser']
dll_excludes = ['mswsock.dll']

data_files = [
    ('files', [os.path.join(WDFWD_DIR, 'wdfwd\\default_config.yml')]),
    ('data', ['botocore_data/_retry.json', 'botocore_data/cacert.pem',
              'botocore_data/endpoints.json']),
    ('data/kinesis/2013-12-02',
        ['botocore_data/kinesis/2013-12-02/service-2.json'])
]


class Target:
    def __init__(self, **kw):
        self.__dict__.update(kw)
        self.version = "0.1.2"
        self.company_name = "Webzen"
        self.copyright = "Copyright (C) 2016 Webzen"
        self.name = "WzDat Forwarder"


target = Target(
    description="A Log / DB Forwarder for WzDat",
    modules=['wdfwd_svc'],
    cmdline_style='pywin32',
)

setup(service=[target],
      options={
          'py2exe': {
              'includes': includes,
              'dll_excludes': dll_excludes,
              }
          },
      data_files=data_files
      )
