import os
import logging
import logging.config

import yaml

from wdfwd.const import BASE_DIR
from wdfwd.get_config import get_config

cfg = get_config()
logging.config.dictConfig(cfg['log'])

