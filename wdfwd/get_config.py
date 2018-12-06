import os
import logging

import yaml

LOCAL_CFG_NAME = 'config.yml'
DIST_FILE = 'library.zip'


def _expand_var(val):
    typ = type(val)
    if typ == list:
        nl = []
        for v in val:
            nl.append(_expand_var(v))
        return nl
    elif typ == dict:
        for k, v in val.iteritems():
            val[k] = _expand_var(v)
        return val
    elif typ == str or typ == unicode:
        return os.path.expandvars(val)
    return val


def _local_cfg_path():
    abd = os.path.dirname(os.path.realpath(__file__))
    dist_file_at = abd.find(DIST_FILE)
    if dist_file_at >= 0:
        abd = abd[:dist_file_at]
    path = os.path.join(abd, LOCAL_CFG_NAME)
    return path


def get_config(envvar='WDFWD_CFG'):
    if envvar in os.environ:
        logging.info('Environment variable cfg: %s', os.environ[envvar])

    lcfg_path = _local_cfg_path()
    lcfg_exists = os.path.isfile(lcfg_path)
    logging.info('Directory local cfg: %s', lcfg_path)
    if lcfg_exists:
        logging.info('Directory local cfg: %s', lcfg_path)

    assert (envvar in os.environ) or lcfg_exists

    path = lcfg_path if lcfg_exists else os.environ[envvar]
    logging.info("Using cfg file: %s", path)
    return _get_config(path)


def _get_config(path):
    logging.info("open '%s' for config", path)
    with open(path, 'r') as f:
        cfg = yaml.load(f)
        cfg = _expand_var(cfg)
        return cfg
    logging.info("done")
