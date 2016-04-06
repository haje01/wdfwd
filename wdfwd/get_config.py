import os
import logging

import yaml


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


def get_config(envvar='WDFWD_CFG'):
    assert envvar in os.environ
    path = os.environ[envvar]
    logging.info('Using environment variable cfg: %s', envvar)
    logging.info("open '%s' for config", path)
    with open(path, 'r') as f:
        cfg = yaml.load(f)
        cfg = _expand_var(cfg)
        return cfg
    logging.info("done")
