import os
import sys
sys.path.append('.')

import click

from wdfwd.get_config import _get_config
from wdfwd.parser import merge_parser_cfg


CONFIG_NAME = 'WDFWD_CFG'


@click.group()
def cli():
    pass


@cli.command()
@click.argument('file_path')
@click.option('--cfg_path', help="Forwarder config file path.")
@click.option('--cfile_idx', default=0, help="Tailing config file index.")
def parser(file_path, cfg_path, cfile_idx):
    if not cfg_path:
        assert CONFIG_NAME in os.environ
        cfg_path = os.environ[CONFIG_NAME]

    cfg = _get_config(cfg_path)
    assert 'tailing'
    assert cfg['tailing']['from']
    if 'parser' in cfg['tailing']:
        gpcfg = cfg['tailing']['parser']
    files = cfg['tailing']['from']
    afile = files[cfile_idx]
    pcfg = afile['file']['parser']
    if gpcfg:
        pcfg = merge_parser_cfg(gpcfg, pcfg)

    from wdfwd import parser as ps
    parser = ps.create_parser(pcfg)

    n_succ = 0
    with open(file_path, 'rt') as f:
        for line in f:
            parsed = parser.parse_line(line)
            if not parsed:
                print "Parsing failed! : '{}'".format(line)
                sys.exit(-1)
            else:
                print parser.parsed
                n_succ += 1
                if n_succ > 10:
                    print 'Printed only first 10 results.'
                    break


@cli.command()
@click.argument('file_path')
@click.option('--cfg_path', help="Forwarder config file path.")
@click.option('--cfile_idx', default=0, help="Tailing config file index.")
def format(file_path, cfg_path, cfile_idx):
    import re

    if not cfg_path:
        assert CONFIG_NAME in os.environ
        cfg_path = os.environ[CONFIG_NAME]

    cfg = _get_config(cfg_path)
    assert 'tailing'
    assert cfg['tailing']['from']
    files = cfg['tailing']['from']
    afile = files[cfile_idx]
    format = afile['file']['format']
    ptrn = re.compile(format)

    with open(file_path, 'rt') as f:
        for line in f:
            try:
                gd = ptrn.search(line).groupdict()
                print(gd)
            except AttributeError:
                print "Parsing failed! : '{}'".format(line)
                sys.exit(-1)


if __name__ == '__main__':
    cli()
