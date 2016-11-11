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
@click.option('--errors-only', is_flag=True, help="Show errors only.")
@click.option('--cont-error', is_flag=True, help="Continue parsing with error.")
def parser(file_path, cfg_path, cfile_idx, errors_only, cont_error):
    if not cfg_path:
        assert CONFIG_NAME in os.environ
        cfg_path = os.environ[CONFIG_NAME]

    cfg = _get_config(cfg_path)
    assert 'tailing' in cfg
    ctail = cfg['tailing']
    assert ctail['from']
    encoding = ctail['file_encoding'] if 'file_encoding' in ctail else None

    gpcfg = ctail['parser'] if 'parser' in ctail else None
    files = ctail['from']
    afile = files[cfile_idx]
    pcfg = afile['file']['parser'] if 'parser' in afile['file'] else None
    if gpcfg:
        pcfg = merge_parser_cfg(gpcfg, pcfg)

    from wdfwd import parser as ps
    parser = ps.create_parser(pcfg, encoding)

    n_succ = 0
    with open(file_path, 'rt') as f:
        prev_compl = 0
        for lno, line in enumerate(f):
            if len(line.strip()) == 0:
                continue
            try:
                parsed = parser.parse_line(line)
            except Exception, e:
                print "Exception occurred!: {}".format(str(e))
                if not cont_error:
                    sys.exit(-1)

            if not parsed:
                print "Parsing failed!({}) : '{}'".format(lno, line)
                if not cont_error:
                    sys.exit(-1)
            else:
                if parser.completed <= prev_compl:
                    continue
                prev_compl = parser.completed
                if not errors_only:
                    print parser.parsed
                    print
                n_succ += 1


@cli.command()
@click.argument('file_path')
@click.option('--cfg_path', help="Forwarder config file path.")
@click.option('--cfile_idx', default=0, help="Tailing config file index.")
@click.option('--errors-only', is_flag=True, help="Show errors only.")
def format(file_path, cfg_path, cfile_idx, errors_only):
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
                if not errors_only:
                    print(gd)
            except AttributeError:
                print "Parsing failed! : '{}'".format(line)
                if not errors_only:
                    sys.exit(-1)


if __name__ == '__main__':
    cli()
