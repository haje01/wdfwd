import os
import sys
sys.path.append('.')

import click

from wdfwd.get_config import _get_config
from wdfwd.util import iter_tail_info, FileTailInfo

CONFIG_NAME = 'WDFWD_CFG'


@click.group()
def cli():
    pass


@cli.command()
@click.argument('file_path')
@click.option('--cfg-path', help="Forwarder config file path.")
@click.option('--cfile-idx', default=0, help="Tailing config file index.")
@click.option('--errors-only', is_flag=True, help="Show errors only.")
@click.option('--cont-error', is_flag=True, help="Continue parsing with error."
              )
def parser(file_path, cfg_path, cfile_idx, errors_only, cont_error):
    if not cfg_path:
        assert CONFIG_NAME in os.environ
        cfg_path = os.environ[CONFIG_NAME]

    cfg = _get_config(cfg_path)
    assert 'tailing' in cfg
    tailc = cfg['tailing']
    targets = list(iter_tail_info(tailc))
    target = targets[cfile_idx]
    assert isinstance(target, FileTailInfo)

    targets = list(iter_tail_info(tailc))
    target = targets[cfile_idx]
    parser = target.parser
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
@click.option('--cfg-path', help="Forwarder config file path.")
@click.option('--cfile-idx', default=0, help="Tailing config file index.")
@click.option('--errors-only', is_flag=True, help="Show errors only.")
def format(file_path, cfg_path, cfile_idx, errors_only):
    import re

    if not cfg_path:
        assert CONFIG_NAME in os.environ
        cfg_path = os.environ[CONFIG_NAME]

    cfg = _get_config(cfg_path)
    assert 'tailing' in cfg
    tailc = cfg['tailing']
    targets = list(iter_tail_info(tailc))
    target = targets[cfile_idx]
    assert isinstance(target, FileTailInfo)
    format = target.format
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
