import os
from datetime import datetime, timedelta
import glob
import random

import pytest

from wdfwd.const import BASE_DIR
from wdfwd.get_config import get_config
from wdfwd.tail import DBConnector, db_execute, TableTailer, FluentCfg,\
    SEND_TERM

cfg_path = os.path.join(BASE_DIR, 'tests', 'cfg_dbtail.yml')
os.environ['WDFWD_CFG'] = cfg_path
cfg = get_config()
assert 'tailing' in cfg
assert 'from' in cfg['tailing']
assert 'db' in cfg['tailing']
tcfg = cfg['tailing']
dcfg = tcfg['db']

DATEFMT = dcfg['datefmt']
MILLSEC_ND = dcfg['millisec_ndigit']
DELIMITER = dcfg['delimiter']
POS_DIR = tcfg['pos_dir']

START_DATE = datetime(2016, 11, 7, 9, 30, 0)
NUM_FILL_LINE = 10


@pytest.fixture(scope='function')
def rmpos():
    poss = glob.glob(os.path.join(POS_DIR, '*.pos'))
    for pos in poss:
        os.remove(pos)


def delete_previous(con, no):
    drop_table(con, no)
    rmpos()


def drop_table(con, no):
    cmd = '''
if object_id('dbo.Log{0}', 'U') is not null
drop table dbo.Log{0}
    '''.format(no)
    rv = db_execute(con, cmd)
    assert rv
    con.cursor.commit()


def create_table(con, no):
    cmd = '''
use DBTailTest;

create table dbo.Log{0}
(
    dtime datetime not null,
    message varchar(255) null
);
    '''.format(no)
    rv = db_execute(con, cmd)
    assert rv
    con.cursor.commit()


def check_table(con, no):
    cmd = '''
select object_id('dbo.Log{0}', 'U')
    '''.format(no)
    db_execute(con, cmd)
    rv = con.cursor.fetchall()
    assert rv[0][0] is not None


def fill_table(con, table_no, n_fill=NUM_FILL_LINE, start=0):
    """Fill table with dummy logs.

    Args:
        con: Instance of DBConnector
        table_no: Log table number
        n_fill(optional): Number of log lines to fill. Defaults to
            NUM_FILL_LINE
        start(optional): Log index start value

    Returns:
        Next log index
    """
    _cmd = '''
insert into dbo.Log{0} values('{1}', {2});
    '''
    for i in range(n_fill):
        ms = int(random.random() * 1000000)
        dtime = START_DATE + timedelta(seconds=i, microseconds=ms)
        dtime = str(dtime)[:-3]
        cmd = _cmd.format(table_no, dtime, "'message {0}'".format(start + i))
        db_execute(con, cmd)
    con.cursor.commit()
    return start + n_fill


@pytest.fixture(scope='module')
def init():
    with DBConnector(tcfg) as con:
        delete_previous(con, 1)
        create_table(con, 1)
        check_table(con, 1)
        next_idx = fill_table(con, 1)
        assert NUM_FILL_LINE == next_idx


@pytest.fixture(scope='function')
def ttail():
    return _ttail()


def _ttail():
    finfo = tcfg['from']
    ainfo = finfo[0]['table']
    table = ainfo['name']
    tinfo = tcfg['to']
    fluent = tinfo['fluent']
    fcfg = FluentCfg(fluent[0], fluent[1])
    tag = ainfo['tag']
    key_col = ainfo['key_col']
    enc = dcfg['encoding']
    tail = TableTailer(
        table,
        tag,
        POS_DIR,
        fcfg,
        DATEFMT,
        key_col,
        delim=DELIMITER,
        encoding=enc,
        millisec_ndigit=MILLSEC_ND)
    return tail


def test_dbtail_units(init, ttail):
    """Test basic units for db tailing"""
    assert ttail.table == 'Log1'
    assert ttail.tag.endswith('wdfwd.dbtail1')
    assert ttail.max_between_data > 0
    assert ttail.send_term == 1
    assert len(ttail.sname) > 0
    assert len(ttail.pdir) > 0
    assert len(ttail.saddr) > 0
    assert ttail.max_between_data > 0
    assert ttail.send_term == SEND_TERM
    assert ttail.encoding == 'UTF8'

    with DBConnector(tcfg) as con:
        assert ttail.is_table_exist(con)
        pos = ttail.get_sent_pos()
        assert '1970-01-01 00:00:00.000' == pos
        assert NUM_FILL_LINE == ttail.get_num_to_send(con, pos)
        it_lines = ttail.select_lines_to_send(con, pos)
        scnt = ttail.send_new_lines(con, it_lines)
        assert NUM_FILL_LINE == scnt

        # Add more logs
