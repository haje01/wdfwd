import os
from datetime import datetime, timedelta
import glob

import pytest

from fluent.sender import MAX_SEND_FAIL

from wdfwd.const import BASE_DIR
from wdfwd.get_config import get_config
from wdfwd.tail import DBConnector, db_execute, TableTailer, FluentCfg,\
    SEND_TERM, DBConnector

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
POS_DIR = tcfg['pos_dir']

START_DATE = datetime(2016, 11, 7, 9, 30, 0)
NUM_FILL_LINE = 100


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


def fill_table(con, no):
    _cmd = '''
insert into dbo.Log{0} values('{1}', {2});
    '''
    for i in range(NUM_FILL_LINE):
        dtime = START_DATE + timedelta(seconds=i)
        cmd = _cmd.format(no, dtime, "'message {0}'".format(i))
        db_execute(con, cmd)
    con.cursor.commit()


@pytest.fixture(scope='module')
def init():
    with DBConnector(tcfg) as con:
        delete_previous(con, 1)
        create_table(con, 1)
        check_table(con, 1)
        fill_table(con, 1)


@pytest.fixture(scope='function')
def ttail():
    return _ttail()


def _ttail():
    finfo = tcfg['from']
    table = finfo[0]['table']['name']
    tinfo = tcfg['to']
    fluent = tinfo['fluent']
    fcfg = FluentCfg(fluent[0], fluent[1])
    tag = finfo[0]['table']['tag']
    enc = dcfg['encoding']
    tail = TableTailer(table, tag, POS_DIR, fcfg, encoding=enc, 
        datefmt=DATEFMT, millisec_ndigit=MILLSEC_ND)
    return tail


def test_dbtail_basic(init, ttail):
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
        assert '1970-01-01 00:00:00.000' == ttail.get_sent_pos()
        assert NUM_FILL_LINE == ttail.get_num_send_line(con)
