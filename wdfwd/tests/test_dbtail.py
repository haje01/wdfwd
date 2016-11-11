import os

import pytest

from wdfwd.const import BASE_DIR
from wdfwd.get_config import get_config
from wdfwd.tail import DBConnector, db_execute

cfg_path = os.path.join(BASE_DIR, 'tests', 'cfg_dbtail.yml')
os.environ['WDFWD_CFG'] = cfg_path
cfg = get_config()
assert 'tailing' in cfg
assert 'from' in cfg['tailing']
assert 'db' in cfg['tailing']
tcfg = cfg['tailing']

DATE = 20161109


def drop_table(con, sdate):
    cmd = '''
if object_id('dbo.Log_{sdate}', 'U') is not null
drop table dbo.Log_{sdate}
    '''.format(sdate=sdate)
    rv = db_execute(con, cmd)
    assert rv
    con.cursor.commit()


def create_table(con, sdate):
    cmd = '''
use DBTailTest;

create table dbo.Log_{}
(
id int not null,
value varchar(255) null
);
    '''.format(sdate)
    rv = db_execute(con, cmd)
    assert rv
    con.cursor.commit()


def check_table(con, sdate):
    cmd = '''
select object_id('dbo.Log_{}', 'U')
    '''.format(sdate)
    db_execute(con, cmd)
    rv = con.cursor.fetchall()
    assert rv[0][0] is not None


def fill_table(con, sdate):
    _cmd = '''
insert into dbo.Log_{} values({}, {});
    '''
    for i in range(100):
        cmd = _cmd.format(sdate, i, "'value {}'".format(i))
        db_execute(con, cmd)
    con.cursor.commit()


@pytest.fixture(scope='module')
def table():
    with DBConnector(tcfg) as con:
        drop_table(con, DATE)
        create_table(con, DATE)
        check_table(con, DATE)
        fill_table(con, DATE)


@pytest.fixture(scope='function')
def dtail():
    return _dtail()


def _dtail():
    dinfo = tcfg['db']
    bdir = finfo['dir']
    tag = finfo['tag']
    tail = FileTailer(bdir, ptrn, tag, pos_dir, fcfg,
                      send_term=0, update_term=0, echo=True,
                      max_between_data=100 * 100)
    return tail


def test_dbtail_init(table):
    pass
