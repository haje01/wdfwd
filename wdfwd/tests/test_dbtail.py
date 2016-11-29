import os
from datetime import datetime, timedelta
import glob
import time

import pytest

from wdfwd.const import BASE_DIR
from wdfwd.get_config import get_config
from wdfwd.tail import DBConnector, db_execute, TableTailer, FluentCfg,\
    db_get_column_idx

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
DUP_QSIZE = dcfg['dup_qsize']
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
    pass


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
        dtime = START_DATE + timedelta(seconds=start + i, microseconds=100000)
        dtime = str(dtime)[:-3]
        cmd = _cmd.format(table_no, dtime, "'message {0}'".format(start + i))
        db_execute(con, cmd)
    con.cursor.commit()
    return start + n_fill


@pytest.fixture(scope='function')
def init():
    with DBConnector(tcfg) as con:
        delete_previous(con, 1)
        create_table(con, 1)
        check_table(con, 1)


@pytest.fixture(scope='function')
def ttail():
    return _ttail()


def _ttail():
    finfo = tcfg['from']
    lines_on_start = tcfg['lines_on_start']
    tinfo = tcfg['to']
    fluent = tinfo['fluent']
    fcfg = FluentCfg(fluent[0], fluent[1])
    send_term = tcfg['send_term']

    ainfo = finfo[0]['table']
    table = ainfo['name']
    tag = ainfo['tag']
    key_col = ainfo['key_col']
    enc = dcfg['encoding']

    tail = TableTailer(
        tcfg,
        table,
        tag,
        POS_DIR,
        fcfg,
        DATEFMT,
        key_col,
        delim=DELIMITER,
        send_term=send_term,
        encoding=enc,
        millisec_ndigit=MILLSEC_ND,
        lines_on_start=lines_on_start,
        dup_qsize=DUP_QSIZE,
        echo=True)
    return tail


def test_dbtail_fill1(init):
    """test DB tailing step by step

    Note: Remove Log1 before run this test

    """
    with DBConnector(tcfg) as con:
        fill_table(con, 1, 10)


def test_dbtail_fill2():
    with DBConnector(tcfg) as con:
        fill_table(con, 1, 5, 10)


def test_dbtail_fill3():
    with DBConnector(tcfg) as con:
        fill_table(con, 1, 50000 - 20, 20)


def test_dbtail_units(init, ttail):
    """Test basic units of db tailing"""
    with DBConnector(tcfg) as con:
        next_idx = fill_table(con, 1)
        assert NUM_FILL_LINE == next_idx
        ttail._store_key_idx_once(con)

    assert ttail.table == 'Log1'
    assert ttail.tag.endswith('wdfwd.dbtail1')
    assert ttail.max_between_data > 0
    assert ttail.send_term == 1
    assert len(ttail.sname) > 0
    assert len(ttail.pdir) > 0
    assert len(ttail.saddr) > 0
    assert ttail.max_between_data > 0
    assert ttail.encoding == 'UTF8'
    assert ttail.key_col == 'dtime'
    assert ttail.dup_qsize == 3

    pos, hashes = ttail.parse_sent_pos("2016-11-07 09:30:09.100\t")
    assert pos == "2016-11-07 09:30:09.100"
    assert hashes == []

    pos, hashes = ttail.parse_sent_pos("2016-11-07 09:30:09.100\t1, 2, 3")
    assert pos == "2016-11-07 09:30:09.100"
    assert hashes == [1, 2, 3]

    with DBConnector(tcfg) as con:
        assert ttail.is_table_exist(con)
        assert 0 == db_get_column_idx(con, 'Log1', 'dtime')
        assert 1 == db_get_column_idx(con, 'Log1', 'message')
        assert 0 == ttail.key_idx

        spos, hashes = ttail.get_sent_pos(con)
        assert "2016-11-07 09:30:05.100" == spos
        assert hashes == []
        cursor = ttail.select_lines_to_send(con, spos)
        sent_hashes = []
        scnt, last_kv = ttail.send_new_lines(con, cursor, sent_hashes)
        assert 5 == scnt
        assert 3 == len(sent_hashes)
        assert last_kv == "2016-11-07 09:30:09.100"
        ttail.save_sent_pos(last_kv, sent_hashes)
        pos, hashes = ttail.get_sent_pos(con)
        assert pos == "2016-11-07 09:30:09.100"
        assert 3 == len(hashes)

        # Try to re-send from start (Test remove duplicates)
        cursor = ttail.select_lines_to_send(con, pos)
        scnt, last_kv = ttail.send_new_lines(con, cursor, sent_hashes)
        assert 0 == scnt
        assert last_kv is None

        # Add more logs
        fill_table(con, 1, 5, 10)
        # check and send message
        t = time.time()
        scnt, netok = ttail.may_send_newlines(t, con)
        assert scnt == 5
        assert netok

        # Add more logs
        fill_table(con, 1, 5, 15)
        # check and send message
        scnt, netok = ttail.may_send_newlines(t + 1, con)
        assert scnt == 5
        assert netok

        assert len(ttail.echo_file.getvalue().splitlines()) == 15
        pos, hashes = ttail.get_sent_pos(con)
        pos == "2016-11-07 09:30:19.100"
        assert len(hashes)


def test_dbtail_no_start_lines(init, ttail):
    with DBConnector(tcfg) as con:
        fill_table(con, 1)
        ttail.lines_on_start = 0
        dtime = ttail.get_initial_pos()
        assert "2016-11-07 09:30:09.100\t" == dtime


def test_dbtail_rmdup(init, ttail):
    with DBConnector(tcfg) as con:
        fill_table(con, 1)
        t = time.time()
        scnt, netok = ttail.may_send_newlines(t, con)
        assert scnt == 5

        # make one duplicate
        fill_table(con, 1, 5, 9)
        scnt, netok = ttail.may_send_newlines(t + 1, con)
        assert scnt == 4
