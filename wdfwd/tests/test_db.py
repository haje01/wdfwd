from datetime import datetime

import pytest

from wdfwd.dump import db
from wdfwd.get_config import get_config


cfg = get_config()
for task in cfg['tasks']:
    cmd = task.keys()[0]
    if cmd == 'sync_db_dump':
        dcfg = task['sync_db_dump']
        dbc = dcfg['db']
        break


def test_tableinfo():
    ti = db.TableInfo("TblLogOpr_")
    assert ti.name == 'TblLogOpr_'
    assert ti == 'TblLogOpr_'
    assert 'TblLogOpr_' == ti
    assert ti + '20140308' == 'TblLogOpr_20140308'
    assert ti + u'20140308' == 'TblLogOpr_20140308'
    with pytest.raises(NotImplementedError):
        assert ti + 3
    assert 'asdf_' + ti == 'asdf_TblLogOpr_'
    assert str(ti) == 'TblLogOpr_'
    assert ti.replace('_', '[_]') == 'TblLogOpr[_]'

    ti += '20140308'
    ti.icols = ['cId', 'cDateReg', 'cMajorType']
    with db.Connector(dcfg) as con:
        ti.build_columns(con)
        assert len(ti.columns) == 3
        assert len(ti.types) == 3

    ti = db.TableInfo("TblMissionPlayLogOpr_20140308")
    ti.ecols = ['cWorldNo', 'cSvrNo']
    with db.Connector(dcfg) as con:
        ti.build_columns(con)
        assert len(ti.columns) == 26
        assert len(ti.types) == 26


def test_basic():
    with db.Connector(dcfg) as con:
        assert con.cursor is not None

        # fetch all tables with matching name
        tbnames = dbc['table']['names']
        subtables = db.table_array(con, tbnames[0])
        assert len(subtables) == 2

        # parse table date
        date = db.get_table_date(con, subtables[0])
        assert date == datetime(2014, 3, 8)

        ftable = subtables[0]
        ti = db.TableInfo(ftable)
        ti.build_columns(con)
        # select rows with fetch size
        gen = db.table_rows(con, ti)
        assert len(gen.next()) == dbc['fetchsize']

        # fast fetch table row count
        assert db.get_table_rowcnt(con, ti) == 40908


def test_dummy_appender():
    with db.Connector(dcfg) as con:
        table = "TblHackLogOpr_20140308"

        def row_cnt():
            db.execute(con, "SELECT COUNT(*) FROM %s" % table)
            return con.cursor.fetchone()[0]

        prev_cnt = row_cnt()
        with db.DummyRowAppender(con, table, 10):
            db.execute(con, "SELECT COUNT(*) FROM %s" % table)
            new_cnt = row_cnt()
            assert new_cnt - prev_cnt == 10
        rol_cnt = row_cnt()
        assert prev_cnt == rol_cnt


def test_connect():
    with db.Connector(dcfg) as con:
        print con.conn, con.cursor
