from datetime import datetime

import pytest

from wdfwd.dump import db
from wdfwd.get_config import get_config
from wdfwd.tests import write_eloa_cfg


cfg = get_config()
for task in cfg['tasks']:
    cmd = task.keys()[0]
    if cmd == 'sync_db_dump':
        dcfg = task['sync_db_dump']
        dbc = dcfg['db']
        break


def test_db_tableinfo():
    ti = db.TableInfo("TblLogOpr_")
    assert ti.name == 'TblLogOpr_'
    assert ti == 'TblLogOpr_'
    assert 'TblLogOpr_' == ti
    assert ti + '20151121' == 'TblLogOpr_20151121'
    assert ti + u'20151121' == 'TblLogOpr_20151121'
    with pytest.raises(NotImplementedError):
        assert ti + 3
    assert 'asdf_' + ti == 'asdf_TblLogOpr_'
    assert str(ti) == 'TblLogOpr_'
    assert ti.replace('_', '[_]') == 'TblLogOpr[_]'

    ti += '20151121'
    ti.icols = ['cId', 'cDateReg', 'cMajorType']
    with db.Connector(dcfg) as con:
        ti.build_columns(con)
        assert len(ti.columns) == 3
        assert len(ti.types) == 3

    ti = db.TableInfo("TblMissionPlayLogOpr_20151122")
    ti.ecols = ['cWorldNo', 'cSvrNo']
    with db.Connector(dcfg) as con:
        ti.build_columns(con)
        assert len(ti.columns) == 26
        assert len(ti.types) == 26


def test_db_basic():
    with db.Connector(dcfg) as con:
        assert con.cursor is not None

        # fetch all tables with matching name
        tbnames = dbc['table']['names']
        subtables = db.table_array(con, tbnames[0])
        assert len(subtables) == 2

        # parse table date
        date = db.get_table_date(con, subtables[0])
        assert date == datetime(2015, 11, 22)

        ftable = subtables[0]
        ti = db.TableInfo(ftable)
        ti.build_columns(con)
        # select rows with fetch size
        gen = db.table_rows(con, ti)
        assert len(gen.next()) == dbc['fetchsize']

        # fast fetch table row count
        assert db.get_table_rowcnt(con, ti) == 11790


def test_db_temp_del():
    with db.Connector(dcfg) as con:
        table = "TblHackLogOpr_20151122"

        def row_cnt():
            db.execute(con, "SELECT COUNT(*) FROM %s" % table)
            return con.cursor.fetchone()[0]

        prev_cnt = row_cnt()
        with db.TemporaryRemoveFirstRow(con, table):
            new_cnt = row_cnt()

        assert prev_cnt - new_cnt == 1
        rol_cnt = row_cnt()
        assert prev_cnt == rol_cnt


def test_db_connect():
    with db.Connector(dcfg) as con:
        print con.conn, con.cursor


def test_db_data_dates():
    dcfg2 = write_eloa_cfg(dcfg)
    with db.Connector(dcfg2) as con:
        dates = db.get_data_dates(con, False)
        assert dates == ['2015/11/23', '2015/11/24', '2015/11/25']


def test_db_table_rowcnt_by_date():
    dcfg2 = write_eloa_cfg(dcfg)
    with db.Connector(dcfg2) as con:
        n_rows = db.table_rowcnt_by_date(con, 'CharStateLog_TBL', '2015/11/23')
        assert n_rows == 375476
