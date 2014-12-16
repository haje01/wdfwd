import os
from datetime import datetime

from wdfwd.get_config import get_config
from wdfwd.const import TABLE_INFO_FILE
from wdfwd import dump
from wdfwd.dump import db

cfg = get_config()
acfg = cfg['app']
dcfg = None
for task in cfg['tasks']:
    cmd = task.keys()[0]
    if cmd == 'sync_db_dump':
        dcfg = task['sync_db_dump']
        break


def test_config():
    svc = acfg['service']
    assert svc['name'] == 'WDFwdTest'


def test_by_names():
    with db.Connector(dcfg) as con:
        tables = db.tables_by_names(con, False)
        assert tables == [u'TblHackLogOpr_20140308', u'TblHackLogOpr_20140309',
                          u'TblLogOpr_20140308', u'TblLogOpr_20140309',
                          u'TblMissionPlayLogOpr_20140308',
                          u'TblMissionPlayLogOpr_20140309']
        tables = db.tables_by_names(con)
        assert tables == [u'TblHackLogOpr_20140308', u'TblLogOpr_20140308',
                          u'TblMissionPlayLogOpr_20140308']


def test_by_dates():
    with db.Connector(dcfg) as con:
        ret = db.tables_by_names(con, False)
        assert ret == [u'TblHackLogOpr_20140308', u'TblHackLogOpr_20140309',
                       u'TblLogOpr_20140308', u'TblLogOpr_20140309',
                       u'TblMissionPlayLogOpr_20140308',
                       u'TblMissionPlayLogOpr_20140309']
        ret2 = db.tables_by_names(con, True)
        assert ret2 == [u'TblHackLogOpr_20140308', u'TblLogOpr_20140308',
                        u'TblMissionPlayLogOpr_20140308']

        dates = db.collect_dates(con, False)
        assert dates == [datetime(2014, 3, 8), datetime(2014, 3, 9)]

        assert db.daily_tables_from_dates(con, dates) ==\
            [[u'TblHackLogOpr_20140308', u'TblLogOpr_20140308',
              u'TblMissionPlayLogOpr_20140308'],
             [u'TblHackLogOpr_20140309', u'TblLogOpr_20140309',
              u'TblMissionPlayLogOpr_20140309']]


def test_db_dump():
    dump.clean_info(dcfg)
    # no dump result check
    with db.Connector(dcfg) as con:
        assert db.daily_tables_by_change(dcfg, con) == \
            [[u'TblHackLogOpr_20140308', u'TblLogOpr_20140308',
              u'TblMissionPlayLogOpr_20140308']]
        daily_tables = db.daily_tables_from_dates(con, db.collect_dates(con))
        for tables in daily_tables:
            dumped = db.dump_tables(dcfg, tables, 2)  # 2 max fetch for speed

        dump.db.write_table_info(dcfg, dumped)

        # check dump result
        folder = dcfg['folder']
        files = os.listdir(folder)
        assert TABLE_INFO_FILE in files

        # check dumped files
        logopr = [f for f in files if '_wdfwd_' not in f and
                  'TblLogOpr' in f][0]
        dlm = dcfg['field_delimiter']

        # and its contents
        with open(os.path.join(folder, logopr), 'r') as f:
            # check header (cols)
            header = f.readline().strip()
            assert len(header.split(dlm)) == 3
            cAccId = f.readline().strip().split(dlm)[0]
            assert "2" == cAccId

        # modify table then detect change
        with db.DummyRowAppender(con, 'TblHackLogOpr_20140308', 10):
            changed_tables = db.daily_tables_by_change(dcfg, con)
            assert changed_tables == [['TblHackLogOpr_20140308']]


def test_dummy_row_appender():
    with db.Connector(dcfg) as con:
        cnt = db.get_table_rowcnt(con, 'TblHackLogOpr_20140308')
        with db.DummyRowAppender(con, 'TblHackLogOpr_20140308', 10):
                _cnt = db.get_table_rowcnt(con, 'TblHackLogOpr_20140308')
                assert _cnt > cnt
        assert db.get_table_rowcnt(con, 'TblHackLogOpr_20140308') == cnt


def test_dump_and_sync():
    dump.clean_info(dcfg)
    dumped = dump.check_dump_db_and_sync(dcfg, 2)
    assert len(dumped) > 0

    # append more rows and check dump info
    with db.Connector(dcfg) as con:
        with db.DummyRowAppender(con, 'TblHackLogOpr_20140308', 10):
            ct = db.daily_tables_by_change(dcfg, con)
            ct = [str(t) for t in ct[0]]
            assert ct == ['TblHackLogOpr_20140308']


def test_croniter():
    from croniter import croniter
    base = datetime(2014, 4, 24, 11, 0)
    sch = acfg['service']['schedule']
    it = croniter(sch, base)
    assert it.get_next(datetime) == datetime(2014, 4, 25, 4, 0)
