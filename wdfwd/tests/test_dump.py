import os
from datetime import datetime

from wdfwd.get_config import get_config
from wdfwd.const import TABLE_INFO_FILE
from wdfwd import dump
from wdfwd.tests import write_eloa_cfg
from wdfwd.dump import db

cfg = get_config()
acfg = cfg['app']
dcfg = None
for task in cfg['tasks']:
    cmd = task.keys()[0]
    if cmd == 'sync_db_dump':
        dcfg = task['sync_db_dump']
        break


def test_dump_config():
    svc = acfg['service']
    assert svc['name'] == 'WDFwdTest'


def test_dump_by_names():
    with db.Connector(dcfg) as con:
        tables = db.tables_by_names(con, False)
        assert tables == \
            [
                u'TblHackLogOpr_20151122', u'TblHackLogOpr_20151123',
                u'TblLogOpr_20151121', u'TblLogOpr_20151122',
                u'TblLogOpr_20151123',
                u'TblMissionPlayLogOpr_20151122',
                u'TblMissionPlayLogOpr_20151123',
                u'TblMissionPlayLogOpr_20151124',
            ]
        tables = db.tables_by_names(con)
        assert tables == \
            [
                u'TblHackLogOpr_20151122',
                u'TblLogOpr_20151121', u'TblLogOpr_20151122',
                u'TblMissionPlayLogOpr_20151122',
                u'TblMissionPlayLogOpr_20151123',
            ]


def test_dump_by_dates():
    with db.Connector(dcfg) as con:
        dates = db.collect_dates(con, False)
        assert dates == [datetime(2015, 11, 21), datetime(2015, 11, 22),
                         datetime(2015, 11, 23), datetime(2015, 11, 24)]

        tables = db.daily_tables_from_dates(con, dates)
        assert tables == \
            [
                [
                    u'TblHackLogOpr_20151121',
                    u'TblLogOpr_20151121',
                    u'TblMissionPlayLogOpr_20151121',
                ],
                [
                    u'TblHackLogOpr_20151122',
                    u'TblLogOpr_20151122',
                    u'TblMissionPlayLogOpr_20151122',
                ],
                [
                    u'TblHackLogOpr_20151123',
                    u'TblLogOpr_20151123',
                    u'TblMissionPlayLogOpr_20151123',
                ],
                [
                    u'TblHackLogOpr_20151124',
                    u'TblLogOpr_20151124',
                    u'TblMissionPlayLogOpr_20151124',
                ],
            ]


def test_dump_db():
    dump.clean_info(dcfg)
    # no dump result check
    with db.Connector(dcfg) as con:
        dtbc = db.daily_tables_by_change(dcfg, con)
        assert dtbc == \
            [
                [
                    u'TblHackLogOpr_20151121',
                    u'TblLogOpr_20151121',
                    u'TblMissionPlayLogOpr_20151121',
                ],
                [
                    u'TblHackLogOpr_20151122',
                    u'TblLogOpr_20151122',
                    u'TblMissionPlayLogOpr_20151122',
                ],
                [
                    u'TblHackLogOpr_20151123',
                    u'TblLogOpr_20151123',
                    u'TblMissionPlayLogOpr_20151123',
                ],
            ]
        daily_tables = db.daily_tables_from_dates(con, db.collect_dates(con))
        for tables in daily_tables:
            # 2 max fetch for speed
            dumped = db.dump_tables(dcfg, con, tables, 2)
            db.write_table_info(dcfg, dumped)

        # check dump result
        folder = dcfg['folder']
        files = os.listdir(folder)
        assert TABLE_INFO_FILE in files

        with open(os.path.join(folder, TABLE_INFO_FILE)) as f:
            tables = []
            for line in f:
                tables.append(line.split(':')[0])
            assert len(tables) == 9

        # check dumped files
        logopr = [fi for fi in files if
                  ('_wdfwd_' not in fi and 'TblLogOpr' in fi and not
                   fi.endswith('.swp'))][0]
        dlm = dcfg['field_delimiter']

        # and its contents
        with open(os.path.join(folder, logopr), 'r') as f:
            # check header (cols)
            header = f.readline().strip()
            assert len(header.split(dlm)) == 3
            cAccId = f.readline().strip().split(dlm)[0]
            assert "17" == cAccId

        # modify table then detect change
        with db.TemporaryRemoveFirstRow(con, 'TblHackLogOpr_20151122'):
            changed_tables = db.daily_tables_by_change(dcfg, con)
            assert changed_tables == [['TblHackLogOpr_20151122']]


def test_dump_daily_table_and_sync():
    dump.clean_info(dcfg)
    dumped = dump.check_dump_db_and_sync(dcfg, 2)
    assert len(dumped) > 0

    # delete a row and check dump info
    with db.Connector(dcfg) as con:
        with db.TemporaryRemoveFirstRow(con, 'TblHackLogOpr_20151122'):
            ct = db.daily_tables_by_change(dcfg, con)
            ct = [str(t) for t in ct[0]]
            assert ct == ['TblHackLogOpr_20151122']


def test_dump_table_updates_and_sync():
    dcfg2 = write_eloa_cfg(dcfg)
    dump.clean_info(dcfg2)
    dumped = dump.check_dump_db_and_sync(dcfg2, 2)
    assert len(dumped) == 4

    # delete a row and check dump info
    with db.Connector(dcfg2) as con:
        with db.TemporaryRemoveFirstRow(con, 'ChatingLog_TBL'):
            ut = [t for t in db.updated_day_tables(dcfg2, con, '2015-11-23')]
            ut = [str(t) for t in ut]
            assert ut == ['ChatingLog_TBL']


def test_dump_croniter():
    from croniter import croniter
    base = datetime(2014, 4, 24, 11, 0)
    sch = acfg['service']['schedule']
    it = croniter(sch, base)
    assert it.get_next(datetime) == datetime(2014, 4, 25, 4, 0)
