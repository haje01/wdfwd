import os
import logging

from wdfwd.const import TABLE_INFO_FILE
from wdfwd.sync import sync_folder, sync_file
from wdfwd.util import remove_file, ChangeDir


def clean_info(dcfg):
    """Clean dump info."""
    logging.info("clean dump info")
    folder = dcfg['folder']
    if os.path.isdir(folder):
        with ChangeDir(folder):
            remove_file(TABLE_INFO_FILE)


def check_dump_db_and_sync(dcfg, max_fetch=None):
    from wdfwd.dump import db
    logging.info("check_dump_db_and_sync")
    dumped = []
    # check changed daily tables
    with db.Connector(dcfg) as con:
        daily_tables = db.daily_tables_by_change(dcfg, con)
    # dump tables
    for tables in daily_tables:
        if len(tables) > 0:
            _sync_dump(dcfg, db, tables, dumped, max_fetch)
    return dumped


def _sync_dump(dcfg, db, tables, dumped, max_fetch):
    # logging.debug('changed daily_tables: ' + str(daily_tables))
    folder = dcfg['folder']
    to_url = dcfg['to_url']
    # dump
    day_dumped = db.dump_tables(dcfg, tables, max_fetch)
    dumped += day_dumped
    # sync dumped files and remove them
    sync_folder(folder, to_url, True)
    # write dumped table info
    if len(day_dumped) > 0:
        ipath = db.write_table_info(dcfg, day_dumped)
        sync_file(ipath, to_url)
