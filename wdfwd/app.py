import time
from datetime import datetime
import logging
import traceback

from croniter import croniter

from wdfwd.get_config import get_config
from wdfwd.dump import check_dump_db_and_sync
from wdfwd.sync import sync_folder, sync_file, sync_files, find_file_by_ptrn


cfg = get_config()
appc = cfg['app']

start_dt = datetime.now()
schedule = appc['service']['schedule']
cit = croniter(schedule, start_dt)
next_dt = cit.get_next(datetime)
logcnt = 0
LOG_SYNC_CNT = 30

force_first_run = appc['service'].get('force_first_run', False)


def run():
    """Run application main."""

    global next_dt, force_first_run
    logging.info("%s run %s", appc['service']['name'], str(time.time()))
    now = datetime.now()
    logging.debug('start_dt: ' + str(start_dt))
    logging.debug('next_dt: ' + str(next_dt))
    logging.debug('now: ' + str(now))

    if now > next_dt or force_first_run:
        if force_first_run:
            logging.debug('Running force first')

        if 'tasks' in cfg:
            try:
                _run_tasks(cfg['tasks'])
            except Exception as e:
                logging.error(traceback.format_exc())
        if force_first_run:
            force_first_run = False
        else:
            next_dt = cit.next(datetime)
            logging.debug('next_dt: ' + str(next_dt))

    if 'log' in cfg:
        lcfg = cfg['log']
        try:
            _sync_log(lcfg)
        except Exception as e:
            logging.error(str(e))
            logging.error(traceback.format_exc())


def _sync_log(lcfg):
    global logcnt
    # sync log
    if 'handlers' in lcfg and 'file' in lcfg['handlers']:
        logcnt += 1
        if logcnt < LOG_SYNC_CNT:
            return
        logcnt = 0
        if 'to_url' in lcfg:
            lpath = lcfg['handlers']['file']['filename']
            logging.debug('log path: ' + lpath)
            to_url = lcfg['to_url']
            sync_file(lpath, to_url)
        else:
            logging.debug('No URL to sync log file to')


def _sync_folder(scfg):
    folder = scfg['folder']
    to_url = scfg['to_url']
    logging.debug("Sync folders", folder)
    sync_folder(folder, to_url)


def _sync_files(scfg):
    bfolder = scfg['base_folder']
    recurse = scfg['recurse']
    ptrn = scfg['filename_pattern']
    to_url = scfg['to_url']
    logging.debug("Sync files", bfolder, ptrn, recurse)
    files = find_file_by_ptrn(bfolder, ptrn, recurse)
    sync_files(bfolder, files, to_url)


def _run_tasks(tasks):
    logging.debug('_run_tasks')
    for task in tasks:
        st = time.time()
        cmd = task.keys()[0]
        logging.debug('cmd: ' + cmd)
        if cmd == 'sync_folder':
            scfg = task['sync_folder']
            _sync_folder(scfg)
        elif cmd == 'sync_files':
            scfg = task['sync_files']
            _sync_files(scfg)
        elif cmd == 'sync_db_dump':
            scfg = task['sync_db_dump']
            if 'db' in scfg:
                # dump db
                check_dump_db_and_sync(scfg)
        logging.debug("elapsed: {}".format(time.time() - st))
