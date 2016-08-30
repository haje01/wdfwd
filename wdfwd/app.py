import time
from datetime import datetime
import traceback

from croniter import croniter

from wdfwd.get_config import get_config
from wdfwd.tail import FileTailer, TailThread
from wdfwd.util import ldebug, linfo, lerror, supress_boto3_log, iter_tail_info
from wdfwd.sync import sync_file


cfg = get_config()
appc = cfg['app']
tailc = cfg.get('tailing')

start_dt = datetime.now()
schedule = appc['service'].get('schedule')
cit = croniter(schedule, start_dt) if schedule else None
next_dt = cit.get_next(datetime) if cit else None
logcnt = 0
LOG_SYNC_CNT = 30

force_first_run = appc['service'].get('force_first_run', False)

tail_threads = []
fsender = None


def start_tailing():
    ldebug("start_tailing-")
    supress_boto3_log()

    if not tailc:
        ldebug("no tailing config. return")
        return

    for i, ti in enumerate(iter_tail_info(tailc)):
        ldebug("start file tail - bdir: '{}', ptrn: '{}', tag: '{}', "
               "pos_dir: '{}', latest: '{}'".format(ti.bdir,
                                                    ti.ptrn,
                                                    ti.tag,
                                                    ti.pos_dir,
                                                    ti.latest))

        tailer = FileTailer(ti.bdir, ti.ptrn, ti.tag, ti.pos_dir, ti.scfg,
                            send_term=ti.send_term, update_term=ti.update_term,
                            elatest=ti.latest, encoding=ti.file_enc,
                            lines_on_start=ti.lines_on_start,
                            max_between_data=ti.max_between_data,
                            format=ti.format, parser=ti.parser,
                            order_ptrn=ti.order_ptrn,
                            reverse_order=ti.reverse_order)

        name = ti.tag
        ldebug("create & start {} thread".format(name))
        trd = TailThread(name, tailer)
        tail_threads.append(trd)
        trd.start()


def stop_tailing():
    ldebug("stop_tailing")
    time.sleep(2)
    for trd in tail_threads:
        trd.exit()


def run_scheduled():
    """Run application main."""

    global next_dt, force_first_run
    if not next_dt:
        return

    ldebug("run_scheduled {}".format(next_dt))

    linfo("{} run {}".format(appc['service']['name'], time.time()))
    now = datetime.now()
    ldebug('start_dt: ' + str(start_dt))
    ldebug('next_dt: ' + str(next_dt))
    ldebug('now: ' + str(now))

    if now > next_dt or force_first_run:
        if force_first_run:
            ldebug('Running force first')

        if 'tasks' in cfg:
            try:
                _run_tasks(cfg['tasks'])
            except Exception as e:
                lerror(traceback.format_exc())
        if force_first_run:
            force_first_run = False
        else:
            next_dt = cit.next(datetime)
            ldebug('next_dt: ' + str(next_dt))

    if 'log' in cfg:
        lcfg = cfg['log']
        try:
            _sync_log(lcfg)
        except Exception as e:
            lerror(str(e))
            lerror(traceback.format_exc())


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
            ldebug('log path: ' + lpath)
            to_url = lcfg['to_url']
            sync_file(lpath, to_url)
        else:
            ldebug('No URL to sync log file to')


def _sync_folder(scfg):
    folder = scfg['folder']
    to_url = scfg['to_url']
    ldebug("Sync folders: " + folder)
    from wdfwd.sync import sync_folder
    sync_folder(folder, to_url)


def _sync_files(scfg):
    bfolder = scfg['base_folder']
    recurse = scfg['recurse']
    ptrn = scfg['filename_pattern']
    to_url = scfg['to_url']
    ldebug("Sync files: {} {} {}".format(bfolder, ptrn, recurse))
    from wdfwd.sync import find_file_by_ptrn
    files = find_file_by_ptrn(bfolder, ptrn, recurse)
    from wdfwd.sync import sync_files
    sync_files(bfolder, files, to_url)


def _sync_file(scfg):
    path = scfg['filepath']
    to_url = scfg['to_url']
    ldebug("Sync single file: {} {}".format(path, to_url))
    sync_file(path, to_url)


def _run_tasks(tasks):
    ldebug('_run_tasks')
    for task in tasks:
        st = time.time()
        cmd = task.keys()[0]
        ldebug('cmd: ' + cmd)
        if cmd == 'sync_folder':
            scfg = task['sync_folder']
            _sync_folder(scfg)
        elif cmd == 'sync_files':
            scfg = task['sync_files']
            _sync_files(scfg)
        elif cmd == 'sync_file':
            scfg = task['sync_file']
            _sync_file(scfg)
        elif cmd == 'sync_db_dump':
            scfg = task['sync_db_dump']
            if 'db' in scfg:
                # dump db
                from wdfwd.dump import check_dump_db_and_sync
                check_dump_db_and_sync(scfg)
        ldebug("elapsed: {}".format(time.time() - st))
