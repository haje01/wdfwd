import os
import time
from datetime import datetime
import traceback

from croniter import croniter

from wdfwd.get_config import get_config
from wdfwd.tail import FileTailer, TailThread, SEND_TERM, UPDATE_TERM
from wdfwd.util import ldebug, linfo, lerror, validate_format


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
    if not tailc:
        ldebug("no tailing config. return")
        return

    file_enc = tailc.get('file_encoding')
    pos_dir = tailc.get('pos_dir')
    if not pos_dir:
        lerror("no position dir info. return")
        return
    lines_on_start = tailc.get('lines_on_start')
    max_between_data = tailc.get('max_between_data')
    afrom = tailc['from']
    fluent = tailc['to'].get('fluent')
    gformat = tailc.get('format')
    linfo("global format: '{}'".format(gformat))
    if gformat:
        validate_format(ldebug, lerror, gformat, False)

    if not fluent:
        lerror("no fluent server info. return")
        return
    else:
        # override cfg for test
        fluent_ip = os.environ.get('WDFWD_TEST_FLUENT_IP', fluent[0])
        fluent_port = int(os.environ.get('WDFWD_TEST_FLUENT_PORT', fluent[1]))
        ldebug("pos_dir {}, fluent_ip {}, fluent_port {}".format(pos_dir,
                                                                 fluent_ip,
                                                                 fluent_port))

    if len(afrom) == 0:
        ldebug("no source info. return")
        return

    for i, src in enumerate(afrom):
        cmd = src.keys()[0]
        if cmd == 'file':
            filec = src[cmd]
            bdir = filec['dir']
            ptrn = filec['pattern']
            latest = filec.get('latest')
            format = filec.get('format')
            order_ptrn = filec.get('order_ptrn')
            ldebug("file format: '{}'".format(format))
            if not format and gformat:
                linfo("file format not exist. use global format instead")
                format = gformat
            tag = filec['tag']
            send_term = filec.get('send_term', SEND_TERM)
            update_term = filec.get('update_term', UPDATE_TERM)
            ldebug("start file tail - bdir: '{}', ptrn: '{}', tag:"
                          "'{}', pos_dir: '{}', fluent: '{}', latest: "
                          "'{}'".format(bdir, ptrn, tag, pos_dir, fluent,
                                        latest))
            tailer = FileTailer(bdir, ptrn, tag, pos_dir, fluent_ip,
                                fluent_port, elatest=latest, encoding=file_enc,
                                lines_on_start=lines_on_start,
                                max_between_data=max_between_data,
                                format=format, order_ptrn=order_ptrn)
            name = "tail{}".format(i+1)
            tailer.trd_name = name
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
    from wdfwd.sync import sync_file
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
