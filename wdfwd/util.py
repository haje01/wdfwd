import os
import logging
import tempfile
import time
import re
import stat
from collections import namedtuple
from subprocess import check_call as _check_call, CalledProcessError
import codecs

import win32file
import boto3
from botocore.exceptions import ClientError


fsender = None
KN_TEST_STREAM = 'wdfwd-test'


def decode(msg, encoding):
    return codecs.decode(msg, encoding, 'replace')


def cap_call(cmd, retry=0, _raise=True, _test=False):
    logging.info('cap_call cmd: {}, retry: {}'.format(cmd, _raise))
    if retry > 0:
        for i in range(retry + 1):
            _raise = False if i < retry else True
            if i > 0:
                logging.debug("{} retry".format(i))
            if _cap_call(cmd, retry, _raise, _test):
                logging.debug("success")
                return
            # sleep for a while, then retry
            logging.debug("wait for a while")
            time.sleep(10)
    else:
        _cap_call(cmd, retry, _raise, _test)


def _cap_call(cmd, retry, _raise, _test=False):
    out = tempfile.TemporaryFile()
    err = tempfile.TemporaryFile()
    res = True
    try:
        logging.info('_cap_call: %s', str(cmd))
        _check_call(cmd, shell=True, stdout=out, stderr=err)
    except CalledProcessError, e:
        logging.error(str(e))
        res = False
        if _raise:
            raise
    finally:
        out.flush()
        err.flush()
        out.seek(0)
        err.seek(0)
        _out = out.read()
        _err = err.read()
        if len(_out) > 0:
            logging.debug(_out)
            if _test:
                print(_out)
        if len(_err) > 0:
            logging.error(_err)
            if _test:
                print(_err)
        if not _raise or res:
            return res


def escape_data_path(path):
    if 'library.zip' in path:
        return path.replace('\\library.zip\\wdfwd', '\\files')
    return path


def log_head(msg):
    logging.critical("==================== %s  ====================" % msg)


def safe_fname(fname):
    """Return safe fname by eliminating chances of dir traversing"""
    return fname.replace('../', '')


class ChangeDir(object):

    def __init__(self, *dirs):
        self.cwd = os.getcwd()
        self.path = os.path.join(*dirs)

    def __enter__(self):
        logging.info('change dir to %s', self.path)
        os.chdir(self.path)

    def __exit__(self, _type, value, tb):
        os.chdir(self.cwd)


def get_fileid(fh):
    info = win32file.GetFileInformationByHandle(fh)
    return sum(info[8:])


class OpenNoLock(object):

    def __init__(self, path):
        self.path = path
        self.handle = None

    def __enter__(self):
        return self.open()

    def open(self):
        self.handle = win32file.CreateFile(self.path, win32file.GENERIC_READ,
                                           win32file.FILE_SHARE_DELETE |
                                           win32file.FILE_SHARE_READ |
                                           win32file.FILE_SHARE_WRITE, None,
                                           win32file.OPEN_EXISTING,
                                           win32file.FILE_ATTRIBUTE_NORMAL,
                                           None)
        return self.handle

    def __exit__(self, _type, value, tb):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        if self.handle:
            win32file.CloseHandle(self.handle)
            self.handle = None
            self.fid = None


def get_dump_fname(_tbname, _date=None):
    tbname = _tbname.split('.')[-1]
    if _date is None:
        return "%s.csv" % tbname
    else:
        date = normalize_date_str(_date)
        return "{}_{}.csv".format(tbname, date)


def normalize_date_str(date):
    return date.replace('-', '')


def remove_file(fpath):
    try:
        if os.path.isfile(fpath):
            os.unlink(fpath)
    except OSError:
        logging.error("Failed: _remove_file " + fpath)


def ensure_endsep(path):
    return path if path.endswith('/') else path + '/'


def init_global_fsender(tag, host, port):
    from fluent.sender import FluentSender

    global fsender
    if fsender is None:
        fsender = FluentSender(tag, host, port)
        linfo("init_global_fsender")


def _log(level, msg):
    if logging.getLogger().getEffectiveLevel() > getattr(logging,
                                                         level.upper()):
        return

    lfun = getattr(logging, level)
    lfun(msg)
    if fsender:
        ts = int(time.time())
        try:
            fsender.emit_with_time(level, ts, {"message": msg})
        except Exception, e:
            logging.error("_log error - fsender.emit_with_time "
                          "'{}'".format(e))


def ldebug(msg):
    _log('debug', msg)


def lerror(msg):
    _log('error', msg)


def linfo(msg):
    _log('info', msg)


def lwarning(msg):
    _log('warning', msg)


def lcritical(msg):
    _log('critical', msg)


def lheader(msg):
    lcritical("============================== {} "
              "==============================".format(msg))


def escape_path(path):
    return path.replace("\\", "__").replace(":", "__")


class InvalidLogFormat(Exception):
    pass


class InvalidOrderPtrn(Exception):
    pass


def validate_format(ldebug, lerror, fmt):
    ldebug("validate_format {}".format(fmt))
    if not fmt:
        return

    if '(?P<dt_>' not in fmt:
        lerror("validate_format - not found 'dt_' part")
        raise InvalidLogFormat()

    # if ('(?P<_json_>' not in fmt) and ('(?P<_text_>' not in fmt):
        # lerror("validate_format - not found <_json_/_text_> part")
        # raise InvalidLogFormat()

    try:
        return re.compile(fmt)
    except Exception, e:
        lerror("validate_format '{}' - invalid format '{}'".format(e, fmt))
        raise InvalidLogFormat()


def validate_order_ptrn(ldebug, lerror, ptrn):
    ldebug("validate_order_ptrn - '{}'".format(ptrn))
    if not ptrn:
        return

    try:
        return re.compile(ptrn)
    except Exception, e:
        lerror("validate_order_ptrn - '{}'".format(e))
        raise InvalidOrderPtrn()


def prepare_kinesis_test(region):
    knc = boto3.client('kinesis', region_name=region)

    while True:
        try:
            ret = knc.describe_stream(StreamName=KN_TEST_STREAM)
        except ClientError as e:
            if 'ResourceNotFoundException' in str(e):
                print("Not found test stream. Create new one")
                knc.create_stream(StreamName=KN_TEST_STREAM, ShardCount=1)
                continue
            else:
                raise

        status = ret['StreamDescription']['StreamStatus']
        if status == 'ACTIVE':
            break
        print("Waiting for kinesis stream active. current status: "
              "{}".format(status))
        time.sleep(4)

    return knc


def aws_lambda_dform(rec):
    if type(rec) is list:
        return [_aws_lambda_dform(r) for r in rec]
    else:
        return _aws_lambda_dform(rec)


def _aws_lambda_dform(rec):
    return dict(
        kinesis=dict(
            partitionKey=rec['PartitionKey'],
            sequenceNumber=rec['SequenceNumber'],
            data=rec['Data'],
            aggregated=True,
            kinesisSchemaVersion="1.0"
        )
    )


def iter_kinesis_records(knc, shid, seqn):
    from base64 import b64decode
    from aws_kinesis_agg.deaggregator import deaggregate_records

    ret = knc.get_shard_iterator(
        StreamName=KN_TEST_STREAM,
        ShardId=shid,
        ShardIteratorType='AT_SEQUENCE_NUMBER',
        StartingSequenceNumber=seqn
    )
    assert 'ShardIterator' in ret
    shdit = ret['ShardIterator']
    while True:
        ret = knc.get_records(ShardIterator=shdit)
        if len(ret['Records']) == 0:
            break
        assert 'Records' in ret
        records = deaggregate_records(aws_lambda_dform(ret['Records']))
        for rec in records:
            data = b64decode(rec['kinesis']['data'])
            yield data

        shdit = ret['NextShardIterator']


def query_aws_client(service, region, access_key, secret_key):
    if ".zip" in __file__:
        base_dir = os.path.dirname(__file__).split(os.path.sep)[:-2]
    else:
        base_dir = os.path.dirname(__file__).split(os.path.sep)
    base_dir = os.path.sep.join(base_dir)
    if ".zip" in __file__:
        data_dir = os.path.join(base_dir, 'data')
    else:
        data_dir = os.path.join(base_dir, 'botocore_data')
    cacert_path = os.path.join(data_dir, 'cacert.pem')
    # print(cacert_path, data_dir)
    session = boto3.session.Session()
    session._loader.search_paths.append(data_dir)

    knc = session.client(service, use_ssl=True, verify=cacert_path,
                         region_name=region, aws_access_key_id=access_key,
                         aws_secret_access_key=secret_key)
    return knc


def supress_boto3_log():
    # supress boto3 logging level
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('nose').setLevel(logging.WARNING)


def ravel_dict(data, sep='_'):
    ret = {}
    _ravel_dict(ret, data, sep, None, True)
    return ret


def _ravel_dict(results, data, sep, _prefix, root):
    if _prefix:
        prefix = '{}{}'.format(_prefix, sep)
    else:
        prefix = ''
    for k, v in data.iteritems():
        key = '{}{}'.format(prefix, k)
        if type(v) is dict:
            _ravel_dict(results, v, sep, key, False)
        else:
            results[key] = v


FileTailInfo = namedtuple('FileTailInfo', [
    'bdir', 'ptrn', 'tag', 'pos_dir', 'scfg', 'send_term', 'update_term',
    'latest', 'file_enc', 'lines_on_start', 'max_between_data', 'format',
    'parser', 'order_ptrn', 'reverse_order', 'max_read_buffer'
])


TableTailInfo = namedtuple('TableTailInfo', [
    'table', 'tag', 'pos_dir', 'scfg', 'datefmt', 'send_term',
    'encoding', 'lines_on_start', 'max_between_data', 'col_names',
    'millisec_ndigit', 'key_idx', 'start_key_sp',
    'latest_rows_sp'])


def iter_tail_info(tailc):
    from wdfwd.tail import FluentCfg, KinesisCfg, DB_SEND_TERM,\
        FILE_UPDATE_TERM, FILE_SEND_TERM

    pos_dir = tailc.get('pos_dir')
    if not pos_dir:
        lerror("no position dir info. return")
        return
    ldebug("pos_dir {}".format(pos_dir))

    lines_on_start = tailc.get('lines_on_start')
    max_between_data = tailc.get('max_between_data')

    afrom = tailc['from']
    fl_cfg = tailc['to'].get('fluent')
    kn_cfg = tailc['to'].get('kinesis')

    # make stream cfg
    if not fl_cfg and not kn_cfg:
        lerror("no fluent / kinesis server info. return")
        return
    elif fl_cfg:
        ip = fl_cfg[0]
        port = int(fl_cfg[1])
        scfg = FluentCfg(ip, port)
        ldebug("fluent: ip {}, port {}".format(ip, port))
    elif kn_cfg:
        stream_name = kn_cfg.get('stream_name')
        region = kn_cfg.get('region')
        access_key = kn_cfg.get('access_key')
        secret_key = kn_cfg.get('secret_key')
        scfg = KinesisCfg(stream_name, region, access_key, secret_key)

    if len(afrom) == 0:
        ldebug("no source info. return")
        return

    for i, src in enumerate(afrom):
        cmd = src.keys()[0]

        if cmd == 'table':
            send_term = tailc.get('send_term', DB_SEND_TERM)
            fromc = src['table']
            yield make_table_tail_info(tailc, fromc, pos_dir, scfg,
                                       lines_on_start, max_between_data,
                                       send_term)
        elif cmd == 'file':
            update_term = tailc.get('update_term', FILE_UPDATE_TERM)
            send_term = tailc.get('send_term', FILE_SEND_TERM)
            fromc = src['file']
            yield make_file_tail_info(tailc, fromc, pos_dir, scfg,
                                      lines_on_start, max_between_data,
                                      send_term, update_term)


def make_table_tail_info(tailc, tablec, pos_dir, scfg, lines_on_start,
                         max_between_data, send_term):
    dbc = tailc['db']
    encoding = dbc.get('encoding')
    datefmt = dbc['datefmt']
    millisec_ndigit = dbc.get('millisec_ndigit', 3)

    table = tablec['name']
    tag = tablec['tag']
    start_key_sp = tablec.get('start_key_sp')
    latest_rows_sp = tablec.get('latest_rows_sp')
    col_names = tablec['col_names']
    key_idx = tablec['key_idx']

    tinfo = TableTailInfo(
        table=table,
        tag=tag,
        pos_dir=pos_dir,
        scfg=scfg,
        datefmt=datefmt,
        send_term=send_term,
        encoding=encoding,
        lines_on_start=lines_on_start,
        max_between_data=max_between_data,
        millisec_ndigit=millisec_ndigit,
        start_key_sp=start_key_sp,
        latest_rows_sp=latest_rows_sp,
        col_names=col_names,
        key_idx=key_idx
    )
    return tinfo


def resolve_format(tailc, filec):
    """Choice between global and local formats.

    Args:
        tailc: tailing section in config file.
        filec: file section under tailing/from section in config file.

    Returns:
        format config dict: Resolved format.
        bool: True if global format is selected.
    """
    gformat = tailc.get('format')
    linfo("global log format: '{}'".format(gformat))
    if gformat:
        validate_format(ldebug, lerror, gformat)

    global_format = False
    format = filec.get('format') if filec is not None else None
    if not format and gformat:
        linfo("file format not exist. use global format instead")
        format = gformat
        global_format = True

    return format, global_format


def resolve_parser(tailc, filec):
    """Choice between global and local parsers.

    Args:
        tailc: tailing section in config file.
        filec: file section under tailing/from section in config file.

    Returns:
        parser instance / parser config dict: global parser instance or parser
            config dict
        bool: True if global format is selected.
    """
    from wdfwd.parser import create_parser, merge_parser_cfg

    file_enc = tailc.get('file_encoding')
    gpcfg = tailc.get('parser')
    linfo("global log parser '{}'".format(gpcfg))
    gparser = create_parser(gpcfg, file_enc) if gpcfg else None
    pcfg = filec.get('parser') if filec is not None else None
    global_parser = False
    if pcfg is not None:
        if gpcfg is not None:
            ldebug("merge local & global parser config")
            pcfg = merge_parser_cfg(gpcfg, pcfg)
        parser = create_parser(pcfg, file_enc)
        ldebug("file parser: '{}'".format(parser))
    else:
        parser = None

    if not parser and gparser:
        linfo("parser not exist. use global parser instead")
        parser = gparser
        global_parser = True

    return parser, global_parser


def make_file_tail_info(tailc, filec, pos_dir, scfg, lines_on_start,
                        max_between_data, send_term, update_term):
    max_read_buffer = tailc.get('max_read_buffer')

    file_enc = tailc.get('file_encoding')

    format, global_format = resolve_format(tailc, filec)
    parser, global_parser = resolve_parser(tailc, filec)

    gorder_ptrn = tailc.get('order_ptrn')
    ldebug("global order ptrn: '{}'".format(gorder_ptrn))
    if gorder_ptrn:
        validate_order_ptrn(ldebug, lerror, gorder_ptrn)

    greverse_order = tailc.get('reverse_order')
    ldebug("global reverse order : '{}'".format(greverse_order))

    if filec:
        bdir = filec.get('dir')
        ptrn = filec.get('pattern')
        ldebug("file pattern: '{}'".format(ptrn))
        latest = filec.get('latest')
        ldebug("file format: '{}'".format(format))
        order_ptrn = filec.get('order_ptrn')
        reverse_order = filec.get('reverse_order')
        tag = filec.get('tag')
    else:
        bdir = ptrn = latest = order_ptrn = tag =\
            send_term = update_term = reverse_order = None

    if not format and not parser:
        lerror("Need format or parser. return")
        return

    if format and parser:
        linfo("Both format & parser exist")
        if global_parser and not global_format:
            linfo("  will use local format.")
            parser = None
        elif global_format and not global_parser:
            linfo("  will use local parser.")
            format = None
        else:
            linfo("  will use parser.")
            format = None

    if order_ptrn is None and gorder_ptrn:
        linfo("file order_ptrn not exist. use global order_ptrn "
              "instead")
        order_ptrn = gorder_ptrn

    if reverse_order is None and greverse_order:
        linfo("file reverse_order not exist. use global reverse_order "
              "instead")
        reverse_order = greverse_order

    tinfo = FileTailInfo(
        bdir=bdir,
        ptrn=ptrn,
        tag=tag,
        pos_dir=pos_dir,
        scfg=scfg,
        send_term=send_term,
        update_term=update_term,
        latest=latest,
        file_enc=file_enc,
        lines_on_start=lines_on_start,
        max_between_data=max_between_data,
        max_read_buffer=max_read_buffer,
        format=format,
        parser=parser,
        order_ptrn=order_ptrn,
        reverse_order=reverse_order)
    return tinfo


def is_file(path):
    ldebug("is_file: {}".format(path))
    try:
        st = os.stat(path)
    except OSError as e:
        lwarning("os.stat error: {}".format(e))
        return False
    mode = st.st_mode
    ldebug("st.st_mode: {}".format(mode))
    return stat.S_ISREG(mode)
