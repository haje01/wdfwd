# -*- coding: utf8 -*-

from __future__ import print_function
import os
import sys
import glob
import time
import threading
import socket
import logging
from StringIO import StringIO
import traceback
import json
import uuid
import msgpack
from collections import namedtuple
from datetime import datetime

import win32file
import pywintypes
from fluent.sender import FluentSender, MAX_SEND_FAIL
from aws_kinesis_agg import aggregator
import pyodbc  # NOQA

from wdfwd.util import OpenNoLock, get_fileid, escape_path, validate_format as\
    _validate_format, validate_order_ptrn as _validate_order_ptrn,\
    query_aws_client

pyodbc.pooling = False

MAX_READ_BUF = 1024 * 1024 * 2  # 2MB
MAX_SEND_RETRY = 5
DB_SEND_TERM = 60        # 60 seconds
FILE_SEND_TERM = 1       # 1 second
FILE_UPDATE_TERM = 5     # 5 seconds
MAX_BETWEEN_DATA = 1024 * 1024
FMT_NO_BODY = 0
FMT_JSON_BODY = 1
FMT_TEXT_BODY = 2
BULK_SEND_SIZE = 200

FluentCfg = namedtuple('FluentCfg', ['host', 'port'])
KinesisCfg = namedtuple('KinesisCfg', ['stream_name', 'region', 'access_key',
                                       'secret_key'])

TABLE_DEFAULT_DATE = datetime(1970, 1, 1, 0, 0, 0)


class NoTarget(Exception):
    pass


class LatestFileChanged(Exception):
    pass


class TailThread(threading.Thread):

    def __init__(self, name, tailer):
        threading.Thread.__init__(self)
        self.name = name
        self.tailer = tailer
        self.linfo("TailThread::__init__ - {}".format(self.name))
        self._exit = False

    def ldebug(self, tabfunc, msg=""):
        _log(self.tailer, 'debug', tabfunc, msg)

    def lwarning(self, tabfunc, msg=""):
        _log(self.tailer, 'warning', tabfunc, msg)

    def lerror(self, tabfunc, msg=""):
        _log(self.tailer, 'error', tabfunc, msg)

    def linfo(self, tabfunc, msg=""):
        _log(self.tailer, 'info', tabfunc, msg)

    def run(self):
        self.linfo("start run")
        self.tailer.update_target(True)

        sltime = 1
        while True:
            try:
                time.sleep(sltime)

                st = time.time()
                self.linfo("TAIL START: sltime {}".format(sltime))
                self.tailer.tmain()
                elapsed = time.time() - st
                self.linfo("TAIL END IN {}".format(elapsed))
                sltime = 0 if elapsed > 1 else 1

                if self._exit:
                    break
            except NoTarget:
                self.lwarning("run", "NoTarget")
            except Exception as e:
                self.linfo("run", str(e))
                tb = traceback.format_exc()
                for line in tb.splitlines():
                    self.lerror(line)

    def exit(self):
        self.linfo("exit")
        self._exit = True


def get_file_lineinfo(path, max_read_buf, post_lines=None):
    pos_tot = 0
    line_tot = 0
    if post_lines:
        with OpenNoLock(path) as fh:
            while True:
                res, data = win32file.ReadFile(fh, max_read_buf, None)
                nbyte = len(data)
                pos_tot += nbyte
                line_tot += len(data.splitlines())
                if nbyte < max_read_buf:
                    break

    pos = 0
    nlines = 0
    with OpenNoLock(path) as fh:
        while True:
            res, data = win32file.ReadFile(fh, max_read_buf, None)
            nbyte = len(data)
            for line in data.splitlines():
                if post_lines and line_tot - nlines <= post_lines:
                    break
                pos += len(line) + 2  # for \r\n
                if pos_tot and pos > pos_tot:
                    pos = pos_tot
                nlines += 1
            if nbyte < max_read_buf:
                break
    return nlines, pos


def _log(tail, level, tabfunc, _msg):
    if logging.getLogger().getEffectiveLevel() > getattr(logging,
                                                         level.upper()):
        return

    lfun = getattr(logging, level)
    if isinstance(tabfunc, int):
        msg = "  " * tabfunc + "{}".format(_msg)
    elif len(_msg) > 0:
        msg = "{} - {}".format(tabfunc, _msg)
    else:
        msg = "{}".format(tabfunc)

    lfun(msg)
    ts = int(time.time())
    if tail.fsender:
        try:
            tail.fsender.emit_with_time("{}".format(level), ts, {"message":
                                                                 msg})
        except Exception as e:
            logging.warning("send fail '{}'".format(e))


class BaseTailer(object):
    def __init__(self, tag, pdir, stream_cfg, send_term,
                 max_send_fail, echo, encoding, lines_on_start,
                 max_between_data):
        """
        Trailer common class initialization

        Args:
            tag: Classification tag for Fluentd
            pdir: Position file directory
            stream_cfg: Log streaming service config (Fluentd / Kinesis)
            strem_cfg: Log transmission time interval
            max_send_fail: Maximum number of retries in case of transmission
                failure
            echo: Whether to save sent messages
            encoding: Original message encoding
            lines_on_start: How many lines of existing log will be resent
                at startup (for debugging)
            max_between_data: When the service is restarted, unsent logs
                smaller than this amount are sent.
        """
        super(BaseTailer, self).__init__()
        self.fsender = self.kclient = None
        self.ksent_seqn = self.ksent_shid = None
        self.linfo("__init__", "max_send_fail: '{}'".format(max_send_fail))
        self.sname = socket.gethostname()
        self.saddr = socket.gethostbyname(self.sname)
        tag = "{}.{}".format(self.sname.lower(), tag)
        self.linfo(1, "tag: '{}'".format(tag))
        self.tag = tag
        self.send_term = send_term
        self.last_send_try = 0
        self.last_update = 0
        self.kpk_cnt = 0  # count for kinesis partition key
        self.pdir = pdir

        max_send_fail = max_send_fail if max_send_fail else MAX_SEND_FAIL
        tstc = type(stream_cfg)
        if tstc == FluentCfg:
            host, port = stream_cfg
            self.fsender = FluentSender(tag, host, port,
                                        max_send_fail=max_send_fail)
        elif tstc == KinesisCfg:
            stream_name, region, access_key, secret_key = stream_cfg
            self.kstream_name = stream_name
            self.ldebug('query_aws_client kinesis {}'.format(region))
            self.kclient = query_aws_client('kinesis', region, access_key,
                                            secret_key)
            self.kagg = aggregator.RecordAggregator()

        self.send_retry = 0
        self.echo_file = StringIO() if echo else None
        self.cache_sent_pos = {}
        self.encoding = encoding
        self.lines_on_start = lines_on_start if lines_on_start else 0
        self.max_between_data = max_between_data if max_between_data else\
            MAX_BETWEEN_DATA

    def ldebug(self, tabfunc, msg=""):
        _log(self, 'debug', tabfunc, msg)

    def linfo(self, tabfunc, msg=""):
        _log(self, 'info', tabfunc, msg)

    def lwarning(self, tabfunc, msg=""):
        _log(self, 'warning', tabfunc, msg)

    def lerror(self, tabfunc, msg=""):
        _log(self, 'error', tabfunc, msg)

    def tmain(self):
        cur = time.time()
        self.ldebug("tmain {}".format(cur))
        return cur

    def read_sent_pos(self, target, con):
        """Update the sent position of the target so far.

        Arguments:
            target: file path for FileTailer, table name for DBTailer
            con(DBConnector): DB Connection

        Returns:
            (position type): Parsed position type
                `int` for FileTailer.
                `datetime` for TableTailer.
        """
        self.linfo("read_sent_pos", "updating for '{}'..".format(target))
        tname = escape_path(target)
        ppath = os.path.join(self.pdir, tname + '.pos')
        pos = None

        if os.path.isfile(ppath):
            with open(ppath, 'r') as f:
                pos = f.readline()
            self.linfo(1, "found pos file - {}: {}".format(ppath, pos))
            parsed_pos = self.parse_sent_pos(pos)
            if parsed_pos is None:
                self.lerror("Invalid pos file: '{}'".format(pos))
                pos = None
            else:
                pos = parsed_pos

        if pos is None:
            pos = self.get_initial_pos(con)
            self.linfo(1, "can't find valid pos for {}, save as "
                          "initial value {}".format(target, pos))
            self._save_sent_pos(target, pos)
            pos = self.parse_sent_pos(pos)
        return pos

    def _save_sent_pos(self, target, pos):
        """Save sent position for a target flie.

        Args:
            target: A target file for which position will be saved.
            pos: Sent position to save.
        """
        self.linfo(1, "_save_sent_pos for {} - {}".format(target, pos))
        tname = escape_path(target)
        path = os.path.join(self.pdir, tname + '.pos')
        try:
            with open(path, 'w') as f:
                f.write("{}\n".format(pos))
        except Exception as e:
            self.lerror("Fail to write pos file: {} {}".format(e, path))
        self.cache_sent_pos[target] = pos

    def _send_newline(self, msg, msgs):
        """Send new lines

        This does not send right away, but waits for a certain number of
        messages to send for efficiency.

        Args:
            msg: A message to send
            msgs: Bulk message buffer
        """
        # self.ldebug("_send_newline {}".format(msg))
        ts = int(time.time())
        self.may_echo(msg)

        msgs.append((ts, msg))
        if len(msgs) >= BULK_SEND_SIZE:
            if self.fsender:
                bytes_ = self._make_fluent_bulk(msgs)
                self.fsender._send(bytes_)
            elif self.kclient:
                self._kinesis_put(msgs)
            msgs[:] = []

    def _send_remain_msgs(self, msgs):
        """Send bulk remain messages."""
        if len(msgs) > 0:
            if self.fsender:
                bytes_ = self._make_fluent_bulk(msgs)
                self.fsender._send(bytes_)
            elif self.kclient:
                self._kinesis_put(msgs)

    def _handle_send_fail(self, e, rbytes):
        """Handle send exception.

        Args:
            e: Exception instance
            rbytes: Size of send message in bytes.

        Raises:
            Re-raise send exception
        """
        self.lwarning(1, "send fail '{}'".format(e))
        self.send_retry += 1
        if self.send_retry < MAX_SEND_RETRY:
            self.lerror(1, "Not exceed max retry({} < {}), will try "
                        "again".format(self.send_retry,
                                       MAX_SEND_RETRY))
            raise
        else:
            self.lerror(1, "Exceed max retry, Giving up this change({}"
                        " Bytes)!!".format(rbytes))

    def _make_fluent_bulk(self, msgs):
        """Make bulk payload for fluentd"""
        tag = '.'.join((self.tag, "data"))
        bulk = [msgpack.packb((tag, ts, data)) for ts, data in msgs]
        return ''.join(bulk)

    def may_echo(self, line):
        """Echo sent message for debugging

        Args:
            line: Sent message
        """
        if self.echo_file:
            self.echo_file.write('{}\n'.format(line))
            self.echo_file.flush()

    def _kinesis_put(self, msgs):
        """Send to AWS Kinesis

        Make aggregated message and send it.

        Args:
            msgs: Messages to send
        """
        self.linfo('_kinesis_put {} messages'.format(len(msgs)))
        self.kpk_cnt += 1  # round robin shards
        for aggd in self._iter_kinesis_aggrec(msgs):
            pk, ehk, data = aggd.get_contents()
            self.linfo("  kinesis aggregated put_record: {} "
                       "bytes".format(len(data)))
            st = time.time()
            ret = self.kclient.put_record(
                StreamName=self.kstream_name,
                Data=data,
                PartitionKey=pk,
                ExplicitHashKey=ehk
            )
            stat = ret['ResponseMetadata']['HTTPStatusCode']
            shid = ret['ShardId']
            seqn = ret['SequenceNumber']
            self.ksent_seqn = seqn
            self.ksent_shid = shid
            elp = time.time() - st
            if stat == 200:
                self.linfo("Kinesis put success in {}: ShardId: {}, "
                           "SequenceNumber: {}".format(elp, shid, seqn))
            else:
                self.error("Kineis put failed in {}!: "
                           "{}".format(elp, ret['ResponseMetadata']))

    def _iter_kinesis_aggrec(self, msgs):
        for msg in msgs:
            data = {'tag_': self.tag + '.data', 'ts_': msg[0]}
            if isinstance(msg[1], dict):
                data.update(msg[1])
            else:
                data['value_'] = msg[1]

            pk = str(uuid.uuid4())
            res = self.kagg.add_user_record(pk, str(data))
            # if payload fits max send size, send it
            if res:
                yield self.kagg.clear_and_get()

        # send remain payload
        yield self.kagg.clear_and_get()


class TableTailer(BaseTailer):

    def __init__(self, dbcfg, table, tag, pdir, stream_cfg, datefmt, col_names,
                 key_idx,
                 send_term=DB_SEND_TERM, max_send_fail=None, echo=False,
                 encoding=None, lines_on_start=None, max_between_data=None,
                 millisec_ndigit=None,
                 start_key_sp=None, latest_rows_sp=None):
        """init TableTailer"""
        super(TableTailer, self).__init__(tag, pdir, stream_cfg,
                                          send_term,
                                          max_send_fail, echo, encoding,
                                          lines_on_start, max_between_data)
        self.linfo(0, "TableTailer - init")
        self.dbcfg = dbcfg
        self.table = table
        self.datefmt = datefmt
        self.col_names = col_names
        self.key_idx = key_idx
        self.key_col = col_names[key_idx]
        self.millisec_ndigit = millisec_ndigit
        # If SP is used, both start_key_sp & latest_rows_sp exist should exist.
        assert (start_key_sp and latest_rows_sp) or (not start_key_sp and
                                                     not latest_rows_sp)
        self.start_key_sp = start_key_sp
        self.latest_rows_sp = latest_rows_sp
        self.use_sp = start_key_sp is not None
        self.linfo("  start_key_sp: {}, latest_rows_sp: {}".
                   format(start_key_sp, latest_rows_sp))
        self.repeat_send = True
        self.max_consec_send = 0
        self._clear_queue_lines()

    def _store_key_idx_once(self, con):
        """Store key column index once.

        Args:
            con(DBConnector): DB connection
        """
        if self.key_idx is None:
            self.key_idx = db_get_column_idx(con, self.table, self.key_col)

    def update_target(self, start=False):
        self.ldebug("dummy update_target - cur table '{}'".format(self.table))

    def is_table_exist(self, con):
        """Check table existence.

        Args:
            con(DBConnector): DB connection

        Returns:
            True if table exists, False otherwise.
        """
        assert not self.use_sp
        tbl = self.table
        if not con.sys_schema:
            tbl = tbl.split('.')[-1]
        cmd = "SELECT NAME FROM SYS.TABLES WHERE NAME"\
              " LIKE '{}'".format(tbl)
        db_execute(con, cmd)
        rv = con.cursor.fetchall()
        exist = len(rv) > 0
        self.ldebug("is_table_exist for {} - {}".format(tbl, rv))
        return exist

    def tmain(self):
        cur = super(TableTailer, self).tmain()

        sent_line, _ = self.may_send_newlines(cur)
        return sent_line

    def get_sent_pos(self, con):
        """Return the sent info that has been sent so far

        Args:
            con(DB Connection): DB Connection

        Returns:
            (key type): Sent position
        """
        pos = self.read_sent_pos(self.table, con)
        return pos

    def save_sent_pos(self, pos):
        """Save sent info for current target.

        Args:
            pos: Position to be saved.
        """
        self._save_sent_pos(self.table, pos)

    def select_lines_to_send(self, con, pos):
        """Select lines to send from position

        Args:
            con(DBConnector): DB connection
            pos(str): Position sent so far

        Returns:
            Cursor to iterate results
        """
        self.ldebug("select_lines_to_send: pos {}".format(pos))
        self.raise_if_notarget()
        if self.use_sp:
            cmd = 'EXEC {} ?'.format(self.latest_rows_sp)
            db_execute(con, cmd, pos)
        else:
            cmd = """
                SELECT * FROM {0} WHERE {1} > '{2}'
            """.format(self.table, self.key_col, pos)
            db_execute(con, cmd)
        return con.cursor

    def may_send_newlines(self, cur, econ=None):
        """Try to send new lines if time has passed enough.

        Notes:
            If repeat_send is True, it will continue to send until there is no
            data to send.

        Args:
            cur: Current time stamp.
            econ(optional): Externally supplied DB Connection.

        Returns:
            sent: Number of sent lines.
            netok: True if sending causes no network problem.
        """
        # self.ldebug("may_send_newlines")

        if cur - self.last_send_try >= self.send_term:
            self.ldebug(1, "{} >= {}".format(cur - self.last_send_try,
                                             self.send_term))

            self.last_send_try = cur

            def _body(self, con):
                if not self.use_sp:
                    self._store_key_idx_once(con)

                if not self.use_sp and not self.is_table_exist(con):
                    self.lwarning("Target table '{}' not exists".format(
                                  self.table))
                    return None, None

                tscnt = 0
                cnt = 0
                while True:
                    scnt, netok = self._may_send_newlines(con)
                    tscnt += scnt
                    if not netok:
                        self.lwarning("Network error. stop")
                        break
                    cnt += 1
                    self.ldebug("{} - sent {}".format(cnt, scnt))
                    if cnt > self.max_consec_send:
                        self.ldebug("  over max_consec_send. stop")
                        break
                    else:
                        self.ldebug("  consecutive send")
                return tscnt, netok

            if econ is None:
                with DBConnector(self.dbcfg, self.ldebug, self.lerror) as con:
                    return _body(self, con)
            else:
                return _body(self, econ)
        return None, None

    def _may_send_newlines(self, con):
        """Send new lines if there are ones content with additional conditions.

        Note:
            Conditions are over last position & no duplicates

        Args:
            con(DBConnector): DB connection

        Returns:
            int: Count of sent lines
            netok: True if sending causes no network problem.
        """
        scnt = 0
        netok = True
        try:
            ppos = self.get_sent_pos(con)
            it_lines = self.select_lines_to_send(con, ppos)
            scnt, pos = self.send_new_lines(con, it_lines)
            if scnt > 0:
                self.save_sent_pos(pos)
        except pywintypes.error as e:
            self.lerror("send error", str(e))
            netok = False
        return scnt, netok

    def make_json(self, cols):
        """Return json from row columns

        Note: Fix datetime format, change encoding together.

        Args:
            cols: Selected row columns

        Returns:
            str: Key value for this row
            str: string of json message
        """
        if self.encoding:
            mcols = zip(self.col_names,
                        [c.strip().decode(self.encoding) if type(c) is str
                         else c for c in cols])
        else:
            mcols = zip(self.col_names,
                        [c.strip() if type(c) is str else c for c in cols])
        mdict = dict(mcols)
        kv = self.conv_datetime(cols[self.key_idx])
        mdict[self.key_col] = kv
        return kv, mdict

    def queue_send_newline(self, dt, msg, msgs):
        """Queue sending new line.

        Note:
            Send queued lines only if new timestamp has changed from
            previous one. Otherwise, queue new line & timestamp.

        Args:
            dt(datetime): Timestamp of new line.
            msg: Message data(JSON)
            msgs: msgs: Bulk message buffer

        Returns:
            None if didn't send anything.
            tuple:
                int: Sent count.
                str: Sent datetime.
        """
        sent_info = None

        if self.queue_lines_dt is None:
            self.queue_lines_dt = dt
            self.queue_lines.append(msg)
        elif self.queue_lines_dt == dt:
            self.queue_lines.append(msg)
        elif self.queue_lines_dt != dt:
            scnt = len(self.queue_lines)
            for ql in self.queue_lines:
                self._send_newline(ql, msgs)
            sent_info = scnt, self.queue_lines_dt
            self.queue_lines = [msg]
            self.queue_lines_dt = dt

        return sent_info

    def send_new_lines(self, con, cursor):
        """Send new lines to stream

        Args:
            con(DBConnector): DB connection
            cursor: Cursor by which new lines have been selected.

        Note:
            cursor has selected messages from last sent date time.
            Message is a json data of each columns.
            Queue message for delayed send to prevent possble loss of last
            lines. New line fires actual sending of previous(queued) message.

        Returns:
            int: Number of sent lines
            str: Last key value
        """
        self.ldebug("send_new_lines")
        scnt = 0
        last_kv = None
        try:
            msgs = []

            for cols in cursor:
                kv, msg = self.make_json(cols)
                sent_info = self.queue_send_newline(kv, msg, msgs)
                if sent_info is not None:
                    scnt += sent_info[0]
                    last_kv = sent_info[1]

            self._clear_queue_lines()
            self._send_remain_msgs(msgs)

        except Exception as e:
            self._handle_send_fail(e, None)

        self.send_retry = 0
        self.ldebug(1, "sent {} lines, last key value '{}'".format(scnt,
                    last_kv))
        return scnt, last_kv

    def _clear_queue_lines(self):
        self.ldebug("_clear_queue_lines")
        self.queue_lines = []
        self.queue_lines_dt = None

    def conv_datetime(self, dtime):
        """Convert a datetime to adapted format string.

        Args:
            dtime(datetime): date time to convert.

        Return:
            str: converted string.
        """
        sdt = dtime.strftime(self.datefmt)
        if '%f' in self.datefmt and self.millisec_ndigit is not None:
            sdt = sdt[:-(6 - self.millisec_ndigit)]
        return sdt

    def get_initial_pos(self, con):
        r"""Return default start position(= datetime, for now)

        Args:
            con(DBConnector): DB Connection

        Note: If lines_on_start is greater than 0, returns date time from
            which that many can be sent, or date of the last log.

        Returns:
            str: Initial position.
        """
        self.ldebug("get_initial_pos: {}".format(self.lines_on_start))
        assert self.lines_on_start >= 0

        lines_on_start = 1 if self.lines_on_start == 0 else self.lines_on_start

        if self.use_sp:
            cmd = 'EXEC {} ?'.format(self.start_key_sp)
            db_execute(con, cmd, lines_on_start)
        else:
            cmd = 'SELECT TOP(1) dtime FROM (SELECT TOP({0}) dtime FROM {1} '\
                  'ORDER BY {2} DESC) a ORDER BY a.dtime'.\
                  format(lines_on_start, self.table, self.key_col)
            db_execute(con, cmd)
        self.ldebug(cmd)
        dtime = con.cursor.fetchone()[0]
        if dtime is None:
            # no data is available
            self.lwarning("Start key is None, No data is available?")

        dtime = self.conv_datetime(dtime)
        self.ldebug("get_initial_pos: {}".format(dtime))
        return "{}".format(dtime)

    def parse_sent_pos(self, pos):
        """Parsing sent position (datetime)

        Args:
            pos(str): Sent position

        Returns:
            (position type): Last sent position
        """
        try:
            pos = pos.strip()
        except ValueError:
            return None
        return pos

    def raise_if_notarget(self):
        if self.table is None:
            raise NoTarget()


class FileTailer(BaseTailer):

    def __init__(self, bdir, ptrn, tag, pdir, stream_cfg,
                 send_term=FILE_SEND_TERM, update_term=FILE_UPDATE_TERM,
                 max_send_fail=None, elatest=None, echo=False, encoding=None,
                 lines_on_start=None, max_between_data=None, format=None,
                 parser=None, order_ptrn=None, reverse_order=False,
                 max_read_buffer=None):

        super(FileTailer, self).__init__(tag, pdir, stream_cfg, send_term,
                                         max_send_fail, echo,
                                         encoding, lines_on_start,
                                         max_between_data)
        self.bdir = bdir
        self.ptrn = ptrn
        self.update_term = update_term
        self.target_path = self.target_fid = None
        self.reverse_order = reverse_order

        self.elatest = elatest
        self.elatest_fid = None
        self.max_read_buffer = max_read_buffer if max_read_buffer else\
            MAX_READ_BUF
        self.linfo("effective format: '{}'".format(format))
        self.format = self.validate_format(format)
        self.fmt_body = self.format_body_type(format)
        self.parser = parser
        self.order_ptrn = self.validate_order_ptrn(order_ptrn)
        self.parser_compl = 0
        self.no_format = False

    def format_body_type(self, format):
        if format:
            if '_json_' in format:
                return FMT_JSON_BODY
            elif '_text_' in format:
                return FMT_TEXT_BODY
        return FMT_NO_BODY

    def raise_if_notarget(self):
        if self.target_path is None:
            raise NoTarget()

    def _tmain_may_update_target(self, cur):
        self.ldebug("_tmain_may_update_target")
        if cur - self.last_update >= self.update_term:
            self.ldebug(1, "{} > {}".format(cur - self.last_update,
                                            self.update_term))
            self.last_update = cur
            self.update_target()

    def _tmain_may_send_newlines(self, cur, scnt, netok):
        self.ldebug("_tmain_may_send_newlines")
        if cur - self.last_send_try >= self.send_term:
            self.ldebug(1, "{} >= {}".format(cur - self.last_send_try,
                                             self.send_term))
            try:
                self.last_send_try = cur
                scnt = self.may_send_newlines()
            except pywintypes.error as e:
                if e[0] == 2 and e[1] == 'CreateFile':
                    # file has been deleted
                    self.lerror("_run", "file '{}' might have been deleted. "
                                "will find new "
                                "target..".format(self.target_path))
                    self.set_target(None)
                else:
                    self.lerror("send error", str(e))
                    netok = False
        return scnt, netok

    def parse_sent_pos(self, pos):
        """Parsing sent position (line no)"""
        return int(pos)

    def tmain(self):
        cur = super(FileTailer, self).tmain()

        # send new lines when need
        sent_line = 0
        netok = True
        # handle if elatest file has been rotated
        latest_rot = False
        if not self.elatest:
            latest_rot = self.handle_file_recreate(cur)
        else:
            try:
                epath = os.path.join(self.bdir, self.elatest)
                latest_rot = self.handle_elatest_rotation(epath, cur)
            except pywintypes.error as e:
                self.lerror("File '{}' open error - {}".format(epath, e))
                self.lwarning("Skip to next turn")
                return

        psent_pos = self.get_sent_pos() if self.target_path else None

        if self.target_path:
            try:
                sent_line, netok = self._tmain_may_send_newlines(cur,
                                                                 sent_line,
                                                                 netok)
            except LatestFileChanged:
                # delegate to handle_elatest_rotation
                self.lwarning("Possibly latest file has been rotated")
                return

        # if nothing sent with net ok, try update target timely
        if not latest_rot and (sent_line == 0 and netok):
            self._tmain_may_update_target(cur)
        return latest_rot, psent_pos, sent_line

    def set_target(self, target):
        changed = self.target_path != target
        if changed:
            self.linfo("set_target from '{}' to '{}'".format(self.target_path,
                                                             target))
            self.target_path = target
            self.target_fid = None
            if target:
                if os.path.isfile(target):
                    with OpenNoLock(target) as fh:
                        self.target_fid = get_fileid(fh)
                else:
                    self.lerror("set_target", "target file '{}' not "
                                "exists".format(target))
        return changed

    def _update_elatest_target(self, files):
        self.linfo("_update_elatest_target")
        epath, efid = self.get_elatest_info()

        if efid:
            # elatest handle updated only after handle_elatest_rotation
            if self.elatest_fid is None:
                # new elatest
                self.elatest_fid = efid
            files.append(epath)

    def handle_file_recreate(self, cur=None):
        ret = 0
        if self.target_path:
            if not os.path.isfile(self.target_path):
                self.lwarning("handle_file_recreate",
                              "target file '{}' has been removed, but new"
                              " file not created yet".format(self.target_path))
                ret = 1
            else:
                with OpenNoLock(self.target_path) as fh:
                    fid = get_fileid(fh)
                if self.target_fid and fid != self.target_fid:
                    self.lwarning("handle_file_recreate",
                                  "target file '{}' has been "
                                  "recreated".format(self.target_path))
                    ret = 2

        if ret > 0:
            self.linfo(1, "reset target to delegate update_target")
            self.save_sent_pos(self.get_initial_pos(None))
            self.set_target(None)
        return ret

    def get_initial_pos(self, con):
        return 0

    def handle_elatest_rotation(self, epath=None, cur=None):
        cur = cur if cur else int(time.time())
        if not epath:
            epath = os.path.join(self.bdir, self.elatest)
        efid = None
        if os.path.isfile(epath):
            with OpenNoLock(epath) as fh:
                efid = get_fileid(fh)

        #  if elatest file has been changed
        if self.elatest_fid and efid != self.elatest_fid:
            if not efid:
                self.lwarning("handle_elatest_rotation",
                              "elatest file has been rotated, but new"
                              " elatest file not created yed.")
            else:
                self.lwarning("handle_elatest_rotation",
                              "elatest file has been rotated({} -> {})."
                              " update_target "
                              "immediately.".format(self.elatest_fid, efid))

            self.elatest_fid = None
            # get pre-elatest, which should exist
            files = self.get_sorted_target_files()
            if len(files) == 0:
                self.lerror(1, "pre-elatest files are not exist!")
                return True
            pre_elatest = files[-1]
            self.linfo("pre-elatest is '{}'".format(pre_elatest))

            self.linfo(1, "move sent pos & clear elatest sent pos")
            # even elatest file not exist, there might be pos file
            self._save_sent_pos(pre_elatest, self.get_sent_pos(epath))
            # reset elatest sent_pos
            self._save_sent_pos(epath, self.get_initial_pos(None))
            self.last_update = cur
            # pre-elatest is target for now
            self.set_target(pre_elatest)
            return True
        return False

    def get_sorted_target_files(self):
        """Return sorted target files.

        If elatest is in target files, remove it.
        Default sort order is ascending alphanumerical order.
        Oldest file appears first, and newest file supposed to be at the
        end.
        """
        reverse = self.reverse_order if self.reverse_order is not None else\
            False
        self.linfo("get_sorted_target_files - reverse {}".format(reverse))

        # remove elatest if it is in this candidates
        files = glob.glob(os.path.join(self.bdir, self.ptrn))
        if self.elatest:
            epath = os.path.join(self.bdir, self.elatest)
            if epath in files:
                files.remove(epath)

        if self.order_ptrn:
            self.linfo("order_ptrn {}".format(self.order_ptrn))
            order_key = {}
            for afile in files:
                match = self.order_ptrn.search(afile)
                if not match:
                    self.lwarning(1, "file order pattern mismatch - "
                                  "'{} - {}'".format(afile))
                    continue
                gd = match.groupdict()
                order_key[afile] = gd['date'] +\
                    ".{:06d}".format(int(gd['order']))
            rv = sorted(files, key=lambda f: order_key[f], reverse=reverse)
            return rv
        else:
            if len(files) > 0 and reverse:
                files = sorted(files, reverse=reverse)
            self.ldebug("files {}".format(files))
            return files

    def update_target(self, start=False):
        self.linfo("update_target")

        files = self.get_sorted_target_files()
        self.linfo(1, "{} target files".format(len(files)))

        if self.elatest is not None:
            self._update_elatest_target(files)

        cnt = len(files)
        newt = False
        if cnt == 0:
            self.set_target(None)
            self.lwarning(1, "NoCandidateFile")
        else:
            if cnt == 1:
                tpath = os.path.join(self.bdir, files[0])
            else:
                tpath = os.path.join(self.bdir, files[-1])
            newt = self.set_target(tpath)

        if newt:
            self.linfo(1, "new target {}".format(self.target_path))
            if start:
                self.start_sent_pos(self.target_path)
            else:
                self.read_sent_pos(self.target_path, None)
        else:
            self.linfo(1, "cur target {}".format(self.target_path))

    def get_file_pos(self, path=None):
        if path is None:
            self.raise_if_notarget()
            tpath = self.target_path
        else:
            tpath = path
        self.ldebug("get_file_pos", "for {}".format(tpath))
        try:
            with OpenNoLock(tpath) as fh:
                return win32file.GetFileSize(fh)
        except pywintypes.error as e:
            err = e[2]
            if self.encoding:
                err = err.decode(self.encoding).encode('utf8')
            self.lerror("get_file_pos", "{} - {}".format(err, tpath))
            raise

    def validate_format(self, fmt):
        return _validate_format(self.linfo, self.lerror, fmt)

    def validate_order_ptrn(self, fmt):
        return _validate_order_ptrn(self.linfo, self.lerror, fmt)

    def _convert_matched_msg(self, match):
        has_body = False
        gd = match.groupdict()

        if self.fmt_body == FMT_JSON_BODY:
            if '_json_' in gd:
                self.linfo(1, "json data found")
                has_body = True
                try:
                    msg = json.loads(gd['_json_'])
                except ValueError:
                    self.lerror("can't parse json data "
                                "'{}'".format(gd['_json_'][:50]))
                    msg = {'message': gd['_json_']}
            else:
                self.lwarning(1, "no json body found: {}'".format(msg[:50]))
        elif self.fmt_body == FMT_TEXT_BODY:
            if '_text_' in gd:
                self.linfo(1, "text data found")
                has_body = True
                msg = {}
                msg['message'] = gd['_text_']
            else:
                self.lwarning(1, "no text body found: '{}'".format(msg[:50]))
        else:
            msg = gd

        if has_body:
            for key in gd.keys():
                if key[0] == '_' and key[-1] == '_':
                    continue
                if key in msg.keys():
                    self.lwarning(1, "'{}' is overwritten from '{}' to "
                                  "'{}'".format(key, msg[key], gd[key]))
                msg[key] = gd[key]
        return msg

    def convert_msg(self, msg):
        self.ldebug("convert_msg")
        if self.encoding:
            msg = msg.decode(self.encoding).encode('utf8')

        parsed = None
        self.ldebug(1, "try match format")
        match = self.format.search(msg)
        if match:
            parsed = self._convert_matched_msg(match)
            self.ldebug(1, "parsed: {}".format(parsed))
            return parsed
        else:
            self.lwarning(1, "can't parse line '{}'".format(msg[:50]))
            return None

    def _read_target_to_end(self, fh):
        self.raise_if_notarget()

        res, lines = win32file.ReadFile(fh, self.max_read_buffer, None)
        nbyte = len(lines)
        if res != 0:
            self.lerror(1, "ReadFile Error! {}".format(res))
            return '', 0
        if nbyte == self.max_read_buffer:
            self.lwarning(1, "Read Buffer Full! Possibly corrupted last line.")

        return lines, nbyte

    def get_elatest_info(self):
        self.linfo("get_elatst_info")
        if self.elatest:
            epath = os.path.join(self.bdir, self.elatest)
            efid = None
            if os.path.isfile(epath):
                with OpenNoLock(epath) as fh:
                    efid = get_fileid(fh)
                    return epath, efid
            else:
                self.lwarning(1, "can not find elatest '{}'".format(epath))
        return None, None

    def may_send_newlines(self):
        self.raise_if_notarget()

        self.ldebug("may_send_newlines")

        # skip if no newlines
        file_pos = self.get_file_pos()
        sent_pos = self.get_sent_pos()
        self.ldebug("file_pos {}, sent_pos {}".format(file_pos, sent_pos))
        if sent_pos >= file_pos:
            if sent_pos > file_pos:
                self.lerror("sent_pos {} > file_pos {}. pos"
                            " mismatch".format(sent_pos, file_pos))
                if self.elatest:
                    raise LatestFileChanged()
            return 0

        # move to last sent pos
        with OpenNoLock(self.target_path) as fh:
            win32file.SetFilePointer(fh, sent_pos, win32file.FILE_BEGIN)
            # read file to the end
            lines, rbytes = self._read_target_to_end(fh)

        scnt = 0
        self.ldebug(1, "sent_pos {} file_pos {} rbytes "
                       "{}".format(sent_pos, file_pos, rbytes))
        if rbytes > 0:
            scnt = self._may_send_newlines(lines, rbytes, scnt,
                                           file_path=self.target_path)

        self.save_sent_pos(sent_pos + rbytes)
        return scnt

    def attach_msg_extra(self, msg):
        if isinstance(msg, dict):
            msg['sname_'] = self.sname
            msg['saddr_'] = self.saddr
            return msg
        else:
            return msg

    def _iterate_lines(self, lines, file_path):
        self.linfo("_iterate_lines")
        if self.parser:
            self.parser.set_file_path(file_path)

        for line in lines.splitlines():
            if len(line) == 0:
                continue

            parsed = None
            if self.format:
                parsed = self.convert_msg(line)
                if not parsed:
                    self.lwarning("can't convert '{}'".format(line))
            elif self.parser:
                if self.parser.parse_line(line):
                    if self.parser.completed > self.parser_compl:
                        parsed = self.parser.parsed
                        self.parser_compl = self.parser.completed
                else:
                    self.lwarning("can't parse '{}'".format(line))
            else:
                self.lwarning("no format / parser exists. send raw "
                              "message")
                yield line

            if parsed:
                yield self.attach_msg_extra(parsed)

    def _may_send_newlines(self, lines, rbytes=None, scnt=0, file_path=None):
        self.ldebug("_may_send_newlines", "sending {} bytes..".format(rbytes))
        if not rbytes:
            rbytes = len(lines)
        try:
            itr = self._iterate_lines(lines, file_path)
            msgs = []
            for msg in itr:
                if not msg:
                    # skip bad form message (can't parse)
                    self.linfo("skip bad form message")
                    continue
                self._send_newline(msg, msgs)
                scnt += 1

            self._send_remain_msgs(msgs)
        except Exception as e:
            self._handle_send_fail(e, rbytes)

        self.send_retry = 0
        return scnt

    def get_sent_pos(self, epath=None):
        tpath = epath if epath else self.target_path
        if tpath in self.cache_sent_pos:
            return self.cache_sent_pos[tpath]

        pos = self.read_sent_pos(tpath, None)
        self.cache_sent_pos[tpath] = pos
        return pos

    def start_sent_pos(self, tpath):
        """Calculate sent pos for new service start"""
        self.ldebug("start_sent_pos", "for '{}'..".format(tpath))
        tname = escape_path(tpath)
        ppath = os.path.join(self.pdir, tname + '.pos')
        file_pos = self.get_file_pos(tpath)
        if os.path.isfile(ppath):
            # between data
            # previous pos file means continuation after restart
            with open(ppath, 'r') as f:
                line = f.readline()
                elm = line.split(',')
                spos = int(elm[0])
            self.ldebug(1, "found pos file - {}: {}".format(ppath, spos))
            if spos > file_pos:
                self.lwarning(1, "sent pos ({}) > file pos ({}). possible file"
                              " change. trim..".format(spos, file_pos))
                spos = file_pos
        else:
            spos = 0

        send_bytes = file_pos - spos
        if send_bytes > self.max_between_data:
            self.lwarning(1, "between data: start from current file pos"
                             " since {} > {}".format(send_bytes,
                                                     self.max_between_data))
            spos = file_pos

        if self.lines_on_start:
            lines, pos = get_file_lineinfo(tpath, self.max_read_buffer,
                                           self.lines_on_start)
            self.lwarning("get_file_lineinfo for lines_on_start "
                          "{} - {} {}".format(self.lines_on_start, lines, pos))
            spos = pos

        self._save_sent_pos(tpath, spos)

    def save_sent_pos(self, pos):
        """Save sent position for current target.

        Args:
            pos: Position to be saved.
        """
        self.linfo("save_sent_pos")
        self.raise_if_notarget()

        # save pos file
        self._save_sent_pos(self.target_path, pos)


def db_execute(con, cmd, *args):
    try:
        con.cursor.execute(cmd.strip(), *args)
    except Exception as e:
        sys.stderr.write(str(e[1]).decode(sys.stdout.encoding) + '\n')
        return False
    return True


def db_get_column_idx(con, table, column):
    """Get column index from a table.

    Args:
        con(DBConnector): DB connection
        table: Table name
        column: Name of the column to find the index
    Returns:
        int: Index of the column
    """
    cmd = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{}'"\
          .format(table)
    db_execute(con, cmd)
    for i, cols in enumerate(con.cursor.fetchall()):
        cname = cols[3]
        if cname == column:
            return i


class DBConnector(object):
    n_instance = 0

    def __init__(self, dcfg, ldebug=print, lerror=print):
        self.conn = None
        self.cursor = None
        dbc = dcfg['db']
        dbcc = dbc['connect']
        self.driver = dbcc['driver']
        self.server = dbcc['server']
        port = dbcc['port']
        if port:
            self.server = '%s,%d' % (self.server, port)
        self.database = dbcc['database']
        self.trustcon = dbcc['trustcon']
        self.read_uncommit = dbcc['read_uncommit'] if 'read_uncommit' in dbcc\
            else True
        self.uid = dbcc['uid']
        self.passwd = dbcc['passwd']
        self.sys_schema = dbc['sys_schema']
        self.ldebug = ldebug
        self.lerror = lerror

    @property
    def txn_iso_level(self):
        cmd = """
SELECT CASE transaction_isolation_level
WHEN 0 THEN 'Unspecified'
WHEN 1 THEN 'ReadUncommitted'
WHEN 2 THEN 'ReadCommitted'
WHEN 3 THEN 'Repeatable'
WHEN 4 THEN 'Serializable'
WHEN 5 THEN 'Snapshot' END AS TRANSACTION_ISOLATION_LEVEL
FROM sys.dm_exec_sessions
WHERE Session_id = @@SPID"""
        self.cursor.execute(cmd)
        rv = self.cursor.fetchall()[0][0]
        return rv

    def __enter__(self):
        global pyodbc

        self.ldebug('DBConnector enter')
        DBConnector.n_instance += 1
        assert DBConnector.n_instance == 1 and "Recommend to have one DB "\
            "connection."

        acs = ''
        if self.trustcon:
            acs = 'Trusted_Connection=yes'
        elif self.uid is not None and self.passwd is not None:
            acs = 'UID=%s;PWD=%s' % (self.uid, self.passwd)
        cs = "DRIVER=%s;Server=%s;Database=%s;%s;" % (self.driver, self.server,
                                                      self.database, acs)
        st = time.time()
        try:
            conn = pyodbc.connect(cs)
        except pyodbc.Error as e:
            self.lerror(e[1])
            return
        else:
            self.ldebug("Connnected in {}".format(time.time() - st))
            self.conn = conn
            self.cursor = conn.cursor()
            self.cursor.execute("SET DATEFORMAT ymd")
            if self.read_uncommit:
                self.ldebug("set read uncommited")
                self.ldebug("old isolation option:"
                            " {}".format(self.txn_iso_level))
                cmd = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
                self.cursor.execute(cmd)
                self.ldebug("new isolation option: "
                            "{}".format(self.txn_iso_level))
        return self

    def __exit__(self, _type, value, tb):
        self.ldebug('db.Connector exit')
        DBConnector.n_instance -= 1

        if self.cursor is not None:
            self.ldebug('cursor.close()')
            self.cursor.close()
        if self.conn is not None:
            self.ldebug('conn.close()')
            self.conn.close()
