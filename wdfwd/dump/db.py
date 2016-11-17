import os
import re
from datetime import datetime

import yaml
import pyodbc  # NOQA

from wdfwd.util import get_dump_fname, normalize_date_str, ldebug, linfo,\
    lwarning, lerror
from wdfwd.get_config import get_config
from wdfwd.const import TABLE_INFO_FILE

pyodbc.pooling = False

conv_map = {}
str_op = str
cfg = get_config()


def enc_op(x):
    if x is not None:
        return x.encode('utf8')
    return ''


def _write_table_header(f, con, delim, tbinfo):
    f.write(delim.join(tbinfo.columns))
    f.write('\n')


def get_table_rowcnt(con, tbname):
    ldebug('get_table_rowcnt ' + tbname)
    if not con.sys_schema:
        tbname = tbname.split('.')[-1]
    ldebug('get_table_rowcnt')
    cmd = '''
select i.rows
from sysindexes i
join sysobjects o on o.id = i.id
where i.indid < 2
and o.name <> 'sysdiagrams'
and o.xtype = 'U'
and o.name = '%s'
''' % tbname
    execute(con, cmd)
    rv = con.cursor.fetchone()
    return rv[0] if rv is not None else 0


class TableInfo(object):

    def __init__(self, info):
        self.columns = None
        self.types = None
        self.str_cols = '*'
        if type(info) == dict:
            self.name = info['name']
            self.icols = info.get('icols', None)
            self.ecols = info.get('ecols', None)
        else:
            self.name = info
            self.icols = self.ecols = None

    def __str__(self):
        return self.name

    def __eq__(self, other):
        if isinstance(other, TableInfo):
            return self.name == other.name
        elif type(other) == str or type(other) == unicode:
            return self.name == other
        else:
            return False

    def __add__(self, other):
        t = type(other)
        if t != str and t != unicode:
            raise NotImplementedError
        else:
            import copy
            c = copy.deepcopy(self)
            c.name += other
            return c

    def __radd__(self, other):
        t = type(other)
        if t != str and t != unicode:
            raise NotImplementedError
        else:
            import copy
            c = copy.deepcopy(self)
            c.name = other + c.name
            return c

    def replace(self, *args, **kwargs):
        return self.name.replace(*args, **kwargs)

    def split(self, *args, **kwargs):
        return self.name.split(*args, **kwargs)

    def build_columns(self, con):
        """Returns columns from the table."""
        if not con.sys_schema:
            tbname = self.name.split('.')[-1]
        ldebug('build_columns {}'.format(tbname))
        cols = []
        typs = []
        cmd = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{}'"\
            .format(tbname)
        execute(con, cmd)
        totalcol = 0
        while True:
            row = con.cursor.fetchone()
            if not row:
                break
            totalcol += 1
            col = row[3].encode('utf8')
            typ = row[7]
            if self.icols is not None and col not in self.icols:
                continue
            elif self.ecols is not None and col in self.ecols:
                continue
            cols.append(col)
            typs.append(typ)
            # ldebug("col: {} [{}]".format(col, typ))
        if len(cols) < totalcol:
            self.str_cols = ', '.join(cols)
        self.columns = cols
        self.types = typs


class TemporaryRemoveFirstRow(object):

    """temporary remove first row from a table."""

    def __init__(self, con, tbname):
        self.con = con
        self.tbname = tbname

    def __enter__(self):
        cmd = "BEGIN TRANSACTION"
        execute(self.con, cmd)
        cmd = "DELETE TOP(1) FROM %s" % self.tbname
        execute(self.con, cmd)

    def __exit__(self, _type, value, tb):
        cmd = "ROLLBACK"
        execute(self.con, cmd)


class Connector(object):

    def __init__(self, dcfg):
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
        self.fetchsize = dbc['fetchsize']
        if 'date_pattern' in dbc['table']:
            self.table_date_ptrn = re.compile(dbc['table']['date_pattern'])
            self.table_date_fmt = dbc['table']['date_format']
            self.no_daily_table = False
        else:
            self.date_column = dbc['table']['date_column']
            self.no_daily_table = True
        self.skip_last = dbc['table'].get('skip_last', True)
        self.sys_schema = dbc['sys_schema']
        self.table_names = [TableInfo(tn) for tn in dbc['table']['names']]

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
        ldebug('db.Connector enter')
        acs = ''
        if self.trustcon:
            acs = 'Trusted_Connection=yes'
        elif self.uid is not None and self.passwd is not None:
            acs = 'UID=%s;PWD=%s' % (self.uid, self.passwd)
        cs = "DRIVER=%s;Server=%s;Database=%s;%s;" % (self.driver, self.server,
                                                      self.database, acs)
        try:
            conn = pyodbc.connect(cs)
        except pyodbc.Error as e:
            lerror(e[1])
            return
        else:
            self.conn = conn
            self.cursor = conn.cursor()
            self.cursor.execute("SET DATEFORMAT ymd")
            if self.read_uncommit:
                ldebug("set read uncommited")
                ldebug("  old isolation option: {}".format(self.txn_iso_level))
                cmd = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
                self.cursor.execute(cmd)
                ldebug("  new isolation option: {}".format(self.txn_iso_level))
        return self

    def __exit__(self, _type, value, tb):
        ldebug('db.Connector exit')
        if self.cursor is not None:
            ldebug('cursor.close()')
            self.cursor.close()
        if self.conn is not None:
            ldebug('conn.close()')
            self.conn.close()


def execute(con, cmd):
    try:
        con.cursor.execute(cmd)
    except pyodbc.ProgrammingError as e:
        lerror(str(e[1]))
        return False
    return True


def table_array(con, prefix):
    """Return table name array by matching prefix."""
    # wild schema for table select
    if not con.sys_schema:
        prefix = '%' + prefix.split('.')[-1]
    ldebug('table_array')
    cmd = "SELECT NAME FROM SYS.TABLES WHERE NAME"\
          " LIKE '%s%%'" % prefix
    execute(con, cmd)
    ldebug('cmd: ' + cmd)
    rows = con.cursor.fetchall()
    ldebug('rowcnt: {}'.format(len(rows)))
    res = []
    if rows is not None:
        res = sorted([row[0] for row in rows])
    return res


def table_rows(con, tbinfo, date=None, max_fetch=None):
    """Returns all rows from the table."""
    ldebug('table_rows')
    if date is None:
        cmd = "SELECT {} FROM {}".format(tbinfo.str_cols, tbinfo)
    else:
        cmd = "SELECT {} FROM {} WHERE CAST(LogTime AS DATE)"\
            " = CAST('{}' as DATETIME);".format(tbinfo.str_cols, tbinfo, date)
    ldebug('cmd: ' + cmd)
    execute(con, cmd)
    fetch_cnt = 0
    while True:
        if max_fetch is not None and fetch_cnt >= max_fetch:
            break
        rows = con.cursor.fetchmany(con.fetchsize)
        fetch_cnt += 1
        if not rows:
            break
        yield rows


def _row_as_strings(row, tbinfo):
    global conv_map
    conv = conv_map[tbinfo]
    return [f(a) for f, a in zip(conv, row)]


def _warm_converter(con, decode_map, tbinfo):
    global conv_map
    if tbinfo not in conv_map:
        funcs = []
        for typ in tbinfo.types:
            if typ[:5] in ['nvarc', 'nchar', 'ntext']:
                op = enc_op
            else:
                op = decode_map[typ] if typ in decode_map else str_op
            funcs.append(op)
        conv_map[tbinfo] = funcs


def collect_dates(con, skip_last_subtable=None):
    ldebug('collect_dates')
    if skip_last_subtable is None:
        skip_last_subtable = con.skip_last
    tables = tables_by_names(con, skip_last_subtable)
    return _collect_dates(con, tables)


def _collect_dates(con, tables):
    dates = set()
    for table in tables:
        date = get_table_date(con, table)
        if date is not None:
            dates.add(date)
    if len(dates) == 0:
        lwarning("No dates from tables: " + str(tables))
    return sorted(dates)


def calc_table_pastday(con, fname):
    fdate = get_table_date(con, fname)
    if 'force_today' in cfg['test']:
        d = cfg['test']['force_today']
        today = datetime.strptime(d, con.table_date_fmt)
    else:
        today = datetime.today()
    return (today - fdate).days


def get_table_date(con, tbname):
    """Return date from table name."""
    m = con.table_date_ptrn.search(tbname)
    if m is not None:
        d = m.groups()[0]
        return datetime.strptime(d, con.table_date_fmt)


def _dump_table(dcfg, decode_map, con, tbinfo, date, max_fetch):
    """Dump (sub)tables to files and returns table name."""
    linfo("dump subtable: {}".format(tbinfo))
    folder = dcfg['folder']
    path = os.path.join(folder, get_dump_fname(tbinfo, date))

    tbinfo.build_columns(con)
    _warm_converter(con, decode_map, tbinfo)
    delim = dcfg['field_delimiter'].encode('utf8')
    with open(path, 'w') as f:
        if date is None:
            cnt = get_table_rowcnt(con, tbinfo)
        else:
            cnt = table_rowcnt_by_date(con, tbinfo, date)
        if cnt > 0:
            _write_table_header(f, con, delim, tbinfo)
            for rows in table_rows(con, tbinfo, date, max_fetch):
                for i, row in enumerate(rows):
                    try:
                        cr = _row_as_strings(row, tbinfo)
                    except UnicodeDecodeError:
                        lerror("UnicodeDecodeError for {} row "
                               "{}".format(tbinfo, i))
                        global conv_map
                        lerror(str(conv_map[tbinfo]))
                    else:
                        f.write(delim.join(cr))
                        f.write('\n')
    if date is None:
        return str(tbinfo)
    else:
        return "{}_{}".format(tbinfo, normalize_date_str(date))


def dump_table_rows_by_date(dcfg, con, tbname, date, max_fetch=None):
    linfo("dump_table_rows_by_date {} {}".format(tbname, date))
    decode_map = _make_decode_map(dcfg)
    dumped = _dump_table(dcfg, decode_map, con, tbname, date, max_fetch)
    return dumped, date


def dump_tables(dcfg, con, tables, max_fetch=None):
    linfo("dump_tables")
    dumped_tables = []
    decode_map = _make_decode_map(dcfg)
    for tbinfo in tables:
        linfo("dump table: {}".format(tbinfo))
        dumped = _dump_table(dcfg, decode_map, con, tbinfo, None, max_fetch)
        if dumped is not None:
            dumped_tables.append(dumped)
    return dumped_tables


def _make_decode_map(dcfg):
    decode_map = {}
    if 'db' in dcfg and 'type_encodings' in dcfg['db']:
        tencs = dcfg['db']['type_encodings']
        if tencs is not None:
            for te in tencs:
                typ = te['type']
                if 'encoding' in te:
                    enc = te['encoding']
                    decode_map[typ] = lambda x: x.decode(enc).encode('utf8')  # NOQA
                elif 'func' in te:
                    func = eval(te['func'])
                    decode_map[typ] = func
    return decode_map


def _escape_underscore(names):
    return [name.replace('_', '[_]') for name in names]


def tables_by_names(con, skip_last_subtable=None):
    ldebug('tables_by_names')
    if skip_last_subtable is None:
        skip_last_subtable = con.skip_last
    tables = []
    tbnames = _escape_underscore(con.table_names)
    counts = {}
    for tbname in tbnames:
        subtables = table_array(con, tbname)
        # ldebug('subtables: ' + str(subtables))
        counts[tbname] = len(subtables)
        if skip_last_subtable and len(subtables) > 1:
            # skip last subtable which might be using now.
            ldebug('skip last table')
            subtables = subtables[:-1]
        tables += subtables
    if len(set(counts.values())) > 1:
        lwarning("Sub-table length mismatch! " + str(counts))
    return tables


def read_table_info(dcfg):
    folder = dcfg['folder']
    rpath = os.path.join(folder, TABLE_INFO_FILE)
    if os.path.isfile(rpath):
        with open(rpath, 'r') as f:
            prev = yaml.load(f.read())
            return prev, rpath
    return None, rpath


def daily_tables_by_change(dcfg, con, skip_last_subtable=None):
    ldebug("daily_tables_by_change")
    if skip_last_subtable is None:
        skip_last_subtable = con.skip_last
    dates = collect_dates(con, skip_last_subtable)
    # ldebug("dates: " + str(dates))
    daily_tables = daily_tables_from_dates(con, dates)
    # ldebug("daily_tables: " + str(daily_tables))
    res, rpath = read_table_info(dcfg)
    if res is None:
        return daily_tables
    changed_daily_tables = []
    for tables in daily_tables:
        tmp = []
        for table in tables:
            oldcnt = res.get(str(table), -1)
            curcnt = get_table_rowcnt(con, table)
            ldebug("check row cnt for '{}' {} - {}".format(table, oldcnt,
                                                           curcnt))
            if oldcnt != curcnt:
                ldebug('append')
                tmp.append(table)
        if tmp:
            changed_daily_tables.append(tmp)
    return changed_daily_tables


def daily_tables_from_dates(con, dates):
    tables = []
    tbnames = con.table_names
    for date in dates:
        daily_tables = []
        for tbname in tbnames:
            daily_tables.append(tbname + datetime.strftime(date,
                                                           con.table_date_fmt))
        tables.append(daily_tables)
    return tables


def write_table_info(dcfg, dumped_tables):
    linfo('write_table_info')
    prev, rpath = read_table_info(dcfg)
    result = {}
    if prev is not None:
        result.update(prev)

    with Connector(dcfg) as con:
        for dinfo in dumped_tables:
            if type(dinfo) == tuple:
                table, date = dinfo
                date_part = '_' + normalize_date_str(date)
                dtable = table.replace(date_part, '')
                cnt = table_rowcnt_by_date(con, dtable, date)
            else:
                table = dinfo
                cnt = get_table_rowcnt(con, table)
            result[str(table)] = cnt

    linfo('writing {}'.format(rpath))
    with open(rpath, 'w') as f:
        f.write(yaml.dump(result, default_flow_style=False))
    return rpath


def table_rowcnt_by_date(con, tbname, date):
    ldebug("table_rowcnt_by_date {} of {}".format(tbname, date))
    cmd = "SELECT COUNT(*) FROM {} WHERE CAST(LogTime as DATE)"\
        " = CAST('{}' as DATETIME);".format(tbname, date)
    execute(con, cmd)
    rows = con.cursor.fetchall()
    return int(rows[0][0])


def get_data_dates(con, skip_last_date=None):
    if skip_last_date is None:
        skip_last_date = con.skip_last

    tbnames = con.table_names
    all_dates = set()
    for tbname in tbnames:
        cmd = "SELECT DISTINCT(CAST({} AS DATE)) FROM {}".\
            format(con.date_column, tbname)
        execute(con, cmd)
        rows = con.cursor.fetchall()
        dates = set([row[0] for row in rows])
        all_dates |= dates

    all_dates = sorted(all_dates)
    if skip_last_date:
        all_dates = all_dates[:-1]

    return all_dates


def updated_day_tables(dcfg, con, date):
    ldebug("updated_day_tables {}".format(date))
    res, rpath = read_table_info(dcfg)
    tbnames = con.table_names
    for tbname in tbnames:
        curcnt = table_rowcnt_by_date(con, tbname, date)
        dtable = "{}_{}".format(str(tbname), normalize_date_str(date))
        changed = res is None or curcnt != res.get(dtable, -1)
        if changed:
            yield tbname
