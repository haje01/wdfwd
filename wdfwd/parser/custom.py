import os
import logging

import json

from wdfwd import parser as ps
from wdfwd.util import ravel_dict


def create_parser(cfg, encoding):
    if cfg == 'FCS':
        return FCS(encoding)
    elif cfg == 'Mocaa':
        return Mocaa(encoding)


class CustomParser(ps.Parser):
    def __init__(self, encoding):
        super(CustomParser, self).__init__(encoding)
        self.buf = {}

    def flush(self):
        if len(self.buf) > 0:
            self.parsed = self.buf
            self.completed += 1
            self.buf = {}

    def save(self, taken):
        self.buf.update(taken)

    def decode_line(self, line):
        if self.encoding:
            return line.decode(self.encoding)
        return line


class FCS(CustomParser):
    def __init__(self, encoding=None):
        logging.debug("init FCS cparser")
        super(FCS, self).__init__(encoding)

        self.Token("time", r'\d{2}:\d{2}:\d{2}\.\d+')
        self.Token("level", r'[IWEF]')
        self.Group("linfo", r'%{level}\d+')
        self.Token("trdid", r'\d+')
        self.Token("srcfile", r'\w+\.\w+')
        self.Token("srcline", r'\d+')
        self.Group("srcinfo", r'%{srcfile}:%{srcline}]?')
        self.Token("msg", r'.*')
        self.Token("key", r'\s*%(\w+)\s*')
        self.Token("value", r'\s*%([\w\.\-]*)\s*')
        self.Token("transaction_id", r'\[%(\d+)\]')
        self.Token("totalms", r'<total: %(\d+) msec>')
        self.keyvalue = self.KeyValue(r'^ %{key} : %{value}\n?')
        self.head = self.Group("head", r'%{linfo} %{time}\s+%{trdid} '
                               '%{srcinfo}\] %{msg}')
        self.thead = self.Group("thead", r'%{linfo} %{time}\s+%{trdid} '
                                '%{srcinfo}\] %{transaction_id} %{totalms}')
        self.method = self.Token("method", r'\s*\[%((\w+))\]')
        self.flush()

    def set_file_path(self, file_path):
        logging.debug("set_file_path {}".format(file_path))
        super(FCS, self).set_file_path(file_path)
        dt = os.path.basename(file_path).split('.')[3].split('-')[0]
        self.date = '{}-{}-{}'.format(dt[:4], dt[4:6], dt[6:8])

    def get_date(self):
        return self.date

    def flush(self):
        super(FCS, self).flush()
        self.prefix = None

    def handle_head(self, taken):
        self.flush()
        time = taken['time']
        dt = '{} {}'.format(self.date, time)
        del taken['time']
        taken['dt_'] = dt
        self.save(taken)
        return True

    def parse_line(self, line):
        line = self.decode_line(line)

        if self.keyvalue.parse(line, self.prefix, True):
            self.save(self.keyvalue.taken)
            return True
        elif self.thead.parse(line, True):
            return self.handle_head(self.thead.taken)
        elif self.head.parse(line, True):
            return self.handle_head(self.head.taken)
        elif self.method.parse(line, True):
            method = self.method.taken['method']
            if method.startswith('Request'):
                self.prefix = 'req'
                self.buf['type'] = method[7:]
            elif method.startswith('Response'):
                self.prefix = 'res'
            else:
                logging.error("Unknown method prefix: '{}'".format(method))
            return True

        return False


class Mocaa(CustomParser):
    def __init__(self, encoding=None):
        super(Mocaa, self).__init__(encoding)

        self.Token('date', r'\d{4}/\d{2}/\d{2}')
        self.Token('time', r'\d{2}:\d{2}:\d{2}')
        self.Token('tz', r'\(%(\+\d{2}\d{2})\)')
        self.Group('datetime', r'%{date} %{time} %{tz}')
        self.head = self.Group('head', r'==== %{datetime} ====')
        self.Token('ltype', r'\[%([^\]]+)\]')
        self.Token('guid', r'\[%(\w+-\w+-\w+-\w+-\w+)\]')
        self.Token('endpoint', r'.*')
        self.Token('elapsed', r'\[%(\d+) ms\]')
        self.reqhead = self.Group('reqhead', r'%{ltype}%{guid} %{endpoint}')
        self.reshead = self.Group('reshead', r'%{ltype}%{guid}%{elapsed} '
                                  '%{endpoint}')
        self.jsonline = self.Token('jsonline', r'{.*}')
        self.resbody = self.Token('resbody', r'\[Body\]')
        self.flush()

    def flush(self):
        super(Mocaa, self).flush()
        self.jsonbegin = False
        self.jsonbody = ''
        self.parsed = ravel_dict(self.parsed)

    def parse_line(self, line):
        line = self.decode_line(line)

        if self.head.parse(line, True):
            self.flush()
            taken = self.head.taken
            date = taken['date'].replace('/', '-')
            time = taken['time']
            tz = taken['tz']
            self.save({'dt_': '{} {} {}'.format(date, time, tz)})
            return True
        elif self.jsonline.parse(line, True):
            data = self.jsonline.taken['jsonline']
            jd = json.loads(data)
            self.save(jd)
            return True
        elif self.jsonbegin:
            self.jsonbody += line
            if line[0] == '}':
                jd = json.loads(self.jsonbody)
                self.save(jd)
                self.jsonbegin = False
                self.jsonbody = ''
            return True
        elif self.reqhead.parse(line, True):
            self.save(self.reqhead.taken)
            return True
        elif self.reshead.parse(line, True):
            self.save(self.reshead.taken)
            return True
        elif self.resbody.parse(line, True):
            self.jsonbegin = True
            return True
        return False
