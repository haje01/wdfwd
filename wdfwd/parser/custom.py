import os
import logging

import json

from wdfwd import parser as ps


def create_parser(cfg):
    if cfg == 'FCS':
        return FCS()


class CustomParser(ps.Parser):
    def __init__(self):
        super(CustomParser, self).__init__()
        self.buf = {}
        self.file_path = None

    def send_reset(self):
        if len(self.buf) > 0:
            self.parsed = self.buf
            self.completed += 1
            self.buf = {}

    def save(self, taken):
        self.buf.update(taken)

    def set_file_path(self, file_path):
        self.file_path = file_path


class FCS(CustomParser):
    def __init__(self):
        logging.debug("init FCS cparser")
        super(FCS, self).__init__()

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
        self.send_reset()

    def set_file_path(self, file_path):
        logging.debug("set_file_path {}".format(file_path))
        super(FCS, self).set_file_path(file_path)
        dt = os.path.basename(file_path).split('.')[3].split('-')[0]
        self.date = '{}-{}-{}'.format(dt[:4], dt[4:6], dt[6:8])

    def get_date(self):
        return self.date

    def send_reset(self):
        super(FCS, self).send_reset()
        self.prefix = None

    def handle_head(self, taken):
        self.send_reset()
        time = taken['time']
        dt = '{} {}'.format(self.date, time)
        del taken['time']
        taken['dt_'] = dt
        self.save(taken)
        return True

    def parse_line(self, line):
        if self.keyvalue.parse(line, self.prefix):
            self.save(self.keyvalue.taken)
            return True
        elif self.thead.parse(line):
            return self.handle_head(self.thead.taken)
        elif self.head.parse(line):
            return self.handle_head(self.head.taken)
        elif self.method.parse(line):
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


class Mocca(CustomParser):
    def __init__(self):
        super(Mocca, self).__init__()

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
        self.reshead = self.Group('reshead', r'%{ltype}%{guid}%{elapsed} %{endpoint}')
        self.jsonline = self.Token('jsonline', r'{.*}')
        self.resbody = self.Token('resbody', r'\[Body\]')
        self.jsonbegin = False
        self.jsonbody = ''
        self.send_reset()

    def send_reset(self):
        super(Mocca, self).send_reset()

    def parse_line(self, line):
        if self.jsonbegin:
            self.jsonbody += line
            if line == '}':
                jd = json.loads(self.jsonbody)
                self.save(jd)
                self.jsonbegin = False
                self.jsonbody = ''
            return True
        elif self.head.parse(line):
            self.send_reset()

            taken = self.head.taken
            date = taken['date'].replace('/', '-')
            time = taken['time']
            tz = taken['tz']
            self.save({'dt_': '{} {} {}'.format(date, time, tz)})
            return True
        elif self.reqhead.parse(line):
            self.save(self.reqhead.taken)
            return True
        elif self.jsonline.parse(line):
            data = self.jsonline.taken['jsonline']
            jd = json.loads(data)
            self.save(jd)
            return True
        elif self.reshead.parse(line):
            self.save(self.reshead.taken)
            return True
        elif self.resbody.parse(line):
            self.jsonbegin = True
            return True
        return False
