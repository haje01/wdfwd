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
        self.sent_cnt = 0

    def send_reset(self):
        if len(self.buf) > 0:
            self.parsed = self.buf
            self.sent_cnt += 1
            self.buf = {}

    def save(self, taken):
        self.buf.update(taken)


class FCS(CustomParser):
    def __init__(self):
        super(FCS, self).__init__()

        self.Token("time", r'\d{2}:\d{2}:\d{2}\.\d+')
        self.Token("errcode", r'E\d+')
        self.Token("code", r'\d+')
        self.Token("srcfile", r'\w+\.\w+')
        self.Token("srcline", r'\d+')
        self.Group("srcinfo", r'%{srcfile}:%{srcline}]?')
        self.Token("msg", r'.*')
        self.Token("key", r'\s*%(\w+)\s*')
        self.Token("value", r'\s*%([\w\.]*)\s*')
        self.keyvalue = self.KeyValue(r'^ %{key} : %{value}\n?')
        self.head = self.Group("head", r'%{errcode} %{time}  %{code} '
                               '%{srcinfo}\] %{msg}')
        self.method = self.Token("method", r'\s*\[%((\w+))\]')
        self.send_reset()

    def send_reset(self):
        super(FCS, self).send_reset()
        self.prefix = None

    def parse_line(self, line):
        if self.keyvalue.parse(line, self.prefix):
            self.save(self.keyvalue.taken)
            return True
        elif self.head.parse(line):
            self.send_reset()
            self.save(self.head.taken)
            return True
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
            self.save({'datetime': '{} {} {}'.format(date, time, tz)})
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

