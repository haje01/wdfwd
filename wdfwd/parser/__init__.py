import re

from wdfwd.util import ldebug


tk_ptrn = re.compile(r'(%{[^}]+})')
tkph_ptrn = re.compile(r'%\(([^)]+)\)')


def _escape_name(name):
    return "%{{{}}}".format(name)


class UnresolvedToken(Exception):
    pass


class CtxChild(object):
    def __init__(self, psr):
        self.psr = psr


class CtxNamed(CtxChild):
    def __init__(self, psr, name):
        super(CtxNamed, self).__init__(psr)
        self.name = name
        key = _escape_name(name)
        self.key = key
        if key in psr.objects:
            raise ValueError("Object '{}' is already exist".format(name))
        psr.objects[key] = self


class RegexObj(CtxNamed):
    def __init__(self, psr, name, regex):
        self.ptrn = re.compile(regex)
        super(RegexObj, self).__init__(psr, name)
        self.regex = regex


class Token(RegexObj):
    def __init__(self, psr, name, regex):
        self._regex = regex
        # resolve refs
        if '%{' in regex:
            tkns = tk_ptrn.findall(regex)
            for tkn in tkns:
                rtkn = psr.objects[tkn]
                rtkn_rx = rtkn._regex
                match = tkph_ptrn.search(rtkn_rx)
                if match:
                    s, e = match.span()
                    rtkn_rx = rtkn_rx[:s] + match.groups()[0] + rtkn_rx[e:]
                regex = regex.replace(tkn, rtkn_rx)

        if '%(' not in regex:
            regex = r'(?P<{}>{})'.format(name, regex)
        else:
            regex = self.resolv_plchld(regex, name)
        super(Token, self).__init__(psr, name, regex)

    def resolv_plchld(self, regex, name):
        match = tkph_ptrn.search(regex)
        if match:
            b, e = match.span()
            ms = match.groups()[0]
            rx = r'(?P<{}>{})'.format(name, ms)
            regex = regex[:b] + rx + regex[e:]
        return regex

    def parse(self, msg):
        match = self.ptrn.match(msg)
        if match:
            self.taken = match.groupdict()
            return True
        else:
            self.taken = None
            return False


class Group(RegexObj):
    def __init__(self, psr, name, regex):
        regex = psr._expand(regex)
        super(Group, self).__init__(psr, name, regex)

    def parse(self, msg):
        match = self.ptrn.match(msg)
        if match:
            self.taken = match.groupdict()
            return True
        else:
            self.taken = None
            return False


class KeyValue(object):
    def __init__(self, psr, regex):
        self.psr = psr
        regex = psr._expand(regex)
        self.ptrn = re.compile(regex)
        self.regex = regex

    def parse(self, msg, prefix=None):
        rd = self.ptrn.findall(msg)
        if rd:
            if not prefix:
                self.taken = dict(rd)
            else:
                self.taken = {'{}-{}'.format(prefix, k): v for k, v in
                              dict(rd).iteritems()}
            return True
        else:
            self.taken = None
            return False


class Format(object):
    def __init__(self, psr, regex):
        self.psr = psr
        regex = psr._expand(regex)
        self.ptrn = re.compile(regex)
        self.regex = regex
        psr.formats.append(self)

    def parse(self, msg):
        match = self.ptrn.match(msg)
        if match:
            self.taken = match.groupdict()
            return True
        else:
            self.taken = None
            return False


class Parser(object):
    def __init__(self):
        self.objects = {}
        self.formats = []
        self.parsed = {}
        self.completed = 0

    def Token(self, name, regex):
        token = Token(self, name, regex)
        return token

    def Group(self, name, regex):
        return Group(self, name, regex)

    def KeyValue(self, regex):
        return KeyValue(self, regex)

    def Format(self, regex):
        return Format(self, regex)

    def _expand(self, regex):
        while True:
            changed = 0
            for ename, elm in self.objects.iteritems():
                if ename in regex:
                    regex = regex.replace(ename, elm.regex)
                    changed += 1
            if changed == 0:
                break
        if '%{' in regex:
            raise UnresolvedToken("Unresolved token remains - '{}'", regex)
        return regex

    def parse_line(self, line):
        for fmt in self.formats:
            if fmt.parse(line):
                self.parsed = fmt.taken
                self.completed += 1
                return True

        self.parsed = None
        return False


def create_parser(cfg):
    ldebug("create_parser {}".format(cfg))
    if 'custom' in cfg:
        ldebug("custom parser '{}'".format(cfg['custom']))
        from wdfwd.parser import custom
        return custom.create_parser(cfg['custom'])

    ps = Parser()
    tokens = cfg['tokens']
    unresolved = True
    while unresolved:
        unresolved = False
        for k, v in tokens.iteritems():
            try:
                ps.Token(k, v)
            except KeyError:
                unresolved = True
            except ValueError, e:
                if 'is already exist' not in str(e):
                    raise

    if 'groups' in cfg:
        groups = cfg['groups']
        for k, v in groups.iteritems():
            ps.Group(k, v)

    formats = cfg['formats']
    for fmt in formats:
        ps.Format(fmt)

    return ps


def merge_parser_cfg(gparser_cfg, lparser_cfg):
    """
    merge global parser cfg into local parser cfg
    """
    if 'tokens' in gparser_cfg:
        for k, v in gparser_cfg['tokens'].iteritems():
            if k not in lparser_cfg:
                if 'tokens' not in lparser_cfg:
                    lparser_cfg['tokens'] = {}
                lparser_cfg['tokens'][k] = v

    if 'groups' in gparser_cfg:
        for k, v in gparser_cfg['groups'].iteritems():
            if k not in lparser_cfg:
                if 'groups' not in lparser_cfg:
                    lparser_cfg['groups'] = {}
                lparser_cfg['groups'][k] = v
    return lparser_cfg
