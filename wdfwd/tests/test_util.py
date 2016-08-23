from StringIO import StringIO

import yaml


def test_util_ravel_dict():
    from wdfwd.util import ravel_dict
    data = dict(
        a=dict(
            b=0
        ),
        c=[1, 2, 3]
    )
    ret = ravel_dict(data)
    assert ret['a_b'] == 0
    assert ret['c'] == [1, 2, 3]
    assert 'a' not in ret


def test_util_tail_info():
    from wdfwd.util import iter_tail_info
    from wdfwd.tail import FluentCfg

    # no global & local parser
    cfg="""
tailing:
    pos_dir: D:\\UTIL\\temp
    from:
        - file:
            parser:
                tokens:
                    test: '.+'
    to:
        fluent: [111.22.33.222, 24224]
    """
    cfg = yaml.load(StringIO(cfg))
    tailc = cfg['tailing']
    tinfos = list(iter_tail_info(tailc))
    assert '%{test}' in tinfos[0].parser.objects.keys()

    # global parser & no local
    cfg="""
tailing:
    pos_dir: D:\\UTIL\\temp
    parser:
        tokens:
            test: '.*'
    from:
        - file:
    to:
        fluent: [111.22.33.222, 24224]
    """
    cfg = yaml.load(StringIO(cfg))
    tailc = cfg['tailing']
    tinfos = list(iter_tail_info(tailc))
    assert '%{test}' in tinfos[0].parser.objects.keys()

    # global format & no local
    cfg="""
tailing:
    pos_dir: D:\\UTIL\\temp
    format: '(?P<dt_>\d{8})'
    from:
        - file:
    to:
        fluent: [111.22.33.222, 24224]
    """
    cfg = yaml.load(StringIO(cfg))
    tailc = cfg['tailing']
    tinfos = list(iter_tail_info(tailc))
    assert tinfos[0].parser is None
    assert tinfos[0].format == '(?P<dt_>\d{8})'

    # global parser & local parser
    cfg="""
tailing:
    pos_dir: D:\\UTIL\\temp
    parser:
        tokens:
            test: '.*'
            foo: '\d'
    from:
        - file:
            parser:
                tokens:
                    test: '.+'
    to:
        fluent: [111.22.33.222, 24224]
    """
    cfg = yaml.load(StringIO(cfg))
    tailc = cfg['tailing']
    tinfos = list(iter_tail_info(tailc))
    assert '%{test}' in tinfos[0].parser.objects.keys()
    assert '(?P<test>.+)' == tinfos[0].parser.objects['%{test}'].regex
    assert '%{foo}' in tinfos[0].parser.objects.keys()
    assert '(?P<foo>\d)' == tinfos[0].parser.objects['%{foo}'].regex

    # global format & local format
    cfg="""
tailing:
    pos_dir: D:\\UTIL\\temp
    format: '(?P<dt_>\d{8})'
    from:
        - file:
            format: '(?P<dt_>\d{10})'
    to:
        fluent: [111.22.33.222, 24224]
    """
    cfg = yaml.load(StringIO(cfg))
    tailc = cfg['tailing']
    tinfos = list(iter_tail_info(tailc))
    assert tinfos[0].format == '(?P<dt_>\d{10})'

    # global format & local parser
    cfg="""
tailing:
    pos_dir: D:\\UTIL\\temp
    format: '(?P<dt_>\d{8})'
    from:
        - file:
            parser:
                tokens:
                    test: '.+'
    to:
        fluent: [111.22.33.222, 24224]
    """
    cfg = yaml.load(StringIO(cfg))
    tailc = cfg['tailing']
    tinfos = list(iter_tail_info(tailc))
    assert tinfos[0].format is None
    assert '%{test}' in tinfos[0].parser.objects.keys()
    assert '(?P<test>.+)' == tinfos[0].parser.objects['%{test}'].regex

    # global parser & local format
    cfg="""
tailing:
    pos_dir: D:\\UTIL\\temp
    parser:
        tokens:
            test: '.*'
    from:
        - file:
            format: '(?P<test>.*)'
    to:
        fluent: [111.22.33.222, 24224]
    """
    cfg = yaml.load(StringIO(cfg))
    tailc = cfg['tailing']
    tinfos = list(iter_tail_info(tailc))
    assert tinfos[0].parser is None
    assert tinfos[0].format is not None

    cfg="""
tailing:
    max_between_data: 1000000
    file_encoding: cp949
    pos_dir: D:\\UTIL\\temp

    parser:
        tokens:
            date: '\d{4}-\d{2}-\d{2}'
            time: '\d{2}:\d{2}:\d{2}.\d{4}'
            dt_: '%{date} %{time}'
            path: '\S+'
            state: '\d+'
            server_ip: '\d+\.\d+\.\d+\.\d+'
            client_ip: '\d+\.\d+\.\d+\.\d+'
            time_taken: '\d+'
            request_data: ['{.*}', 'ravel(json(_))']
            exception: '.*'
        groups:
            debug: '%{dt_},%{path}\s?,%{state},%{server_ip},%{client_ip},%{time_taken},%{request_data}'
            errfatal: '%{dt_},%{path}\s?,%{state},%{server_ip},%{client_ip},%{request_data}?,%{exception}'
            info: '%{dt_},%{path}\s?,%{state},%{server_ip},%{client_ip},%{time_taken}'

    from:
        - file:
            dir: D:\\Web_Log\\Bill.MyPC
            tag: front.bill.mypc.debug
            pattern: Debug*.log
            latest: Debug.log
            parser:
                formats:
                    - '%{debug}'

        - file:
            dir: C:\\inetpub\\logs\\LogFiles\W3SVC4
            pattern: u_ex*.log
            tag: iis.front.alpha-pcsh
            format: '(?P<dt_>\S+ \S+) (?P<s_ip>\S+) (?P<cs_method>\S+) (?P<cs_uri_stem>\S+) (?P<cs_uri_query>\S+) (?P<s_port>\S+) (?P<cs_username>\S+) (?P<c_ip>\S+) (?P<cs_useragent>\S+)'
    to:
        fluent: [111.22.33.222, 24224]
    """
    cfg = yaml.load(StringIO(cfg))
    tailc = cfg['tailing']
    tinfos = list(iter_tail_info(tailc))
    assert len(tinfos) == 2
    i1, i2 = tinfos

    assert i1.bdir == 'D:\\Web_Log\\Bill.MyPC'
    assert i1.ptrn == 'Debug*.log'
    assert i1.tag == 'front.bill.mypc.debug'
    assert i1.pos_dir == 'D:\\UTIL\\temp'
    assert i1.scfg == FluentCfg(host='111.22.33.222', port=24224)
    assert i1.send_term == 1
    assert i1.update_term == 5
    assert i1.latest == 'Debug.log'
    assert i1.file_enc == 'cp949'
    assert i1.lines_on_start is None
    assert i1.max_between_data == 1000000
    assert i1.format is None
    assert i1.parser is not None

    assert i2.bdir == 'C:\\inetpub\\logs\\LogFiles\\W3SVC4'
    assert i2.ptrn == 'u_ex*.log'
    assert i2.tag == 'iis.front.alpha-pcsh'
    assert i2.pos_dir == 'D:\\UTIL\\temp'
    assert i2.scfg == FluentCfg(host='111.22.33.222', port=24224)
    assert i2.send_term == 1
    assert i2.update_term == 5
    assert i2.latest == None
    assert i2.file_enc == 'cp949'
    assert i2.lines_on_start == None
    assert i2.max_between_data == 1000000
    assert i2.format is not None
    assert i2.parser is None

