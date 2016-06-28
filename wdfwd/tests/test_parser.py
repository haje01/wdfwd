import re

import pytest

from wdfwd import parser as ps
from wdfwd.parser import custom


class DummySender(object):
    def __init__(self):
        pass

    def send(self, data):
        pass


def test_parser_fcs():

    sender = DummySender()
    fcs = custom.FCS(sender)
    assert fcs.parse_line("E0324 09:26:51.754881  2708 fcs_client.cpp:225] connection closed : 997")
    assert len(fcs.data) == 6
    assert fcs.sent_cnt == 0

    assert fcs.parse_line("E0324 11:37:52.508764  3304 communicator.hpp:128] [8371] response sync")
    assert fcs.sent_cnt == 1

    fcs.parse_line("[RequestValidateAuthenticationKey]")
    assert fcs.data['type'] == 'ValidateAuthenticationKey'
    fcs.parse_line(" packet_length : 67")
    assert fcs.data["req.packet_length"] == '67'

    fcs.parse_line(" packet_type : 0x26")
    fcs.parse_line(" transaction_id : 8371")
    fcs.parse_line(" account_no : 1862710")
    fcs.parse_line(" authentication_key : D7665F56-29E2-4B80-BD8F-C5D37C3654CA")
    fcs.parse_line(" client_ip : 116.121.77.141")
    assert fcs.data["req.client_ip"] == '116.121.77.141'

    fcs.parse_line("[ResponseValidateAuthenticationKey]")
    fcs.parse_line(" packet_length : 44")
    assert fcs.data["res.packet_length"] == '44'
    fcs.parse_line(" packet_type : 0x26")
    fcs.parse_line(" transaction_id : 8371")
    fcs.parse_line(" result_code : 90213")
    fcs.parse_line(" condition_type : 0x64")
    fcs.parse_line(" user_no : 0")
    fcs.parse_line(" user_id : ")
    fcs.parse_line(" account_no : 0")
    fcs.parse_line(" account_id : ")
    fcs.parse_line(" account_type : 0x00")
    fcs.parse_line(" block_state : 0x00")
    fcs.parse_line(" pcbang_index : 0")
    fcs.parse_line(" phone_auth : ")
    fcs.parse_line(" is_phone_auth : 0")
    fcs.parse_line(" auth_ip : ")
    assert fcs.data["res.auth_ip"] == ''

    fcs.parse_line("E0324 11:39:31.027815  3316 communicator.hpp:128] [8481] response sync")
    assert len(fcs.data) == 6
    assert fcs.sent_cnt == 2


def test_parser_basic():
    psr = ps.Parser()

    with pytest.raises(ps.UnresolvedToken):
        psr._expand("%{unknown}")

    with pytest.raises(re.error):
        psr.Token("date", r'\d[')
    assert "%{date}" not in psr.objects

    with pytest.raises(ValueError):
        psr.Token("datetime", r'%{date}:%{time}')

    token = psr.Token("date", r'\d{4}/\d{2}/\d{2}')
    assert token.name == 'date'
    assert token.key == '%{date}'
    assert "%{date}" in psr.objects
    assert token.regex == r'(?P<date>\d{4}/\d{2}/\d{2})'

    with pytest.raises(ValueError):
        psr.Token("date", r'\d{2}/\d{2}/\d{2}')

    psr.Token("time", r'\d{2}:\d{2}:\d{2}')
    psr.Token("millis", r'\.\d+')
    group = psr.Group("timem", r'%{time}%{millis}')
    assert group.regex == r'(?P<time>\d{2}:\d{2}:\d{2})(?P<millis>\.\d+)'
    psr.Group("datetime", r'%{date} %{timem}')
    group = psr.Group("dt_utc", r'%{datetime} \+0000')
    group.parse("2016/06/10 12:35:02.312 +0000")
    taken = group.taken
    assert len(taken) == 3
    assert 'date' in taken and 'time' in taken and 'millis' in taken

    psr.Token("errcode", r'E\d+')
    psr.Token("code", r'\d+')
    psr.Token("srcfile", r'\w+\.\w+')
    psr.Token("srcline", r'\d+')
    psr.Group("srcinfo", r'%{srcfile}:%{srcline}')

    psr.Token("msg", r'.*')
    token = psr.Token("brnum", r'\[%(\d+)\]')
    token.regex == r'\[(?P<brnum>\d+)]'
    assert token.parse("[61207]")
    assert token.taken['brnum'] == '61207'
    psr.Token("brname", r'\[(\w+)\]')

    token = psr.Token("key", r'\s*%(\w+)\s*')
    assert token.parse("  keyname  ")
    assert token.taken['key'] == 'keyname'

    psr.Token("value", r'\s*%([\w\.]+)\s*')
    kv = psr.KeyValue("%{key}=%{value},?")
    assert kv.parse("aaa=1,bbb=2,ccc=3")
    assert kv.taken['aaa'] == '1'
    assert kv.taken['bbb'] == '2'
    assert kv.taken['ccc'] == '3'


SAMPLE_API = """2016-04-21 20:42:29.331	GetAccountInformation	1	606f9d8c-9491-4fdc-9476-f701e84863bf	272	{
  "AccountIdentifierNo": "9999998946",
  "ServiceCode": "SVR001",
  "ClientIp": "1.1.1.1",
  "CountryCode": "KR",
  "Path": 6,
  "TraceId": "606f9d8c-9491-4fdc-9476-f701e84863bf"
}	{
  "AccountInfo": {
    "AccountNo": 807265,
    "AccountIdentifierNo": null,
    "AccountId": "**0120011",
    "AccountStatus": "",
    "SignUpDatetime": "0001-01-01T00:00:00",
    "UserNo": 36973,
    "DuplicationInformation": "**\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
    "ConnectingInformation": "**\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
    "Birthday": "1987-01-01T00:00:00",
    "Gender": "**",
    "UserName": "** must be at least 1 byte",
    "Email": "",
    "MobilePhone": "",
    "NickName": "**0120011",
    "Phone": ""
  },
  "Return": true,
  "ReturnCode": 1,
  "TraceId": "606f9d8c-9491-4fdc-9476-f701e84863bf"
}"""


def test_parser_create():
    cfg = """
parser:
    tokens:
        date: '\d{4}-\d{2}-\d{2}'
        time: '\d{2}:\d{2}:\d{2}'
        level: 'DEBUG|INFO|ERROR'
        src_file: '\w+\.\w+'
        src_line: '\d+'
        msg: '.*'
    groups:
        datetime: '%{date} %{time}'
        src_info: '%{src_file}:%{src_line}'
    formats:
        - '%{datetime} %{level} %{src_info} %{msg}'
        - '%{datetime} %{level} %{msg}'
    """
    import yaml
    from StringIO import StringIO
    cfg = yaml.load(StringIO(cfg))
    psr = ps.create_parser(cfg['parser'])
    assert psr.parse_line('2016-06-28 12:33:21 DEBUG foo.py:37 Init success')
    assert psr.data['date'] == '2016-06-28'
    assert psr.data['level'] == 'DEBUG'
    assert psr.data['src_line'] == '37'

    assert psr.parse_line('2016-06-29 17:50:11 ERROR Critical error!')
    assert psr.data['level'] == 'ERROR'
    assert psr.data['msg'] == 'Critical error!'

    assert not psr.parse_line('2016-06-29 ERROR')


def test_parser_create_custom():
    cfg = """
parser:
    custom: FCS
    """
    import yaml
    from StringIO import StringIO
    cfg = yaml.load(StringIO(cfg))
    sender = DummySender()
    psr = ps.create_parser(cfg['parser'], sender)
    assert isinstance(psr, custom.FCS)
