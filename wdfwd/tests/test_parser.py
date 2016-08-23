import re
from StringIO import StringIO

import yaml
import pytest

from wdfwd import parser as ps
from wdfwd.parser import custom


def test_parser_basic():
    psr = ps.Parser()
    psr.set_file_path("dummy_path")

    with pytest.raises(ps.UnresolvedToken):
        psr._expand("%{unknown}")

    with pytest.raises(re.error):
        psr.Token("date", r'\d[')
    assert "%{date}" not in psr.objects

    token = psr.Token("date", r'\d{4}/\d{2}/\d{2}')
    assert token.name == 'date'
    assert token.key == '%{date}'
    assert "%{date}" in psr.objects
    assert token.regex == r'(?P<date>\d{4}/\d{2}/\d{2})'

    with pytest.raises(ValueError):
        psr.Token("date", r'\d{2}/\d{2}/\d{2}')

    psr.Token("time", r' %(\d{2}:\d{2}:\d{2})')
    dt = psr.Token("datetime", r'%{date}%{time}')
    assert dt.regex == r'(?P<datetime>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})'
    assert dt.parse("2016/06/30 10:20:30")
    assert dt.taken['datetime'] == '2016/06/30 10:20:30'
    psr.Token("millis", r'\.\d+')
    group = psr.Group("timem", r'%{time}%{millis}')
    assert group.regex == r' (?P<time>\d{2}:\d{2}:\d{2})(?P<millis>\.\d+)'
    psr.Group("dtime", r'%{date} %{timem}')
    group = psr.Group("dt_utc", r'%{dtime} \+0000')
    group.parse("2016/06/10  12:35:02.312 +0000")
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
    cfg = yaml.load(StringIO(cfg))
    psr = ps.create_parser(cfg['parser'])
    assert psr.parse_line('2016-06-28 12:33:21 DEBUG foo.py:37 Init success')
    assert psr.parsed['date'] == '2016-06-28'
    assert psr.parsed['level'] == 'DEBUG'
    assert psr.parsed['src_line'] == '37'

    assert psr.parse_line('2016-06-29 17:50:11 ERROR Critical error!')
    assert psr.parsed['level'] == 'ERROR'
    assert psr.parsed['msg'] == 'Critical error!'

    assert not psr.parse_line('2016-06-29 ERROR')


def test_parser_create_custom():
    cfg = """
parser:
    custom: FCS
    """
    import yaml
    from StringIO import StringIO
    cfg = yaml.load(StringIO(cfg))
    psr = ps.create_parser(cfg['parser'])
    assert isinstance(psr, custom.FCS)


def test_parser_fcs():
    fcs = custom.FCS()
    fcs.set_file_path("dummy_path\FCSAdapter.dll.log.20160420-092124.19316")
    assert fcs.get_date() == '2016-04-20'
    assert fcs.parse_line("E0324 09:26:51.754881  2708 fcs_client.cpp:225] connection closed : 997")
    assert fcs.buf['dt_'] == '2016-04-20 09:26:51.754881'
    assert fcs.buf['level'] == 'E'
    assert len(fcs.buf) == 6
    assert fcs.completed == 0

    assert fcs.parse_line("E0324 11:37:52.508764  3304 communicator.hpp:128] [8371] response sync")
    assert len(fcs.parsed) == 6
    assert fcs.completed == 1

    assert fcs.parse_line(" [RequestValidateAuthenticationKey]")
    assert fcs.buf['type'] == 'ValidateAuthenticationKey'
    fcs.parse_line("  packet_length : 67")
    assert fcs.buf["req-packet_length"] == '67'

    fcs.parse_line("  packet_type : 0x26")
    fcs.parse_line("  transaction_id : 8371")
    fcs.parse_line("  account_no : 1862710")
    fcs.parse_line("  authentication_key : D7665F56-29E2-4B80-BD8F-C5D37C3654CA")
    assert fcs.buf["req-authentication_key"] == "D7665F56-29E2-4B80-BD8F-C5D37C3654CA"
    fcs.parse_line("  client_ip : 116.121.77.141")
    assert fcs.buf["req-client_ip"] == '116.121.77.141'

    fcs.parse_line(" [ResponseValidateAuthenticationKey]")
    fcs.parse_line("  packet_length : 44")
    assert fcs.buf["res-packet_length"] == '44'
    fcs.parse_line("  packet_type : 0x26")
    fcs.parse_line("  transaction_id : 8371")
    fcs.parse_line("  result_code : 90213")
    fcs.parse_line("  condition_type : 0x64")
    fcs.parse_line("  user_no : 0")
    fcs.parse_line("  user_id : ")
    fcs.parse_line("  account_no : 0")
    fcs.parse_line("  account_id : ")
    fcs.parse_line("  account_type : 0x00")
    fcs.parse_line("  block_state : 0x00")
    fcs.parse_line("  pcbang_index : 0")
    fcs.parse_line("  phone_auth : ")
    fcs.parse_line("  is_phone_auth : 0")
    fcs.parse_line("  auth_ip : ")
    assert fcs.buf["res-auth_ip"] == ''

    fcs.parse_line("E0324 11:39:31.027815  3316 communicator.hpp:128] [8481] response sync")
    assert len(fcs.parsed) == 28
    assert fcs.parsed["res-auth_ip"] == ''
    assert len(fcs.buf) == 6
    assert fcs.completed == 2

    assert fcs.parse_line("I0420 11:48:24.739433 26224 communicator.hpp:124] [3362162] response sync")
    fcs.parse_line(" [ResponseGetPCRoomGuid]")
    fcs.parse_line("  packet_length : 14")
    fcs.parse_line("  packet_type : 0x34")
    fcs.parse_line("  transaction_id : 3362162")
    fcs.parse_line("  result_code : 1")
    fcs.parse_line("  condition_type : 0x64")
    fcs.parse_line("  pc_room_guid : 0")
    assert fcs.buf['level'] == 'I'

    fcs.parse_line("I0420 11:48:24.739433 26224 communicator.hpp:133] [3362162] <total: 0 msec>")
    assert fcs.buf['transaction_id'] == '3362162'
    assert fcs.buf['totalms'] == '0'


def test_parser_mocaa():
    moc = custom.Mocaa()
    # dtregx = moc.objects['%{datetime}']
    # assert dtregx.regex == r'(?P<datetime>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} \(\+\d{2}\d{2}\))'
    assert moc.parse_line("==== 2016/06/01 02:51:19 (+0900) ====")
    assert moc.parse_line("[API Request][c6b3d85e-1f60-47e8-a07c-4379db9c2bc6] /v2/contents/start")
    assert moc.buf['ltype'] == 'API Request'
    assert moc.parse_line('{"service_code":"SVC009","store_type":"playstore","params":"","client_ip":"192.168.0.11"}')

    assert moc.parse_line('==== 2016/06/01 02:51:19 (+0900) ====')
    assert moc.completed == 1
    assert moc.parsed['ltype'] == 'API Request'
    assert moc.parse_line('[API Response][c6b3d85e-1f60-47e8-a07c-4379db9c2bc6][898 ms] /v2/contents/start')
    assert moc.parse_line('[Body]')
    assert moc.parse_line('{')
    assert moc.parse_line('  "return_code": 1,')
    assert moc.parse_line('  "url": {')
    assert moc.parse_line('    "notice_url": "",')
    assert moc.parse_line('    "event_url": "",')
    assert moc.parse_line('    "cs_url": "",')
    assert moc.parse_line('    "faq_url": "",')
    assert moc.parse_line('    "coupon_url": ""')
    assert moc.parse_line('  },')
    assert moc.parse_line('  "maintenance": {')
    assert moc.parse_line('    "is_open": true,')
    assert moc.parse_line('    "message": ""')
    assert moc.parse_line('  },')
    assert moc.parse_line('  "webzen_kr_auth": null,')
    assert moc.parse_line('  "webzen_global_auth": null,')
    assert moc.parse_line('  "naver_auth": null,')
    assert moc.parse_line('  "google_plus_auth": null,')
    assert moc.parse_line('  "t_store": null,')
    assert moc.parse_line('  "n_store": null,')
    assert moc.parse_line('  "push_notification": {')
    assert moc.parse_line('    "gcm": "392205420186"')
    assert moc.parse_line('  },')
    assert moc.parse_line('  "google_play_game_service": {')
    assert moc.parse_line('    "client_id": "",')
    assert moc.parse_line('    "client_secret": ""')
    assert moc.parse_line('  },')
    assert moc.parse_line('  "google_auth": null,')
    assert moc.parse_line('  "memo": "success"')
    assert moc.parse_line('}')
    assert moc.buf['memo'] == 'success'

    assert moc.parse_line("==== 2016/06/01 02:51:20 (+0900) ====")
    assert moc.completed == 2


def test_parser_transform():
    cfg = """
parser:
    tokens:
        date: '\d{4}-\d{2}-\d{2}'
        time: '\d{2}:\d{2}:\d{2}.\d{4}'
        dt_: '%{date} %{time}'
        request_data: ['{.*}', 'prefix(lower(ravel(json(_))), "req")']
        exception: '.*'
    groups:
        debug: '%{dt_},%{request_data}'
    formats:
        - '%{debug}'
    """
    cfg = yaml.load(StringIO(cfg))
    psr = ps.create_parser(cfg['parser'])
    tok = psr.objects['%{request_data}']
    ok = tok.parse('{"PostData" : {"val": 1, "lval": [1,2,3]},"GetData" : {}}')
    assert ok
    assert 'request_data' not in tok.taken
    assert "req-postdata_val" in tok.taken
    assert "req-postdata_lval" in tok.taken
    assert "req-getdata" not in tok.taken

    assert psr.objects['%{request_data}'].tfunc_lmap
    psr.parse_line('2016-07-25 00:00:00.7083,{"PostData" : {"val": 1, "lval": [1,2,3]},"GetData" : {}}')
    assert "req-postdata_val" in psr.parsed
    assert "req-postdata_lval" in psr.parsed
    assert "req-getdata" not in psr.parsed
