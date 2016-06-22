import re

import pytest

from wdfwd import parser as ps


def test_parser_regex():
    SAMPLE = '''
hello world
hi there
'''
    ptrn = re.compile(r'^(\w+)\s+(\w+)$', re.MULTILINE)
    pos = 0
    while True:
        match = ptrn.search(SAMPLE, pos)
        if not match:
            break
        span = match.span()
        pos = span[1] + 1
        print match.groups()

    txt = "\[%(\d+)\]"
    match = re.compile(r'%\(([^)]+)\)').search("\[%(\d+)\]")
    b, e = match.span()
    txt = "{}{}{}".format(txt[:b], "(?P<{}>{})".format("pname" , match.groups()[0]), txt[e:])
    import pdb; pdb.set_trace()  # XXX BREAKPOINT
    pass




SAMPLE_FCS1 = """Log file created at: 2016/03/24 09:26:51
Running on machine: R2FLD037
Log line format: [IWEF]mmdd hh:mm:ss.uuuuuu threadid file:line] msg
E0324 09:26:51.754881  2708 fcs_client.cpp:225] connection closed : 997
E0324 09:26:51.761718  1296 fcs_client.cpp:225] connection closed : 121
E0325 09:37:45.272496  1240 communicator.hpp:92] [59948] fail to receive. timeout
 [RequestGetPCRoomGuid]
  packet_length : 23
  packet_type : 0x34
  transaction_id : 59948
  client_ip : 211.226.71.155
E0325 10:03:27.231928  2300 communicator.hpp:92] [61207] fail to receive. timeout
 [RequestGetPCRoomGuid]
  packet_length : 21
  packet_type : 0x34
  transaction_id : 61207
  client_ip : 61.107.34.25"""


def test_parser_basic():
    ctx = ps.Context()

    with pytest.raises(ps.UnresolvedToken):
        ctx._expand("%{unknown}")

    with pytest.raises(re.error):
        ctx.Token("date", r'\d[')
    assert "%{date}" not in ctx.objects

    with pytest.raises(ValueError):
        ctx.Token("datetime", r'%{date}:%{time}')
    with pytest.raises(ValueError):
        ctx.Token("keyvalue", r'@{\d,?}')
    with pytest.raises(ValueError):
        ctx.Group("keyvalue", r'@{\d,?}')

    token = ctx.Token("date", r'\d{4}/\d{2}/\d{2}')
    assert token.name == 'date'
    assert token.key == '%{date}'
    assert "%{date}" in ctx.objects
    assert token.regex == r'(?P<date>\d{4}/\d{2}/\d{2})'

    with pytest.raises(ValueError):
        ctx.Token("date", r'\d{2}/\d{2}/\d{2}')

    ctx.clear_values()
    ctx.Token("time", r'\d{2}:\d{2}:\d{2}')
    ctx.Token("millis", r'\.\d+')
    group = ctx.Group("timem", r'%{time}%{millis}')
    assert group.regex == r'(?P<time>\d{2}:\d{2}:\d{2})(?P<millis>\.\d+)'
    ctx.Group("datetime", r'%{date} %{timem}')
    group = ctx.Group("dt_utc", r'%{datetime} \+0000')
    res = group.parse("2016/06/10 12:35:02.312 +0000")
    assert len(res) == 3
    assert 'date' in ctx.values and 'time' in ctx.values and 'millis' in ctx.values

    ctx.Token("errcode", r'E\d+')
    ctx.Token("code", r'\d+')
    ctx.Token("srcfile", r'\w+\.\w+')
    ctx.Token("srcline", r'\d+')
    ctx.Group("srcinfo", r'%{srcfile}:%{srcline}')

    ctx.Token("msg", r'.*')
    token = ctx.Token("brnum", r'\[%(\d+)\]')
    token.regex == r'\[(?P<brnum>\d+)]'
    assert token.parse("[61207]") == "61207"
    assert ctx.values['brnum'] == "61207"
    ctx.Token("brname", r'\[(\w+)\]')

    token = ctx.Token("key", r'\s*%(\w+)\s*')
    assert token.parse("  keyname  ") == "keyname"
    ctx.Token("value", r'\s*%([\w\.]+)\s*')
    ctx.Group("keyvalue", r'%{key} : %{value}')
    l_repeat = ctx.Line('begin @{%{keyvalue},?} mid @{%{keyvalue},?} end')
    assert len(l_repeat.ptrns) == 5
    res = l_repeat.parse("begin aaa : 1, bbb : 2 mid ccc : 3 end")
    assert res['aaa'] == '1'
    assert res['bbb'] == '2'
    assert res['ccc'] == '3'

    l_one_head = ctx.Line(r'%{errcode} %{timem}  %{code} %{srcinfo}\] %{msg}')
    res = l_one_head.parse("E0324 09:26:51.754881  2708 fcs_client.cpp:225] connection closed : 997")
    assert 'errcode' in res
    assert len(res) == 7
    assert res['srcline'] == '225'

    l_keyvalue = ctx.Line(r'@{%{keyvalue}}')
    res = l_keyvalue.parse("packet_length : 23")
    assert 'packet_length' in res.keys()
    assert res['packet_length'] == '23'

    ctx.clear_values()
    SAMPLE = """E0325 10:03:27.231928  2300 communicator.hpp:92] [61207] fail to receive. timeout
 [RequestGetPCRoomGuid]
  packet_length : 21
  packet_type : 0x34
  transaction_id : 61207
  client_ip : 61.107.34.25"""
    line = ctx.Line("%{errcode} %{timem}  %{code} %{srcinfo}\] %{brnum} %{msg}\n %{brname}\n@{%{keyvalue}\n?}")
    line.parse(SAMPLE)
    assert ctx.values['brnum'] == '61207'
    assert ctx.values['packet_type'] == '0x34'
    assert ctx.values['packet_length'] == '21'
    assert ctx.values['transaction_id'] == '61207'
    assert ctx.values['client_ip'] == '61.107.34.25'

SAMPLE_FCS2 = """Log file created at: 2016/03/24 11:37:52
Running on machine: R2CHN001
Log line format: [IWEF]mmdd hh:mm:ss.uuuuuu threadid file:line] msg
E0324 11:37:52.508764  3304 communicator.hpp:128] [8371] response sync
 [RequestValidateAuthenticationKeyForR2]
  packet_length : 67
  packet_type : 0x26
  transaction_id : 8371
  account_no : 1862710
  authentication_key : D7665F56-29E2-4B80-BD8F-C5D37C3654CA
  client_ip : 116.121.77.141
 [ResponseValidateAuthenticationKeyForR2]
  packet_length : 44
  packet_type : 0x26
  transaction_id : 8371
  result_code : 90213
  condition_type : 0x64
  user_no : 0
  user_id :
  account_no : 0
  account_id :
  account_type : 0x00
  block_state : 0x00
  pcbang_index : 0
  phone_auth :
  is_phone_auth : 0
  auth_ip : """

SAMPLE_KRT = """BEGIN.IPCHECK
    PACKET.REQ.IPCHECK|Date=2016-04-26 10:53:14.832|IpAddress=61.33.92.200|AccountGUID=46|Reserved1=0|Reserved2=0|Reserved3=0
    BIZ.REQ.IPCHECK|Date=2016-04-26 10:53:14.832|IpAddress=61.33.92.200
    BIZ.RES.IPCHECK|Date=2016-04-26 10:53:14.863|Return=True|ReturnCode=1|ProviderAccountNo=0
    BIZ.RES.IPCHECK|Date=2016-04-26 10:53:14.863|AccountGUID=46|RoomGUID=0|ResultCode=0|Reserved1=0|Reserved2=0|Reserved3=0
END|46-0|RecvQueueCount=0|SendQueueCount=0"""

SAMPLE_MINT = """2016-01-20 15:27:40.773	GetVirtualAccountList	Exception has been thrown by the target of an invocation.	0750bdb3-e9d3-4588-9607-49565d8aeaf1	537	{"ProviderCode":"PRC001","AccountNo":7,"UserNo":3,"PaymentNo":0,"TransactionId":null,"MethodCode":"PMC164","CurrencyCode":null,"BankName":null,"BankAccount":null,"Amount":0,"ProviderName":null,"ProviderClass":null,"ValidatePeriod":0,"PgTransactionId":null,"Desc":null,"DetailDesc":null,"ClientIp":"10.1.30.131","CountryCode":null,"Path":2,"TraceId":"0750bdb3-e9d3-4588-9607-49565d8aeaf1"}	   at System.RuntimeTypeHandle.CreateInstance(RuntimeType type, Boolean publicOnly, Boolean noCheck, Boolean& canBeCached, RuntimeMethodHandleInternal& ctor, Boolean& bNeedSecurityCheck)
    at System.RuntimeType.CreateInstanceSlow(Boolean publicOnly, Boolean skipCheckThis, Boolean fillCache, StackCrawlMark& stackMark)
    at System.Activator.CreateInstance[T]()
    at MINT.Base.Library.SafeProxy.Using[T,E](Action`1 action, E& exception)
    at MINT.Base.Library.PrivateCaller.GetKeyViaKeyServer(String& key)
    at MINT.Base.Library.PrivateCaller.GetKey(String& key)
    at MINT.Billing.Provider.Payment.KR.PaidPayment.GetVirtualAccountList(RequestVirtualAccount model)
at DynamicModule.ns.Wrapped_IPaidPayment_363e535d5ca846d9b9d33bfa228d6b84.<GetVirtualAccountList_DelegateImplementation>__12(IMethodInvocation inputs, GetNextInterceptionBehaviorDelegate getNext)"""

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


