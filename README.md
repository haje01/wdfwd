    __    _______ _____ __    _______
    \\ /\ //||  ) ||==  \\ /\ //||  )
     \V/\V/ ||_// ||     \V/\V/ ||_//

# wdfwd


wdfwd

유닉스 계열의 OS에는 각종 로그와 데이터를 포워딩하기 위한 좋은 솔루션들이 많이 있다. 그러나 윈도우를 위한 포워더는 부족하다. wdfwd(Windows Data Forwarder)는 윈도우 서비스로 설치되어 파일 로그나 DB에 저장된 로그를 포워딩해준다.

## 특징

- 로그 파일 또는 DB(SQLServer)에 저장된 로그 테이블을 실시간 전송
- [Fluentd](http://www.fluentd.org) 또는 [AWS Kinesis](https://aws.amazon.com/ko/kinesis/) 로의 전송 지원
- 윈도우 서비스 방식으로 설치되어 리부팅 시 자동 시작
- 다양한 형식의 로그를 파싱

## 빌드

빌드를 위해 아래를 설치한다.

- [python 2.x](https://www.python.org/downloads/)
- [py2exe](http://www.py2exe.org)

git으로 소스를 clone 하고, 해당 디렉토리로 들어간 후 `build.bat`를 실행한다. 정상적으로 빌드가 끝나면 `wdfwd\dist`폴더 아래에 빌드 결과물이 저장되어 있을 것이다. 이 폴더를 압축 후 서버로 복사하여 설치할 것이다.

## 설치

### wdfwd 설치
1. 미리 폴더 전체로 압축해둔 wdfwd 압축파일을 푼다
1. `files/default-config.yml`을 `config.yml`로 복사
1. 임시 작업 폴더를 만든다 예) `c:\wdfwd-temp`

## 기본 설정

위에서 복사해둔 `config.yml`을 notepad로 열고 편집한다. (이때 항상 이것으로 오픈되도록 설정해두면 편리하다)

여러가지 값들이 있으나, 기본 값을 이용하고, 설명된 부분만 설정 후 이용하면 될것이다.

### app 설정

어플리케이션 관련 설정이다.

    app:
        debug: false
        service:
            # NOTE: Your service name
            name: wfdwd
            caption: Windows Data Forwarder
            # NOTE: Cron style schedule: m h dom mon dow
            schedule: "0 4 * * *"
            force_first_run: true

`service` > `name` - 서비스로 등록될 이름

`service` > `schedule` - 포워더가 동작할 시간을 crontab 방식으로 표기한다. 특정 시간이나 날짜 등 다양한 실행 조건을 표현할 수 있다.

`service` > `force_first_run` - 최초 서비스 실행 시 무조건 한 번 동작한다.

### log 설정

wdfwd 자체 로그 관련 설정이다.

    log:
        version: 1
        formatters:
            simpleFormatter:
                format: '%(asctime)s [%(levelname)s] - %(message)s'
                datefmt: '%Y/%m/%d %H:%M:%S'
        handlers:
            file:
                class: logging.handlers.RotatingFileHandler
                formatter: simpleFormatter
                level: DEBUG
                # NOTE: Log file shall be located within dump folder
                filename: # LOG-FILE-PATH ex) C:\wdfwd-temp\_wdfwd_log.txt
                maxBytes: 10485760
                backupCount: 10
        root:
            level: DEBUG
            handlers: [file]
        to_url: # Log Stream Endpoint ex) [FLUENTD_SERVER_IP, FLUENTD_SERVER_PORT]

`handlers` > `file` > `filename` - wdfwd 자체 로그의 위치이다. 준비 단계에서 만들어둔 작업 폴더 아래 로그파일명 (예: `_wdfwd_log.txt` )까지의 풀 경로를 기입한다.

`to_url` - wdfwd 자체 로그가 전송될 로그 스트림 서버의 정보이다. AWS Kinesis도 가능하지만, 대개 Fluentd를 사용한다. Fluentd의 경우 `[Fluentd 서버 IP, Fluentd 서버 포트]` 형식으로 설정한다.


## 실시간 로그 테일링(Tailing) 설정

테일링은 지정된 로그 파일이나 로그성 DB 테이블의 변경된 끝 부분 만을 네트워크를 통해 전송한다.

테일링을 사용하면 실시간으로 로그를 전송할 수 있다. Fluentd와 AWS Kinesis로 전송할 수 있다. 이를 위해서는 각각 Fluentd가 설치된 로그 중계 용 서버 또는 AWS Kinesis Stream + Kinesis Comsumer가 필요하다.

설정 파일은 `app`과 `log` 섹션은 거의 비슷하나, `tailing` 섹션이 추가되었다. 아래는 간단한 파일 테일링의 예이다.

    tailing:
        file_encoding: cp949
        pos_dir: D:\wdfwd-temp\
        format: '(?P<dt_>\d+-\d+-\d+ \d+:\d+:\S+)\s(?P<level>[^:\s]+):?\s(?P<_json_>.+)’
        from:
            - file:
                dir: D:\WebLog\Billing\Logs
                pattern: "[[]Error[]]*-*-*.log"
                tag: billing.error
            - file:
                dir: D:\WebLog\Billing\Logs
                pattern: "[[]Fatal[]]*-*-*.log"
                tag: billing.fatal
        to:
            fluent: # [FLUENTD_SERVER_IP, FLUENTD_SERVER_PORT]

### tailing

테일링에서 공통적으로 사용하는 설정이 이곳에 들어간다.

`file_encoding` - 로그 파일의 캐릭터 인코딩. 예) `cp949` 이 인코딩에서 UTF8로 변환된다.

`pos_dir` - 로그 파일별 전송위치를 기록하는 포지션 파일이 저장되는 경로 예) `c:\wdfwd-temp\`

`format` - 로그 행을 파싱하기 위한 정규식 예) `'(?P<dt_>\d+-\d+-\d+ \d+:\d+:\S+)\s(?P<level>[^:\s]+):?\s(?P<_json_>.+)’`

*포맷은 파일별로 다르게 지정할 수 있다.*

포맷의 경우 로그 일시(datetime)를 나타내는 `dt_`필드가 꼭 들어가야 한다. 로그 메시지 바디를 별도 파싱하지 않고 JSON혹은 Plain Text로 보낼 경우는 `_json_` 과 `_text_` 중 하나를 사용한다.

- `_json_` 로그 메시지의 형식이 json인 경우
- `_text_` 로그 메시지의 형식이 text인 경우

이외의 필드는 자유롭게 정의할 수 있다. 예) `level`

*단, JSON 로그의 경우 JSON의 키와 로그 필드 이름이 충돌하지 않도록 주의하자.*

#### db

테일링의 대상이 DB에 있는 테이블인 경우 아래와 같이 `db` 섹션을 정의해 준다.

    tailing:
        db:
            connect:
                driver: "{SQL Server}"
                server: localhost
                port:
                database: # Target Database Name
                trustcon: false
                read_uncommit: true
                uid: # DB User
                passwd: # DB Password
            encoding: UTF8
            datefmt: "%Y-%m-%d %H:%M:%S.%f"
            millisec_ndigit: 3
            sys_schema: false

* `encoding` - DB의 캐릭터 인코딩. 이 인코딩에서 UTF8로 변환된다.
* `datefmt` - DB의 일시(date time) 포맷
* `millisec_ndigit` - 밀리세컨드 부분의 자리수 (SQLServer의 경우 3이다.)
* `sys_schema` - 대상 테이블 이름에 네임 스키마가 명시되어 있는지 여부

#### from

테일링의 대상을 설정한다.

##### file

테일링의 대상이 파일인 경우.

    from:
        - file:
            dir: # LOG_DIR
            pattern: mylog-*.txt
            tag: myapp.mylog1

* `dir` - 로그 파일이 남는 디렉토리 경로
* `pattern` - 파일 이름의 패턴. [Unix 스타일의 경로명 패턴](https://docs.python.org/2/library/glob.html)을 받아 들인다.(`[`을 escape하기 위해서는 `[[]`을 사용하는 것에 주의)
* `tag` - Fluentd 용 태그

wdfwd는 대상 디렉토리에서 패턴에 일치하는 로그 파일은 일단 대상 파일로 선택한다. 그 후 문자열 기준으로 내림차순 소팅하여 가장 첫번째의 것을 현재 기록되고 있는 로그로 보고 테일링을 시도한다. 예를 들어 다음과 같은 파일들이 있다면:

    log\mylog-20160931.txt
    log\mylog-20161001.txt
    log\mylog-20161002.txt

이것을 내림차순 소팅하면 아래와 같게 되고

    log\mylog-20161002.txt
    log\mylog-20161001.txt
    log\mylog-20160931.txt

첫 번째로 오는 `log\mylog-20161002.txt`을 현재 로그로 본다.

###### Unix 스타일 로그 로테이션 대응

Unix 계열에서는 전통적으로 아래와 같은 식으로 로그 로테이션을 한다.

    log\mylog.txt
    log\mylog.txt.1
    log\mylog.txt.2

위에서 `log\mylog.txt`가 가장 최신 로그 파일이고, `mylog.txt.1`이 그 다음 최신, `mylog.txt.2`가 가장 오래된 로그이다.

이 경우는 다음과 같이 설정한다.

    - file:
        dir: # LOG_DIR
        pattern: mylog.txt.*
        latest: mylog.txt
        reverse_order: true
        tag: myapp.mylog2

* `latest` - 바뀌지 않는 최신 파일을 명시적으로 지정한다.
* `reverse_order` - `true`로 설정하면 문자열 기준으로 오름차순 소팅으로 바꾼다.

이렇게 하면 wfdwd 가 최신 파일 순서대로 인식하여 처리하게 된다.

##### table

테일링의 대상이 DB 테이블인 경우.

    - table:
        name: Log1
        col_names: ['dtime', 'message']
        key_idx: 0
        tag: wdfwd.dbtail1

* `name` - 대상 테이블 이름
* `col_names` - 테이블의 컬럼 이름들
* `key_idx` - 테이블의 키 컬럼의 인덱스. 로그성 테이블의 경우 일반적으로 `datetime` 타입의 컬럼이 지정된다.
* `tag` - 목적지 스트리밍 서비스에서 사용될 분류 태그

MS SQLServer에서 SP(Stored Procedure)를 써야만 한다면, 아래와 같이 설정할 수 있다.

    - table:
        name: Log2
        start_key_sp: uspGetStartDatetime
        latest_rows_sp: uspGetLatestRows
        key_idx: 0
        tag: wdfwd.dbtail2

* `name` - 대상 테이블 이름
* `start_key_sp` - 테일링을 시작할 키를 요청하는 SP. `int`형 인자를 받는데, 그 값은 lines_on_start가 건네진다.
* `latest_rows_sp` - 지정 시간 이후의 최신 로그 행을 요청. `datetime`형 인자를 받는데, 최근 보내진 행의 키가 건네진다.
* `key_idx` - SP 반환된 행에서 키로 사용될 컬럼의 인덱스
* `tag` - 목적지 스트리밍 서비스에서 사용될 분류 태그

#### to

테일링의 목적지를 설정. Fluentd나 AWS Kinesis 같은 데이터 스트리밍 서비스 정보를 지정한다.

##### fluent

Fluentd 서버로 보낸다. `[IP주소, 포트]` 형식을 따른다.

    to:
        fluent: [FLUENTD_SERVER_IP, FLUENTD_SERVER_PORT]

##### kinesis

Kinesis 스트림으로 보낸다. 다음과 같은 형식을 따른다.

    to:
        kinesis:
            access_key: # AWS Access Key Id
            secret_key: # AWS Secret Access Key
            stream_name: # AWS Kinesis Stream Name
            region: # AWS Region ex) ap-northeast-2

*Kinesis 스트림으로 보낼 때는 파이썬의 [aws_kinesis_agg](https://pypi.python.org/pypi/aws_kinesis_agg/1.0.0)모듈을 사용해서 Aggregation된 형태로 전송된다. 따라서 Kinesis Comsumer 쪽에서 Deaggregation 작업이 필요하다.*

## 복잡한 로그 파일 파싱하기

DB는 정규화된 로그가 남고 있겠지만, 파일에는 다양한 형태의 로그가 남을 수 있다. 여기서는 이를 파싱하는 방법을 설명한다.

### 파서 정의

정규식이 너무 복잡해지거나, 여러가지 포맷이 있는 로그의 경우 `parser`를 정의하여 사용한다. 파서의 정의는 설정파일의 `tailing` 또는 `tailing/from/file`아래에 올 수 있다. 전자의 경우 모든 로그 파일에 적용되며, 후자의 경우 특정 파일에만 적용되는 파서이다.

파서는 토큰, 그룹, 포맷으로 구성된다. 크게 다음과 같은 형식을 가진다.

    parser:
        tokens:
            ..
        groups:
            ..
        formats:
            ..

#### 토큰
토큰은 정규식의 기본 단위이다. 아래와 같이 '토큰명: 토큰 정규식`의 형태로 정의된다.

    tokens:
        date: '\d{4}-\d{2}-\d{2}'
        time: '\d{2}:\d{2}:\d{2}'
        level: 'DEBUG|INFO|ERROR'
        src_file: '\w+\.\w+'
        src_line: '\d+'
        msg: '.*'

매칭된 토큰의 이름은 필드명으로 사용된다. 그리고 토큰은 아래와 같이 다른 토큰을 참조할 수 있다.

    tokens:
        date: '\d{4}-\d{2}-\d{2}'
        time: '\d{2}:\d{2}:\d{2}'
        dt_: %{date} %{time}


#### 그룹
그룹은 하나 이상의 토큰이나 다른 그룹을 참조해서 구성한다. 토큰과 다른 점은 *그룹명이 필드로 저장되지 않는다*는 점이다. 대신 그룹을 구성하는 각 토큰명이 필드로 저장된다.

    groups:
        datetime: '%{date} %{time}'
        src_info: '%{src_file}:%{src_line}'

위의 예에서 `datetime` 또는 `src_info` 그룹이 매칭되면, 각각 `date`와 `time` 또는 `src_file`과 `src_line`의 두 필드로 값이 저장된다.

#### 포맷
앞에서 정의한 토큰과 그룹을 조합해서 최종적인 포맷을 만든다. 포맷은 하나 이상 정의할 수 있다. 실재 로그의 각 행을 파싱할 때, 앞의 포맷부터 시도하여 정규식 매칭에 성공하면 그것을 저장하게 된다. 따라서 **자주 나오는 로그 형태를 먼저 정의**하는 것이 파싱 속도를 올리기 위해 중요하다.

    formats:
        - '%{datetime} %{level} %{src_info} %{msg}'
        - '%{datetime} %{level} %{msg}'

파서 하나에 다양한 포맷을 등록하면, 글로벌(`tailing` 아래의) 파서 하나로 모든 파일을 파싱할 수 있으나, 파싱 속도가 느려질 수 있기에 적절하게 사용하자. 

### 토큰 트랜스폼 함수

로그 안에 JSON형태의 필드가 있고, 이것을 파싱해서 보내야 한다면 토큰 트랜스폼 함수를 사용한다.
아래와 같이 토큰 정규식과 트랜스폼(변환) 함수의 쌍으로 정의한다.

    tokens:
        jsondata: ['{.*}', 'json(_)']

여기서 `_`는 토큰 정규식에 매칭된 결과를 가지고 있다. 이것을 json형식으로 파싱한다는 뜻이다.

예를 들어 다음과 같은 로그 행을 분석하기 위해:

    2016-03-19 {'A': {'B': 1}, 'C': 2}

아래와 같은 파서 정의를 한다.

    tokens:
        date: '\d{4}-\d{2}-\d{2}'
        jsondata: ['{.*}', 'json(_)']



정상적으로 파싱되면 아래와 같은 결과 json이 만들어 진다.

    {'date': '2016-03-19', 'A': {'B': 1}, 'C': 2}

#### 연쇄 트랜스폼

트랜스폼된 결과는 다른 트랜스폼 함수를 통해 다시 변경될 수 있다.

위의 예에서 'A'는 중첩된 dictionary 형의 값을 가지고 있다. 이것은 Elasticsearch의 검색에는 좋지 않다. 이것을 펼쳐주기 위해서 `ravel`을 사용한다.

    jsondata: ['{.*}', 'ravel(json(_))']

이렇게 하면 아래와 같이 펼쳐진 결과가 된다.

    {'date': '2016-03-19', 'A_B': 1, 'C': 2}

만약, 이 결과에서 키를 소문자화 하고 싶다면 `lower`를 사용한다.

    jsondata: ['{.*}', 'lower(ravel(json(_)))']

결과는 다음과 같다.

    {'date': '2016-03-19', 'a_b': 1, 'c': 2}


#### 토큰 명과 충돌 방지

json 파싱된 결과는 다른 토큰의 결과와 합쳐지는데, 이때 같은 키로 충돌이 생길 수 있다. 다음의 예에서

    2016-03-19 {'DATE': '2001-01-01'}

위의 파서로 파싱하면 다음과 같은 잘못된 결과가 나온다.

    {'date': '2001-01-01'}

이를 방지하기 위해 `prefix`를 사용하자.

    jsondata: ['{.*}', 'prefix(lower(ravel(json(_))), 'data')']

다음과 같이 접두어가 붙어 충돌을 방지할 수 있다.

    {'date': '2016-03-19', 'data-date': '2001-01-01'}

### 포맷 / 파서 테스트 하기

함께 배포되는 `cfgtest.exe` 를 사용해 설정 파일 내 format 또는 parser를 쉽게 테스트 할 수 있다.

사용 방법은 아래와 같다.

    > cfgtest format --help
    Usage: test format [OPTIONS] FILE_PATH

    Options:
    --cfg-path TEXT      Forwarder config file path.
    --cfile-idx INTEGER  Tailing config file index.
    --help               Show this message and exit.

    > test parser --help
    Usage: test parser [OPTIONS] FILE_PATH

    Options:
    --cfg-path TEXT      Forwarder config file path.
    --cfile-idx INTEGER  Tailing config file index.
    --help               Show this message and exit.


공통적으로 `--cfg-path`로 설정파일 경로를 지정하고, `--cfile-idx`로 `tailing` 아래 `file`의 인덱스를 지정한다.

예를 들어 format 정규식을 테스트 하려면 다음과 같이 한다.

    > test format --cfg-path "C:\wdfwd\config.yml" --cfile-idx 0 "C:\logs\test.log"

이때 미리 환경변수 `WDFWD_CFG` 가 설정되어 있다면 `--cfg-path` 는 생략 가능하다.

정상적으로 파싱이 되면 파싱된 결과를 출력하고, 실패하면 다음과 같이 에러 메시지와 함께 실패한 행을 출력한다.

    Parsing failed! : '16-05-25 00:25:14, 1, 1213, 7, lee, 10.100.40.100, 1213'

## 커스텀 파서

멀티 라인으로 구성되어 있는 복잡한 로그를 파싱할 때는 커스텀 파서를 이용한다. 커스텀 파서는 `wdfwd/parser/custom.py`에 클래스로 구현되어야 한다. 아래와 같이 설정파일의 custom필드에 클래스 명을 기입하여 사용한다.

    parser:
        custom: FCS

## 실행하기

### 환경변수 설정

윈도우의 `제어판` > `고급 시스템 설정` > `환경 변수`에 다음과 같이 설정한다.

 `WDFWD_CFG`을 wdfwd 폴더 내 `config.yml`파일의 경로로(예: `c:\wdfwd-버전\config.yaml`) 설정

### 서비스 운용

터미널을 열고 wdfwd가 설치된 폴더로 이동(`cd`) 후 아래와 같이 진행한다.

#### 서비스 설치
*만약 기존에 설치된 버전이 있으면, 반드시 아래의 서비스 제거 방법을 참고하여 제거해준다.*

윈도우 재부팅 시에도 서비스가 자동으로 실행되도록 설치한다.

`wdfwd_svc.exe —-startup=auto install`

#### 서비스 시작
다음과 같이 서비스를 시작하고

`wdfwd_svc.exe start`

태스크 매니저 등을 이용해 `wdfwd`서비스가 동작하고 있는지 확인한다.

#### 서비스 중단

`wdfwd_svc.exe stop`

#### 서비스 제거
서비스가 실행 중이면 먼저 중단하고 제거해준다.

`wdfwd_svc.exe remove`


## 팁

### 서비스 설치시 에러가 나거나 시작되지 않는 경우

임시 작업 폴더 아래의 wdfwd 로그 파일(예: `c:\wdfwd-temp\_wdfwd_log.txt`)내 에러 메시지를 확인한다.

### 네트워크 문제

로그 스트림 서비스로 전송이 안되는 경우는 네트워크 방화벽 설정을 다시 확인하고, 윈도우 내 방화벽도 확인한다.

#### 윈도우 방화벽 설정 (wf.msc)
- '공용 프로필’에서 아웃바운드 연결이 허용되어 있지 않으면, 아웃 바운드 규칙 클릭 > 프로필로 필터링 > 도메일 프로필로 필터링으로 등록된 것이 있는지 확인한다.
- 없으면 '새 규칙’ > 프로그램 지정 > 연결 허용 -> 도메인, 개인, 공용 모두 켬(기본) -> 이름과 설명 추가

### DB 테일링시 에러 발생

#### 테이블 권한 확인
해당 테이블의 읽기 또는 SP 호출 권한이 있는지 확인한다.

#### DB 네트워크 확인
wdfwd가 설치된 장비에서 DB로의 접속이 가능한지 확인한다.

### 전송이 자주 실패할 때
L7 스위치의 ACK Flooding 정책에 따라 막히는 때가 있다. 시스템 관리자에 확인 후 예외 등록을 요청하자.

### Fluentd나 Kinesis로 테일링 시 네트워크가 불안정하면?
포워더는 일정 횟수(5번)까지 재전송을 시도하고, 계속 실패하면 그 매시지를 버린다. 즉 로그 유실이 일어난다. 그 후 다음 메시지를 받아와 전송을 시도한다. 만약, 네트워크가 안정화 되어 전송이 가능하게 되면 메시지를 정상적으로 보내게 된다.

### 서비스 재 시작시 로그 손실 없게
서비스 중 설정을 바꾸고 재시작 하는 경우, 포워더는 서비스 종료 시점부터 재시작 시점까지 추가된 로그의 양을 측정하여 특정 양보다 적으면 보내고, 크면 추가된 로그를 포기하고 새로 추가되는 로그만 보내게 된다.(이것은 포워더가 시작할 때 너무 많은 기존 로그를 한꺼번에 보내지 않도록 하기 위함이다.) 이 기준 양을 `max_between_data`로 설정할 수 있다. 기본적으로 서비스 재시작에 로그가 유실되지 않도록 넉넉한 값으로 잡혀있으니, 대부분의 경우 별도로 설정하지 않아도 될 것이다.

### 테스트 시 매번 로그 추가하기가 번거로우면
설정을 바꿔가면서 포워더가 잘 동작하는지 테스트할 때, 매번 새로운 로그를 생성해야 하는 것이 번거로울 수 있다. 이때 `lines_on_start`값을 이용할 수 있다. 이 값을 0이상 주면, 포워더 서비스가 기동할 때 새로운 로그가 없더라도, 기존 로그에서 그 숫자 만큼의 행을 다시 보내주기에, 테스트가 편리해진다.
