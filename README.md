# WdFwd

유닉스 계열의 OS에는 각종 로그를 포워딩하기 위한 좋은 솔루션들이 많이 있다. 그러나 윈도우를 위한 로그 포워더는 부족하다. WdFwd(=WzDat Log Forwarder)는 윈도우 서비스로 설치되어 파일 로그나 DB에 저장된 로그를 분석을 위해 포워딩해준다.

WdFwd는 [WzDat](https://github.com/haje01/wzdat)을 위해 만들어 졌으나, 다양한 로그 전송의 용도로 사용될 수 있다.

## 특징

- 윈도우 용 rsync를 통한 압축/차분(Differential) 로그 포워딩
- Fluentd 로의 실시간 로그 테일링
- 윈도우 서비스 방식으로 설치되어 리부팅 시 자동 시작
- 다양한 형식의 로그를 포워딩
- DB(SQLServer)에 저장된 로그 테이블을 덤프 후 포워딩
- crontab 형식의 스케쥴 표기

## 빌드

빌드를 위해 아래를 설치한다.

- [python 2.x](https://www.python.org/downloads/)
- [py2exe](http://www.py2exe.org)

git으로 소스를 clone 하고, 해당 디렉토리로 들어간 후 `build.bat`를 실행한다. 정상적으로 빌드가 끝나면 `wdfwd\dist`폴더 아래에 빌드 결과물이 저장되어 있을 것이다. 이 폴더를 압축 후 서버로 복사하여 설치할 것이다.

## rsync로 로그 동기

rsync를 사용하면 서버에 있는 파일을 정해진 스케쥴로 목적지에 동기한다.

### 방화벽 오픈

서버의 경우 대부분 방화벽 뒤에 존재하기에, 서버에서 WzDat 서버(=로그 수집 서버)로 네트워크 인가(포트 `873`)가 필요하다.
 
### 설치

#### cwRsync 설치
[cwRsync](https://www.itefix.net/cwrsync)는 윈도우 용 rsync 클라이언트이다. 홈페이지에서 Free 버전을 받아 설치한다.

#### WdFwd 설치
1. 미리 폴더 전체로 압축해둔 WdFwd 압축파일을 푼다
1. `files/default-config.yml`을 `config.yml`로 복사
1. 임시 작업 폴더를 만든다 예) `c:\wdfwd-temp`

### 설정하기
위에서 복사해둔 `config.yml`을  notepad로 열고 편집한다. (이때 항상 이것으로 오픈되도록 설정해두면 편리하다) 

여러가지 값들이 있으나, 기본 값을 이용하고, 설명된 부분만 설정 후 이용하면 될것이다.

#### app 설정

어플리케이션 관련 설정이다.

    app:
        debug: false
        service:
            # NOTE: Your service name
            name: WDFwd
            caption: WzDat Log / DB Forwarder
            # NOTE: Cron style schedule: m h dom mon dow
            schedule: "0 4 * * *"
            force_first_run: true
        rsync_path: # RSYNC-EXE-FILE-PATH
        # Limit rsync bandwidth in KB/Sec. 0 for no limit, skip for default
        rsync_bwlimit: 5120  # 5 MByte/Sec, Default

`service` > `name` - 서비스로 등록될 이름
    
`service` > `schedule` - 포워더가 동작할 시간을 crontab 방식으로 표기한다. 특정 시간이나 날짜 등 다양한 실행 조건을 표현할 수 있다.

`service` > `force_first_run` - 최초 서비스 실행 시 무조건 한 번 동작한다.

`rsync_path` - cwRsync 폴더 내 `rsync.exe`까지의 풀 경로명으로 지정

`rsync_bwlimit` - 한꺼번에 많은 로그를 전송하면 서비스 장비의 네트워크에 무리가 갈 수 있기에, 대역폭 제한을 걸 수 있다. 기본은 초당 5M 바이트이다.

#### log 설정

WdFwd 자체 로그 관련 설정이다.
    
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
        to_url: # RSYNC-SERVER-URL-FOR-LOG ex) rsync-user@myserver.net::rsync-backup/myprj

`handlers` > `file` > `filename` - WdFwd 자체 로그의 위치이다. 준비 단계에서 만들어둔 작업 폴더 아래 로그파일명 (예: `_wdfwd_log.txt` )까지의 풀 경로를 기입한다.

`to_url` - WdFwd 자체 로그가 전송될 URL이다. 포워더 서비스가 잘 동작하고 있는지 확인할 때 용이하다.  `rsync-user@myserver.net::rsync-backup/myprj`형식으로 기입한다. 
   
#### tasks 설정

여기에서 실재 로그 포워딩을 위한 작업(Task)를 설정한다. 작업에는 다음과 같은 종류가 있다.

##### sync_folder

지정된 폴더 전체를 동기한다. 로그 전용 폴더가 있으면 그것을 지정하면 된다.

        # Plain folder sync
        - sync_folder:
            folder: # TARGET-FOLDER-PATH ex) C:\MyApp\slog
            to_url: # RSYNC-SERVER-URL-FOR-LOG ex) rsync-user@myserver.net::rsync-backup/myprj/mysvr/log
    
`folder` - 동기할 대상 폴더

`to_url` - 대상 폴더가 전송될 URL이다. `rsync-user@myserver.net::rsync-backup/myprj/mysvr/log`형식으로 기입한다. 


##### sync_files

지정된 패턴에 맞는 파일들만 동기한다. 덤프 파일 등의 동기에 사용한다.

        # (Recursive) file sync with filename pattern
        - sync_files:
            base_folder: # TARGET-FOLDER-PATH ex) C:\MyApp\\dump
            filename_pattern: # TARGET-FILE-PATTERN ex) "*.dmp"
            recurse: true
            # rsync server url
            to_url: # RSYNC-SERVER-URL-FOR-DUMP ex) rsync-user@myserver.net::rsync-backup/myprj/mysvr/dump
     
`base_folder` - 파일을 찾기위한 기본 폴더

`filename_pattern` - 대상 파일의 패턴 ( 예: `"*.dmp"`)

`recurse` - 하위 폴더로 재귀적으로 검색할 지 여부

`to_url` - 대상 파일이 전송될 URL이다. `rsync-user@myserver.net::rsync-backup/myprj/mysvr/dump`형식으로 기입한다. 

##### sync_db_dump

DB(= SQLServer)에 남고 있는 로그 테이블을 로컬 CSV 파일로 덤프 후, 그것을 포워딩한다.
   
        # Dump DB to CSVs, then sync them
        - sync_db_dump:
            # NOTE: Dump folder where DB dumped .csv files are located.
            folder: # DUMP-TARGET-FOLDER-PATH ex) C:\wdfwd-temp
            field_delimiter: "\t"

            db:
                # NOTE: Local DB connection info
                connect:
                    driver: "{SQL Server}"
                    server: .\SQLEXPRESS
                    port:
                    database: MyApp
                    trustcon: true
                    read_uncommit: true
                    uid:
                    passwd:
                fetchsize: 1000
                table:
                    # NOTE: Table names to be dumped.
                    names:
                        - BIP.TblItemCreateDeleteRecord_
                        - BIP.TblItemEnfcStat_
                        - LogOpr.TblHackLogOpr_
                        - LogOpr.TblLogOpr_
                        - LogOpr.TblMissionPlayLogOpr_
                    # for daily tables
                    date_pattern: ".*_(\\d{8})"
                    date_format: "%Y%m%d"
                    # for non-daily tables only
                    #   date_column: 'LogTime'
                    skip_last: true
                sys_schema: false
                type_encodings:
                    # specify encoding for a db type when conversion needed
                    # ex) - {type: 'varchar', encoding: 'cp949'}
                    # ex) - {type: 'varchar', func: 'lambda x: x.encode('utf8')}
            # rsync server url
            to_url: # RSYNC-SERVER-URL-FOR-DBLOG ex) rsync-user@myserver.net::rsync-backup/myprj/mysvr/dblog

`folder` - DB에서 덤프한 CSV 파일이 임시적으로 저장될 폴더. 준비 과정에서 만들어둔 임시 작업 폴더 (예: `C:\wdfwd-temp`)를 기입한다.
`field_delimiter` - CSV파일의 구분자. Tab문자(`"\t"`)을 사용하는 것이 좋다.

`db` > `connect` > `server` - DB 서버의 IP

`db` > `connect` > `port` - DB 서버의 포트

`db` > `connect` > `database` - DB 이름

`db` > `connect` > `read_uncommit` - DB 서버가 로컬에 있는 경우만 `true` 아니면 `false`

`db` > `connect` > `trustcon` - 테이블을 읽는 동안 락이 걸리지 않도록 `true`로 해두는 것이 좋다.

`db` > `connect` > `uid` - DB 계정 ID

`db` > `connect` > `passwd`- DB 계정 암호

`db` > `fetchsize` - 한 번에 읽어올 행 수

`db` > `table` > `names` - 읽어올 테이블 명 리스트

`db` > `table` > `date_pattern` - 날자 별로 구분된 테이블의 경우, 날자를 찾기 위한 정규표현식 패턴

`db` > `table` > `date_format` - 날자 별로 구분된 테이블의 경우, 날자 형식

`db` > `table` > `date_column` - 날자 별로 구분된 테이블이 아닌 경우, 날자가 있는 컬럼 명

`db` > `table` > `skip_last` - 날자 별로 구분된 테이블의 경우, 가장 최근(=오늘)의 테이블의 덤프는 생략하기

`db` > `type_encodings` - 컬럼의 타입 별로 인코딩을 지정
	예) `{type: 'varchar', encoding: 'cp949'}`
	예) `{type: 'varchar', func: 'lambda x: x.encode('utf8')}`

`db` > `to_url` - CSV 파일이 전송될 URL이다. `rsync-user@myserver.net::rsync-backup/myprj/mysvr/dblog`형식으로 기입한다. 

## Fluent로 로그 테일링(Tailing) 

*이 기능은 아직 개발 중으로, 버그나 설정 파일 형식의 변경이 있을 수 있다.*

테일링은 지정된 파일의 변경된 끝 부분 만을 네트워크를 통해 전송한다. 

현재 테일링의 목적지는 [Fluentd](http://www.fluentd.org) 서버이다. Fluentd를 사용하면 rsync를 사용할 때 보다 설정이 간단하고, 무엇보다 실시간으로 로그를 전송할 수 있다.이를 위해서 Fluentd가 설치된 로그 중계 용 서버가 필요하다.

설정 파일은 `app`과 `log` 섹션은 거의 비슷하나, `tailing` 섹션이 추가되었다. 아래를 참고하자.

    tailing:
        file_encoding: cp949
        pos_dir: D:\wdfwd-temp\
        format: '(?P<dt_>\d+-\d+-\d+ \d+:\d+:\S+)\s(?P<level>[^:\s]+):?\s(?P<_json_>.+)’
        from:
            # MyPCBang
            - file:
                dir: D:\Web_Log\Billing.MyPCBang\Logs
                pattern: "[[]Error[]]*-*-*.log"
                tag: billing.mypcbang.error
            - file:
                dir: D:\Web_Log\Billing.MyPCBang\Logs
                pattern: "[[]Fatal[]]*-*-*.log"
                tag: billing.mypcbang.fatal 
        to:
            fluent: ['52.79.170.169', 24224]

### tailing 설정

테일링에서 공통적으로 사용하는 설정이 이곳에 들어간다.

`file_encoding` - 로그 파일의 캐릭터 인코딩. 예) `cp949`

`pos_dir` - 로그 파일별 전송위치를 기록하는 포지션 파일이 저장되는 경로 예) `c:\wdfwd-temp\`

`format` - 로그 행을 파싱하기 위한 정규식 예) `'(?P<dt_>\d+-\d+-\d+ \d+:\d+:\S+)\s(?P<level>[^:\s]+):?\s(?P<_json_>.+)’`

*포맷은 파일별로 다르게 지정할 수 있다.*

포맷의 경우 `dt_`필드 와 `_json_` 와 `_text_` 중 하나가 꼭 들어가야 한다.

- `dt_` 로그 행이 기록된 일시(datetime)
- `_json_` 로그 메시지의 형식이 json인 경우
- `_text_` 로그 메시지의 형식이 text인 경우

이외의 필드는 자유롭게 정의할 수 있다. 예) `level`

*단, JSON 로그의 경우 JSON의 키와 로그 필드 이름이 충돌하지 않도록 주의하자.*

#### from

테일링의 대상을 설정한다.

##### file

테일링의 대상이 파일인 경우.

`dir` - 파일이 남는 디렉토리 경로

`pattern` - 파일 이름의 패턴. [Unix 스타일의 경로명 패턴](https://docs.python.org/2/library/glob.html)을 받아 들인다.(`[`을 escape하기 위해 `[[]`을 사용하는 것에 주의)

`tag` - Fluentd에서 참고할 태그

#### to 

테일링의 목적지를 설정.

##### fluent

Fluentd 서버로 보낸다. `[IP주소, 포트]` 형식을 따른다.

## 실행하기

### 환경변수 설정

윈도우의 `제어판` > `고급 시스템 설정` > `환경 변수`에 다음과 같이 설정한다.

 `WDFWD_CFG`을 WdFwd 폴더 내 `config.yml`파일의 경로로 (예: `c:\wdfwd-버전\config.yaml`) 로

### 서비스 운용

터미널을 열고 WdFwd가 설치된 폴더로 이동(`cd`) 후 아래와 같이 진행한다.

#### 서비스 설치 
*만약 기존에 설치된 버전이 있으면, 반드시 아래의 제거 방법을 참고하여 먼저 제거해준다.*

윈도우 재부팅 시에도 서비스가 자동으로 실행되도록 설치한다.

`wdfwd_svc.exe —startup=auto install`

#### 서비스 시작 
다음과 같이 서비스를 시작하고

`wdfwd_svc.exe start`

태스크 매니저 등을 이용해 `WDFwd`서비스가 동작하고 있는지 확인한다.

#### 서비스 중단
`wdfwd_svc.exe stop`

#### 서비스 제거
서비스가 실행 중이면 먼저 중단하고,

`wdfwd_svc.exe stop`

제거해준다.

`wdfwd_svc.exe remove`


## 팁

### 서비스 설치시 에러가 나거나 시작되지 않는 경우

임시 작업 폴더 아래의 WdFwd로그 파일(예: `c:\wdfwd-temp\_wdfwd_log.txt`) 내 에러 메시지를 확인한다.

### 네트워크 문제

rsync를 통한 전송이 안되는 경우는 네트워크 방화벽 설정을 다시 확인하고, 윈도우 내 방화벽도 확인한다.

#### 윈도우 방화벽 설정 (wf.msc)
- '공용 프로필’에서 아웃바운드 연결이 허용되어 있지 않으면, 아웃 바운드 규칙 클릭 > 프로필로 필터링 > 도메일 프로필로 필터링으로 등록된 것이 있는지 확인한다. 
- 없으면 '새 규칙’ > 프로그램 지정 > 연결 허용 -> 도메인, 개인, 공용 모두 켬(기본) -> 이름과 설명 추가

### DB 덤프시 에러 발생

#### DB 계정 정보 확인
DB 계정 ID 혹은 암호를 확인한다.

#### 테이블 권한 확인
해당 테이블의 읽기 권한이 있는지 확인한다.

#### DB 네트워크 확인
WdFwd가 설치된 장비에서 DB로의 접속이 가능한지 확인한다.

### DNS or IP?

설정파일에서 장비의 주소를 기입할 때:

- WzDat 서버(=로그 수집 서버)가 클라우드에 있는 경우 Cloud IP가 변할 수 있기에, 가급적 DNS이름으로 신청
- 그러나 윈도우 서버에 DNS관련 기능의 추가가 불가능한 경우, 어쩔 수 없이 IP기반으로 해야한다.

### rsync가 자주 실패할 때
L7 스위치의 ACK Flooding 정책에 따라 막히는 때가 있다. 시스템 관리자에 확인 후 예외 등록을 요청하자.

