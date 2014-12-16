# WzDat Forwarder

A Log / DB Forwarder for WzDat. This is an Windows Service Application.

## Prerequisite
Folowing modules are essential for building and testing.

[py2exe](http://www.py2exe.org/ py2exe)

[py.test](http://pytest.org/latest/ py.test)


## Test
<pre>
set WDFWD_CFG=Your-wdfwd-home\wdfwd\tests\test_config.yaml  # set test config
cd Your-wdfwd-home
py.test
</pre>


## Config
Copy default config file as your custom config.
<pre>
copy wdfwd\default_config.yaml my_config.yaml
</pre>

Open and edit it as your need, then set it as active config.

<pre>
set WDFWD_CFG=Your-wdfwd-home\my_config.yaml  # set real config
</pre>

To activate your config file **for service, you need to set WDFWD_CFG as system environment variable** in Windows control panel.

### Usual fields for customizing

You are generally customize follwing fields:

<pre>
app:
    debug: false
    service:
        # NOTE: Your service name
        name: WDFwd
        caption: WzDat Log / DB Forwarder
        # NOTE: Cron style schedule: m h dom mon dow
        schedule: "0 4 * * *"
        force_first_run: false
    rsync_path: # RSYNC-EXE-FILE-PATH

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
            filename: # LOG-FILE-PATH ex) C:\wdfwd-tmp\_wdfwd_log.txt
            maxBytes: 10485760
            backupCount: 10
    root:
        level: DEBUG
        handlers: [file]
    to_url: # RSYNC-SERVER-URL-FOR-LOG ex) rsync-user@myserver.net::rsync-backup/myprj

tasks:
    # Plain folder sync
    - sync_folder:
        folder: # TARGET-FOLDER-PATH ex) C:\MyApp\slog
        to_url: # RSYNC-SERVER-URL-FOR-LOG ex) rsync-user@myserver.net::rsync-backup/myprj/mysvr/log

    # (Recursive) file sync with filename pattern
    - sync_files:
        base_folder: # TARGET-FOLDER-PATH ex) C:\MyApp\\dump
        filename_pattern: # TARGET-FILE-PATTERN ex) "*.dmp"
        recurse: true
        # rsync server url
        to_url: # RSYNC-SERVER-URL-FOR-DUMP ex) rsync-user@myserver.net::rsync-backup/myprj/mysvr/dump
 
    # Dump DB to CSVs, then sync them
    - sync_db_dump:
        # NOTE: Dump folder where DB dumped .csv files are located.
        folder: # DUMP-TARGET-FOLDER-PATH ex) C:\wdfwd-tmp
        field_delimiter: "|"
        db:
            # NOTE: Local DB connection info
            connect:
                driver: "{SQL Server}"
                server: .\SQLEXPRESS
                port:
                database: C9
                trustcon: true
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
                date_pattern: ".*_(\\d{8})"
                date_format: "%Y%m%d"
                skip_last: true
            sys_schema: false
            type_encodings:
                # specify encoding for a db type when conversion needed
                # ex) - {type: 'varchar', encoding: 'cp949'}
                # ex) - {type: 'varchar', func: 'lambda x: x.encode('utf8')}
        # rsync server url
        to_url: # RSYNC-SERVER-URL-FOR-DBLOG ex) rsync-user@myserver.net::rsync-backup/myprj/mysvr/dblog
</pre>

## Build & Install as Windows Service
<pre>
cd Your-wdfwd-home\wdfwd\
python setup.py py2exe
dist\wdfwd_svc.exe --startup auto install
</pre>


## Managing Service

### Start
<pre>
dist\wdfwd_svc.exe start
</pre>

### Restart
<pre>
dist\wdfwd_svc.exe restart
</pre>

### Stop
<pre>
dist\wdfwd_svc.exe stop
</pre>

### Remove
You don't have to remove service to install new build as long as it has been stopped.

<pre>
dist\wdfwd_svc.exe remove
</pre>


## Tips

- If you want to re-sync all your files by force, just delete <code>_wdfwd_dumped.yaml</code> file in your dump folder.


