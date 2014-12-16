0.0.5.1
=======
- reduce retry 10 -> 5
- remove redundant cols log
- cap_call prints stdout explicitly

0.0.5.0
=======
- change YAML extension as .yml

0.0.4.9
=======
- table dump support inclusive/exclusive columns

0.0.4.8
=======
- connect db for each daily dump

0.0.4.7
=======
- fix duplicate write_table_info
- rsync option -P for sync_folder
- rsync retry 10 times

0.0.4.6
=======
- catch exception for sync_log
- measure run_task time
- rsync option -P (--partial --progress)
