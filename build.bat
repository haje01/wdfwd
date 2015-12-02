cd wdfwd
dist\wdfwd_svc.exe stop
dist\wdfwd_svc.exe remove
python setup.py py2exe
dist\wdfwd_svc.exe --startup auto install
dist\wdfwd_svc.exe start
cd ..
