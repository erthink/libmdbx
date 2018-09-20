@echo off

del test.db test.db-lck

:loop

mdbx_test.exe --pathname=test.db --dont-cleanup-after basic > test.log
if errorlevel 1 goto fail

mdbx_chk.exe -nvvv test.db > chk.log
if errorlevel 1 goto fail
goto loop

:fail
echo FAILED
