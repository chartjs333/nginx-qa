@echo off
chcp 65001 >nul
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\nqt35\client\sdw-continuation-20261104\release-final\start_all.ps1"
set "startupExitCode=%ERRORLEVEL%"
if not "%startupExitCode%"=="0" echo Startup failed. Read the error above; no later step was started.
pause
exit /b %startupExitCode%
