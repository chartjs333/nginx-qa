@echo off
setlocal

cd /d "%~dp0"

if exist ".env" (
    echo Loading local environment from .env...
    for /f "usebackq eol=# tokens=1,* delims==" %%A in (".env") do (
        if not "%%A"=="" set "%%A=%%B"
    )
) else (
    echo Local .env was not found. Telegram integration will remain disabled unless variables are set externally.
)

rem Optional Telegram configuration directly in this launcher.
rem Fill TELEGRAM_BOT_TOKEN only in your local copy. Do not commit a real token.
if not defined TELEGRAM_BOT_TOKEN set "TELEGRAM_BOT_TOKEN="
if not defined TELEGRAM_WEBHOOK_SECRET set "TELEGRAM_WEBHOOK_SECRET=CHANGE_ME_WITH_A_RANDOM_SECRET"
rem Sequential URL: https://YOUR_PUBLIC_HOST/api/v1/telegram/agents/sequential
rem Parallel URL:   https://YOUR_PUBLIC_HOST/api/v1/telegram/agents/parallel
if not defined TELEGRAM_WEBHOOK_URL set "TELEGRAM_WEBHOOK_URL=https://YOUR_PUBLIC_HOST/api/v1/telegram/agents/sequential"
if not defined TELEGRAM_WEBHOOK_AUTO_REGISTER set "TELEGRAM_WEBHOOK_AUTO_REGISTER=0"
if not defined TELEGRAM_DROP_PENDING_UPDATES set "TELEGRAM_DROP_PENDING_UPDATES=0"
if not defined TELEGRAM_ALLOWED_CHAT_IDS set "TELEGRAM_ALLOWED_CHAT_IDS="
if not defined TELEGRAM_ALLOWED_USER_IDS set "TELEGRAM_ALLOWED_USER_IDS="
if not defined TELEGRAM_HISTORY_CHAT_ID set "TELEGRAM_HISTORY_CHAT_ID="
if not defined TELEGRAM_HISTORY_MESSAGE_THREAD_ID set "TELEGRAM_HISTORY_MESSAGE_THREAD_ID="

if not exist ".venv\Scripts\python.exe" (
    echo Creating local Python environment...
    python -m venv .venv
    if errorlevel 1 (
        echo Failed to create virtual environment.
        exit /b 1
    )
)

call ".venv\Scripts\activate.bat"

echo Installing dependencies...
python -m pip install --upgrade pip
if errorlevel 1 exit /b 1

python -m pip install -r requirements.txt
if errorlevel 1 exit /b 1

python configure_telegram.py
if errorlevel 1 exit /b 1

if not defined NGINX_QA_AUTO_RESTART set "NGINX_QA_AUTO_RESTART=1"
if not defined NGINX_QA_RESTART_DELAY_SECONDS set "NGINX_QA_RESTART_DELAY_SECONDS=5"
if not defined NGINX_QA_RUNTIME_LOG set "NGINX_QA_RUNTIME_LOG=runtime_state\server-monitor.log"

for %%D in ("%NGINX_QA_RUNTIME_LOG%") do if not exist "%%~dpD" mkdir "%%~dpD"

:run_server
echo Starting FastAPI on port 8025...
python main.py
set "NGINX_QA_EXIT_CODE=%ERRORLEVEL%"

if "%NGINX_QA_EXIT_CODE%"=="0" goto server_stopped

echo [%date% %time%] ERROR: FastAPI exited with code %NGINX_QA_EXIT_CODE%.
>> "%NGINX_QA_RUNTIME_LOG%" echo [%date% %time%] ERROR: FastAPI exited with code %NGINX_QA_EXIT_CODE%.

if /i not "%NGINX_QA_AUTO_RESTART%"=="1" exit /b %NGINX_QA_EXIT_CODE%

echo Restarting FastAPI in %NGINX_QA_RESTART_DELAY_SECONDS% seconds...
timeout /t %NGINX_QA_RESTART_DELAY_SECONDS% /nobreak >nul
goto run_server

:server_stopped
echo FastAPI stopped normally.
endlocal
exit /b 0
