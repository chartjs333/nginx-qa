@echo off
setlocal

cd /d "%~dp0"

rem Telegram configuration template. Keep real credentials in .env; values from
rem .env or the parent process override these safe, non-working defaults.
if not defined TELEGRAM_BOT_TOKEN set "TELEGRAM_BOT_TOKEN="
if not defined TELEGRAM_WEBHOOK_SECRET set "TELEGRAM_WEBHOOK_SECRET="
if not defined TELEGRAM_WEBHOOK_URL set "TELEGRAM_WEBHOOK_URL=https://example.com/api/v1/telegram/agents/sequential"
if not defined TELEGRAM_WEBHOOK_AUTO_REGISTER set "TELEGRAM_WEBHOOK_AUTO_REGISTER=0"
if not defined TELEGRAM_DROP_PENDING_UPDATES set "TELEGRAM_DROP_PENDING_UPDATES=0"
if not defined TELEGRAM_ALLOWED_CHAT_IDS set "TELEGRAM_ALLOWED_CHAT_IDS="
if not defined TELEGRAM_ALLOWED_USER_IDS set "TELEGRAM_ALLOWED_USER_IDS="
if not defined TELEGRAM_HISTORY_CHAT_ID set "TELEGRAM_HISTORY_CHAT_ID="
if not defined TELEGRAM_HISTORY_MESSAGE_THREAD_ID set "TELEGRAM_HISTORY_MESSAGE_THREAD_ID="

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

echo Starting FastAPI on port 8025...
python main.py

endlocal
