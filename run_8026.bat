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

echo Starting FastAPI on port 8026...
python -m uvicorn main:app --host 0.0.0.0 --port 8026

endlocal
