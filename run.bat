@echo off
setlocal

cd /d "%~dp0"

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

echo Starting FastAPI on port 8025...
python main.py

endlocal
