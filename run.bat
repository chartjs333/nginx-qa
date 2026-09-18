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
if not defined TELEGRAM_WEBHOOK_MODE set "TELEGRAM_WEBHOOK_MODE=sequential"
if not defined CLOUDFLARED_QUICK_TUNNEL_ORIGIN set "CLOUDFLARED_QUICK_TUNNEL_ORIGIN=http://localhost:8025"

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
if errorlevel 1 goto launch_failed

set "NGINX_QA_TUNNEL_STARTED=0"
set "NGINX_QA_BOOTSTRAP_STARTED=0"
call :resolve_quick_tunnel_setting
if errorlevel 1 goto launch_failed
if /i "%NGINX_QA_QUICK_TUNNEL_ENABLED%"=="1" call :start_bootstrap_server
if errorlevel 1 goto launch_failed
if /i "%NGINX_QA_QUICK_TUNNEL_ENABLED%"=="1" call :start_quick_tunnel
if errorlevel 1 goto launch_failed

python configure_telegram.py
if errorlevel 1 goto launch_failed
call :stop_bootstrap_server
if errorlevel 1 goto launch_failed

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

if /i not "%NGINX_QA_AUTO_RESTART%"=="1" goto server_failed

echo Restarting FastAPI in %NGINX_QA_RESTART_DELAY_SECONDS% seconds...
timeout /t %NGINX_QA_RESTART_DELAY_SECONDS% /nobreak >nul
goto run_server

:server_stopped
echo FastAPI stopped normally.
call :stop_quick_tunnel
endlocal
exit /b 0

:server_failed
call :stop_quick_tunnel
endlocal & exit /b %NGINX_QA_EXIT_CODE%

:launch_failed
set "NGINX_QA_EXIT_CODE=%ERRORLEVEL%"
if "%NGINX_QA_EXIT_CODE%"=="0" set "NGINX_QA_EXIT_CODE=1"
call :stop_bootstrap_server
call :stop_quick_tunnel
endlocal & exit /b %NGINX_QA_EXIT_CODE%

:resolve_quick_tunnel_setting
set "NGINX_QA_QUICK_TUNNEL_ENABLED=%CLOUDFLARED_QUICK_TUNNEL%"
if defined NGINX_QA_QUICK_TUNNEL_ENABLED exit /b 0

set "NGINX_QA_QUICK_TUNNEL_ENABLED=0"
set TELEGRAM_WEBHOOK_URL 2>nul | findstr /i /c:"YOUR_PUBLIC_HOST" /c:"replace-me.example" /c:"https://example.com/" >nul
if not errorlevel 1 set "NGINX_QA_QUICK_TUNNEL_ENABLED=1"
exit /b 0

:start_quick_tunnel
if /i not "%TELEGRAM_WEBHOOK_MODE%"=="sequential" if /i not "%TELEGRAM_WEBHOOK_MODE%"=="parallel" (
    echo TELEGRAM_WEBHOOK_MODE must be sequential or parallel.
    exit /b 1
)

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0cloudflared_quick_tunnel.ps1" -Action Start -Origin "%CLOUDFLARED_QUICK_TUNNEL_ORIGIN%"
if errorlevel 1 exit /b 1

set "NGINX_QA_TUNNEL_URL="
if exist "runtime_state\cloudflared-quick-tunnel.url" set /p NGINX_QA_TUNNEL_URL=<"runtime_state\cloudflared-quick-tunnel.url"
if not defined NGINX_QA_TUNNEL_URL (
    echo Cloudflare Quick Tunnel started without a public URL.
    exit /b 1
)

powershell.exe -NoLogo -NoProfile -Command "$deadline = [DateTime]::UtcNow.AddSeconds(45); while ([DateTime]::UtcNow -lt $deadline) { try { $response = Invoke-WebRequest -Uri ('%NGINX_QA_TUNNEL_URL%/'); if ($response.StatusCode -eq 200) { exit 0 } } catch {}; Start-Sleep -Milliseconds 750 }; exit 1"
if errorlevel 1 (
    echo Cloudflare Quick Tunnel URL did not become publicly reachable.
    exit /b 1
)

set "NGINX_QA_TUNNEL_STARTED=1"
set "TELEGRAM_WEBHOOK_URL=%NGINX_QA_TUNNEL_URL%/api/v1/telegram/agents/%TELEGRAM_WEBHOOK_MODE%"
set "TELEGRAM_WEBHOOK_AUTO_REGISTER=1"
echo Telegram webhook for this run: %TELEGRAM_WEBHOOK_URL%
exit /b 0

:start_bootstrap_server
powershell.exe -NoLogo -NoProfile -Command "$connection = Get-NetTCPConnection -LocalPort 8025 -State Listen -ErrorAction SilentlyContinue; if ($connection) { exit 1 }; exit 0"
if errorlevel 1 (
    echo Port 8025 is already in use; refusing to start a second FastAPI process.
    exit /b 1
)

if not exist "runtime_state" mkdir "runtime_state"
powershell.exe -NoLogo -NoProfile -Command "Start-Process -FilePath '%~dp0.venv\Scripts\python.exe' -ArgumentList 'main.py' -WorkingDirectory '%~dp0' -WindowStyle Hidden -RedirectStandardOutput '%~dp0runtime_state\telegram-bootstrap.out.log' -RedirectStandardError '%~dp0runtime_state\telegram-bootstrap.err.log' | Out-Null"
if errorlevel 1 (
    echo FastAPI bootstrap process could not be started.
    exit /b 1
)
set "NGINX_QA_BOOTSTRAP_STARTED=1"

powershell.exe -NoLogo -NoProfile -Command "$deadline = [DateTime]::UtcNow.AddSeconds(30); while ([DateTime]::UtcNow -lt $deadline) { try { $response = Invoke-WebRequest -Uri 'http://127.0.0.1:8025/' -UseBasicParsing -TimeoutSec 2; if ($response.StatusCode -eq 200) { exit 0 } } catch {}; Start-Sleep -Milliseconds 500 }; exit 1"
if errorlevel 1 (
    echo FastAPI bootstrap process did not become ready on port 8025.
    call :stop_bootstrap_server
    exit /b 1
)
exit /b 0

:stop_bootstrap_server
if not "%NGINX_QA_BOOTSTRAP_STARTED%"=="1" exit /b 0
powershell.exe -NoLogo -NoProfile -Command "$connection = Get-NetTCPConnection -LocalPort 8025 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1; if ($connection) { $process = Get-CimInstance Win32_Process -Filter ('ProcessId = ' + $connection.OwningProcess) -ErrorAction SilentlyContinue; if ($process -and $process.CommandLine -match 'main\.py') { Stop-Process -Id $connection.OwningProcess -Force -ErrorAction Stop } }; $deadline = [DateTime]::UtcNow.AddSeconds(10); while ([DateTime]::UtcNow -lt $deadline -and (Get-NetTCPConnection -LocalPort 8025 -State Listen -ErrorAction SilentlyContinue)) { Start-Sleep -Milliseconds 250 }; if (Get-NetTCPConnection -LocalPort 8025 -State Listen -ErrorAction SilentlyContinue) { exit 1 }"
if errorlevel 1 exit /b 1
set "NGINX_QA_BOOTSTRAP_STARTED=0"
exit /b 0

:stop_quick_tunnel
if not "%NGINX_QA_TUNNEL_STARTED%"=="1" exit /b 0
echo Stopping Cloudflare Quick Tunnel...
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0cloudflared_quick_tunnel.ps1" -Action Stop
set "NGINX_QA_TUNNEL_STARTED=0"
exit /b 0
