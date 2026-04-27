@echo off
setlocal EnableExtensions DisableDelayedExpansion
chcp 65001 >nul
cd /d "%~dp0"

set "DOCKER_CONTAINER=barca-control-center-db-1"
set "BARCA_DB_HOST=127.0.0.1"
set "BARCA_DB_PORT=5432"
set "BARCA_DB_NAME=barca"
set "BARCA_DB_USER=postgres"
set "BARCA_DB_PASSWORD=postgres"
set "DOCKER_POSTGRES_DB="
set "DOCKER_POSTGRES_USER="
set "DOCKER_POSTGRES_PASSWORD="
set "PYTHON_MODE="
set "PYTHON_EXE="
set "PYTHON_LABEL="
set "UI_PORT_AUTOSELECTED="
set "UI_PORT_WAS_SET=1"

if not defined BARCA_UI_PORT (
  set "BARCA_UI_PORT=8080"
  set "UI_PORT_WAS_SET=0"
)

call :resolve_python
if errorlevel 1 (
  echo ERRORE: interprete Python non trovato.
  echo Cercati: .venv\Scripts\python.exe, venv\Scripts\python.exe, ..\..\..\Scripts\python.exe, python, py -3
  pause
  exit /b 1
)

call :hydrate_db_from_docker
if not defined BARCA_UI_KEEP_OTHERS (
  call :stop_existing_ui_processes
)

call :resolve_ui_port
if errorlevel 1 (
  echo ERRORE: nessuna porta disponibile per la UI.
  echo Porta richiesta: %BARCA_UI_PORT%
  echo Se vuoi forzarne una diversa, imposta BARCA_UI_PORT prima di avviare lo script.
  pause
  exit /b 1
)

echo === BARCA Control Center ===
echo Avvio interfaccia web su http://localhost:%BARCA_UI_PORT%
if defined UI_PORT_AUTOSELECTED echo Nota: porta 8080 occupata, uso automaticamente la porta %BARCA_UI_PORT%.
echo.

if not defined BARCA_DB_HOST set "BARCA_DB_HOST=localhost"
if not defined BARCA_DB_PORT set "BARCA_DB_PORT=5432"
if not defined BARCA_DB_SSLMODE set "BARCA_DB_SSLMODE=prefer"

if not defined BARCA_DB_KEEP_ENV (
  if defined DOCKER_POSTGRES_DB if defined DOCKER_POSTGRES_USER if defined DOCKER_POSTGRES_PASSWORD (
    if /I "%BARCA_DB_HOST%"=="localhost" call :apply_docker_credentials
    if /I "%BARCA_DB_HOST%"=="127.0.0.1" call :apply_docker_credentials
  )
)

set "MISSING_DB=0"
if not defined BARCA_DB_NAME set "MISSING_DB=1"
if not defined BARCA_DB_USER set "MISSING_DB=1"
if not defined BARCA_DB_PASSWORD set "MISSING_DB=1"

set "MISSING_DB="
if not defined BARCA_DB_HOST set "MISSING_DB=1"
if not defined BARCA_DB_PORT set "MISSING_DB=1"
if not defined BARCA_DB_NAME set "MISSING_DB=1"
if not defined BARCA_DB_USER set "MISSING_DB=1"
if not defined BARCA_DB_PASSWORD set "MISSING_DB=1"

if defined MISSING_DB (
  echo.
  echo ERRORE: configurazione DB incompleta.
  echo Variabili richieste: BARCA_DB_HOST, BARCA_DB_NAME, BARCA_DB_USER, BARCA_DB_PASSWORD
  echo In alternativa avvia il container Docker "%DOCKER_CONTAINER%" con POSTGRES_DB, POSTGRES_USER e POSTGRES_PASSWORD.
  pause
  exit /b 1
)

echo Database target: %BARCA_DB_HOST%:%BARCA_DB_PORT%\%BARCA_DB_NAME% ^(%BARCA_DB_USER%^)
call :wait_for_db
if errorlevel 1 (
  echo.
  echo ERRORE: PostgreSQL non raggiungibile su %BARCA_DB_HOST%:%BARCA_DB_PORT%.
  echo Controlla Docker, firewall o le variabili BARCA_DB_*.
  pause
  exit /b 1
)

echo Interprete Python: %PYTHON_LABEL%
call :run_python -m pip install -r requirements.txt
if %ERRORLEVEL% NEQ 0 (
  echo.
  echo ERRORE: install dipendenze fallita.
  pause
  exit /b 1
)

call :validate_db_auth
if errorlevel 1 (
  call :retry_with_docker_credentials
  if errorlevel 1 (
    echo.
    echo ERRORE: autenticazione PostgreSQL fallita per %BARCA_DB_USER%@%BARCA_DB_HOST%:%BARCA_DB_PORT%\%BARCA_DB_NAME%.
    echo Verifica BARCA_DB_NAME, BARCA_DB_USER e soprattutto BARCA_DB_PASSWORD.
    if defined DOCKER_POSTGRES_PASSWORD (
      echo Il container "%DOCKER_CONTAINER%" espone credenziali diverse da quelle attualmente in uso.
    )
    pause
    exit /b 1
  )
)

call :run_python -m uvicorn enterprise_ui:app --host 0.0.0.0 --port %BARCA_UI_PORT%
pause
exit /b %ERRORLEVEL%

:hydrate_db_from_docker
docker inspect %DOCKER_CONTAINER% >nul 2>&1
if errorlevel 1 (
  goto :eof
)

set "DOCKER_RUNNING="
for /f "usebackq delims=" %%S in (`docker inspect -f "{{.State.Running}}" %DOCKER_CONTAINER% 2^>nul`) do set "DOCKER_RUNNING=%%S"
if /I not "%DOCKER_RUNNING%"=="true" (
  echo Avvio container PostgreSQL "%DOCKER_CONTAINER%"...
  docker start %DOCKER_CONTAINER% >nul 2>&1
)

for /f "usebackq tokens=1,* delims==" %%A in (`docker inspect %DOCKER_CONTAINER% --format "{{range .Config.Env}}{{println .}}{{end}}" 2^>nul`) do (
  if /I "%%A"=="POSTGRES_DB" (
    set "DOCKER_POSTGRES_DB=%%B"
    if not defined BARCA_DB_NAME set "BARCA_DB_NAME=%%B"
  )
  if /I "%%A"=="POSTGRES_USER" (
    set "DOCKER_POSTGRES_USER=%%B"
    if not defined BARCA_DB_USER set "BARCA_DB_USER=%%B"
  )
  if /I "%%A"=="POSTGRES_PASSWORD" (
    set "DOCKER_POSTGRES_PASSWORD=%%B"
    if not defined BARCA_DB_PASSWORD set "BARCA_DB_PASSWORD=%%B"
  )
)
goto :eof

:apply_docker_credentials
if /I "%BARCA_DB_NAME%"=="%DOCKER_POSTGRES_DB%" if /I "%BARCA_DB_USER%"=="%DOCKER_POSTGRES_USER%" if "%BARCA_DB_PASSWORD%"=="%DOCKER_POSTGRES_PASSWORD%" goto :eof
echo Uso credenziali PostgreSQL lette dal container Docker "%DOCKER_CONTAINER%".
set "BARCA_DB_NAME=%DOCKER_POSTGRES_DB%"
set "BARCA_DB_USER=%DOCKER_POSTGRES_USER%"
set "BARCA_DB_PASSWORD=%DOCKER_POSTGRES_PASSWORD%"
goto :eof

:stop_existing_ui_processes
echo Controllo istanze UI gia' attive...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$items = Get-CimInstance Win32_Process | Where-Object { ($_.CommandLine -like '*uvicorn enterprise_ui:app*') -and ($_.ProcessId -ne $PID) }; if(-not $items){ exit 0 }; foreach($p in $items){ try { Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop } catch {} }" >nul
timeout /t 1 >nul
goto :eof

:wait_for_db
powershell -NoProfile -ExecutionPolicy Bypass -Command "$hostName=$env:BARCA_DB_HOST; $port=[int]$env:BARCA_DB_PORT; for($i=0; $i -lt 20; $i++){ try { $client = New-Object Net.Sockets.TcpClient; $iar = $client.BeginConnect($hostName, $port, $null, $null); if($iar.AsyncWaitHandle.WaitOne(1000, $false) -and $client.Connected){ $client.EndConnect($iar); $client.Close(); exit 0 }; $client.Close() } catch {}; Start-Sleep -Seconds 2 }; exit 1" >nul
exit /b %ERRORLEVEL%

:resolve_ui_port
call :is_port_free %BARCA_UI_PORT%
if not errorlevel 1 exit /b 0

if "%UI_PORT_WAS_SET%"=="1" exit /b 1

for %%P in (8081 8082 8083 8084 8085 8086 8087 8088 8089 8090) do (
  call :is_port_free %%P
  if not errorlevel 1 (
    set "BARCA_UI_PORT=%%P"
    set "UI_PORT_AUTOSELECTED=1"
    exit /b 0
  )
)
exit /b 1

:is_port_free
powershell -NoProfile -ExecutionPolicy Bypass -Command "$port=[int]'%~1'; try { $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Any, $port); $listener.Start(); $listener.Stop(); exit 0 } catch { exit 1 }" >nul
exit /b %ERRORLEVEL%

:validate_db_auth
call :run_python -c "import psycopg; from db_sync import get_db_dsn; conn=psycopg.connect(get_db_dsn()); conn.close()"
exit /b %ERRORLEVEL%

:retry_with_docker_credentials
if not defined DOCKER_POSTGRES_DB exit /b 1
if not defined DOCKER_POSTGRES_USER exit /b 1
if not defined DOCKER_POSTGRES_PASSWORD exit /b 1
if /I "%BARCA_DB_NAME%"=="%DOCKER_POSTGRES_DB%" if /I "%BARCA_DB_USER%"=="%DOCKER_POSTGRES_USER%" if "%BARCA_DB_PASSWORD%"=="%DOCKER_POSTGRES_PASSWORD%" exit /b 1

echo Credenziali DB correnti rifiutate: provo quelle del container Docker "%DOCKER_CONTAINER%"...
set "BARCA_DB_NAME=%DOCKER_POSTGRES_DB%"
set "BARCA_DB_USER=%DOCKER_POSTGRES_USER%"
set "BARCA_DB_PASSWORD=%DOCKER_POSTGRES_PASSWORD%"
call :validate_db_auth
if errorlevel 1 exit /b 1
echo Credenziali Docker applicate con successo.
exit /b 0

:resolve_python
for %%P in ("%~dp0.venv\Scripts\python.exe" "%~dp0venv\Scripts\python.exe" "%~dp0..\..\..\Scripts\python.exe") do (
  if exist "%%~fP" (
    set "PYTHON_MODE=exe"
    set "PYTHON_EXE=%%~fP"
    set "PYTHON_LABEL=%%~fP"
    exit /b 0
  )
)

where python >nul 2>&1
if not errorlevel 1 (
  set "PYTHON_MODE=python"
  set "PYTHON_LABEL=python"
  exit /b 0
)

where py >nul 2>&1
if not errorlevel 1 (
  set "PYTHON_MODE=py"
  set "PYTHON_LABEL=py -3"
  exit /b 0
)

exit /b 1

:run_python
if /I "%PYTHON_MODE%"=="exe" (
  "%PYTHON_EXE%" %*
  exit /b %ERRORLEVEL%
)
if /I "%PYTHON_MODE%"=="python" (
  python %*
  exit /b %ERRORLEVEL%
)
if /I "%PYTHON_MODE%"=="py" (
  py -3 %*
  exit /b %ERRORLEVEL%
)
exit /b 9009
