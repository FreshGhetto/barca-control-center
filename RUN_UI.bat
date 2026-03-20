@echo off
setlocal EnableExtensions DisableDelayedExpansion
chcp 65001 >nul
cd /d "%~dp0"

set "DOCKER_CONTAINER=barca-postgres"

echo === BARCA Control Center ===
echo Avvio interfaccia web su http://localhost:8080
echo.

if not defined BARCA_DB_HOST set "BARCA_DB_HOST=localhost"
if not defined BARCA_DB_PORT set "BARCA_DB_PORT=5432"
if not defined BARCA_DB_SSLMODE set "BARCA_DB_SSLMODE=prefer"

set "MISSING_DB=0"
if not defined BARCA_DB_NAME set "MISSING_DB=1"
if not defined BARCA_DB_USER set "MISSING_DB=1"
if not defined BARCA_DB_PASSWORD set "MISSING_DB=1"

if "%MISSING_DB%"=="1" (
  echo Configurazione DB non completa: provo a leggere i parametri da Docker...
  call :hydrate_db_from_docker
)

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

python -m pip install -r requirements.txt
if %ERRORLEVEL% NEQ 0 (
  echo.
  echo ERRORE: install dipendenze fallita.
  pause
  exit /b 1
)

python -m uvicorn enterprise_ui:app --host 0.0.0.0 --port 8080
pause
exit /b %ERRORLEVEL%

:hydrate_db_from_docker
docker inspect %DOCKER_CONTAINER% >nul 2>&1
if errorlevel 1 (
  echo Container "%DOCKER_CONTAINER%" non trovato: uso solo le variabili ambiente disponibili.
  goto :eof
)

set "DOCKER_RUNNING="
for /f "usebackq delims=" %%S in (`docker inspect -f "{{.State.Running}}" %DOCKER_CONTAINER% 2^>nul`) do set "DOCKER_RUNNING=%%S"
if /I not "%DOCKER_RUNNING%"=="true" (
  echo Avvio container PostgreSQL "%DOCKER_CONTAINER%"...
  docker start %DOCKER_CONTAINER% >nul 2>&1
)

for /f "usebackq tokens=1,* delims==" %%A in (`docker inspect %DOCKER_CONTAINER% --format "{{range .Config.Env}}{{println .}}{{end}}" 2^>nul`) do (
  if /I "%%A"=="POSTGRES_DB" if not defined BARCA_DB_NAME set "BARCA_DB_NAME=%%B"
  if /I "%%A"=="POSTGRES_USER" if not defined BARCA_DB_USER set "BARCA_DB_USER=%%B"
  if /I "%%A"=="POSTGRES_PASSWORD" if not defined BARCA_DB_PASSWORD set "BARCA_DB_PASSWORD=%%B"
)
goto :eof

:wait_for_db
powershell -NoProfile -ExecutionPolicy Bypass -Command "$hostName=$env:BARCA_DB_HOST; $port=[int]$env:BARCA_DB_PORT; for($i=0; $i -lt 20; $i++){ try { $client = New-Object Net.Sockets.TcpClient; $iar = $client.BeginConnect($hostName, $port, $null, $null); if($iar.AsyncWaitHandle.WaitOne(1000, $false) -and $client.Connected){ $client.EndConnect($iar); $client.Close(); exit 0 }; $client.Close() } catch {}; Start-Sleep -Seconds 2 }; exit 1" >nul
exit /b %ERRORLEVEL%
