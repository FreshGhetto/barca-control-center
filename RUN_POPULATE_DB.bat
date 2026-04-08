@echo off
setlocal EnableExtensions DisableDelayedExpansion
chcp 65001 >nul
cd /d "%~dp0"
set "PYTHON_MODE="
set "PYTHON_EXE="
set "PYTHON_LABEL="

echo === BARCA DB Bootstrap ===
echo Modalita' raw -> DB.
echo I file CSV/Excel vengono usati solo per popolare PostgreSQL.
echo Il motore operativo continuera' poi a lavorare solo dal DB.
echo.

call :resolve_python
if errorlevel 1 (
  echo ERRORE: interprete Python non trovato.
  echo Cercati: .venv\Scripts\python.exe, venv\Scripts\python.exe, ..\..\..\Scripts\python.exe, python, py -3
  pause
  exit /b 1
)

echo Interprete Python: %PYTHON_LABEL%
call :run_python -m pip install -r requirements.txt
IF %ERRORLEVEL% NEQ 0 (
  echo.
  echo ERRORE: Python/pip non disponibile.
  echo Suggerimento: usa il python del tuo venv, es:
  echo   C:\PythonEnvs\_Envs\ml_env\Scripts\python -m pip install -r requirements.txt
  echo   C:\PythonEnvs\_Envs\ml_env\Scripts\python populate_db_from_raw.py --db-create-schema
  pause
  exit /b 1
)

call :run_python populate_db_from_raw.py --db-create-schema
pause
exit /b %ERRORLEVEL%

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
