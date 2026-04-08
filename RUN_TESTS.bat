@echo off
setlocal EnableExtensions DisableDelayedExpansion
chcp 65001 >nul
cd /d "%~dp0"

set "PYTHON_MODE="
set "PYTHON_EXE="
set "PYTHON_LABEL="
for %%P in ("%~dp0.venv\Scripts\python.exe" "%~dp0venv\Scripts\python.exe" "%~dp0..\..\..\Scripts\python.exe") do (
  if exist "%%~fP" (
    set "PYTHON_MODE=exe"
    set "PYTHON_EXE=%%~fP"
    set "PYTHON_LABEL=%%~fP"
    goto :run_tests
  )
)

where python >nul 2>&1
if not errorlevel 1 (
  set "PYTHON_MODE=python"
  set "PYTHON_LABEL=python"
  goto :run_tests
)

where py >nul 2>&1
if not errorlevel 1 (
  set "PYTHON_MODE=py"
  set "PYTHON_LABEL=py -3"
  goto :run_tests
)

echo ERRORE: interprete Python non trovato.
exit /b 1

:run_tests
echo Eseguo regression suite BARCA...
echo Interprete Python: %PYTHON_LABEL%
call :run_python -m unittest discover -s tests -v
exit /b %ERRORLEVEL%

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
