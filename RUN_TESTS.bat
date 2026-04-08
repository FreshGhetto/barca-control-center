@echo off
setlocal EnableExtensions DisableDelayedExpansion
chcp 65001 >nul
cd /d "%~dp0"

set "PYTHON_EXE="
for %%P in ("%~dp0.venv\Scripts\python.exe" "%~dp0venv\Scripts\python.exe" "%~dp0..\..\..\Scripts\python.exe") do (
  if exist "%%~fP" (
    set "PYTHON_EXE=%%~fP"
    goto :run_tests
  )
)

where python >nul 2>&1
if not errorlevel 1 (
  set "PYTHON_EXE=python"
  goto :run_tests
)

where py >nul 2>&1
if not errorlevel 1 (
  set "PYTHON_EXE=py -3"
  goto :run_tests
)

echo ERRORE: interprete Python non trovato.
exit /b 1

:run_tests
echo Eseguo regression suite BARCA...
call %PYTHON_EXE% -m unittest discover -s tests -v
exit /b %ERRORLEVEL%
