@echo off
chcp 65001 >nul
echo === BARCA Stock Allocation Engine ===
echo 1) Metti i file in .\input\ come:
echo    - sales_YYYY-MM.csv  (ANALISI ARTICOLI)
echo    - stock_YYYY-MM.csv  (SITUAZIONE ARTICOLI)
echo.

REM Install dipendenze (se gia' presenti non fa danni)
python -m pip install -r requirements.txt
IF %ERRORLEVEL% NEQ 0 (
  echo.
  echo ERRORE: Python/pip non disponibile. 
  echo Suggerimento: usa il python del tuo venv, es:
  echo   C:\PythonEnvs\_Envs\ml_env\Scripts\python -m pip install -r requirements.txt
  echo   C:\PythonEnvs\_Envs\ml_env\Scripts\python app.py
  pause
  exit /b 1
)

python app.py
pause
