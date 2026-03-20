@echo off
chcp 65001 >nul
echo === BARCA Unified Engine ===
echo 1) Metti i file raw in .\incoming\ (consigliato, ingest automatico):
echo    - report vendite / giacenze / ordini (csv o excel)
echo 2) In alternativa metti i file gia' standard in .\input\ come:
echo    - sales_YYYY-MM.csv  (ANALISI ARTICOLI)
echo    - stock_YYYY-MM.csv  (SITUAZIONE ARTICOLI)
echo 3) Opzionale: metti i file ordini in .\input\orders\ (^*_sd_1/2/3.csv)
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
