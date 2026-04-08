# Onboarding Operativo BARCA

Questa guida serve per riprendere in mano il progetto velocemente o passarne il controllo a chi lo vede per la prima volta.

## 1. Mappa rapida

- Runtime operativo DB-only: `app.py`
- Bootstrap DB da raw file: `populate_db_from_raw.py`
- Import storico shop-level: `populate_db_from_shop_reports.py`
- Sync storico dettaglio ordini: `sync_order_detail_history.py`
- Backend/UI FastAPI: `enterprise_ui.py`
- Sync PostgreSQL: `db_sync.py`
- Schema DB: `db/schema.sql`
- Hardening ruoli/permessi: `db/hardening_v1.sql`
- Regression suite: cartella `tests/`

## 2. PostgreSQL: quello che devi sapere subito

Setup visto in questo repository:

- Container Docker atteso: `barca-postgres`
- Host applicativo tipico: `localhost`
- Porta tipica: `5432`
- Database applicativo tipico: `barca`
- Utente login tipico: `barca_user`

Variabili ambiente usate dal codice:

- `BARCA_DB_HOST`
- `BARCA_DB_PORT`
- `BARCA_DB_NAME`
- `BARCA_DB_USER`
- `BARCA_DB_PASSWORD`
- `BARCA_DB_SSLMODE`
- `BARCA_DB_CONNECT_TIMEOUT`

Nota importante:

- la password non va hardcodata nel repository
- se stai usando il container Docker locale, recuperala dai parametri del container

Comando utile per leggere le credenziali correnti del container:

```powershell
docker inspect barca-postgres --format "{{range .Config.Env}}{{println .}}{{end}}"
```

Da li' trovi in genere:

- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

Test rapido autenticazione DB:

```powershell
$env:BARCA_DB_HOST="localhost"
$env:BARCA_DB_PORT="5432"
$env:BARCA_DB_NAME="barca"
$env:BARCA_DB_USER="barca_user"
$env:BARCA_DB_PASSWORD="..."
$env:BARCA_DB_SSLMODE="prefer"
$env:BARCA_DB_CONNECT_TIMEOUT="2"
python -c "import psycopg; from db_sync import get_db_dsn; conn=psycopg.connect(get_db_dsn()); print('OK'); conn.close()"
```

## 3. Ruoli e permessi DB

Nel file `db/hardening_v1.sql` sono definiti anche due ruoli applicativi minimi:

- `barca_app_ro`
  - `NOLOGIN`
  - sola lettura
- `barca_app_rw`
  - `NOLOGIN`
  - lettura/scrittura

Il login effettivo che il progetto usa di default e' `barca_user`.

Quindi:

- `barca_user` e' l'utente di connessione principale nell'ambiente locale visto durante il debug
- `barca_app_ro` e `barca_app_rw` sono ruoli di permesso da usare in setup piu' rigorosi

## 4. Modalita' operative principali

### Bootstrap iniziale da file raw

Usa i CSV/Excel in `input/` e `input/orders/` solo per popolare il DB.

```powershell
python populate_db_from_raw.py --skip-ingest --db-create-schema
```

### Runtime operativo normale

Legge dal DB, calcola distribuzione, ricostruisce ordini dal DB e risincronizza una nuova run.

```powershell
python app.py --sync-db
```

### UI web

```bat
RUN_UI.bat
```

La UI prova anche a leggere automaticamente le credenziali dal container `barca-postgres`.

## 5. Script da ricordare

- `RUN.bat`
  - esegue il runtime principale
- `RUN_POPULATE_DB.bat`
  - bootstrap DB da file raw
- `RUN_UI.bat`
  - avvia la UI FastAPI/uvicorn con check DB
- `RUN_TESTS.bat`
  - esegue la regression suite automatica

## 6. Test e regressioni

La suite automatica copre:

- import/compile smoke dei moduli Python principali
- bootstrap raw su DB temporaneo creato da zero
- runtime `app.py --sync-db` su DB temporaneo
- import storico shop-level
- sync storico dettaglio ordini
- API dashboard
- export XLSX dashboard
- API catalogo
- showcase sync e async
- ingest e quarantine
- stop run UI e lock di concorrenza sui job

Comando standard:

```powershell
python -m unittest discover -s tests -v
```

Oppure:

```bat
RUN_TESTS.bat
```

I test di integrazione:

- creano database PostgreSQL temporanei
- li eliminano automaticamente a fine test
- provano a recuperare le credenziali dal container `barca-postgres` se la shell non e' allineata

## 7. Problemi gia' emersi e gia' corretti

Bug trovati durante il debug profondo:

- `db_sync.py`
  - su DB pulito leggeva view catalogo prima di applicare lo schema
- `db_sync.py`
  - mancava `connect_timeout`, quindi con DB non raggiungibile API/UI potevano restare in attesa troppo a lungo
- `ingest_agent.py`
  - dichiarava supporto `.xls` ma usava `openpyxl`, che quel formato non lo legge
- `catalog_excel.py`
  - workbook non chiuso correttamente in lettura generator-based
- `enterprise_ui.py`
  - runner UI lasciava aperto `proc.stdout`
- `catalog_legacy/barca_catalog_generator.py`
  - uso deprecato di Pillow

## 8. Percorso consigliato per chi arriva nuovo

1. Leggere `README.md`
2. Leggere questa guida operativa
3. Verificare il DB con il comando di autenticazione
4. Eseguire `RUN_TESTS.bat`
5. Se il DB e' vuoto: `python populate_db_from_raw.py --skip-ingest --db-create-schema`
6. Poi eseguire `python app.py --sync-db`
7. Se serve la UI: `RUN_UI.bat`

## 9. Quando qualcosa non torna

Controlli rapidi:

- `git status`
- `python -m unittest discover -s tests -v`
- `python qa_checks.py`
- `docker inspect barca-postgres --format "{{range .Config.Env}}{{println .}}{{end}}"`
- `/api/db/status` dalla UI

Se il DB non risponde:

- controlla che `BARCA_DB_PASSWORD` nella shell coincida con `POSTGRES_PASSWORD` del container
- controlla che il container `barca-postgres` sia avviato
- controlla che `BARCA_DB_NAME` e `BARCA_DB_USER` siano coerenti

## 10. Nota finale

Il progetto ora ha una copertura di regressione molto migliore, ma non esiste la garanzia matematica di “zero bug”.
La strategia corretta e':

- usare la suite ad ogni modifica
- aggiungere test ogni volta che emerge un nuovo bug
- tenere questa guida aggiornata quando cambiano credenziali, container o workflow
