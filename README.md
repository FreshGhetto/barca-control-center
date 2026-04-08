# BARCA Unified Engine (Distribuzione + Ordini)

## Struttura cartelle
- `incoming/` → cartella drop raw (`csv`/`xlsx`) usata solo per popolare il DB
- `input/` → file mensili intermedi (`sales_YYYY-MM.csv`, `stock_YYYY-MM.csv`) usati solo nel bootstrap DB
- `input/orders/` → (opzionale) CSV progetto ordini (`*_sd_1.csv`, `*_sd_2.csv`, `*_sd_3.csv`)
- `input/orders/history_detail/` → (opzionale) report `ANALISI ARTICOLI` con `Raffronta con venduto nel periodo` per arricchire storico ordini con marchio, colore, materiale e venduto periodo
- `output/` → export diagnostici del motore DB-only (`clean_*`, `suggested_transfers.csv`, `features_after.csv`)
- `config/` → configurazioni (`lista-negozi.xlsx`, opzionale `lista-negozi_integrato.xlsx`)
- `data/raw_original/` → copie archiviate dei file raw originali con nome sorgente
- `analysis.py` → sostituisce il vecchio notebook con analisi riusabili dal motore e CLI diagnostica DB-first

Capacita' negozi:
- `config/lista-negozi_integrato.xlsx` e' la configurazione usata dal motore se presente.
- `config/shop_capacity_overrides.csv` permette di forzare negozi specifici (es. `CO`) quando il file sorgente ha righe sporche o incoerenti.
- Per rigenerarla dai file capacita' negozi usa:

```bash
python ops/rebuild_shop_capacity_config.py
```

## Cosa fa
- Se devi alimentare il DB da file, usa `populate_db_from_raw.py`:
  - esegue ingest raw
  - parse di vendite/stock
  - armonizzazione clean inputs
  - bootstrap opzionale del modulo ordini
  - sync completa nel DB PostgreSQL
- Il runtime operativo `app.py` e la UI lavorano poi **solo dal DB**:
  - leggono snapshot sales/stock gia' salvati
  - leggono output ordini gia' salvati
  - producono trasferimenti, diagnostica e nuova run DB
- L'ingest da raw:
  - riconosce automaticamente il tipo file (vendite, stock, ordini sd_1/2/3/4, listini/ricarichi, storico articoli con venduto periodo)
  - converte eventuali Excel in CSV
  - rinomina in formato standard e deposita in `input/` e `input/orders/`
  - manda i file non riconosciuti in `incoming/_quarantine`
  - traccia report in `output/ingest/ingest_report_latest.json`
- Nel bootstrap DB legge 2 CSV mensili esportati dal gestionale:
  - `sales_YYYY-MM.csv` (report **ANALISI ARTICOLI**)
  - `stock_YYYY-MM.csv` (report **SITUAZIONE ARTICOLI**)
- Pulisce i dati (numeri italiani, sigle negozi) e genera:
  - `output/clean_sales.csv`
  - `output/clean_articles.csv`
- Calcola trasferimenti consigliati con motore **ibrido AI + algoritmico**:
  - domanda prevista = blend tra modello AI (ridge out-of-fold) e formule business
  - formule business: vendite recenti, sellout, copertura stock, service level per fascia
  - priorità **fasce alte**
  - donatori scelti tra fasce più basse e **vendite locali più basse**
  - negozi riceventi ammessi solo se l'articolo ha gia' avuto venduto locale
  - ranking riceventi basato su fascia, venduto locale, buchi taglie core e stock basso
  - micro-movimenti **anche 1 paio**
  - rispetto della **capacità negozio** (se disponibile in config) con buffer operativo
    per evitare saturazione completa del magazzino
  - protezione stockout sui donatori (mantiene una scorta minima)
  - limiti operativi realistici di movimentazione inbound/outbound per negozio
    in ogni run (evita piani teorici irrealizzabili)
  - pianificazione spedizioni coerente con organizzazione logistica:
    - mar: `BS-LN`
    - mer: `RI-BO`, `AU-MC`, `NV-ME2-VR`
    - gio: `OR-AR`, `CO-TV`, `PD-CA`
    - ven: `MI`, `SM`
    - `RM`/`EU`: corriere + bancali ogni 2 giorni
    - `PM`: come `RM/EU` ma con consolidamento minimo
    - `SD`: spedizione a soglia quantità pronta
  - se non si riesce a completare una run “senza buchi” (taglie centrali),
    sposta l’eccesso verso **outlet** (fascia 6–7)
- Esclude i fittizi: `MR, MP, SP, SPW`
- Regole speciali:
  - `M4` = magazzino vero (donatore)
  - `WEB` = canale online (riceve solo da M4)
- Modulo ordini integrato (opzionale):
  - rileva automaticamente i bundle stagionali `*_sd_1/2/3.csv`
  - usa anche i report storici `ANALISI ARTICOLI` con `Raffronta con venduto nel periodo` per arricchire `fact_order_source`
  - produce previsione acquisti **modello matematico** per stagione corrente e continuativa
  - se presenti 3 stagioni continuative e `scikit-learn` installato, calcola anche **RF + Ibrido**
  - logga tutti gli step in `output/orders/orders_run_log.txt`

## Come si usa (Windows)
Bootstrap DB da raw:
1. Metti i raw in `incoming/` (consigliato): CSV/Excel esportati dal gestionale.
2. Esegui `python populate_db_from_raw.py --db-create-schema` la prima volta, oppure doppio click su `RUN_POPULATE_DB.bat`.
3. Verifica la run `raw_input_sync` nel DB.

Runtime operativo DB-only:
1. Doppio click su `RUN.bat` oppure esegui `python app.py --sync-db`.
2. Il motore legge solo snapshot e ordini dal DB.
3. Leggi risultati in `output/` e nella dashboard.

Interfaccia enterprise web (non Streamlit):
1. Doppio click su `RUN_UI.bat` oppure avvia da terminale.
2. Apri `http://localhost:8080`.
3. Avvia run pipeline dalla dashboard e monitora stato/output/DB in tempo reale.
4. `Developer Mode` (toggle in alto) mostra comando completo, log raw e dettagli debug.

CLI utile:

```bash
python populate_db_from_raw.py --db-create-schema
python populate_db_from_raw.py --skip-orders
python populate_db_from_raw.py --skip-ingest --sales-file input/sales_2026-03.csv --stock-file input/stock_2026-03.csv
python app.py --source-db
python app.py --source-db --source-db-run-id <uuid_run>
python app.py --orders-source-db --orders-source-db-run-id <uuid_run>
python app.py --source-db --orders-source-db --sync-db
python -m uvicorn enterprise_ui:app --host 0.0.0.0 --port 8080
python app.py --skip-orders
python app.py --sync-db
python app.py --sync-db --db-create-schema
python db_sync.py --create-schema
python db_sync.py
python analysis.py --article 59/5040CTM
```

Modalita' DB-first (pipeline guidata dal database):
- usa `--source-db` per leggere `clean_sales/clean_articles` dall'ultimo snapshot DB completato
- usa `--source-db-run-id <uuid>` per forzare una run specifica come sorgente
- usa `--orders-source-db` per ricostruire gli output del modulo ordini dalle tabelle DB
- usa `--orders-source-db-run-id <uuid>` per forzare una run specifica ordini come sorgente
- la sync DB ora salva anche le sorgenti ordini in `fact_order_source` e `fact_order_source_size`
- `analysis.py` legge anch'esso solo dal DB e genera diagnostiche riusabili (`article_shop_signals.csv`, `stock_integrity_report.csv`)

## Database PostgreSQL (free, no license)
Il progetto ora supporta sync su PostgreSQL (open source, senza licenza commerciale).

Variabili ambiente richieste:
- `BARCA_DB_HOST`
- `BARCA_DB_PORT` (default `5432`)
- `BARCA_DB_NAME`
- `BARCA_DB_USER`
- `BARCA_DB_PASSWORD`
- `BARCA_DB_SSLMODE` (opzionale, default `prefer`)

Esempio Windows PowerShell:

```powershell
$env:BARCA_DB_HOST="localhost"
$env:BARCA_DB_PORT="5432"
$env:BARCA_DB_NAME="barca"
$env:BARCA_DB_USER="barca_user"
$env:BARCA_DB_PASSWORD="********"
$env:BARCA_DB_SSLMODE="prefer"
python db_sync.py --create-schema
python db_sync.py
```

Per eseguire pipeline + sync DB in un unico run:

```bash
python app.py --sync-db --db-create-schema
```

Hardening DB enterprise (consigliato in produzione):

```sql
-- DataGrip: apri Query Console su database "barca"
-- poi esegui il contenuto di db/hardening_v1.sql
```

Il file è: `db/hardening_v1.sql`

## Regression Suite
Il repository include ora una suite automatica `unittest` che copre:
- compile/import smoke dei moduli Python principali
- bootstrap raw su database PostgreSQL temporaneo creato da zero
- runtime DB-only con `app.py --sync-db`
- import storico shop-level e sync storico dettaglio ordini
- API FastAPI per dashboard, export XLSX, catalogo e showcase

Esecuzione:

```powershell
python -m unittest discover -s tests -v
```

Oppure su Windows:

```bat
RUN_TESTS.bat
```

Note operative:
- i test di integrazione usano PostgreSQL e creano database temporanei dedicati, poi li eliminano a fine esecuzione
- se le variabili `BARCA_DB_*` non sono allineate, la suite prova a recuperare automaticamente le credenziali dal container Docker `barca-postgres`

Guida operativa completa per onboarding, DB, utenti, ruoli, container, script e recovery:

- `docs/onboarding_operativo_barca.md`

Hardening DB v2 (retention/maintenance + runbook partizionamento):
- esegui `db/hardening_v2.sql` in DataGrip
- include funzioni:
  - `sp_retention_preview(keep_last_runs)`
  - `sp_retention_apply(keep_last_runs)`
  - viste `vw_table_storage_mb`, `vw_run_fact_counts`, `vw_partition_candidates`

Backup/restore automatico (Windows + Docker):

```powershell
powershell -File .\ops\backup_barca.ps1
powershell -File .\ops\restore_barca.ps1 -BackupFile "C:\BARCA\backups\2026-03-19\barca_barca_YYYYMMDD_HHMMSS.dump"
powershell -File .\ops\install_daily_backup_task.ps1 -RunAt "02:30"
```

## Output principali
- `suggested_transfers.csv` → trasferimenti consigliati aggregati per spedizione
- `suggested_transfers_detailed.csv` → dettaglio micro-mosse (1 riga = 1 paio/taglia)
- `alignment_report.csv` → interventi automatici di allineamento sales/stock (se necessari)
- `shipment_plan.csv` → piano spedizioni con data prevista, policy e stato consolidamento
- `shipment_summary.csv` → riepilogo operativo giornaliero per tratta/policy
- `features_after.csv` → stato stock post-simulazione + info fascia/demand/capacità
- `article_shop_signals.csv` → segnali decisionali sorgente/destinazione usati dall'allocatore
- `stock_integrity_report.csv` → controllo coerenza taglie/giacenza/consegnato per articolo-negozio
- `demand_diagnostics.csv` → diagnostica del modello ibrido (DemandRule, DemandAI, blend, DemandHybrid)
- `ingest/ingest_report_latest.json` → report ingest (classificazione, routing, errori)
- `ingest/ingest_report_latest.csv` → dettaglio file processati/quarantena
- `orders/orders_summary.json` → riepilogo esecuzione modulo ordini
- `orders/orders_run_log.txt` → log passo-passo del modulo ordini
- `orders/orders_current_previsione_math.csv` → acquisti futuri continuativi (corrente)
- `orders/orders_continuativa_previsione_math.csv` → acquisti stagione continuativa (math)
- `orders/orders_continuativa_previsione_rf.csv` → previsione RF (se disponibile)
- `orders/orders_continuativa_previsione_ibrida.csv` → previsione ibrida (se disponibile)

## Note
Se il tuo Python non ha le librerie, `RUN.bat` prova a installarle da `requirements.txt`.

## QA rapida
Per validare automaticamente coerenza dati e vincoli operativi dopo ogni run:

```bash
python qa_checks.py
```

Il report viene salvato in `output/qa_report.json`.
