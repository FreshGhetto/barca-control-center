# Backup DB

Questa cartella contiene i dump PostgreSQL usati dal progetto.

Struttura:
- `restore-points/`: dump importanti da tenere distinti e facili da riconoscere
- `incoming/`: dump ricevuti dall'esterno prima di essere applicati o archiviati
- `YYYY-MM-DD/`: backup operativi creati dagli script

Restore points attuali:
- `restore-points/2026-04-26_pre-update/`
  - stato del DB precedente agli aggiornamenti del 29 aprile 2026
- `restore-points/2026-04-29_pre-restore/`
  - backup di sicurezza del DB fatto subito prima del rollback del 29 aprile 2026

Regole:
- salva qui i dump creati da `ops/backup_barca.ps1`
- usa file `.dump` o `.backup`
- non tenere dump dentro `db/`, che deve restare riservata a schema e script SQL versionati
- la cartella e' ignorata da Git, salvo questo file di documentazione
