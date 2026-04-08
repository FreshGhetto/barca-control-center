# Formati Input BARCA

Questa nota serve a chi apre il progetto per la prima volta e deve capire quali file il sistema sa gia' riconoscere, dove li deposita e quali concetti logici si aspetta.

## Idea chiave

Il sistema non lavora bene con "qualunque file Excel/CSV".
Lavora bene con famiglie di report note, anche se:

- cambia il nome del file
- cambia la stagione
- cambia il reparto
- cambia il valore di categoria, tipologia, marchio, fornitore, colore, materiale
- alcuni header equivalenti si alternano, per esempio `Marchio` / `Brand` / `Fornitore`

Quello che deve restare coerente e' lo schema logico del report.

## Famiglie raw gestite da `incoming/`

### 1. `sales_report`

- sorgente tipica: `ANALISI ARTICOLI`
- uso: bootstrap vendite/flow articolo-negozio
- destinazione: `input/sales_YYYY-MM.csv`
- segnali usati per riconoscerlo:
  - `ANALISI ARTICOLI`
  - `NEGOZIO` o `SHOP`
  - colonne di flusso come `CON.` / `CONSEGNATO`
  - colonne di venduto come `VEND.` / `VENDUTO`

Campi logici attesi:

- articolo
- negozio
- consegnato
- venduto
- periodo
- sellout
- valori economici

### 2. `stock_report`

- sorgente tipica: `SITUAZIONE ARTICOLI PER NEGOZIO`
- uso: bootstrap stock/taglie
- destinazione: `input/stock_YYYY-MM.csv`
- segnali usati per riconoscerlo:
  - `SITUAZIONE ARTICOLI`
  - `NEG`
  - `GIAC`
  - `VEN`

Campi logici attesi:

- articolo
- descrizione
- negozio
- ricevuto / giacenza / consegnato / venduto
- sellout
- taglie

### 3. `orders_prices`

- sorgente tipica: `Analisi Listini e Ricarichi`
- uso: prezzi acquisto-vendita e fasce articolo
- destinazione: `input/orders/<stagione>_prezzo_acq-ven.csv`
- segnali usati per riconoscerlo:
  - `ANALISI LISTINI E RICARICHI`
  - `PREZZO ACQUIS.`
  - `PREZZO VENDITA`
  - `FASCE PRZ.`

### 4. `orders_sd_1`

- sorgente tipica: `ANALISI ARTICOLI` totale articolo
- uso: bundle ordini, vista per categoria/tipologia/marchio/articolo
- destinazione: `input/orders/<stagione>_sd_1.csv`
- segnali usati per riconoscerlo:
  - `ANALISI ARTICOLI`
  - `TIPOLOGIA`
  - `MARCHIO` oppure `BRAND` oppure `FORNITORE`
  - `ARTICOLO`

Campi logici attesi:

- reparto
- categoria
- tipologia
- marchio/brand/fornitore
- articolo
- consegnato / venduto / periodo / giacenza

### 5. `orders_sd_2`

- sorgente tipica: `ANALISI ARTICOLI` per colore/materiale
- uso: bundle ordini arricchito per colore e materiale
- destinazione: `input/orders/<stagione>_sd_2.csv`
- segnali usati per riconoscerlo:
  - `ANALISI ARTICOLI`
  - `COLORE`
  - `MATERIALE`
  - `ARTICOLO`

### 6. `orders_sd_3`

- sorgente tipica: `Analisi per singola taglia`
- uso: bundle ordini per taglia
- destinazione: `input/orders/<stagione>_sd_3.csv`
- segnali usati per riconoscerlo:
  - `ANALISI PER SINGOLA TAGLIA`
  - `TAG`
  - `TOT`

### 7. `orders_sd_4`

- sorgente tipica: listino/fasce articolo
- uso: bundle ordini con fascia prezzo
- destinazione: `input/orders/<stagione>_sd_4.csv`
- segnali usati per riconoscerlo:
  - `ANALISI ARTICOLI`
  - `FASCE PRZ`
  - `LISTINO` o `PREZZO`

### 8. `orders_history_detail`

- sorgente tipica: `ANALISI ARTICOLI` con `Raffronta con venduto nel periodo`
- uso: storico dettaglio ordini per marchio/colore/materiale
- destinazione: `input/orders/history_detail/<stagione>_articoli_venduto_periodo.csv`
- segnali usati per riconoscerlo:
  - `ANALISI ARTICOLI`
  - `RAFFRONTA CON VENDUTO NEL PERIODO`
  - `COLORE`
  - `MATERIALE`
  - `MARCHIO` oppure `BRAND` oppure `FORNITORE`

## Import storici fuori da `incoming/`

Questi non passano dal drop raw standard ma sono comunque formati noti del progetto.

### Report shop-level storici

- script: `populate_db_from_shop_reports.py`
- input tipico: cartelle tipo `output/tmp/shop_reports_uomo_all_16/`
- formato: `.xls`, `.xlsx`, `.csv`
- struttura logica:
  - stagione nel nome file
  - reparto nel nome file o nel contenuto
  - righe articolo-negozio con stock e venduto

### Storico dettaglio ordini

- script: `sync_order_detail_history.py`
- input tipico: `input/orders/history_detail/`
- formato: report stagionali gia' normalizzati in CSV

## Sinonimi logici importanti

Questi sono i casi piu' comuni da trattare come equivalenti:

- marchio: `MARCHIO`, `BRAND`, in alcuni casi `FORNITORE`
- negozio: `NEGOZIO`, `NEG`, `SHOP`
- venduto: `VEND`, `VENDUTO`
- consegnato: `CON`, `CONSEGNATO`
- giacenza: `GIAC`, `GIACENZA`
- periodo: `PERIO`, `PERIODO`

## Limite corretto da tenere a mente

Il sistema puo' diventare molto tollerante verso varianti dello stesso report.
Non puo' indovinare in modo affidabile un formato completamente nuovo con semantica diversa.

Se arriva una nuova famiglia vera di file, la mossa corretta e':

1. aggiungere una nuova regola di riconoscimento
2. definire lo schema logico canonico
3. aggiungere un test di ingest dedicato
