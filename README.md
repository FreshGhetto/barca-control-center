# BARCA Stock Allocation Engine (v1)

## Struttura cartelle
- `input/` → file mensili in ingresso (`sales_YYYY-MM.csv`, `stock_YYYY-MM.csv`)
- `output/` → file generati dal motore (`clean_*`, `suggested_transfers.csv`, `features_after.csv`)
- `config/` → configurazioni (`lista-negozi.xlsx`, opzionale `lista-negozi_integrato.xlsx`)
- `data/raw_original/` → copie archiviate dei file raw originali con nome sorgente

## Cosa fa
- Legge 2 CSV mensili esportati dal gestionale:
  - `sales_YYYY-MM.csv` (report **ANALISI ARTICOLI**)
  - `stock_YYYY-MM.csv` (report **SITUAZIONE ARTICOLI**)
- Pulisce i dati (numeri italiani, sigle negozi) e genera:
  - `output/clean_sales.csv`
  - `output/clean_articles.csv`
- Calcola trasferimenti consigliati con motore **ibrido AI + algoritmico**:
  - domanda prevista = blend tra modello AI (ridge out-of-fold) e formule business
  - formule business: vendite recenti, sellout, copertura stock, service level per fascia
  - priorità **fasce alte**
  - donatori scelti tra fasce più basse e **vendite nel periodo più basse**
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

## Come si usa (Windows)
1. Metti i file in `input/` con questi nomi:
   - `sales_YYYY-MM.csv`
   - `stock_YYYY-MM.csv`
2. Doppio click su `RUN.bat`
3. Leggi i risultati in `output/`

## Output principali
- `suggested_transfers.csv` → trasferimenti consigliati aggregati per spedizione
- `suggested_transfers_detailed.csv` → dettaglio micro-mosse (1 riga = 1 paio/taglia)
- `alignment_report.csv` → interventi automatici di allineamento sales/stock (se necessari)
- `shipment_plan.csv` → piano spedizioni con data prevista, policy e stato consolidamento
- `shipment_summary.csv` → riepilogo operativo giornaliero per tratta/policy
- `features_after.csv` → stato stock post-simulazione + info fascia/demand/capacità
- `demand_diagnostics.csv` → diagnostica del modello ibrido (DemandRule, DemandAI, blend, DemandHybrid)

## Note
Se il tuo Python non ha le librerie, `RUN.bat` prova a installarle da `requirements.txt`.

## QA rapida
Per validare automaticamente coerenza dati e vincoli operativi dopo ogni run:

```bash
python qa_checks.py
```

Il report viene salvato in `output/qa_report.json`.
