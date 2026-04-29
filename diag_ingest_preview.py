"""
Simula l'ingest senza toccare il DB né spostare file.
Mostra dove ogni file verrebbe salvato e quante righe parserebbe.
"""
import sys, csv, datetime as dt
sys.path.insert(0, '.')
from pathlib import Path
from barca_control_center.ingest_agent import (
    _read_preview, _classify_file, _extract_season_code,
    _extract_report_date, _target_path
)
from barca_control_center.parse_data_v2 import parse_sales, parse_articles

ROOT = Path(r'C:\Users\ufficio2\Desktop\barca\barca-control-center')
FOLDER = Path(r'C:\Users\ufficio2\Desktop\stampe da aggiungere')

SUPPORTED_EXT = {'.csv', '.xlsx', '.xls', '.xlsm'}

print("=" * 80)
print("SIMULAZIONE INGEST (solo lettura, nessun file spostato)")
print("=" * 80)
print()

conflicts = {}

for f in sorted(FOLDER.iterdir()):
    if f.suffix.lower() not in SUPPORTED_EXT:
        continue

    preview = _read_preview(f)
    cls = _classify_file(f, preview)
    kind = cls['kind']
    season = _extract_season_code(preview, f.name)
    rdate = _extract_report_date(preview)
    target = _target_path(ROOT, kind, rdate, season)

    status = 'OK RICONOSCIUTO' if kind != 'unknown' and target else '!! QUARANTINE'

    print(f"FILE:    {f.name}")
    print(f"  Tipo:  {kind}  (conf={cls['confidence']:.0%})")
    print(f"  Stagione: {season or '???'}   Data: {rdate or '???'}")
    print(f"  Destinazione: {target.relative_to(ROOT) if target else 'QUARANTINE'}")
    print(f"  Stato: {status}")

    if target:
        key = str(target)
        if key in conflicts:
            print(f"  !! CONFLITTO: sovrascrivera' {conflicts[key]}")
        conflicts[key] = f.name

    # Prova a parsare per contare le righe
    if kind == 'sales_report':
        try:
            rows = parse_sales(str(f))
            print(f"  Parse test: {len(rows)} righe vendite")
        except Exception as e:
            print(f"  Parse test: ERRORE - {e}")
    elif kind == 'stock_report':
        try:
            rows = parse_articles(str(f))
            print(f"  Parse test: {len(rows)} righe stock")
        except Exception as e:
            print(f"  Parse test: ERRORE - {e}")

    print()

print("=" * 80)
print("RIEPILOGO DESTINAZIONI:")
for target_path, fname in sorted(conflicts.items()):
    print(f"  {fname:30s} -> {target_path.replace(str(ROOT)+'\\', '')}")

