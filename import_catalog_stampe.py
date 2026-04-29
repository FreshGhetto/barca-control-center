"""
Script per importare i cataloghi Excel (26e e 26g) con il parser corretto (v2)
che gestisce columns header float/vuote (bug taglia 39).
"""
import os
import sys
import time
from pathlib import Path

try:
    import dotenv; dotenv.load_dotenv()
except ImportError:
    pass

# aggiungi il root al path così troviamo barca_control_center
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from barca_control_center.catalog_service import import_catalog_to_db

STAMPE = Path(r"C:\Users\ufficio2\Desktop\stampe da aggiungere")

excel_files = [
    STAMPE / "26e_excel.xls",
    STAMPE / "26g_excel.xls",
]

price_files = [
    STAMPE / "26e_listino.csv",
    STAMPE / "26e_saldo.csv",
    STAMPE / "26e_colore.csv",
    STAMPE / "26e_marchio.csv",
    STAMPE / "26g_listino.csv",
    STAMPE / "26g_saldo.csv",
    STAMPE / "26g_colore.csv",
    STAMPE / "26g_marchio.csv",
]

# Filtra solo i file che esistono
excel_files = [p for p in excel_files if p.exists()]
price_files = [p for p in price_files if p.exists()]

print(f"Excel trovati: {[p.name for p in excel_files]}")
print(f"CSV trovati:   {[p.name for p in price_files]}")
print()

if not excel_files and not price_files:
    print("ERRORE: nessun file trovato in", STAMPE)
    sys.exit(1)

last_stage = ""

def progress_cb(event):
    global last_stage
    stage = event.get("stage", "")
    msg = event.get("message", "")
    pct = event.get("progress")
    pct_str = f" [{pct:.0f}%]" if pct is not None else ""
    if stage != last_stage or "completato" in msg.lower() or "errore" in msg.lower():
        print(f"{pct_str} {msg}")
        last_stage = stage

t0 = time.time()
try:
    result = import_catalog_to_db(
        root=ROOT,
        excel_files=excel_files,
        price_files=price_files,
        sheet=0,
        create_schema=True,
        verbose=True,
        progress_cb=progress_cb,
    )
    elapsed = time.time() - t0
    print()
    print(f"✓ Import completato in {elapsed:.1f}s")
    print(f"  run_id: {result['run_id']}")
    print(f"  stagioni: {result.get('catalog_seasons', [])}")
    print(f"  conteggi:")
    for k, v in (result.get("counts") or {}).items():
        print(f"    {k}: {v}")
except Exception as e:
    print(f"\n✗ ERRORE durante l'import: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

