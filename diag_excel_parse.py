"""Diagnostica struttura reale dell'Excel e confronto con il parser attuale."""
import sys, json
sys.path.insert(0, '.')
from pathlib import Path
from barca_control_center.catalog_excel import (
    ensure_xlsx, _iter_rows_from_xlsx, _find_table_header,
    _norm_str, _as_float, parse_situazione_articoli_excel
)

TARGET_ARTICLE = "68/502042BE"

for xls_name in ["26e_excel.xls", "26g_excel.xls"]:
    xls = Path(r"C:\Users\ufficio2\Desktop\stampe da aggiungere") / xls_name
    if not xls.exists():
        print(f"File non trovato: {xls}")
        continue

    print(f"\n{'='*70}")
    print(f"FILE: {xls_name}")
    print(f"{'='*70}")

    xlsx = ensure_xlsx(xls)
    _, rows_gen = _iter_rows_from_xlsx(xlsx, sheet=0)

    table = None
    cur_art = ""
    rows_printed = 0
    article_rows = []

    for i, row in enumerate(rows_gen):
        if i < 50 or (table is None):
            non_empty = [(j, v) for j, v in enumerate(row) if v is not None and str(v).strip()]
            if non_empty:
                print(f"  row {i:4}: {non_empty}")

        h = _find_table_header(row)
        if h and table is None:
            table = h
            sc = json.loads(h["size_cols_json"])
            print(f"\n  *** HEADER RILEVATO a row {i} ***")
            print(f"      neg={h['neg']}, giac={h['giac']}, con={h['con']}, ven={h['ven']}, perc={h['perc']}")
            print(f"      size_cols: {sc}\n")

        # Cerca righe dell'articolo target
        if table and TARGET_ARTICLE in str(row):
            article_rows.append((i, row))

    if article_rows:
        print(f"\n  --- Righe che contengono {TARGET_ARTICLE} ---")
        sc = json.loads(table["size_cols_json"])
        for i, row in article_rows:
            neg = row[table["neg"]] if table["neg"] < len(row) else "?"
            giac = row[table["giac"]] if table["giac"] < len(row) else "?"
            sizes = {lbl: row[idx] for lbl, idx in sc.items() if idx < len(row)}
            print(f"    row {i}: NEG={neg}, GIAC={giac}, sizes={sizes}")

    # Adesso esegui il parse completo e mostra le righe per l'articolo target
    print(f"\n  --- Output parse_situazione_articoli_excel per {TARGET_ARTICLE} ---")
    df = parse_situazione_articoli_excel(xlsx, sheet=0)
    sub = df[df["articolo"] == TARGET_ARTICLE]
    if sub.empty:
        print(f"  ATTENZIONE: articolo {TARGET_ARTICLE} NON trovato nel DataFrame!")
    else:
        print(f"  Trovate {len(sub)} righe")
        for _, r in sub.iterrows():
            sizes = json.loads(r["sizes_json"]) if r["sizes_json"] else {}
            print(f"    neg={r['neg']:4s} giac={r['giac']:5.0f} sizes={sizes}")

