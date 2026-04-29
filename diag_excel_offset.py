"""
Diagnostica approfondita per trovare l'offset tra header e dati nel file Excel.
Mostra per ogni header rilevato la struttura header vs dati reali.
"""
import sys, json
sys.path.insert(0, '.')
from pathlib import Path
from barca_control_center.catalog_excel import (
    ensure_xlsx, _iter_rows_from_xlsx, _find_table_header,
    _norm_str, _as_float, _parse_size_label
)

for xls_name in ["26e_excel.xls"]:
    xls = Path(r"C:\Users\ufficio2\Desktop\stampe da aggiungere") / xls_name
    xlsx = ensure_xlsx(xls)
    _, rows_gen = _iter_rows_from_xlsx(xlsx, sheet=0)

    table = None
    header_count = 0
    rows_list = list(rows_gen)

    for i, row in enumerate(rows_list):
        h = _find_table_header(row)
        if h:
            sc = json.loads(h["size_cols_json"])
            if not sc:
                continue
            header_count += 1
            print(f"\n{'='*60}")
            print(f"HEADER #{header_count} a row {i}")
            print(f"  neg={h['neg']}, giac={h['giac']}, con={h['con']}, ven={h['ven']}")
            print(f"  size_cols: {sc}")

            # Mostra le prime 3 righe dati dopo questo header
            data_rows_shown = 0
            for j in range(i+1, min(i+50, len(rows_list))):
                next_row = rows_list[j]
                # Salta righe vuote
                if not any(v is not None and str(v).strip() for v in next_row):
                    continue
                # Fermati se c'è un nuovo header
                if _find_table_header(next_row):
                    break

                neg_val = next_row[h['neg']] if h['neg'] < len(next_row) else None
                if not _norm_str(neg_val):
                    continue
                neg_s = _norm_str(neg_val).upper()
                if neg_s in {"NEG", "GIAC", "CON", "VEN", "%VEN", "ARTICOLO", "PAGINA"}:
                    continue

                # Mostra colonne rilevanti
                relevant = {}
                # Colonna 9 e dintorni
                for idx in range(7, min(20, len(next_row))):
                    v = next_row[idx]
                    if v is not None:
                        relevant[idx] = v

                print(f"\n  Dati row {j} (neg={neg_s}):")
                print(f"    Celle non-None da col7 in poi: {relevant}")

                # Cosa legge il parser ATTUALE per ogni taglia
                print(f"    Parser attuale legge:")
                for lbl, col_idx in sc.items():
                    v = next_row[col_idx] if col_idx < len(next_row) else "N/A"
                    print(f"      taglia {lbl} → col {col_idx} → valore: {v!r}")

                # Prova offset +1
                print(f"    Con offset +1 (col_idx+1) leggerebbe:")
                for lbl, col_idx in sc.items():
                    v = next_row[col_idx+1] if col_idx+1 < len(next_row) else "N/A"
                    print(f"      taglia {lbl} → col {col_idx+1} → valore: {v!r}")

                data_rows_shown += 1
                if data_rows_shown >= 2:
                    break

            if header_count >= 3:
                print("\n[... limitato ai primi 3 header ...]")
                break

