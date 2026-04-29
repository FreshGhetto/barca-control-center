"""Test rapido della dashboard API - verifica filtri trasferimenti."""
import json, urllib.request, sys

url = "http://localhost:8080/api/dashboard?table_limit=200"
print(f"Carico dashboard: {url}")
try:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        data = json.loads(raw)

    transfers = data.get("tables", {}).get("transfer_proposals", [])
    print(f"\n=== TRANSFER ROWS CARICATI: {len(transfers)} ===")
    if transfers:
        first = transfers[0]
        print(f"Esempio: article={first.get('article_code')} reparto='{first.get('reparto')}' categoria='{first.get('categoria')}' marchio='{first.get('marchio')}' qty={first.get('qty')}")

        reparti = sorted({r.get('reparto','') for r in transfers if r.get('reparto')})
        categorie = sorted({r.get('categoria','') for r in transfers if r.get('categoria')})
        marchi = sorted({r.get('marchio','') for r in transfers if r.get('marchio')})
        print(f"\nReparti disponibili per filtro ({len(reparti)}): {reparti}")
        print(f"Categorie disponibili per filtro ({len(categorie)}): {categorie[:8]}...")
        print(f"Marchi disponibili per filtro ({len(marchi)}): {marchi[:8]}...")

        # Verifica M4
        to_m4 = [r for r in transfers if r.get('to_shop_code') == 'M4']
        to_sd = [r for r in transfers if r.get('to_shop_code') == 'SD']
        print(f"\nTrasferimenti verso M4: {len(to_m4)} righe")
        print(f"Trasferimenti verso SD: {len(to_sd)} righe")
    else:
        print("ATTENZIONE: Nessuna transfer row!")

    # KPIs
    kpis = data.get("kpis", {})
    print(f"\n=== KPI ===")
    print(f"transfer_rows (totale DB): {kpis.get('transfer_rows')}")
    print(f"transfer_qty_total: {kpis.get('transfer_qty_total')}")

except Exception as e:
    print(f"Errore: {type(e).__name__}: {e}")
    sys.exit(1)

