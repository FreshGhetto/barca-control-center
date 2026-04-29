"""Test dashboard con timeout lungo."""
import json, urllib.request, sys, time

t_start = time.time()
url = "http://localhost:8080/api/dashboard?table_limit=200"
print(f"Carico dashboard: {url}")
try:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        raw = resp.read()
        data = json.loads(raw)
    elapsed = time.time() - t_start
    print(f"Risposta OK in {elapsed:.1f}s")

    transfers = data.get("tables", {}).get("transfer_proposals", [])
    print(f"Transfer rows: {len(transfers)}")
    if transfers:
        reparti = sorted({r.get('reparto','') for r in transfers if r.get('reparto')})
        categorie = sorted({r.get('categoria','') for r in transfers if r.get('categoria')})
        marchi = sorted({r.get('marchio','') for r in transfers if r.get('marchio')})
        print(f"Reparti: {reparti}")
        print(f"Categorie ({len(categorie)}): {categorie[:5]}")
        print(f"Marchi ({len(marchi)}): {marchi[:3]}")
        to_m4 = [r for r in transfers if r.get('to_shop_code') == 'M4']
        to_sd = [r for r in transfers if r.get('to_shop_code') == 'SD']
        print(f"Verso M4: {len(to_m4)} / verso SD: {len(to_sd)}")
    kpis = data.get("kpis", {})
    print(f"KPI transfer_rows={kpis.get('transfer_rows')} qty={kpis.get('transfer_qty_total')}")
except Exception as e:
    print(f"Errore dopo {time.time()-t_start:.1f}s: {type(e).__name__}: {e}")
    sys.exit(1)

