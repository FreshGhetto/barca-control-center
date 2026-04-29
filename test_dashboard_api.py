"""Test rapido della dashboard API per verificare filtri trasferimenti."""
import sys, json
import urllib.request

try:
    url = "http://localhost:8080/api/dashboard?table_limit=50"
    with urllib.request.urlopen(url, timeout=10) as resp:
        data = json.loads(resp.read())
    transfers = data.get("tables", {}).get("transfer_proposals", [])
    print(f"Transfer rows loaded: {len(transfers)}")
    if transfers:
        first = transfers[0]
        print(f"First row: article={first.get('article_code')} reparto={first.get('reparto')} categoria={first.get('categoria')} marchio={first.get('marchio')} qty={first.get('qty')}")
        reparti = sorted({r.get('reparto','') for r in transfers if r.get('reparto')})
        categorie = sorted({r.get('categoria','') for r in transfers if r.get('categoria')})
        marchi = sorted({r.get('marchio','') for r in transfers if r.get('marchio')})
        print(f"Reparti trovati: {reparti}")
        print(f"Categorie trovate: {categorie[:5]}")
        print(f"Marchi trovati: {marchi[:5]}")
    else:
        print("Nessuna transfer row!")
except Exception as e:
    print(f"Errore: {e}")

