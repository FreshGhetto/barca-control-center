from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Set


@dataclass
class CatalogStoreRow:
    store: str
    giac: float = 0.0
    con: float = 0.0
    ven: float = 0.0
    perc_ven: float = 0.0
    sizes: Dict[int, float] = field(default_factory=dict)


@dataclass
class CatalogArticle:
    code: str
    season: str = ""
    season_code: str = ""
    season_label: str = ""
    description: str = ""
    color: str = ""
    supplier: str = ""
    reparto: str = ""
    categoria: str = ""
    tipologia: str = ""
    giac: float = 0.0
    con: float = 0.0
    ven: float = 0.0
    perc_ven: float = 0.0
    size_totals: Dict[int, float] = field(default_factory=dict)
    stores: Dict[str, CatalogStoreRow] = field(default_factory=dict)
    source_files: Set[str] = field(default_factory=set)

    def recompute_totals(self) -> None:
        stores = [row for key, row in self.stores.items() if str(key or "").upper() != "XX"]
        self.giac = sum(row.giac for row in stores)
        self.con = sum(row.con for row in stores)
        self.ven = sum(row.ven for row in stores)
        self.perc_ven = (self.ven / self.con * 100.0) if self.con else 0.0

        totals: Dict[int, float] = {}
        for row in stores:
            for size, qty in row.sizes.items():
                totals[size] = totals.get(size, 0.0) + float(qty or 0.0)
        self.size_totals = dict(sorted(totals.items(), key=lambda item: item[0]))


StoreRow = CatalogStoreRow
Article = CatalogArticle
