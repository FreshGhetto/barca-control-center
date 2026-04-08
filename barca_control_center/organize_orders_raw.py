from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple


CSV_EXT = {".csv"}
NAME_SD_RE = re.compile(r"^(?P<season>[0-9]{2}[A-Za-z])_sd_(?P<part>[1-4])\.csv$", re.IGNORECASE)


def _read_preview(path: Path, max_lines: int = 120) -> str:
    encodings = ("utf-8", "utf-8-sig", "cp1252", "latin-1")
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="replace") as f:
                lines = []
                for i, line in enumerate(f):
                    if i >= max_lines:
                        break
                    lines.append(line.rstrip("\n"))
                return "\n".join(lines)
        except Exception:
            continue
    return ""


def _extract_season_code(preview: str, filename: str) -> Optional[str]:
    txt = preview.upper()
    m = re.search(r"STAGIONE[:\s]*([0-9]{2}[A-Z])", txt)
    if m:
        return m.group(1).lower()
    m = re.search(r"([0-9]{2}[A-Za-z])_sd_[1-4]", filename, flags=re.IGNORECASE)
    if m:
        return m.group(1).lower()
    return None


def _classify(path: Path, preview: str) -> Tuple[str, Optional[str]]:
    name = path.name
    upper = preview.upper()

    m_sd = NAME_SD_RE.match(name)
    if m_sd:
        return f"sd_{m_sd.group('part')}", m_sd.group("season").lower()

    if "ANALISI LISTINI E RICARICHI" in upper:
        season = _extract_season_code(preview, name)
        return "prezzo_acq-ven", season

    if "ANALISI PER SINGOLA TAGLIA" in upper:
        season = _extract_season_code(preview, name)
        return "sd_3", season

    if "ANALISI ARTICOLI" in upper and "TIPOLOGIA" in upper and "MARCHIO" in upper:
        season = _extract_season_code(preview, name)
        return "sd_1", season

    if "ANALISI ARTICOLI" in upper and "COLORE" in upper and "MATERIALE" in upper:
        season = _extract_season_code(preview, name)
        return "sd_2", season

    if "ANALISI ARTICOLI" in upper and "FASCE PRZ." in upper and "LISTINO" in upper:
        season = _extract_season_code(preview, name)
        return "sd_4", season

    if "_SD_1" in name.upper():
        season = _extract_season_code(preview, name)
        return "sd_1", season
    if "_SD_2" in name.upper():
        season = _extract_season_code(preview, name)
        return "sd_2", season
    if "_SD_3" in name.upper():
        season = _extract_season_code(preview, name)
        return "sd_3", season
    if "_SD_4" in name.upper():
        season = _extract_season_code(preview, name)
        return "sd_4", season

    return "unknown", _extract_season_code(preview, name)


def _season_bucket(season_code: Optional[str]) -> str:
    if not season_code:
        return "misc"
    season_code = season_code.lower().strip()
    ch = season_code[-1]
    if ch in ("i", "e"):
        return "corrente_i"
    if ch in ("y", "g"):
        return "continuativa_y"
    return "misc"


def _target_path(out_root: Path, season_code: Optional[str], kind: str) -> Optional[Path]:
    if kind == "unknown":
        return None

    season = season_code or "unknown"
    bucket = _season_bucket(season_code)
    folder = out_root / bucket / season
    folder.mkdir(parents=True, exist_ok=True)

    if kind.startswith("sd_"):
        return folder / f"{season}_sd_{kind.split('_')[-1]}.csv"
    if kind == "prezzo_acq-ven":
        return folder / f"{season}_prezzo_acq-ven.csv"
    return None


def organize_orders_raw(src_root: Path, out_root: Optional[Path] = None, verbose: bool = True) -> Dict[str, object]:
    if not src_root.exists():
        raise FileNotFoundError(f"Source path does not exist: {src_root}")

    out = out_root or (src_root / "_ordinato")
    out.mkdir(parents=True, exist_ok=True)

    meta_dir = out / "_meta"
    duplicates_dir = meta_dir / "duplicates"
    unclassified_dir = out / "_unclassified"
    meta_dir.mkdir(parents=True, exist_ok=True)
    duplicates_dir.mkdir(parents=True, exist_ok=True)
    unclassified_dir.mkdir(parents=True, exist_ok=True)

    files = [
        p
        for p in src_root.rglob("*")
        if p.is_file()
        and p.suffix.lower() in CSV_EXT
        and "_ordinato" not in p.parts
    ]
    files.sort(key=lambda p: p.stat().st_mtime)

    rows: List[Dict[str, str]] = []
    for p in files:
        preview = _read_preview(p)
        kind, season_code = _classify(p, preview)
        target = _target_path(out, season_code, kind)
        row = {
            "source": str(p),
            "kind": kind,
            "season_code": season_code or "",
            "target": "",
            "status": "",
            "note": "",
        }

        if target is None:
            q_name = f"{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}__{p.name}"
            q_path = unclassified_dir / q_name
            shutil.copy2(p, q_path)
            row["target"] = str(q_path)
            row["status"] = "unclassified"
            row["note"] = "not recognized"
            rows.append(row)
            if verbose:
                print(f"[ORGANIZE] UNCLASSIFIED {p.name}")
            continue

        if target.exists():
            # Keep newest in canonical target, archive previous as duplicate snapshot.
            existing_mtime = target.stat().st_mtime
            incoming_mtime = p.stat().st_mtime
            if incoming_mtime > existing_mtime:
                dup_name = f"dup__{target.stem}__{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                shutil.copy2(target, duplicates_dir / dup_name)
                shutil.copy2(p, target)
                row["status"] = "replaced"
                row["note"] = "newer source replaced previous target"
            else:
                dup_name = f"dup__{Path(p.name).stem}__{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                shutil.copy2(p, duplicates_dir / dup_name)
                row["status"] = "duplicate"
                row["note"] = "older or same source archived as duplicate"
        else:
            shutil.copy2(p, target)
            row["status"] = "organized"
            row["note"] = ""

        row["target"] = str(target)
        rows.append(row)
        if verbose:
            print(f"[ORGANIZE] {row['status'].upper()} {p.name} -> {target.relative_to(out)}")

    summary = {
        "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
        "source_root": str(src_root),
        "organized_root": str(out),
        "total_files_seen": len(files),
        "organized": sum(1 for r in rows if r["status"] == "organized"),
        "replaced": sum(1 for r in rows if r["status"] == "replaced"),
        "duplicates": sum(1 for r in rows if r["status"] == "duplicate"),
        "unclassified": sum(1 for r in rows if r["status"] == "unclassified"),
        "rows": rows,
    }

    (meta_dir / "organize_report.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    with open(meta_dir / "organize_report.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["source", "kind", "season_code", "target", "status", "note"])
        writer.writeheader()
        writer.writerows(rows)

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Organizza i raw ordini in una struttura standard.")
    parser.add_argument("source_root", type=Path, help="Cartella root con i raw ordini.")
    parser.add_argument(
        "--out-root",
        type=Path,
        default=None,
        help="Cartella output ordinata (default: <source_root>/_ordinato)",
    )
    parser.add_argument("--quiet", action="store_true", help="Riduce output console.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = organize_orders_raw(args.source_root, args.out_root, verbose=not args.quiet)
    print(json.dumps({k: v for k, v in summary.items() if k != "rows"}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

