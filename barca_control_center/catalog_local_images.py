from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple
import re

from PIL import Image


_CODE_RE = re.compile(r"(?<!\d)(\d{1,3})\s*[/_.-]+\s*([A-Z0-9]{2,})(?![A-Z0-9])", re.IGNORECASE)
_VALID_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}


def _extract_codes_from_text(value: str) -> List[str]:
    s = str(value or "").strip().upper()
    if not s:
        return []
    return [f"{m.group(1)}/{m.group(2)}" for m in _CODE_RE.finditer(s)]


def normalize_code(value: str) -> str:
    s = str(value or "").strip().upper().replace(" ", "")
    matches = _extract_codes_from_text(s)
    if not matches:
        return s
    return matches[-1]


def _extract_code_from_name(name: str) -> Optional[str]:
    matches = _extract_codes_from_text(name)
    if not matches:
        return None
    # Prefer the last match: in archive paths the code is usually near the end.
    return matches[-1]


def _is_position_file(path: Path, *, position: str, allow_variants: bool) -> bool:
    stem = path.stem.lower().strip()
    pos = (position or "").strip().lower()
    if not pos:
        return False
    if stem == pos:
        return True
    if not allow_variants:
        return False
    return stem.startswith(f"{pos}_") or stem.startswith(f"{pos}-") or stem.startswith(f"{pos} ")


def _candidate_score(path: Path, *, position: str, matched_position: bool) -> Tuple[int, int, int, int]:
    stem = path.stem.lower().strip()
    pos = (position or "").strip().lower()
    if matched_position:
        exact = 2 if stem == pos else 1
    else:
        # fallback candidate when no xl/xl_* file exists for that code
        exact = 0
    ext_rank = {
        ".jpg": 5,
        ".jpeg": 4,
        ".png": 3,
        ".webp": 2,
        ".bmp": 1,
    }.get(path.suffix.lower(), 0)
    # Prefer shorter names if score/extension tie; then prefer deeper folders.
    return exact, ext_rank, -len(path.name), len(path.parts)


def code_lookup_variants(code: str) -> List[str]:
    """
    Build lookup variants for local index compatibility:
      - exact normalized code (e.g. 048/ABC12)
      - underscore form used in filenames (e.g. 048_ABC12)
      - numeric part without leading zeros (e.g. 48/ABC12)
      - numeric part padded to 2/3 digits (e.g. 48 -> 048)
    """
    base = normalize_code(code)
    if not base:
        return []
    m = _CODE_RE.search(base)
    if not m:
        compact = base.replace(" ", "")
        out = [compact]
        if "/" in compact:
            out.append(compact.replace("/", "_"))
        if "_" in compact:
            out.append(compact.replace("_", "/"))
        # keep insertion order
        seen = set()
        dedup: List[str] = []
        for x in out:
            if x and x not in seen:
                seen.add(x)
                dedup.append(x)
        return dedup

    num_raw = m.group(1)
    art = m.group(2).upper()
    num_int = str(int(num_raw)) if num_raw.isdigit() else num_raw
    variants: List[str] = []
    for n in (num_raw, num_int, num_int.zfill(2), num_int.zfill(3)):
        slash = f"{n}/{art}"
        underscore = f"{n}_{art}"
        if slash not in variants:
            variants.append(slash)
        if underscore not in variants:
            variants.append(underscore)
    return variants


def lookup_local_image_path(
    code: str,
    flat_index: Mapping[str, str | Path],
) -> Optional[Path]:
    raw = str(code or "").strip().upper()
    raw_compact = raw.replace(" ", "")
    # First try direct/raw forms (useful when the index already contains filename-like keys).
    for key in (raw, raw_compact, raw_compact.replace("/", "_"), raw_compact.replace("_", "/")):
        if not key:
            continue
        p = flat_index.get(key)
        if p:
            return Path(p)

    # Then try normalized compatibility variants.
    for key in code_lookup_variants(code):
        p = flat_index.get(key)
        if p:
            return Path(p)
    return None


def _find_code_from_path(img_path: Path, *, season_root: Path) -> Optional[str]:
    try:
        rel = img_path.relative_to(season_root)
    except Exception:
        return None
    # 1) direct match from filename (handles patterns like 48_LU50PBI.jpg)
    code_from_file = _extract_code_from_name(rel.stem)
    if code_from_file:
        return code_from_file

    parents = list(rel.parts[:-1])  # no filename
    # 2) match from nearest parent folders
    for part in reversed(parents):
        code = _extract_code_from_name(part)
        if code:
            return code

    # 3) fallback on full relative path text
    code_from_rel = _extract_code_from_name(str(rel).replace("\\", "/"))
    if code_from_rel:
        return code_from_rel
    return None


def scan_local_images(
    *,
    root_dir: str | Path,
    season_names: Sequence[str],
    position: str = "xl",
    allow_position_variants: bool = True,
) -> Tuple[Dict[str, Dict[str, Path]], Dict[str, int]]:
    """
    Scan local archive and return:
      - season_index: {season_name: {code: image_path}}
      - summary counters
    """
    root = Path(root_dir)
    season_index: Dict[str, Dict[str, Path]] = {}

    files_seen = 0
    position_matches = 0
    code_matches = 0
    fallback_non_position_used = 0

    for season in season_names:
        season_root = root / season
        if not season_root.exists() or not season_root.is_dir():
            continue

        best_by_code: Dict[str, Tuple[Tuple[int, int, int, int], Path]] = {}

        for p in season_root.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix.lower() not in _VALID_IMAGE_EXTS:
                continue

            files_seen += 1

            matched_position = _is_position_file(p, position=position, allow_variants=allow_position_variants)
            if matched_position:
                position_matches += 1

            code = _find_code_from_path(p, season_root=season_root)
            if not code:
                continue
            code = normalize_code(code)
            code_matches += 1

            score = _candidate_score(p, position=position, matched_position=matched_position)
            prev = best_by_code.get(code)
            if prev is None or score > prev[0]:
                best_by_code[code] = (score, p)

        fallback_non_position_used += sum(1 for (score, _) in best_by_code.values() if score[0] == 0)
        season_index[season] = {code: pp for code, (_, pp) in best_by_code.items()}

    summary = {
        "files_seen": files_seen,
        "position_matches": position_matches,
        "code_matches": code_matches,
        "fallback_non_position_used": fallback_non_position_used,
        "seasons": len(season_index),
        "codes_total_unique": len(
            {
                code
                for season_map in season_index.values()
                for code in season_map.keys()
            }
        ),
    }
    return season_index, summary


def flatten_index(
    season_index: Mapping[str, Mapping[str, Path]],
    *,
    season_priority: Sequence[str],
) -> Dict[str, Path]:
    """Flatten season index into a single code->path map with season-priority order."""
    out: Dict[str, Path] = {}
    for season in season_priority:
        season_map = season_index.get(season, {})
        for code, img_path in season_map.items():
            path_obj = Path(img_path)
            keys = list(code_lookup_variants(code))

            # Add filename-derived aliases (e.g. 38_043BE.jpg) when they contain a code.
            stem_code = _extract_code_from_name(path_obj.stem)
            if stem_code:
                for kk in code_lookup_variants(stem_code):
                    if kk not in keys:
                        keys.append(kk)

            for key in keys:
                if key not in out:
                    out[key] = path_obj
    return out


def export_renamed_images(
    season_index: Mapping[str, Mapping[str, Path]],
    *,
    output_dir: str | Path,
) -> Dict[str, object]:
    """
    Export images in folders by season:
      output_dir/<season>/<code_with_underscore>.jpg
    """
    out_base = Path(output_dir)
    out_base.mkdir(parents=True, exist_ok=True)

    copied = 0
    season_counts: Dict[str, int] = {}
    errors: List[str] = []

    for season, season_map in season_index.items():
        season_dir = out_base / str(season).strip()
        season_dir.mkdir(parents=True, exist_ok=True)
        count = 0

        for code, src in season_map.items():
            src_path = Path(src)
            out_name = f"{normalize_code(code).replace('/', '_')}.jpg"
            dst = season_dir / out_name

            try:
                with Image.open(src_path) as im:
                    rgb = im.convert("RGB")
                    rgb.save(dst, "JPEG", quality=95, subsampling=0, optimize=True)
                copied += 1
                count += 1
            except Exception as e:
                errors.append(f"{season}\t{code}\t{type(e).__name__}:{e}")

        season_counts[season] = count

    return {
        "copied": copied,
        "season_counts": season_counts,
        "errors": errors,
        "output_dir": str(out_base),
    }


def load_local_image_bytes(
    code: str,
    flat_index: Mapping[str, str | Path],
) -> Tuple[Optional[bytes], Optional[str]]:
    p = lookup_local_image_path(code, flat_index)
    if not p:
        return None, "local_not_found"
    if not p.exists() or not p.is_file():
        return None, "local_file_missing"
    try:
        return p.read_bytes(), None
    except Exception as e:
        return None, f"local_read_error:{type(e).__name__}"
