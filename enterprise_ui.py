from __future__ import annotations

import csv
import io
import json
import os
import re
import subprocess
import sys
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from db_sync import get_db_dsn

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None


ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "ui" / "static"
SETTINGS_PATH = ROOT / "config" / "ui_settings.json"
RUN_HISTORY_PATH = ROOT / "output" / "ui" / "runs_history.json"

OUTPUT_FILES = [
    "clean_sales.csv",
    "clean_articles.csv",
    "alignment_report.csv",
    "suggested_transfers.csv",
    "suggested_transfers_detailed.csv",
    "shipment_plan.csv",
    "shipment_summary.csv",
    "features_after.csv",
    "demand_diagnostics.csv",
    "qa_report.json",
    "orders/orders_summary.json",
    "orders/orders_run_log.txt",
]

DASHBOARD_TABLE_COLUMNS: Dict[str, List[str]] = {
    "transfer_proposals": ["article_code", "size", "from_shop_code", "to_shop_code", "reason", "qty"],
    "order_proposals": ["module", "season_code", "mode", "article_code", "totale_qty", "predizione_vendite", "budget_acquisto"],
    "critical_articles": ["article_code", "shop_code", "demand_hybrid", "stock_after", "deficit"],
    "next_current_candidates": [
        "from_cont_season",
        "article_code",
        "categoria",
        "tipologia",
        "marchio",
        "colore",
        "materiale",
        "venduto_periodo",
        "giacenza",
        "applied_factor",
        "predicted_current_qty",
        "delta_vs_stock",
        "predicted_budget",
        "transition_score",
    ],
}


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _ensure_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)


def _status_to_ui(status: Optional[str]) -> str:
    s = (status or "").strip().lower()
    if not s:
        return "queued"
    mapping = {
        "completed": "success",
        "ok": "success",
        "success": "success",
        "done": "success",
        "failed": "failed",
        "error": "failed",
        "running": "running",
        "queued": "queued",
        "stopped": "stopped",
        "cancelled": "stopped",
    }
    return mapping.get(s, s)


def _friendly_status(status: Optional[str]) -> str:
    ui = _status_to_ui(status)
    mapping = {
        "running": "In corso",
        "success": "Completata",
        "failed": "Errore",
        "stopped": "Interrotta",
        "queued": "In attesa",
    }
    return mapping.get(ui, str(status or "n/a"))


def _friendly_run_type(run_type: Optional[str]) -> str:
    rt = str(run_type or "").strip().lower()
    mapping = {
        "app_pipeline": "Aggiornamento completo",
        "manual_sync": "Sincronizzazione database",
        "app_pipeline_ui": "Avvio manuale da console",
    }
    return mapping.get(rt, str(run_type or "Run"))


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _canonical_module_name(value: Optional[str]) -> str:
    module = str(value or "").strip().lower()
    if module in {"corrente", "current"}:
        return "current"
    if module in {"continuativa", "continuative"}:
        return "continuativa"
    return module


def _friendly_module_name(value: Optional[str]) -> str:
    module = _canonical_module_name(value)
    mapping = {
        "current": "Corrente",
        "continuativa": "Continuativa",
        "distribuzione": "Distribuzione",
    }
    return mapping.get(module, str(value or "Run"))


def _friendly_mode(value: Optional[str]) -> str:
    mode = str(value or "").strip().lower()
    mapping = {
        "math": "Base",
        "rf": "Storico",
        "hybrid": "Ibrido",
    }
    return mapping.get(mode, str(value or "n/d"))


def _friendly_season_label(code: Optional[str], module_name: Optional[str] = None) -> Optional[str]:
    raw = _clean_text(code)
    if not raw:
        return None
    year_label = None
    match = re.search(r"(\d{2,4})", raw)
    if match:
        year_num = int(match.group(1))
        if 0 <= year_num < 100:
            year_num += 2000
        year_label = str(year_num)
    module_label = _friendly_module_name(module_name) if module_name else None
    if module_label and year_label:
        return f"{module_label} {year_label} ({raw})"
    if year_label:
        return f"{year_label} ({raw})"
    if module_label:
        return f"{module_label} {raw}"
    return raw


def _iso_sort_ts(value: Optional[str]) -> float:
    if not value:
        return 0.0
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


class RunOptions(BaseModel):
    source_db: bool = True
    source_db_run_id: Optional[str] = None
    skip_ingest: bool = True
    incoming_root: Optional[str] = None
    keep_incoming: bool = False
    skip_orders: bool = False
    orders_root: Optional[str] = None
    orders_source_db: bool = True
    orders_source_db_run_id: Optional[str] = None
    orders_math_only: bool = False
    orders_coverage: float = Field(default=1.20, ge=0.0)
    sync_db: bool = True
    db_create_schema: bool = False


class DeveloperModePayload(BaseModel):
    enabled: bool


@dataclass
class PipelineRun:
    run_id: str
    created_at: str
    options: Dict[str, Any]
    status: str = "queued"  # queued|running|success|failed|stopped
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    return_code: Optional[int] = None
    command: List[str] = field(default_factory=list)
    log_lines: List[str] = field(default_factory=list)
    error: Optional[str] = None

    def to_public(self, developer_mode: bool) -> Dict[str, Any]:
        run_type_value = "app_pipeline_ui"
        business_context = _run_business_context({}, run_type_value)
        data = {
            "run_id": self.run_id,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "status": _status_to_ui(self.status),
            "status_raw": self.status,
            "status_label": _friendly_status(self.status),
            "return_code": self.return_code,
            "options": self.options,
            "error": self.error,
            "log_tail": self.log_lines[-30:],
            "source": "ui",
            "run_type": run_type_value,
            "run_type_label": business_context["friendly_run_type"],
            "business_context": business_context,
            "can_stop": self.status == "running",
        }
        if developer_mode:
            data["command"] = self.command
            data["log_size"] = len(self.log_lines)
        return data


class SettingsStore:
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.data = {"developer_mode": False}
        self._load()

    def _load(self):
        if not self.path.exists():
            return
        try:
            self.data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            self.data = {"developer_mode": False}

    def _save(self):
        _ensure_parent(self.path)
        self.path.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")

    def get(self) -> Dict[str, Any]:
        with self.lock:
            return dict(self.data)

    def set_developer_mode(self, enabled: bool) -> Dict[str, Any]:
        with self.lock:
            self.data["developer_mode"] = bool(enabled)
            self._save()
            return dict(self.data)


class RunManager:
    def __init__(self, history_path: Path):
        self.history_path = history_path
        self.lock = threading.Lock()
        self.runs: List[PipelineRun] = []
        self.current_run_id: Optional[str] = None
        self.current_proc: Optional[subprocess.Popen] = None
        self._load_history()

    def _load_history(self):
        if not self.history_path.exists():
            return
        try:
            rows = json.loads(self.history_path.read_text(encoding="utf-8"))
            for item in rows[:100]:
                self.runs.append(
                    PipelineRun(
                        run_id=item["run_id"],
                        created_at=item["created_at"],
                        options=item.get("options", {}),
                        status=item.get("status", "queued"),
                        started_at=item.get("started_at"),
                        ended_at=item.get("ended_at"),
                        return_code=item.get("return_code"),
                        command=item.get("command", []),
                        log_lines=item.get("log_tail", []),
                        error=item.get("error"),
                    )
                )
        except Exception:
            self.runs = []

    def _save_history(self):
        _ensure_parent(self.history_path)
        payload = []
        for run in self.runs[:100]:
            payload.append(
                {
                    "run_id": run.run_id,
                    "created_at": run.created_at,
                    "started_at": run.started_at,
                    "ended_at": run.ended_at,
                    "status": run.status,
                    "return_code": run.return_code,
                    "options": run.options,
                    "command": run.command,
                    "log_tail": run.log_lines[-200:],
                    "error": run.error,
                }
            )
        self.history_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    @staticmethod
    def _build_command(opts: Dict[str, Any]) -> List[str]:
        cmd = [sys.executable, "app.py"]

        def add_flag(flag: str, value: bool):
            if value:
                cmd.append(flag)

        def add_opt(flag: str, value: Optional[str]):
            if value is not None and str(value).strip():
                cmd.extend([flag, str(value)])

        add_flag("--source-db", bool(opts.get("source_db")))
        add_opt("--source-db-run-id", opts.get("source_db_run_id"))
        add_flag("--skip-ingest", bool(opts.get("skip_ingest")))
        add_opt("--incoming-root", opts.get("incoming_root"))
        add_flag("--keep-incoming", bool(opts.get("keep_incoming")))
        add_flag("--skip-orders", bool(opts.get("skip_orders")))
        add_opt("--orders-root", opts.get("orders_root"))
        add_flag("--orders-source-db", bool(opts.get("orders_source_db")))
        add_opt("--orders-source-db-run-id", opts.get("orders_source_db_run_id"))
        add_flag("--orders-math-only", bool(opts.get("orders_math_only")))
        if opts.get("orders_coverage") is not None:
            cmd.extend(["--orders-coverage", str(opts["orders_coverage"])])
        add_flag("--sync-db", bool(opts.get("sync_db")))
        add_flag("--db-create-schema", bool(opts.get("db_create_schema")))
        return cmd

    def _append_log(self, run: PipelineRun, line: str):
        run.log_lines.append(line.rstrip("\n"))
        if len(run.log_lines) > 6000:
            run.log_lines = run.log_lines[-6000:]

    def _runner_thread(self, run_id: str):
        run: Optional[PipelineRun] = None
        with self.lock:
            run = next((r for r in self.runs if r.run_id == run_id), None)
            if run is None:
                return
            run.status = "running"
            run.started_at = _now_iso()
            cmd = self._build_command(run.options)
            run.command = cmd
            self._append_log(run, "$ " + " ".join(cmd))
            self.current_run_id = run_id
            self._save_history()

        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
            with self.lock:
                self.current_proc = proc

            assert proc.stdout is not None
            for line in proc.stdout:
                with self.lock:
                    if run is None:
                        break
                    self._append_log(run, line)

            rc = proc.wait()
            with self.lock:
                if run is not None:
                    run.return_code = int(rc)
                    if run.status != "stopped":
                        run.status = "success" if rc == 0 else "failed"
                    run.ended_at = _now_iso()
                    self._save_history()
        except Exception as exc:
            with self.lock:
                if run is not None:
                    run.status = "failed"
                    run.error = str(exc)
                    run.ended_at = _now_iso()
                    self._append_log(run, f"[UI] ERRORE: {exc}")
                    self._save_history()
        finally:
            with self.lock:
                self.current_proc = None
                self.current_run_id = None

    def start_run(self, options: Dict[str, Any]) -> PipelineRun:
        with self.lock:
            active = next((r for r in self.runs if r.status == "running"), None)
            if active is not None:
                raise RuntimeError(f"Esiste gia' una run in corso: {active.run_id}")

            run = PipelineRun(run_id=str(uuid.uuid4()), created_at=_now_iso(), options=options)
            self.runs.insert(0, run)
            self._save_history()

        th = threading.Thread(target=self._runner_thread, args=(run.run_id,), daemon=True)
        th.start()
        return run

    def stop_run(self, run_id: str) -> PipelineRun:
        with self.lock:
            run = next((r for r in self.runs if r.run_id == run_id), None)
            if run is None:
                raise KeyError(run_id)
            if run.status != "running":
                return run
            proc = self.current_proc
            if proc is None:
                return run
            proc.terminate()
            run.status = "stopped"
            run.ended_at = _now_iso()
            self._append_log(run, "[UI] Stop richiesto dall'utente.")
            self._save_history()
            return run

    def list_runs(self, limit: int = 20) -> List[PipelineRun]:
        with self.lock:
            return list(self.runs[:limit])

    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        with self.lock:
            return next((r for r in self.runs if r.run_id == run_id), None)

    def get_logs(self, run_id: str, tail: int) -> List[str]:
        with self.lock:
            run = next((r for r in self.runs if r.run_id == run_id), None)
            if run is None:
                raise KeyError(run_id)
            if tail <= 0:
                return run.log_lines
            return run.log_lines[-tail:]


settings_store = SettingsStore(SETTINGS_PATH)
run_manager = RunManager(RUN_HISTORY_PATH)

app = FastAPI(title="BARCA Control Center", version="1.0.0")
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def _developer_mode() -> bool:
    return bool(settings_store.get().get("developer_mode", False))


def _output_summary() -> List[Dict[str, Any]]:
    out_root = ROOT / "output"
    rows: List[Dict[str, Any]] = []
    for rel in OUTPUT_FILES:
        p = out_root / rel
        row: Dict[str, Any] = {
            "file": rel,
            "exists": p.exists(),
            "size_bytes": None,
            "modified_at": None,
            "rows": None,
        }
        if p.exists():
            st = p.stat()
            row["size_bytes"] = int(st.st_size)
            row["modified_at"] = datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds")
            if p.suffix.lower() == ".csv":
                try:
                    with open(p, "r", encoding="utf-8", errors="ignore") as fh:
                        row["rows"] = max(sum(1 for _ in fh) - 1, 0)
                except Exception:
                    row["rows"] = None
        rows.append(row)
    return rows


def _db_status() -> Dict[str, Any]:
    if psycopg is None:
        return {"connected": False, "reason": "psycopg non installato"}

    try:
        dsn = get_db_dsn()
    except Exception as exc:
        return {"connected": False, "reason": str(exc)}

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, run_type, status, started_at, finished_at, metadata
                    FROM public.etl_run
                    ORDER BY COALESCE(finished_at, started_at) DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                latest = None
                if row:
                    metadata = row[5] if isinstance(row[5], dict) else {}
                    business_context = _run_business_context(metadata, row[1])
                    latest = {
                        "run_id": str(row[0]),
                        "run_type": row[1],
                        "run_type_label": business_context["friendly_run_type"],
                        "status": _status_to_ui(row[2]),
                        "status_raw": row[2],
                        "status_label": _friendly_status(row[2]),
                        "started_at": row[3].isoformat() if row[3] else None,
                        "finished_at": row[4].isoformat() if row[4] else None,
                        "business_context": business_context,
                    }

                try:
                    cur.execute("SELECT * FROM public.vw_latest_run_counts")
                    c_row = cur.fetchone()
                    cols = [d.name for d in cur.description] if cur.description else []
                    latest_counts = (
                        {col: _json_value(value) for col, value in zip(cols, c_row)}
                        if c_row
                        else {}
                    )
                except Exception:
                    latest_counts = {}

            return {"connected": True, "latest_run": latest, "latest_counts": latest_counts}
    except Exception as exc:
        return {"connected": False, "reason": str(exc)}


def _db_recent_runs(limit: int = 50) -> List[Dict[str, Any]]:
    if psycopg is None:
        return []

    try:
        dsn = get_db_dsn()
    except Exception:
        return []

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, run_type, status, started_at, finished_at, metadata
                    FROM public.etl_run
                    ORDER BY COALESCE(finished_at, started_at) DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    for row in rows:
        metadata = row[5] if isinstance(row[5], dict) else {}
        started_at = row[3].isoformat() if row[3] else None
        ended_at = row[4].isoformat() if row[4] else None
        status_raw = row[2]
        business_context = _run_business_context(metadata, row[1])
        out.append(
            {
                "run_id": str(row[0]),
                "created_at": started_at,
                "started_at": started_at,
                "ended_at": ended_at,
                "status": _status_to_ui(status_raw),
                "status_raw": status_raw,
                "return_code": None,
                "options": {},
                "error": None,
                "log_tail": [],
                "source": "db",
                "run_type": row[1],
                "run_type_label": business_context["friendly_run_type"],
                "status_label": _friendly_status(status_raw),
                "metadata": metadata,
                "business_context": business_context,
                "can_stop": False,
            }
        )
    return out


def _db_run_detail(run_id: str) -> Optional[Dict[str, Any]]:
    if psycopg is None:
        return None

    try:
        dsn = get_db_dsn()
    except Exception:
        return None

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, run_type, status, started_at, finished_at, metadata
                    FROM public.etl_run
                    WHERE run_id = %s::uuid
                    LIMIT 1
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
    except Exception:
        return None

    if not row:
        return None

    metadata = row[5] if isinstance(row[5], dict) else {}
    started_at = row[3].isoformat() if row[3] else None
    ended_at = row[4].isoformat() if row[4] else None
    status_raw = row[2]
    business_context = _run_business_context(metadata, row[1])
    return {
        "run_id": str(row[0]),
        "created_at": started_at,
        "started_at": started_at,
        "ended_at": ended_at,
        "status": _status_to_ui(status_raw),
        "status_raw": status_raw,
        "status_label": _friendly_status(status_raw),
        "return_code": None,
        "options": {},
        "error": None,
        "log_tail": [],
        "source": "db",
        "run_type": row[1],
        "run_type_label": business_context["friendly_run_type"],
        "metadata": metadata,
        "business_context": business_context,
        "can_stop": False,
    }


def _filter_runs(
    rows: List[Dict[str, Any]],
    source: str = "all",
    status: Optional[str] = None,
    run_type: Optional[str] = None,
    q: Optional[str] = None,
) -> List[Dict[str, Any]]:
    source_norm = (source or "all").strip().lower()
    status_text = status.strip() if isinstance(status, str) else ""
    run_type_text = run_type.strip() if isinstance(run_type, str) else ""
    q_text = q.strip() if isinstance(q, str) else ""
    status_norm = _status_to_ui(status_text.lower()) if status_text else ""
    run_type_norm = run_type_text.lower()
    q_norm = q_text.lower()

    out: List[Dict[str, Any]] = []
    for row in rows:
        row_source = str(row.get("source", "")).strip().lower()
        row_status = _status_to_ui(str(row.get("status", "")))
        row_status_raw = str(row.get("status_raw", "")).strip().lower()
        row_run_type = str(row.get("run_type", "")).strip().lower()
        row_run_type_label = str(row.get("run_type_label", "")).strip().lower()
        row_run_id = str(row.get("run_id", "")).strip().lower()

        if source_norm in {"db", "ui"} and row_source != source_norm:
            continue
        if status_norm and row_status != status_norm and row_status_raw != status_norm:
            continue
        if run_type_norm and run_type_norm not in row_run_type and run_type_norm not in row_run_type_label:
            continue
        if q_norm:
            ctx = row.get("business_context") if isinstance(row.get("business_context"), dict) else {}
            ctx_summary = str(ctx.get("summary", "")).strip().lower()
            ctx_curr = " ".join(str(x).strip().lower() for x in (ctx.get("current_seasons") or []))
            ctx_cont = " ".join(str(x).strip().lower() for x in (ctx.get("continuativa_seasons") or []))
            hay = " ".join([row_run_id, row_run_type, row_status, row_status_raw, row_source, ctx_summary, ctx_curr, ctx_cont])
            if q_norm not in hay:
                continue
        out.append(row)
    return out


def _sort_runs(rows: List[Dict[str, Any]], sort_by: str = "started_at", sort_dir: str = "desc") -> List[Dict[str, Any]]:
    sort_key = (sort_by or "started_at").strip().lower()
    reverse = (sort_dir or "desc").strip().lower() != "asc"

    if sort_key in {"started_at", "created_at", "ended_at"}:
        rows.sort(
            key=lambda r: max(
                _iso_sort_ts(r.get(sort_key)),
                _iso_sort_ts(r.get("started_at")),
                _iso_sort_ts(r.get("created_at")),
            ),
            reverse=reverse,
        )
        return rows

    if sort_key == "return_code":
        rows.sort(
            key=lambda r: (
                r.get("return_code") is None,
                (r.get("return_code") if isinstance(r.get("return_code"), int) else 0),
            ),
            reverse=reverse,
        )
        return rows

    rows.sort(key=lambda r: str(r.get(sort_key, "")).strip().lower(), reverse=reverse)
    return rows


def _combined_recent_runs(
    developer_mode: bool,
    source: str = "all",
    status: Optional[str] = None,
    run_type: Optional[str] = None,
    q: Optional[str] = None,
    sort_by: str = "started_at",
    sort_dir: str = "desc",
    fetch_target: int = 200,
) -> List[Dict[str, Any]]:
    fetch_limit = min(10000, max(300, fetch_target * 8))
    local_runs = [r.to_public(developer_mode) for r in run_manager.list_runs(limit=fetch_limit)]
    db_runs = _db_recent_runs(limit=fetch_limit)

    out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for row in local_runs:
        run_id = str(row.get("run_id", "")).strip()
        if run_id:
            seen.add(run_id)
        out.append(row)

    for row in db_runs:
        run_id = str(row.get("run_id", "")).strip()
        if run_id and run_id in seen:
            continue
        out.append(row)

    out = _filter_runs(out, source=source, status=status, run_type=run_type, q=q)
    out = _sort_runs(out, sort_by=sort_by, sort_dir=sort_dir)
    return out


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        try:
            return float(value)
        except Exception:
            return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _fetch_dict_rows(cur) -> List[Dict[str, Any]]:
    cols = [d.name for d in (cur.description or [])]
    out: List[Dict[str, Any]] = []
    for row in cur.fetchall():
        item: Dict[str, Any] = {}
        for idx, col in enumerate(cols):
            item[col] = _json_value(row[idx])
        out.append(item)
    return out


def _fetch_chart_rows(cur) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in cur.fetchall():
        label = _json_value(row[0]) if len(row) > 0 else ""
        value = _json_value(row[1]) if len(row) > 1 else 0
        try:
            num = float(value) if value is not None else 0.0
        except Exception:
            num = 0.0
        out.append({"label": str(label or "n/a"), "value": num})
    return out


def _run_business_context(metadata: Optional[Dict[str, Any]], run_type: Optional[str]) -> Dict[str, Any]:
    md = metadata if isinstance(metadata, dict) else {}
    orders_jobs = md.get("orders_jobs") if isinstance(md.get("orders_jobs"), list) else []
    source_jobs = md.get("order_source_jobs") if isinstance(md.get("order_source_jobs"), list) else []

    def _collect_seasons(jobs: List[Dict[str, Any]], module_name: str) -> List[str]:
        vals: List[str] = []
        module_key = _canonical_module_name(module_name)
        for item in jobs:
            if not isinstance(item, dict):
                continue
            if _canonical_module_name(item.get("module")) != module_key:
                continue
            season = _clean_text(item.get("season"))
            if season and season not in vals:
                vals.append(season)
        return vals

    def _collect_modes(jobs: List[Dict[str, Any]], module_name: str) -> List[str]:
        vals: List[str] = []
        module_key = _canonical_module_name(module_name)
        for item in jobs:
            if not isinstance(item, dict):
                continue
            if _canonical_module_name(item.get("module")) != module_key:
                continue
            mode = _clean_text(item.get("mode"))
            if mode and mode not in vals:
                vals.append(mode)
        return vals

    current_seasons = _collect_seasons(orders_jobs or source_jobs, "current")
    cont_seasons = _collect_seasons(orders_jobs or source_jobs, "continuativa")
    modules: List[str] = []
    if current_seasons:
        modules.append("corrente")
    if cont_seasons:
        modules.append("continuativa")
    if not modules and str(run_type or "").strip().lower() == "app_pipeline":
        modules.append("distribuzione")

    current_modes = _collect_modes(orders_jobs, "current")
    cont_modes = _collect_modes(orders_jobs, "continuativa")
    current_season_labels = [_friendly_season_label(code, "current") or code for code in current_seasons]
    cont_season_labels = [_friendly_season_label(code, "continuativa") or code for code in cont_seasons]
    current_mode_labels = [_friendly_mode(code) for code in current_modes]
    cont_mode_labels = [_friendly_mode(code) for code in cont_modes]

    season_parts: List[str] = []
    if current_season_labels:
        season_parts.append(", ".join(current_season_labels))
    if cont_season_labels:
        season_parts.append(", ".join(cont_season_labels))
    summary_short = " + ".join(season_parts) if season_parts else _friendly_run_type(run_type)

    method_parts: List[str] = []
    if current_mode_labels:
        method_parts.append(f"Metodo corrente: {', '.join(current_mode_labels)}")
    if cont_mode_labels:
        method_parts.append(f"Metodo continuativa: {', '.join(cont_mode_labels)}")

    summary = f"{summary_short} · {' · '.join(method_parts)}" if method_parts else summary_short
    title = summary_short
    notes: List[str] = []
    if str(run_type or "").strip().lower() == "manual_sync" and bool(md.get("create_schema")):
        notes.append("schema aggiornato")

    return {
        "modules": modules,
        "current_seasons": current_seasons,
        "continuativa_seasons": cont_seasons,
        "current_season_labels": current_season_labels,
        "continuativa_season_labels": cont_season_labels,
        "current_modes": current_modes,
        "continuativa_modes": cont_modes,
        "current_mode_labels": current_mode_labels,
        "continuativa_mode_labels": cont_mode_labels,
        "summary_short": summary_short,
        "summary": summary,
        "title": title,
        "notes": notes,
        "friendly_run_type": _friendly_run_type(run_type),
    }


def _to_float(value: Any) -> float:
    try:
        return float(value if value is not None else 0.0)
    except Exception:
        return 0.0


def _fetch_kpi_core(cur, run_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT
          (SELECT count(*) FROM public.dim_shop) AS shop_count,
          (SELECT count(*) FROM public.dim_article) AS article_count,
          (SELECT count(*) FROM public.fact_sales_snapshot WHERE run_id = %s::uuid) AS sales_rows,
          (SELECT count(*) FROM public.fact_stock_snapshot WHERE run_id = %s::uuid) AS stock_rows,
          (SELECT count(*) FROM public.fact_transfer_suggestion WHERE run_id = %s::uuid) AS transfer_rows,
          (SELECT COALESCE(sum(qty), 0) FROM public.fact_transfer_suggestion WHERE run_id = %s::uuid) AS transfer_qty_total,
          (SELECT count(*) FROM public.fact_feature_state WHERE run_id = %s::uuid) AS feature_rows,
          (SELECT count(*) FROM public.fact_order_forecast WHERE run_id = %s::uuid) AS order_rows,
          (SELECT COALESCE(sum(totale_qty), 0) FROM public.fact_order_forecast WHERE run_id = %s::uuid) AS order_qty_total,
          (SELECT COALESCE(sum(budget_acquisto), 0) FROM public.fact_order_forecast WHERE run_id = %s::uuid) AS order_budget_total,
          (SELECT COALESCE(avg(sellout_clamped), 0) FROM public.fact_sales_snapshot WHERE run_id = %s::uuid) AS avg_sellout_clamped,
          (
            SELECT count(*)
            FROM public.fact_feature_state
            WHERE run_id = %s::uuid
              AND demand_hybrid IS NOT NULL
              AND stock_after IS NOT NULL
              AND (demand_hybrid - stock_after) > 0
          ) AS critical_rows_total,
          (
            SELECT COALESCE(sum(demand_hybrid - stock_after), 0)
            FROM public.fact_feature_state
            WHERE run_id = %s::uuid
              AND demand_hybrid IS NOT NULL
              AND stock_after IS NOT NULL
              AND (demand_hybrid - stock_after) > 0
          ) AS critical_deficit_total,
          (SELECT count(DISTINCT to_shop_code) FROM public.fact_transfer_suggestion WHERE run_id = %s::uuid) AS target_shops,
          (SELECT count(DISTINCT from_shop_code) FROM public.fact_transfer_suggestion WHERE run_id = %s::uuid) AS source_shops
        """,
        (run_id, run_id, run_id, run_id, run_id, run_id, run_id, run_id, run_id, run_id, run_id, run_id, run_id),
    )
    row = cur.fetchone() or [0] * 15
    out = {
        "shop_count": int(row[0] or 0),
        "article_count": int(row[1] or 0),
        "sales_rows": int(row[2] or 0),
        "stock_rows": int(row[3] or 0),
        "transfer_rows": int(row[4] or 0),
        "transfer_qty_total": _to_float(row[5]),
        "feature_rows": int(row[6] or 0),
        "order_rows": int(row[7] or 0),
        "order_qty_total": _to_float(row[8]),
        "order_budget_total": _to_float(row[9]),
        "avg_sellout_clamped": _to_float(row[10]),
        "critical_rows_total": int(row[11] or 0),
        "critical_deficit_total": _to_float(row[12]),
        "target_shops": int(row[13] or 0),
        "source_shops": int(row[14] or 0),
    }
    transfer_rows_count = max(out["transfer_rows"], 1)
    out["transfer_avg_qty"] = _to_float(out["transfer_qty_total"] / transfer_rows_count)
    return out


def _build_kpi_deltas(current_kpis: Dict[str, Any], baseline_kpis: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    tracked_keys = [
        "shop_count",
        "article_count",
        "sales_rows",
        "stock_rows",
        "transfer_rows",
        "transfer_qty_total",
        "transfer_avg_qty",
        "feature_rows",
        "order_rows",
        "order_qty_total",
        "order_budget_total",
        "avg_sellout_clamped",
        "critical_rows_total",
        "critical_deficit_total",
        "target_shops",
        "source_shops",
        "next_current_candidates",
        "next_current_qty_total",
        "next_current_budget_total",
        "next_current_positive_delta_count",
        "next_current_delta_positive_total",
    ]
    out: Dict[str, Any] = {}
    for key in tracked_keys:
        cur_v = _to_float(current_kpis.get(key))
        base_v = _to_float((baseline_kpis or {}).get(key)) if baseline_kpis else 0.0
        abs_delta = cur_v - base_v
        pct_delta = None if base_v == 0 else (abs_delta / base_v) * 100.0
        out[key] = {
            "current": cur_v,
            "previous": base_v,
            "abs": abs_delta,
            "pct": pct_delta,
        }
    return out


def _dashboard_export_rows(payload: Dict[str, Any], table_key: str) -> List[Dict[str, Any]]:
    rows = payload.get("tables", {}).get(table_key)
    if not isinstance(rows, list):
        return []
    cols = DASHBOARD_TABLE_COLUMNS.get(table_key) or []
    if not cols and rows:
        cols = list(rows[0].keys())
    out: List[Dict[str, Any]] = []
    for row in rows:
        item: Dict[str, Any] = {}
        for col in cols:
            item[col] = row.get(col)
        out.append(item)
    return out


def _resolve_dashboard_run(cur, run_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if isinstance(run_id, str) and run_id.strip():
        run_text = run_id.strip()
        try:
            uuid.UUID(run_text)
        except Exception:
            return None
        cur.execute(
            """
            SELECT run_id, run_type, status, started_at, finished_at, metadata
            FROM public.etl_run
            WHERE run_id = %s::uuid
            LIMIT 1
            """,
            (run_text,),
        )
    else:
        cur.execute(
            """
            SELECT run_id, run_type, status, started_at, finished_at, metadata
            FROM public.etl_run
            ORDER BY COALESCE(finished_at, started_at) DESC
            LIMIT 1
            """
        )

    row = cur.fetchone()
    if not row:
        return None
    metadata = row[5] if isinstance(row[5], dict) else {}
    business_context = _run_business_context(metadata, row[1])
    return {
        "run_id": str(row[0]),
        "run_type": row[1],
        "run_type_label": business_context["friendly_run_type"],
        "status": _status_to_ui(row[2]),
        "status_raw": row[2],
        "status_label": _friendly_status(row[2]),
        "started_at": row[3].isoformat() if row[3] else None,
        "finished_at": row[4].isoformat() if row[4] else None,
        "metadata": metadata,
        "business_context": business_context,
    }


def _dashboard_payload(run_id: Optional[str], table_limit: int = 30) -> Dict[str, Any]:
    if psycopg is None:
        return {"connected": False, "reason": "psycopg non installato"}

    try:
        dsn = get_db_dsn()
    except Exception as exc:
        return {"connected": False, "reason": str(exc)}

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                run = _resolve_dashboard_run(cur, run_id)
                if run is None:
                    return {
                        "connected": True,
                        "run": None,
                        "kpis": {},
                        "charts": {},
                        "tables": {},
                        "reason": "Nessuna run disponibile",
                    }

                rid = run["run_id"]
                kpis = _fetch_kpi_core(cur, rid)

                baseline_run = None
                baseline_kpis = None
                kpi_deltas: Dict[str, Any] = {}

                cur.execute(
                    """
                    SELECT COALESCE(finished_at, started_at) AS anchor_ts
                    FROM public.etl_run
                    WHERE run_id = %s::uuid
                    LIMIT 1
                    """,
                    (rid,),
                )
                anchor_row = cur.fetchone()
                anchor_ts = anchor_row[0] if anchor_row else None
                prev_row = None

                if anchor_ts is not None:
                    cur.execute(
                        """
                        SELECT run_id, run_type, status, started_at, finished_at, metadata
                        FROM public.etl_run
                        WHERE run_id <> %s::uuid
                          AND COALESCE(finished_at, started_at) < %s
                        ORDER BY COALESCE(finished_at, started_at) DESC
                        LIMIT 1
                        """,
                        (rid, anchor_ts),
                    )
                    prev_row = cur.fetchone()

                if prev_row is None:
                    cur.execute(
                        """
                        SELECT run_id, run_type, status, started_at, finished_at, metadata
                        FROM public.etl_run
                        WHERE run_id <> %s::uuid
                        ORDER BY COALESCE(finished_at, started_at) DESC
                        LIMIT 1
                        """,
                        (rid,),
                    )
                    prev_row = cur.fetchone()

                if prev_row:
                    prev_metadata = prev_row[5] if isinstance(prev_row[5], dict) else {}
                    prev_business_context = _run_business_context(prev_metadata, prev_row[1])
                    baseline_run = {
                        "run_id": str(prev_row[0]),
                        "run_type": prev_row[1],
                        "run_type_label": prev_business_context["friendly_run_type"],
                        "status": _status_to_ui(prev_row[2]),
                        "status_raw": prev_row[2],
                        "status_label": _friendly_status(prev_row[2]),
                        "started_at": prev_row[3].isoformat() if prev_row[3] else None,
                        "finished_at": prev_row[4].isoformat() if prev_row[4] else None,
                        "business_context": prev_business_context,
                    }
                    baseline_kpis = _fetch_kpi_core(cur, baseline_run["run_id"])

                cur.execute(
                    """
                    SELECT to_shop_code AS label, SUM(qty) AS value
                    FROM public.fact_transfer_suggestion
                    WHERE run_id = %s::uuid
                    GROUP BY to_shop_code
                    ORDER BY value DESC
                    LIMIT 12
                    """,
                    (rid,),
                )
                transfer_to = _fetch_chart_rows(cur)

                cur.execute(
                    """
                    SELECT from_shop_code AS label, SUM(qty) AS value
                    FROM public.fact_transfer_suggestion
                    WHERE run_id = %s::uuid
                    GROUP BY from_shop_code
                    ORDER BY value DESC
                    LIMIT 12
                    """,
                    (rid,),
                )
                transfer_from = _fetch_chart_rows(cur)

                cur.execute(
                    """
                    SELECT COALESCE(reason, 'n/a') AS label, SUM(qty) AS value
                    FROM public.fact_transfer_suggestion
                    WHERE run_id = %s::uuid
                    GROUP BY COALESCE(reason, 'n/a')
                    ORDER BY value DESC
                    LIMIT 12
                    """,
                    (rid,),
                )
                transfer_reason = _fetch_chart_rows(cur)

                cur.execute(
                    """
                    SELECT
                      CONCAT(COALESCE(season_code, 'n/a'), ' · ', COALESCE(mode, 'n/a')) AS label,
                      SUM(COALESCE(totale_qty, 0)) AS value
                    FROM public.fact_order_forecast
                    WHERE run_id = %s::uuid
                    GROUP BY season_code, mode
                    ORDER BY value DESC
                    LIMIT 12
                    """,
                    (rid,),
                )
                orders_by_season_mode = _fetch_chart_rows(cur)

                cur.execute(
                    """
                    SELECT
                      module AS label,
                      SUM(COALESCE(totale_qty, 0)) AS value
                    FROM public.fact_order_forecast
                    WHERE run_id = %s::uuid
                    GROUP BY module
                    ORDER BY value DESC
                    LIMIT 10
                    """,
                    (rid,),
                )
                orders_by_module = _fetch_chart_rows(cur)

                cur.execute(
                    """
                    SELECT
                      COALESCE(mode, 'n/a') AS label,
                      SUM(COALESCE(totale_qty, 0)) AS value
                    FROM public.fact_order_forecast
                    WHERE run_id = %s::uuid
                    GROUP BY COALESCE(mode, 'n/a')
                    ORDER BY value DESC
                    LIMIT 10
                    """,
                    (rid,),
                )
                orders_by_mode = _fetch_chart_rows(cur)

                cur.execute(
                    """
                    SELECT
                      shop_code AS label,
                      SUM(demand_hybrid - stock_after) AS value
                    FROM public.fact_feature_state
                    WHERE run_id = %s::uuid
                      AND demand_hybrid IS NOT NULL
                      AND stock_after IS NOT NULL
                      AND (demand_hybrid - stock_after) > 0
                    GROUP BY shop_code
                    ORDER BY value DESC
                    LIMIT 12
                    """,
                    (rid,),
                )
                critical_by_shop = _fetch_chart_rows(cur)

                cur.execute(
                    """
                    SELECT article_code, size, from_shop_code, to_shop_code, COALESCE(reason, '') AS reason, qty
                    FROM public.fact_transfer_suggestion
                    WHERE run_id = %s::uuid
                    ORDER BY qty DESC, article_code ASC
                    LIMIT %s
                    """,
                    (rid, table_limit),
                )
                transfer_rows = _fetch_dict_rows(cur)

                cur.execute(
                    """
                    SELECT module, season_code, mode, article_code, totale_qty, predizione_vendite, budget_acquisto
                    FROM public.fact_order_forecast
                    WHERE run_id = %s::uuid
                    ORDER BY totale_qty DESC NULLS LAST, article_code ASC
                    LIMIT %s
                    """,
                    (rid, table_limit),
                )
                order_rows = _fetch_dict_rows(cur)

                cur.execute(
                    """
                    SELECT
                      article_code,
                      shop_code,
                      demand_hybrid,
                      stock_after,
                      (demand_hybrid - stock_after) AS deficit
                    FROM public.fact_feature_state
                    WHERE run_id = %s::uuid
                      AND demand_hybrid IS NOT NULL
                      AND stock_after IS NOT NULL
                      AND (demand_hybrid - stock_after) > 0
                    ORDER BY deficit DESC
                    LIMIT %s
                    """,
                    (rid, table_limit),
                )
                critical_rows = _fetch_dict_rows(cur)

                cur.execute(
                    """
                    WITH latest_cont AS (
                        SELECT season_code
                        FROM public.fact_order_source
                        WHERE run_id = %s::uuid
                          AND module = 'continuativa'
                          AND season_code IS NOT NULL
                        GROUP BY season_code
                        ORDER BY season_code DESC
                        LIMIT 1
                    ),
                    global_factor AS (
                        SELECT COALESCE(
                            AVG(
                                CASE
                                    WHEN COALESCE(os.venduto_periodo, 0) > 0
                                    THEN fo.totale_qty / NULLIF(os.venduto_periodo, 0)
                                    ELSE NULL
                                END
                            ),
                            1.0
                        ) AS factor
                        FROM public.fact_order_forecast fo
                        JOIN public.fact_order_source os
                          ON os.run_id = fo.run_id
                         AND os.module = 'current'
                         AND os.article_code = fo.article_code
                         AND os.season_code IS NOT DISTINCT FROM fo.season_code
                        WHERE fo.run_id = %s::uuid
                          AND fo.module = 'current'
                          AND fo.mode = 'math'
                    ),
                    factor_by_attr AS (
                        SELECT
                          os.categoria,
                          os.tipologia,
                          AVG(
                              CASE
                                  WHEN COALESCE(os.venduto_periodo, 0) > 0
                                  THEN fo.totale_qty / NULLIF(os.venduto_periodo, 0)
                                  ELSE NULL
                              END
                          ) AS factor
                        FROM public.fact_order_forecast fo
                        JOIN public.fact_order_source os
                          ON os.run_id = fo.run_id
                         AND os.module = 'current'
                         AND os.article_code = fo.article_code
                         AND os.season_code IS NOT DISTINCT FROM fo.season_code
                        WHERE fo.run_id = %s::uuid
                          AND fo.module = 'current'
                          AND fo.mode = 'math'
                        GROUP BY os.categoria, os.tipologia
                    )
                    SELECT
                      c.season_code AS from_cont_season,
                      c.article_code,
                      c.categoria,
                      c.tipologia,
                      c.marchio,
                      c.colore,
                      c.materiale,
                      COALESCE(c.venduto_periodo, 0) AS venduto_periodo,
                      COALESCE(c.giacenza, 0) AS giacenza,
                      COALESCE(c.prezzo_acquisto, 0) AS prezzo_acquisto,
                      COALESCE(fa.factor, gf.factor, 1.0) AS applied_factor,
                      GREATEST(0, ROUND(COALESCE(c.venduto_periodo, 0) * COALESCE(fa.factor, gf.factor, 1.0))) AS predicted_current_qty,
                      ROUND(
                          GREATEST(0, ROUND(COALESCE(c.venduto_periodo, 0) * COALESCE(fa.factor, gf.factor, 1.0)))
                          - COALESCE(c.giacenza, 0),
                          2
                      ) AS delta_vs_stock,
                      ROUND(
                          GREATEST(0, ROUND(COALESCE(c.venduto_periodo, 0) * COALESCE(fa.factor, gf.factor, 1.0)))
                          * COALESCE(c.prezzo_acquisto, 0),
                          2
                      ) AS predicted_budget,
                      ROUND(COALESCE(c.venduto_periodo, 0) / NULLIF(COALESCE(c.giacenza, 0) + 1, 0), 4) AS transition_score
                    FROM public.fact_order_source c
                    JOIN latest_cont lc ON lc.season_code = c.season_code
                    LEFT JOIN factor_by_attr fa
                      ON fa.categoria IS NOT DISTINCT FROM c.categoria
                     AND fa.tipologia IS NOT DISTINCT FROM c.tipologia
                    CROSS JOIN global_factor gf
                    WHERE c.run_id = %s::uuid
                      AND c.module = 'continuativa'
                    ORDER BY transition_score DESC, predicted_current_qty DESC, c.article_code ASC
                    """,
                    (rid, rid, rid, rid),
                )
                next_current_all = _fetch_dict_rows(cur)
                next_current_rows = next_current_all[:table_limit]

                next_current_by_category_map: Dict[str, float] = {}
                next_current_delta_positive_map: Dict[str, float] = {}
                next_current_qty_total = 0.0
                next_current_budget_total = 0.0
                next_current_positive_count = 0
                next_current_delta_positive_total = 0.0
                for row in next_current_all:
                    cat = str(row.get("categoria") or "n/a")
                    qty = float(row.get("predicted_current_qty") or 0.0)
                    budget = float(row.get("predicted_budget") or 0.0)
                    delta = float(row.get("delta_vs_stock") or 0.0)
                    next_current_by_category_map[cat] = next_current_by_category_map.get(cat, 0.0) + qty
                    next_current_qty_total += qty
                    next_current_budget_total += budget
                    if delta > 0:
                        next_current_positive_count += 1
                        next_current_delta_positive_total += delta
                        next_current_delta_positive_map[cat] = next_current_delta_positive_map.get(cat, 0.0) + delta

                next_current_by_category = [
                    {"label": k, "value": v}
                    for k, v in sorted(next_current_by_category_map.items(), key=lambda kv: kv[1], reverse=True)[:12]
                ]
                next_current_delta_positive_by_category = [
                    {"label": k, "value": v}
                    for k, v in sorted(next_current_delta_positive_map.items(), key=lambda kv: kv[1], reverse=True)[:12]
                ]

                kpis["next_current_candidates"] = int(len(next_current_all))
                kpis["next_current_qty_total"] = float(next_current_qty_total)
                kpis["next_current_budget_total"] = float(next_current_budget_total)
                kpis["next_current_positive_delta_count"] = int(next_current_positive_count)
                kpis["next_current_delta_positive_total"] = float(next_current_delta_positive_total)
                kpi_deltas = _build_kpi_deltas(kpis, baseline_kpis)

                cur.execute(
                    """
                    SELECT season_code, SUM(COALESCE(totale_qty, 0)) AS qty
                    FROM public.fact_order_forecast
                    WHERE run_id = %s::uuid
                      AND season_code IS NOT NULL
                    GROUP BY season_code
                    ORDER BY season_code DESC
                    LIMIT 2
                    """,
                    (rid,),
                )
                season_rows = cur.fetchall()
                if season_rows:
                    latest_code, latest_qty = season_rows[0][0], _to_float(season_rows[0][1])
                    kpis["season_latest_code"] = str(latest_code) if latest_code is not None else None
                    kpis["season_latest_qty"] = latest_qty
                    if len(season_rows) > 1:
                        prev_code, prev_qty_raw = season_rows[1][0], season_rows[1][1]
                        prev_qty = _to_float(prev_qty_raw)
                        kpis["season_prev_code"] = str(prev_code) if prev_code is not None else None
                        kpis["season_prev_qty"] = prev_qty
                        kpis["season_qty_delta"] = latest_qty - prev_qty
                        kpis["season_qty_delta_pct"] = None if prev_qty == 0 else ((latest_qty - prev_qty) / prev_qty) * 100.0
                    else:
                        kpis["season_prev_code"] = None
                        kpis["season_prev_qty"] = None
                        kpis["season_qty_delta"] = None
                        kpis["season_qty_delta_pct"] = None
                else:
                    kpis["season_latest_code"] = None
                    kpis["season_latest_qty"] = None
                    kpis["season_prev_code"] = None
                    kpis["season_prev_qty"] = None
                    kpis["season_qty_delta"] = None
                    kpis["season_qty_delta_pct"] = None

                return {
                    "connected": True,
                    "run": run,
                    "baseline_run": baseline_run,
                    "kpis": kpis,
                    "kpi_deltas": kpi_deltas,
                    "charts": {
                        "transfer_to": transfer_to,
                        "transfer_from": transfer_from,
                        "transfer_reason": transfer_reason,
                        "orders_by_season_mode": orders_by_season_mode,
                        "orders_by_module": orders_by_module,
                        "orders_by_mode": orders_by_mode,
                        "critical_by_shop": critical_by_shop,
                        "next_current_by_category": next_current_by_category,
                        "next_current_delta_positive_by_category": next_current_delta_positive_by_category,
                    },
                    "tables": {
                        "transfer_proposals": transfer_rows,
                        "order_proposals": order_rows,
                        "critical_articles": critical_rows,
                        "next_current_candidates": next_current_rows,
                    },
                }
    except Exception as exc:
        return {"connected": False, "reason": str(exc)}


@app.get("/")
def index():
    page = STATIC_DIR / "index.html"
    if not page.exists():
        raise HTTPException(status_code=404, detail="UI non trovata")
    return FileResponse(page)


@app.get("/api/health")
def api_health():
    return {"ok": True, "time": _now_iso(), "active_run_id": run_manager.current_run_id}


@app.get("/api/settings")
def api_settings():
    return settings_store.get()


@app.post("/api/settings/developer-mode")
def api_set_developer_mode(payload: DeveloperModePayload):
    return settings_store.set_developer_mode(payload.enabled)


@app.post("/api/run")
def api_start_run(options: RunOptions):
    payload = options.model_dump() if hasattr(options, "model_dump") else options.dict()
    try:
        run = run_manager.start_run(payload)
        return {"ok": True, "run": run.to_public(_developer_mode())}
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@app.get("/api/runs")
def api_runs(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    source: str = Query(default="all"),
    status: Optional[str] = Query(default=None),
    run_type: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    sort_by: str = Query(default="started_at"),
    sort_dir: str = Query(default="desc"),
):
    dev = _developer_mode()
    source_norm = (source or "all").strip().lower()
    if source_norm not in {"all", "db", "ui"}:
        raise HTTPException(status_code=422, detail="source deve essere: all, db, ui")

    sort_by_norm = (sort_by or "started_at").strip().lower()
    if sort_by_norm not in {"run_id", "source", "run_type", "status", "started_at", "ended_at", "return_code"}:
        raise HTTPException(
            status_code=422,
            detail="sort_by deve essere: run_id, source, run_type, status, started_at, ended_at, return_code",
        )
    sort_dir_norm = (sort_dir or "desc").strip().lower()
    if sort_dir_norm not in {"asc", "desc"}:
        raise HTTPException(status_code=422, detail="sort_dir deve essere: asc, desc")

    status_value = status.strip() if isinstance(status, str) else None
    run_type_value = run_type.strip() if isinstance(run_type, str) else None
    q_value = q.strip() if isinstance(q, str) else None

    all_runs = _combined_recent_runs(
        developer_mode=dev,
        source=source_norm,
        status=status_value,
        run_type=run_type_value,
        q=q_value,
        sort_by=sort_by_norm,
        sort_dir=sort_dir_norm,
        fetch_target=offset + limit,
    )
    total = len(all_runs)
    runs = all_runs[offset : offset + limit]
    return {
        "runs": runs,
        "total": total,
        "offset": offset,
        "limit": limit,
        "has_more": (offset + limit) < total,
    }


@app.get("/api/runs/{run_id}")
def api_run_detail(run_id: str):
    run = run_manager.get_run(run_id)
    dev = _developer_mode()
    if run is not None:
        return run.to_public(dev)

    db_run = _db_run_detail(run_id)
    if db_run is not None:
        if not dev:
            db_run = dict(db_run)
            db_run.pop("metadata", None)
        return db_run

    raise HTTPException(status_code=404, detail="run non trovata")


@app.get("/api/runs/{run_id}/logs")
def api_run_logs(run_id: str, tail: int = Query(default=200, ge=0, le=5000)):
    if not _developer_mode():
        raise HTTPException(status_code=403, detail="Developer mode disattivato")
    if run_manager.get_run(run_id) is None:
        raise HTTPException(status_code=404, detail="log disponibili solo per run avviate da questa UI")
    try:
        logs = run_manager.get_logs(run_id, tail=tail)
        return {"run_id": run_id, "lines": logs}
    except KeyError:
        raise HTTPException(status_code=404, detail="run non trovata")


@app.post("/api/runs/{run_id}/stop")
def api_run_stop(run_id: str):
    if run_manager.get_run(run_id) is None:
        if _db_run_detail(run_id) is not None:
            raise HTTPException(status_code=409, detail="Stop disponibile solo per run avviate da questa UI")
        raise HTTPException(status_code=404, detail="run non trovata")
    try:
        run = run_manager.stop_run(run_id)
        return {"ok": True, "run": run.to_public(_developer_mode())}
    except KeyError:
        raise HTTPException(status_code=404, detail="run non trovata")


@app.get("/api/outputs")
def api_outputs():
    return {"generated_at": _now_iso(), "files": _output_summary()}


@app.get("/api/db/status")
def api_db_status():
    return _db_status()


@app.get("/api/dashboard/runs")
def api_dashboard_runs(limit: int = Query(default=100, ge=1, le=500)):
    rows = _db_recent_runs(limit=limit)
    runs = []
    for row in rows:
        runs.append(
            {
                "run_id": row.get("run_id"),
                "run_type": row.get("run_type"),
                "run_type_label": row.get("run_type_label"),
                "status": row.get("status"),
                "status_raw": row.get("status_raw"),
                "status_label": row.get("status_label"),
                "started_at": row.get("started_at"),
                "ended_at": row.get("ended_at"),
                "business_context": row.get("business_context"),
            }
        )
    return {"runs": runs}


@app.get("/api/dashboard")
def api_dashboard(
    run_id: Optional[str] = Query(default=None),
    table_limit: int = Query(default=30, ge=5, le=200),
):
    return _dashboard_payload(run_id=run_id, table_limit=table_limit)


@app.get("/api/dashboard/export")
def api_dashboard_export(
    table_key: str = Query(...),
    run_id: Optional[str] = Query(default=None),
    fmt: str = Query(default="xlsx"),
    table_limit: int = Query(default=50000, ge=50, le=200000),
):
    key = (table_key or "").strip().lower()
    if key not in DASHBOARD_TABLE_COLUMNS:
        raise HTTPException(status_code=422, detail=f"table_key non valido: {table_key}")

    fmt_norm = (fmt or "xlsx").strip().lower()
    if fmt_norm not in {"xlsx", "csv"}:
        raise HTTPException(status_code=422, detail="fmt deve essere 'xlsx' o 'csv'")

    payload = _dashboard_payload(run_id=run_id, table_limit=table_limit)
    if not payload.get("connected", False):
        raise HTTPException(status_code=409, detail=payload.get("reason") or "dashboard non disponibile")
    if not payload.get("run"):
        raise HTTPException(status_code=404, detail="run dashboard non trovata")

    rows = _dashboard_export_rows(payload, key)
    cols = DASHBOARD_TABLE_COLUMNS.get(key) or []

    run_obj = payload.get("run") or {}
    run_short = str(run_obj.get("run_id") or "na")[:8]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if fmt_norm == "csv":
        filename = f"barca_{key}_{run_short}_{ts}.csv"
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        data = out.getvalue().encode("utf-8-sig")
        return StreamingResponse(
            io.BytesIO(data),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    try:
        import pandas as pd
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Export Excel non disponibile: {exc}")

    filename = f"barca_{key}_{run_short}_{ts}.xlsx"
    df = pd.DataFrame(rows, columns=cols)
    out_xlsx = io.BytesIO()
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="dashboard")
    out_xlsx.seek(0)

    return StreamingResponse(
        out_xlsx,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("enterprise_ui:app", host="0.0.0.0", port=8080, reload=False)
