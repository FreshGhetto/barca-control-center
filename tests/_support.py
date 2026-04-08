from __future__ import annotations

import contextlib
import csv
import importlib
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional, Sequence

import psycopg
from openpyxl import Workbook
from psycopg import sql


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "db" / "schema.sql"
PYTHON_EXE = Path(sys.executable)
ORDER_DETAIL_HISTORY_DIR = ROOT / "input" / "orders" / "history_detail"


def _connection_kwargs(env: Mapping[str, str]) -> Dict[str, Any]:
    return {
        "host": env.get("BARCA_DB_HOST", "localhost"),
        "port": int(env.get("BARCA_DB_PORT", "5432")),
        "dbname": env.get("BARCA_DB_NAME", "barca"),
        "user": env.get("BARCA_DB_USER", "barca_user"),
        "password": env.get("BARCA_DB_PASSWORD", ""),
        "sslmode": env.get("BARCA_DB_SSLMODE", "prefer"),
        "connect_timeout": int(env.get("BARCA_DB_CONNECT_TIMEOUT", "2")),
    }


def _probe_db(env: Mapping[str, str]) -> Optional[str]:
    try:
        with psycopg.connect(**_connection_kwargs(env)) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return None
    except Exception as exc:  # pragma: no cover - exercised in integration env only
        return str(exc)


def _docker_postgres_env(container: str = "barca-postgres") -> Dict[str, str]:
    try:
        completed = subprocess.run(
            [
                "docker",
                "inspect",
                container,
                "--format",
                "{{range .Config.Env}}{{println .}}{{end}}",
            ],
            cwd=str(ROOT),
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
        )
    except Exception:
        return {}

    values: Dict[str, str] = {}
    for raw_line in completed.stdout.splitlines():
        if "=" not in raw_line:
            continue
        key, value = raw_line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def resolved_db_env() -> Dict[str, str]:
    env = dict(os.environ)
    env.setdefault("BARCA_DB_HOST", "localhost")
    env.setdefault("BARCA_DB_PORT", "5432")
    env.setdefault("BARCA_DB_NAME", "barca")
    env.setdefault("BARCA_DB_USER", "barca_user")
    env.setdefault("BARCA_DB_SSLMODE", "prefer")
    env.setdefault("BARCA_DB_CONNECT_TIMEOUT", "2")

    error = _probe_db(env)
    if error is None:
        return env

    docker_env = _docker_postgres_env()
    if docker_env:
        env["BARCA_DB_NAME"] = docker_env.get("POSTGRES_DB", env["BARCA_DB_NAME"])
        env["BARCA_DB_USER"] = docker_env.get("POSTGRES_USER", env["BARCA_DB_USER"])
        env["BARCA_DB_PASSWORD"] = docker_env.get("POSTGRES_PASSWORD", env.get("BARCA_DB_PASSWORD", ""))
        error = _probe_db(env)
        if error is None:
            return env

    raise unittest.SkipTest(f"PostgreSQL non disponibile per i test di integrazione: {error}")


@contextlib.contextmanager
def patched_env(updates: Mapping[str, str]) -> Iterator[None]:
    old_values: Dict[str, Optional[str]] = {key: os.environ.get(key) for key in updates}
    try:
        for key, value in updates.items():
            os.environ[key] = value
        yield
    finally:
        for key, old_value in old_values.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


@contextlib.contextmanager
def temporary_database(base_env: Mapping[str, str], prefix: str = "barca_test_") -> Iterator[Dict[str, str]]:
    db_name = prefix + uuid.uuid4().hex[:8]
    admin_kwargs = _connection_kwargs(base_env)

    with psycopg.connect(**admin_kwargs) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

    test_env = dict(base_env)
    test_env["BARCA_DB_NAME"] = db_name
    try:
        yield test_env
    finally:
        with psycopg.connect(**admin_kwargs) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
                    (db_name,),
                )
                cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))


def apply_schema(env: Mapping[str, str]) -> None:
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with psycopg.connect(**_connection_kwargs(env)) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()


def run_cli(args: Sequence[str], env: Mapping[str, str], timeout: int = 180) -> subprocess.CompletedProcess[str]:
    merged_env = dict(os.environ)
    merged_env.update(env)
    return subprocess.run(
        [str(PYTHON_EXE), *args],
        cwd=str(ROOT),
        env=merged_env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )


def assert_completed(testcase: unittest.TestCase, completed: subprocess.CompletedProcess[str], command: Sequence[str]) -> None:
    testcase.assertEqual(
        completed.returncode,
        0,
        msg=(
            f"Command failed: {' '.join(command)}\n"
            f"STDOUT:\n{completed.stdout}\n"
            f"STDERR:\n{completed.stderr}"
        ),
    )


def fetch_scalar(env: Mapping[str, str], query: str, params: Sequence[Any] = ()) -> Any:
    with psycopg.connect(**_connection_kwargs(env)) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            row = cur.fetchone()
    return row[0] if row else None


def fetch_row(env: Mapping[str, str], query: str, params: Sequence[Any] = ()) -> Optional[tuple[Any, ...]]:
    with psycopg.connect(**_connection_kwargs(env)) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            return cur.fetchone()


def fetch_json_rows(env: Mapping[str, str], query: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
    with psycopg.connect(**_connection_kwargs(env)) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            return cur.fetchall()


def latest_run_id(env: Mapping[str, str], run_type: str) -> Optional[str]:
    value = fetch_scalar(
        env,
        """
        SELECT run_id::text
        FROM etl_run
        WHERE run_type = %s
          AND status = 'completed'
        ORDER BY COALESCE(finished_at, started_at) DESC
        LIMIT 1
        """,
        (run_type,),
    )
    return str(value) if value else None


def wait_for_status(fetch_fn, *, success: set[str], failure: set[str], timeout: float = 60.0, interval: float = 1.0):
    deadline = time.time() + timeout
    last_payload = None
    while time.time() < deadline:
        payload = fetch_fn()
        last_payload = payload
        state = str(payload.get("status") or payload.get("status_raw") or "").lower()
        if state in success:
            return payload
        if state in failure:
            raise AssertionError(f"Job failed with payload: {json.dumps(payload, ensure_ascii=False, indent=2)}")
        time.sleep(interval)
    raise AssertionError(f"Timeout waiting for status. Last payload: {json.dumps(last_payload, ensure_ascii=False, indent=2)}")


def load_enterprise_ui():
    import enterprise_ui

    return importlib.reload(enterprise_ui)


def write_minimal_catalog_xlsx(target_dir: Path, *, article_code: str = "19/3830160N", season_code: str = "25Y") -> Path:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Catalogo"
    rows = [
        ["STAGIONE", season_code, f"STAGIONE {season_code}"],
        ["FORNITORE", "ACME SUPPLIER"],
        ["REPARTO", "SCARPE UOMO"],
        ["CATEGORIA", "MOCASSINO"],
        ["MARCHIO", "ACME"],
        ["TIPOLOGIA", "CLASSIC"],
        ["ARTICOLO", "", "DESCRIZIONE", "COLORE", "NEG", "GIAC", "CON", "VEN", "% VEN", "40", "41"],
        [article_code, "", "MOCASSINO TEST", "NERO", "AR", 1, 3, 2, 66.67, 1, 0],
        ["", "", "", "", "WEB", 0, 1, 1, 100.0, 0, 1],
    ]
    for row in rows:
        sheet.append(row)

    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / "catalogo_test.xlsx"
    workbook.save(path)
    workbook.close()
    return path


def write_minimal_shop_report_csv(target_dir: Path, *, season_code: str = "25i") -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    destination = target_dir / f"{season_code}_donna.csv"
    rows = [
        ["ARTICOLO", "", "DESCRIZIONE", "COLORE", "NEG", "GIAC", "CON", "VEN", "% VEN", "35", "36", "37"],
        ["59/SHOPTEST", "", "BALLERINA TEST", "NERO", "AR", "2", "5", "3", "60", "1", "1", "0"],
        ["59/SHOPTEST", "", "BALLERINA TEST", "NERO", "WEB", "1", "2", "1", "50", "0", "1", "0"],
    ]
    with destination.open("w", encoding="latin1", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)
    return destination


class TempPathMixin:
    temp_dir: tempfile.TemporaryDirectory[str]

    @classmethod
    def make_temp_dir(cls) -> Path:
        cls.temp_dir = tempfile.TemporaryDirectory(prefix="barca_tests_")
        return Path(cls.temp_dir.name)

    @classmethod
    def cleanup_temp_dir(cls) -> None:
        temp_dir = getattr(cls, "temp_dir", None)
        if temp_dir is not None:
            temp_dir.cleanup()
