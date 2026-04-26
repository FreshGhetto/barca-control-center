from __future__ import annotations

import contextlib
import json
import time
import unittest
import uuid

from fastapi.testclient import TestClient
import psycopg

from tests._support import (
    TempPathMixin,
    apply_schema,
    fetch_json_rows,
    fetch_scalar,
    latest_run_id,
    load_enterprise_ui,
    patched_env,
    resolved_db_env,
    run_cli,
    temporary_database,
    wait_for_status,
    write_minimal_order_detail_csv,
    write_minimal_shop_report_csv,
)


def _ensure_completed(completed, command) -> None:
    if completed.returncode != 0:
        raise AssertionError(
            f"Command failed: {' '.join(command)}\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
        )


class DatabaseIntegrationTestCase(unittest.TestCase):
    db_ctx = None
    db_env: dict[str, str]
    base_env: dict[str, str]

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_env = resolved_db_env()
        cls.db_ctx = temporary_database(cls.base_env, prefix="barca_it_")
        cls.db_env = cls.db_ctx.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            if cls.db_ctx is not None:
                cls.db_ctx.__exit__(None, None, None)
        finally:
            super().tearDownClass()

    @contextlib.contextmanager
    def ui_client(self):
        with patched_env(self.db_env):
            module = load_enterprise_ui()
            with TestClient(module.app) as client:
                yield client


class FreshPipelineWorkflowTests(DatabaseIntegrationTestCase):
    raw_run_id: str
    app_run_id: str
    feature_article: str

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        bootstrap_cmd = ["populate_db_from_raw.py", "--skip-ingest", "--db-create-schema"]
        bootstrap = run_cli(bootstrap_cmd, env=cls.db_env, timeout=420)
        _ensure_completed(bootstrap, bootstrap_cmd)

        app_cmd = ["app.py", "--sync-db"]
        app_run = run_cli(app_cmd, env=cls.db_env, timeout=420)
        _ensure_completed(app_run, app_cmd)

        cls.raw_run_id = latest_run_id(cls.db_env, "raw_input_sync") or ""
        cls.app_run_id = latest_run_id(cls.db_env, "app_pipeline") or ""
        cls.feature_article = str(
            fetch_scalar(
                cls.db_env,
                """
                SELECT article_code
                FROM fact_feature_state
                WHERE run_id = %s::uuid
                ORDER BY article_code
                LIMIT 1
                """,
                (cls.app_run_id,),
            )
            or ""
        )

    def test_pipeline_runs_exist_and_have_facts(self) -> None:
        self.assertTrue(self.raw_run_id)
        self.assertTrue(self.app_run_id)
        sales_rows = fetch_scalar(
            self.db_env,
            "SELECT COUNT(*) FROM fact_sales_snapshot WHERE run_id = %s::uuid",
            (self.app_run_id,),
        )
        stock_rows = fetch_scalar(
            self.db_env,
            "SELECT COUNT(*) FROM fact_stock_snapshot WHERE run_id = %s::uuid",
            (self.app_run_id,),
        )
        transfer_rows = fetch_scalar(
            self.db_env,
            "SELECT COUNT(*) FROM fact_transfer_suggestion WHERE run_id = %s::uuid",
            (self.app_run_id,),
        )
        self.assertGreater(int(sales_rows or 0), 0)
        self.assertGreater(int(stock_rows or 0), 0)
        self.assertGreater(int(transfer_rows or 0), 0)

    def test_qa_checks_are_clean(self) -> None:
        qa_cmd = ["qa_checks.py"]
        completed = run_cli(qa_cmd, env=self.db_env, timeout=120)
        _ensure_completed(completed, qa_cmd)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload.get("errors"), [])
        self.assertEqual(payload.get("warnings"), [])

    def test_dashboard_api_endpoints_and_exports(self) -> None:
        self.assertTrue(self.feature_article)
        with self.ui_client() as client:
            health = client.get("/api/health")
            settings = client.get("/api/settings")
            db_status = client.get("/api/db/status")
            outputs = client.get("/api/outputs")
            runs = client.get("/api/runs?limit=5")
            dashboard_runs = client.get("/api/dashboard/runs?limit=20")
            detail = client.get(f"/api/runs/{self.app_run_id}")
            dashboard = client.get(f"/api/dashboard?run_id={self.app_run_id}&table_limit=5")
            article_detail = client.get(
                f"/api/dashboard/article-detail?run_id={self.app_run_id}&article_code={self.feature_article}"
            )
            article_export = client.get(
                f"/api/dashboard/article-detail/export?run_id={self.app_run_id}&article_code={self.feature_article}"
            )
            table_export = client.get(
                f"/api/dashboard/export?run_id={self.app_run_id}&table_key=transfer_proposals&fmt=xlsx&table_limit=500"
            )

        self.assertEqual(health.status_code, 200)
        self.assertEqual(settings.status_code, 200)
        self.assertEqual(db_status.status_code, 200)
        self.assertTrue(db_status.json()["connected"])
        self.assertEqual(outputs.status_code, 200)
        self.assertEqual(runs.status_code, 200)
        self.assertEqual(dashboard_runs.status_code, 200)
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(dashboard.status_code, 200)
        self.assertEqual(article_detail.status_code, 200)
        self.assertEqual(article_export.status_code, 200)
        self.assertEqual(
            article_export.headers.get("content-type"),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        self.assertEqual(table_export.status_code, 200)
        self.assertEqual(
            table_export.headers.get("content-type"),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    def test_ui_run_manager_can_launch_pipeline(self) -> None:
        with self.ui_client() as client:
            response = client.post(
                "/api/run",
                json={
                    "source_db_run_id": self.raw_run_id,
                    "skip_orders": True,
                    "sync_db": True,
                    "db_create_schema": False,
                },
            )
            self.assertEqual(response.status_code, 200)
            run_id = response.json()["run"]["run_id"]

            final = wait_for_status(
                lambda: client.get(f"/api/runs/{run_id}").json(),
                success={"success"},
                failure={"failed", "stopped"},
                timeout=180.0,
                interval=1.0,
            )
            self.assertEqual(final.get("return_code"), 0)

    def test_ui_run_manager_can_stop_running_pipeline(self) -> None:
        with self.ui_client() as client:
            response = client.post(
                "/api/run",
                json={
                    "source_db_run_id": self.raw_run_id,
                    "skip_orders": False,
                    "sync_db": True,
                    "db_create_schema": False,
                },
            )
            self.assertEqual(response.status_code, 200, msg=response.text)
            run_id = response.json()["run"]["run_id"]
            time.sleep(1.0)
            stop = client.post(f"/api/runs/{run_id}/stop")
            self.assertEqual(stop.status_code, 200, msg=stop.text)

            final = wait_for_status(
                lambda: client.get(f"/api/runs/{run_id}").json(),
                success={"stopped"},
                failure={"failed"},
                timeout=60.0,
                interval=1.0,
            )
            self.assertEqual(final.get("status_raw"), "stopped")


class HistoricalImportsWorkflowTests(DatabaseIntegrationTestCase, TempPathMixin):
    raw_history_run_id: str
    detail_history_run_id: str

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        fixture_root = cls.make_temp_dir()
        stock_dir = fixture_root / "shop_reports"
        detail_dir = fixture_root / "history_detail"
        write_minimal_shop_report_csv(stock_dir)
        write_minimal_order_detail_csv(detail_dir)

        apply_schema(cls.db_env)

        shop_cmd = [
            "populate_db_from_shop_reports.py",
            "--stock-dir",
            str(stock_dir),
        ]
        shop_run = run_cli(shop_cmd, env=cls.db_env, timeout=240)
        _ensure_completed(shop_run, shop_cmd)

        detail_cmd = [
            "sync_order_detail_history.py",
            "--detail-dir",
            str(detail_dir),
        ]
        detail_run = run_cli(detail_cmd, env=cls.db_env, timeout=240)
        _ensure_completed(detail_run, detail_cmd)

        cls.raw_history_run_id = latest_run_id(cls.db_env, "raw_input_sync") or ""
        cls.detail_history_run_id = latest_run_id(cls.db_env, "detail_history_sync") or ""

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            cls.cleanup_temp_dir()
        finally:
            super().tearDownClass()

    def test_shop_report_history_import_creates_sales_and_stock(self) -> None:
        self.assertTrue(self.raw_history_run_id)
        metadata = fetch_json_rows(
            self.db_env,
            """
            SELECT metadata->>'season_code', metadata->>'source_kind'
            FROM etl_run
            WHERE run_id = %s::uuid
            """,
            (self.raw_history_run_id,),
        )[0]
        sales_rows = fetch_scalar(
            self.db_env,
            "SELECT COUNT(*) FROM fact_sales_snapshot WHERE run_id = %s::uuid",
            (self.raw_history_run_id,),
        )
        stock_rows = fetch_scalar(
            self.db_env,
            "SELECT COUNT(*) FROM fact_stock_snapshot WHERE run_id = %s::uuid",
            (self.raw_history_run_id,),
        )
        self.assertEqual(metadata[0], "25i")
        self.assertEqual(metadata[1], "shop_stock_report")
        self.assertGreater(int(sales_rows or 0), 0)
        self.assertGreater(int(stock_rows or 0), 0)

    def test_detail_history_sync_populates_order_source(self) -> None:
        self.assertTrue(self.detail_history_run_id)
        order_rows = fetch_scalar(
            self.db_env,
            "SELECT COUNT(*) FROM fact_order_source WHERE run_id = %s::uuid",
            (self.detail_history_run_id,),
        )
        article_rows = fetch_scalar(
            self.db_env,
            """
            SELECT COUNT(*)
            FROM dim_article
            WHERE reparto IS NOT NULL
              AND article_code IN (
                SELECT article_code FROM fact_order_source WHERE run_id = %s::uuid
              )
            """,
            (self.detail_history_run_id,),
        )
        self.assertGreater(int(order_rows or 0), 0)
        self.assertGreater(int(article_rows or 0), 0)


class DashboardSeasonContextTests(DatabaseIntegrationTestCase):
    def test_dashboard_runs_expose_latest_pair_from_available_db_seasons(self) -> None:
        apply_schema(self.db_env)
        run_id = str(uuid.uuid4())
        metadata = {
            "orders_jobs": [
                {"module": "current", "season": "25i", "mode": "math"},
                {"module": "continuativa", "season": "25y", "mode": "math"},
            ],
            "order_source_jobs": [
                {"module": "current", "season": "25i"},
                {"module": "continuativa", "season": "25y"},
            ],
        }
        conn_kwargs = {
            "host": self.db_env.get("BARCA_DB_HOST", "localhost"),
            "port": int(self.db_env.get("BARCA_DB_PORT", "5432")),
            "dbname": self.db_env.get("BARCA_DB_NAME", "barca"),
            "user": self.db_env.get("BARCA_DB_USER", "barca_user"),
            "password": self.db_env.get("BARCA_DB_PASSWORD", ""),
            "sslmode": self.db_env.get("BARCA_DB_SSLMODE", "prefer"),
            "connect_timeout": int(self.db_env.get("BARCA_DB_CONNECT_TIMEOUT", "2")),
        }
        with psycopg.connect(**conn_kwargs) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO etl_run (run_id, run_type, status, metadata) VALUES (%s::uuid, %s, %s, %s::jsonb)",
                    (run_id, "raw_input_sync", "completed", json.dumps(metadata, ensure_ascii=False)),
                )
                cur.execute(
                    """
                    INSERT INTO dim_article (article_code, description, reparto, categoria, tipologia, marchio, colore, materiale)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    ("59/PAIRTEST", "ARTICOLO TEST COPPIA", "SCARPE DONNA", "BALLERINA", "TEST", "ACME", "NERO", "PELLE"),
                )
                cur.execute(
                    """
                    INSERT INTO fact_order_forecast (
                      run_id, module, season_code, mode, article_code, totale_qty, predizione_vendite, prezzo_acquisto, budget_acquisto
                    ) VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (run_id, "current", "25i", "math", "59/PAIRTEST", 5, 7, 10, 50),
                )
                cur.execute(
                    """
                    INSERT INTO fact_order_source (
                      run_id, module, season_code, article_code, categoria, tipologia, marchio, colore, materiale,
                      descrizione, venduto_totale, venduto_periodo, giacenza, venduto_extra, fascia_prezzo, prezzo_listino, prezzo_acquisto, prezzo_vendita
                    ) VALUES
                      (%s::uuid, 'current', '26E', '59/PAIRTEST', 'BALLERINA', 'TEST', 'ACME', 'NERO', 'PELLE', 'TEST 26E', 10, 5, 2, 0, 'MEDIO', 100, 50, 80),
                      (%s::uuid, 'continuativa', '26G', '59/PAIRTEST', 'BALLERINA', 'TEST', 'ACME', 'NERO', 'PELLE', 'TEST 26G', 12, 6, 3, 0, 'MEDIO', 100, 50, 80)
                    """,
                    (run_id, run_id),
                )
            conn.commit()

        with self.ui_client() as client:
            dashboard_runs = client.get("/api/dashboard/runs?limit=50")

        self.assertEqual(dashboard_runs.status_code, 200)
        row = next((item for item in dashboard_runs.json()["runs"] if item.get("run_id") == run_id), None)
        self.assertIsNotNone(row)
        ctx = row["business_context"]
        self.assertEqual(ctx.get("current_seasons"), ["25i"])
        self.assertEqual(ctx.get("continuativa_seasons"), ["25y"])
        self.assertIn("26E", ctx.get("available_current_seasons", []))
        self.assertIn("26G", ctx.get("available_continuativa_seasons", []))
        self.assertEqual(ctx.get("latest_pair_codes"), ["26E", "26G"])


class DashboardRunSelectionTests(DatabaseIntegrationTestCase):
    def test_dashboard_prefers_rich_app_run_over_newer_raw_snapshot(self) -> None:
        apply_schema(self.db_env)
        raw_run_id = str(uuid.uuid4())
        app_run_id = str(uuid.uuid4())
        conn_kwargs = {
            "host": self.db_env.get("BARCA_DB_HOST", "localhost"),
            "port": int(self.db_env.get("BARCA_DB_PORT", "5432")),
            "dbname": self.db_env.get("BARCA_DB_NAME", "barca"),
            "user": self.db_env.get("BARCA_DB_USER", "barca_user"),
            "password": self.db_env.get("BARCA_DB_PASSWORD", ""),
            "sslmode": self.db_env.get("BARCA_DB_SSLMODE", "prefer"),
            "connect_timeout": int(self.db_env.get("BARCA_DB_CONNECT_TIMEOUT", "2")),
        }
        with psycopg.connect(**conn_kwargs) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO dim_shop (shop_code, shop_name, fascia)
                    VALUES (%s, %s, %s)
                    """,
                    [
                        ("AR", "AR", 1),
                        ("BO", "BO", 2),
                    ],
                )
                cur.execute(
                    """
                    INSERT INTO dim_article (article_code, description, reparto, categoria, tipologia, marchio, colore, materiale)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    ("59/RUNSEL", "ARTICOLO TEST RUN", "SCARPE DONNA", "BALLERINA", "TEST", "ACME", "NERO", "PELLE"),
                )
                cur.execute(
                    """
                    INSERT INTO etl_run (run_id, run_type, status, started_at, finished_at, metadata)
                    VALUES (%s::uuid, %s, %s, %s::timestamptz, %s::timestamptz, %s::jsonb)
                    """,
                    (
                        app_run_id,
                        "app_pipeline",
                        "completed",
                        "2026-04-08T10:00:00+02:00",
                        "2026-04-08T10:00:10+02:00",
                        json.dumps(
                            {
                                "counts": {
                                    "fact_transfer_suggestion": 1,
                                    "fact_feature_state": 1,
                                    "fact_sales_snapshot": 1,
                                    "fact_stock_snapshot": 1,
                                }
                            }
                        ),
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO etl_run (run_id, run_type, status, started_at, finished_at, metadata)
                    VALUES (%s::uuid, %s, %s, %s::timestamptz, %s::timestamptz, %s::jsonb)
                    """,
                    (
                        raw_run_id,
                        "raw_input_sync",
                        "completed",
                        "2026-04-08T11:00:00+02:00",
                        "2026-04-08T11:00:10+02:00",
                        json.dumps(
                            {
                                "counts": {
                                    "fact_transfer_suggestion": 0,
                                    "fact_feature_state": 0,
                                    "fact_sales_snapshot": 1,
                                    "fact_stock_snapshot": 1,
                                }
                            }
                        ),
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO fact_sales_snapshot (
                      run_id, snapshot_at, article_code, shop_code, consegnato_qty, venduto_qty, periodo_qty, altro_venduto_qty, sellout_percent, sellout_clamped
                    ) VALUES
                      (%s::uuid, %s::timestamptz, %s, %s, %s, %s, %s, %s, %s, %s),
                      (%s::uuid, %s::timestamptz, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        app_run_id,
                        "2026-04-08T10:00:00+02:00",
                        "59/RUNSEL",
                        "AR",
                        5,
                        2,
                        2,
                        0,
                        40,
                        40,
                        raw_run_id,
                        "2026-04-08T11:00:00+02:00",
                        "59/RUNSEL",
                        "AR",
                        5,
                        1,
                        1,
                        0,
                        20,
                        20,
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO fact_stock_snapshot (
                      run_id, snapshot_at, article_code, shop_code, ricevuto, giacenza, consegnato, venduto, sellout_percent, size_38, size_39, valore_giac
                    ) VALUES
                      (%s::uuid, %s::timestamptz, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),
                      (%s::uuid, %s::timestamptz, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        app_run_id,
                        "2026-04-08T10:00:00+02:00",
                        "59/RUNSEL",
                        "AR",
                        0,
                        4,
                        5,
                        2,
                        40,
                        2,
                        2,
                        100,
                        raw_run_id,
                        "2026-04-08T11:00:00+02:00",
                        "59/RUNSEL",
                        "AR",
                        0,
                        4,
                        5,
                        1,
                        20,
                        2,
                        2,
                        100,
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO fact_feature_state (
                      run_id, article_code, shop_code, fascia, is_outlet, role, demand_hybrid, observed_sales_signal, stock_after,
                      missing_core_sizes, destination_priority_score, source_priority_score, size_38, size_39
                    ) VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (app_run_id, "59/RUNSEL", "AR", 1, False, "STORE", 6, 2, 3, 1, 50, 10, 1, 2),
                )
                cur.execute(
                    """
                    INSERT INTO fact_transfer_suggestion (
                      run_id, article_code, size, from_shop_code, to_shop_code, reason, qty
                    ) VALUES (%s::uuid, %s, %s, %s, %s, %s, %s)
                    """,
                    (app_run_id, "59/RUNSEL", 38, "AR", "BO", "Top-up", 1),
                )
            conn.commit()

        with self.ui_client() as client:
            dashboard_runs = client.get("/api/dashboard/runs?limit=20")
            dashboard = client.get("/api/dashboard?table_limit=5")

        self.assertEqual(dashboard_runs.status_code, 200)
        self.assertEqual(dashboard.status_code, 200)
        runs = dashboard_runs.json()["runs"]
        self.assertEqual(runs[0]["run_id"], app_run_id)
        self.assertIn("metadata", runs[0])
        payload = dashboard.json()
        self.assertEqual(payload["run"]["run_id"], app_run_id)
        self.assertGreater(payload["kpis"]["transfer_rows"], 0)
