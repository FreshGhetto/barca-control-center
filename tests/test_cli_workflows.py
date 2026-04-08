from __future__ import annotations

import contextlib
import json
import time
import unittest

from fastapi.testclient import TestClient

from tests._support import (
    ORDER_DETAIL_HISTORY_DIR,
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
        write_minimal_shop_report_csv(stock_dir)

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
            str(ORDER_DETAIL_HISTORY_DIR),
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
