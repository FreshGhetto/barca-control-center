from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from tests._support import (
    TempPathMixin,
    apply_schema,
    fetch_scalar,
    load_enterprise_ui,
    patched_env,
    resolved_db_env,
    temporary_database,
    wait_for_status,
    write_minimal_catalog_xlsx,
)


class CatalogApiWorkflowTests(unittest.TestCase, TempPathMixin):
    db_ctx = None
    db_env: dict[str, str]
    catalog_run_id: str
    article_code: str
    season_code: str
    supplier: str

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        base_env = resolved_db_env()
        cls.db_ctx = temporary_database(base_env, prefix="barca_cat_")
        cls.db_env = cls.db_ctx.__enter__()
        apply_schema(cls.db_env)

        fixture_root = cls.make_temp_dir()
        catalog_xlsx = write_minimal_catalog_xlsx(fixture_root)

        with patched_env(cls.db_env):
            module = load_enterprise_ui()
            with TestClient(module.app) as client:
                with catalog_xlsx.open("rb") as fh:
                    start = client.post(
                        "/api/catalog/import",
                        files={
                            "excel_files": (
                                catalog_xlsx.name,
                                io.BytesIO(fh.read()),
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            )
                        },
                        data={"sheet": "0", "create_schema": "false"},
                    )
                if start.status_code != 200:
                    raise AssertionError(start.text)
                job_id = start.json()["job"]["job_id"]
                final = wait_for_status(
                    lambda: client.get(f"/api/catalog/import-jobs/{job_id}").json(),
                    success={"success"},
                    failure={"failed"},
                    timeout=120.0,
                    interval=1.0,
                )
                cls.catalog_run_id = str(final["run_id"])

        cls.article_code = str(
            fetch_scalar(
                cls.db_env,
                """
                SELECT article_code
                FROM fact_catalog_article_store_snapshot
                WHERE run_id = %s::uuid
                  AND store_code <> 'XX'
                ORDER BY article_code
                LIMIT 1
                """,
                (cls.catalog_run_id,),
            )
            or ""
        )
        cls.season_code = str(
            fetch_scalar(
                cls.db_env,
                """
                SELECT season_code
                FROM fact_catalog_article_store_snapshot
                WHERE run_id = %s::uuid
                  AND store_code <> 'XX'
                ORDER BY season_code
                LIMIT 1
                """,
                (cls.catalog_run_id,),
            )
            or ""
        )
        cls.supplier = str(
            fetch_scalar(
                cls.db_env,
                """
                SELECT supplier
                FROM fact_catalog_article_store_snapshot
                WHERE run_id = %s::uuid
                  AND store_code = 'XX'
                ORDER BY supplier
                LIMIT 1
                """,
                (cls.catalog_run_id,),
            )
            or ""
        )

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            cls.cleanup_temp_dir()
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

    def test_catalog_endpoints_return_data(self) -> None:
        self.assertTrue(self.catalog_run_id)
        self.assertTrue(self.article_code)
        self.assertTrue(self.season_code)
        self.assertTrue(self.supplier)

        with self.ui_client() as client:
            status = client.get(f"/api/catalog/status?run_id={self.catalog_run_id}")
            articles = client.get(f"/api/catalog/articles?run_id={self.catalog_run_id}&limit=10&offset=0")
            detail = client.get(
                f"/api/catalog/article-detail?run_id={self.catalog_run_id}&article_code={self.article_code}&season_code={self.season_code}"
            )
            active = client.get("/api/catalog/import-jobs/active")

        self.assertEqual(status.status_code, 200)
        self.assertTrue(status.json()["available"])
        self.assertIn(self.supplier, status.json()["facets"]["suppliers"])
        self.assertEqual(articles.status_code, 200)
        self.assertGreater(len(articles.json()["rows"]), 0)
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["summary"]["article_code"], self.article_code)
        self.assertEqual(active.status_code, 200)
        self.assertIsNone(active.json()["job"])

    def test_catalog_showcase_export_and_downloads(self) -> None:
        with self.ui_client() as client:
            export = client.post(
                "/api/catalog/showcase/export",
                json={
                    "run_id": self.catalog_run_id,
                    "export_mode": "html",
                    "primary_source": "web",
                    "allow_fallback": False,
                    "selected_seasons": [self.season_code],
                    "selected_reparti": [],
                    "selected_suppliers": [self.supplier],
                    "selected_categories": [],
                    "selected_brands": [],
                    "manual_codes_text": self.article_code,
                    "photo_root": "",
                    "photo_position": "xl",
                    "allow_position_variants": True,
                    "jpg_layout": "minimal",
                },
            )
            self.assertEqual(export.status_code, 200, msg=export.text)
            body = export.json()
            download = client.get(body["download_url"])
            html = client.get(body["html_preview_url"])

        self.assertEqual(download.status_code, 200)
        self.assertEqual(download.headers.get("content-type"), "application/zip")
        self.assertEqual(html.status_code, 200)
        self.assertTrue(html.headers.get("content-type", "").startswith("text/html"))
        self.assertEqual(body["summary"]["filters"]["selected_suppliers"], [self.supplier])

    def test_catalog_import_lock_rejects_parallel_jobs(self) -> None:
        with tempfile.TemporaryDirectory(prefix="barca_catalog_lock_") as tmp_dir:
            catalog_xlsx = write_minimal_catalog_xlsx(Path(tmp_dir), article_code="09/LOCKTEST")

            with self.ui_client() as client:
                with catalog_xlsx.open("rb") as first_fh:
                    first = client.post(
                        "/api/catalog/import",
                        files={
                            "excel_files": (
                                catalog_xlsx.name,
                                io.BytesIO(first_fh.read()),
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            )
                        },
                        data={"sheet": "0", "create_schema": "false"},
                    )
                self.assertEqual(first.status_code, 200, msg=first.text)

                with catalog_xlsx.open("rb") as second_fh:
                    second = client.post(
                        "/api/catalog/import",
                        files={
                            "excel_files": (
                                catalog_xlsx.name,
                                io.BytesIO(second_fh.read()),
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            )
                        },
                        data={"sheet": "0", "create_schema": "false"},
                    )
                self.assertEqual(second.status_code, 409)

                job_id = first.json()["job"]["job_id"]
                wait_for_status(
                    lambda: client.get(f"/api/catalog/import-jobs/{job_id}").json(),
                    success={"success"},
                    failure={"failed"},
                    timeout=120.0,
                    interval=1.0,
                )

    def test_catalog_showcase_async_job_lock_and_status(self) -> None:
        payload = {
            "run_id": self.catalog_run_id,
            "export_mode": "html",
            "primary_source": "web",
            "allow_fallback": False,
            "selected_seasons": [self.season_code],
            "selected_reparti": [],
            "selected_suppliers": [self.supplier],
            "selected_categories": [],
            "selected_brands": [],
            "manual_codes_text": self.article_code,
            "photo_root": "",
            "photo_position": "xl",
            "allow_position_variants": True,
            "jpg_layout": "minimal",
        }
        with self.ui_client() as client:
            first = client.post("/api/catalog/showcase/jobs", json=payload)
            self.assertEqual(first.status_code, 200, msg=first.text)
            second = client.post("/api/catalog/showcase/jobs", json=payload)
            self.assertEqual(second.status_code, 409)

            job_id = first.json()["job"]["job_id"]
            final = wait_for_status(
                lambda: client.get(f"/api/catalog/showcase/jobs/{job_id}").json(),
                success={"success"},
                failure={"failed"},
                timeout=180.0,
                interval=1.0,
            )
            latest = client.get("/api/catalog/showcase/jobs/latest")
            active = client.get("/api/catalog/showcase/jobs/active")

        self.assertEqual(final.get("status"), "success")
        self.assertEqual(latest.status_code, 200)
        self.assertEqual(latest.json()["job"]["job_id"], job_id)
        self.assertEqual(active.status_code, 200)
        self.assertIsNone(active.json()["job"])
