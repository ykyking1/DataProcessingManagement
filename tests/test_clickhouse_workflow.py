from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "dagster"))

from clickhouse_driver import Client

from clickhouse_workflow import reconcile_auair_clickhouse_run


@unittest.skipUnless(
    os.getenv("CLICKHOUSE_INTEGRATION_TEST") == "1",
    "set CLICKHOUSE_INTEGRATION_TEST=1 to use the local ClickHouse service",
)
class ClickHouseWorkflowIntegrationTest(unittest.TestCase):
    database = "dpm_clickhouse_workflow_test"
    table = "telemetry_storage"
    visible_view = "telemetry_committed"
    commit_table = "telemetry_workflow_commits"

    def setUp(self) -> None:
        self.previous_environment = {
            key: os.environ.get(key)
            for key in (
                "CLICKHOUSE_DATABASE",
                "CLICKHOUSE_AUAIR_TABLE",
                "CLICKHOUSE_AUAIR_VISIBLE_VIEW",
                "CLICKHOUSE_AUAIR_COMMIT_TABLE",
            )
        }
        os.environ.update(
            {
                "CLICKHOUSE_DATABASE": self.database,
                "CLICKHOUSE_AUAIR_TABLE": self.table,
                "CLICKHOUSE_AUAIR_VISIBLE_VIEW": self.visible_view,
                "CLICKHOUSE_AUAIR_COMMIT_TABLE": self.commit_table,
            }
        )
        admin = Client(
            host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
            port=int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")),
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123"),
        )
        admin.execute(f"DROP DATABASE IF EXISTS `{self.database}`")
        admin.execute(f"CREATE DATABASE `{self.database}`")
        admin.disconnect()

        self.client = Client(
            host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
            port=int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")),
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123"),
            database=self.database,
        )
        self.client.execute(
            f"""
            CREATE TABLE `{self.table}`
            (
                value UInt32,
                source_batch_id String,
                dagster_run_id String DEFAULT ''
            )
            ENGINE = MergeTree
            ORDER BY (source_batch_id, dagster_run_id, value)
            """
        )
        self.client.execute(
            f"""
            CREATE TABLE `{self.commit_table}`
            (
                source_batch_id String,
                dagster_run_id String,
                committed_at DateTime64(6, 'UTC') DEFAULT now64(6)
            )
            ENGINE = MergeTree
            ORDER BY (source_batch_id, committed_at, dagster_run_id)
            """
        )
        self.client.execute(
            f"""
            CREATE VIEW `{self.visible_view}` AS
            SELECT telemetry.*
            FROM `{self.table}` AS telemetry
            LEFT JOIN
            (
                SELECT
                    source_batch_id,
                    argMax(dagster_run_id, committed_at) AS active_dagster_run_id
                FROM `{self.commit_table}`
                GROUP BY source_batch_id
            ) AS committed
                ON committed.source_batch_id = telemetry.source_batch_id
            WHERE telemetry.dagster_run_id = ifNull(
                committed.active_dagster_run_id,
                ''
            )
            """
        )

    def tearDown(self) -> None:
        self.client.disconnect()
        admin = Client(
            host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
            port=int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")),
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123"),
        )
        admin.execute(f"DROP DATABASE IF EXISTS `{self.database}`")
        admin.disconnect()
        for key, value in self.previous_environment.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_success_switches_visibility_and_is_idempotent(self) -> None:
        self.client.execute(
            f"INSERT INTO `{self.table}` VALUES",
            [
                (1, "batch-a", ""),
                (2, "batch-a", "run-success"),
            ],
        )
        self.assertEqual(
            self.client.execute(
                f"SELECT value FROM `{self.visible_view}` ORDER BY value"
            ),
            [(1,)],
        )

        result = reconcile_auair_clickhouse_run(
            batch_id="batch-a",
            dagster_run_id="run-success",
            commit=True,
        )
        repeated = reconcile_auair_clickhouse_run(
            batch_id="batch-a",
            dagster_run_id="run-success",
            commit=True,
        )

        self.assertEqual(result.row_count, 1)
        self.assertEqual(repeated.row_count, 1)
        self.assertEqual(
            self.client.execute(
                f"SELECT value FROM `{self.visible_view}` ORDER BY value"
            ),
            [(2,)],
        )
        self.assertEqual(
            self.client.execute(
                f"SELECT count() FROM `{self.commit_table}` "
                "WHERE source_batch_id = 'batch-a' "
                "AND dagster_run_id = 'run-success'"
            )[0][0],
            1,
        )

    def test_failure_removes_pending_rows(self) -> None:
        self.client.execute(
            f"INSERT INTO `{self.table}` VALUES",
            [(7, "batch-failed", "run-failed")],
        )
        self.assertEqual(
            self.client.execute(f"SELECT count() FROM `{self.visible_view}`")[0][0],
            0,
        )

        result = reconcile_auair_clickhouse_run(
            batch_id="batch-failed",
            dagster_run_id="run-failed",
            commit=False,
        )

        self.assertEqual(result.row_count, 1)
        self.assertEqual(
            self.client.execute(f"SELECT count() FROM `{self.table}`")[0][0],
            0,
        )


if __name__ == "__main__":
    unittest.main()
