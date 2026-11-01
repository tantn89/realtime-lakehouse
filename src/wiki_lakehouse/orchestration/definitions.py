"""Dagster definitions: what exists, how it connects, and when it runs.

- The Spark streams are not Dagster jobs (they run continuously); their tables appear as
  external assets so lineage, checks and freshness live in one graph with the dbt marts.
- dbt gold marts rebuild every 5 minutes; health checks run every 5 minutes; Iceberg
  maintenance runs hourly.
- A gold refresh is skipped while the previous one is still queued or running: the DuckDB
  gold file has a single writer, so an overlapping run would fail on its lock.
"""

# No `from __future__ import annotations`: Dagster inspects the real type of `context`.
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    AssetSpec,
    DagsterInstance,
    DagsterRunStatus,
    DefaultScheduleStatus,
    Definitions,
    RunRequest,
    RunsFilter,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SkipReason,
    define_asset_job,
    schedule,
)
from dagster_dbt import DbtCliResource, dbt_assets

from wiki_lakehouse.orchestration.iceberg_maintenance import iceberg_maintenance
from wiki_lakehouse.orchestration.lakehouse_checks import CHECKS

DBT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "dbt"))

stream_tables = [
    AssetSpec(["bronze", "recentchange_raw"], group_name="streaming", kinds={"spark", "iceberg"},
              description="Raw Kafka records, written by the bronze stream."),
    AssetSpec(["silver", "edits"], group_name="streaming", kinds={"spark", "iceberg"},
              deps=[["bronze", "recentchange_raw"]],
              description="Typed, deduplicated edits, written by the silver stream."),
    AssetSpec(["silver", "editor_sessions"], group_name="streaming", kinds={"spark", "iceberg"},
              deps=[["silver", "edits"]],
              description="Editor sessions (20-minute gap), written by the sessions stream."),
]


@dbt_assets(manifest=DBT_DIR / "target" / "manifest.json")
def gold_marts(context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator[Any]:
    yield from dbt.cli(["build"], context=context).stream()


refresh_gold = define_asset_job("refresh_gold", selection=AssetSelection.assets(gold_marts))
lakehouse_checks = define_asset_job(
    "lakehouse_checks", selection=AssetSelection.checks(*CHECKS))

IN_FLIGHT = [DagsterRunStatus.QUEUED, DagsterRunStatus.NOT_STARTED, DagsterRunStatus.STARTING,
             DagsterRunStatus.STARTED]


def in_flight_run_id(instance: DagsterInstance, job_name: str) -> str | None:
    """Id of a run of `job_name` that is queued or running, if any."""
    runs = instance.get_runs(filters=RunsFilter(job_name=job_name, statuses=IN_FLIGHT), limit=1)
    return runs[0].run_id if runs else None


# Running by default, so `docker compose up` alone gives a working pipeline.
@schedule(job=refresh_gold, cron_schedule="*/5 * * * *",
          default_status=DefaultScheduleStatus.RUNNING)
def refresh_gold_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    run_id = in_flight_run_id(context.instance, refresh_gold.name)
    if run_id:
        return SkipReason(f"refresh_gold run {run_id} is still queued or running")
    return RunRequest()


defs = Definitions(
    assets=[*stream_tables, gold_marts],
    asset_checks=CHECKS,
    jobs=[refresh_gold, lakehouse_checks, iceberg_maintenance],
    schedules=[
        refresh_gold_schedule,
        ScheduleDefinition(job=lakehouse_checks, cron_schedule="*/5 * * * *",
                           default_status=DefaultScheduleStatus.RUNNING),
        ScheduleDefinition(job=iceberg_maintenance, cron_schedule="7 * * * *",
                           default_status=DefaultScheduleStatus.RUNNING),
    ],
    resources={"dbt": DbtCliResource(project_dir=str(DBT_DIR), profiles_dir=str(DBT_DIR))},
)
