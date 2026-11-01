# Code standards

Conventions the repo actually follows. The configuration files are the authority; this page
records the reasons.

## Tooling

| Tool | Setting | Source |
|---|---|---|
| ruff | line length 100, rules `E F I B UP SIM RUF`, target py310 | `pyproject.toml` |
| mypy | `strict = true`, files `src` and `scripts` (scripts are type-checked too) | `pyproject.toml` |
| pytest | `testpaths = ["tests"]`, quiet output | `pyproject.toml` |
| Python | `>=3.10` declared; images and CI use 3.11 | `pyproject.toml`, `docker/*/Dockerfile`, CI |

Run the same checks as CI:

```bash
pip install -e ".[dev]"
ruff check src tests scripts
mypy
pytest
```

The package declares no runtime dependencies. Extras: `producer` (httpx, confluent-kafka,
jsonschema), `spark` (pyspark 3.5.3, for local transform tests), `dev`. PySpark normally comes from
the Spark image.

## Naming

- Python modules and scripts are `snake_case` because they must be importable. Names are long
  and descriptive on purpose (`resume_cursor.py`, `iceberg_maintenance.py`) so a search for a
  concept lands on one file.
- Docs, plans and non-Python assets use kebab-case (`docs/code-standards.md`); Terraform and
  compose files follow their tools' conventions.
- Iceberg tables are `<layer>.<name>`: `bronze.recentchange_raw`, `silver.edits`,
  `silver.editor_sessions`, `ops.stream_progress`. Constants for them live in
  `streaming/lakehouse_tables.py`; import them, do not retype the strings.
- Environment variables share the `LAKEHOUSE_` prefix (see `spark_session.py`).

## Annotations: where `from __future__ import annotations` is not used

Producer, streaming, reliability, benchmarks and `scripts/` use `from __future__ import
annotations`. The orchestration modules (`definitions.py`, `lakehouse_checks.py`,
`iceberg_maintenance.py`) deliberately do not, and each says so in a comment. Dagster inspects the
real type of `context` and other annotations at decoration time; postponed (string) annotations
break that. Dagster and dagster-dbt are installed only in the orchestration image, so mypy
relaxes `disallow_untyped_decorators` for `wiki_lakehouse.orchestration.*`.

## Driver scripts: standard library only

`scripts/prove_exactly_once.py` and `scripts/run_benchmarks.py` import nothing outside the
standard library. They only drive `docker compose` and parse marker lines (`COUNTS `,
`RESULT `) printed by containers, so they run on any host, including CI, without installing
the project. Keep it that way; put logic that needs Spark or Kafka in `src/wiki_lakehouse/` and
run it in a container.

## Code style

- Comments explain why (an invariant, a rejected option, a failure that happened), not what.
  Module docstrings state purpose and guarantees; read them before changing a module.
- Transformations are pure functions over DataFrames (`streaming/edit_transforms.py`) so they
  are unit-testable without a stream.
- Publishing sits behind a small protocol (`Publisher`) so routing and replay test without a
  broker; `confluent_kafka` is imported lazily for the same reason.
- Observability must not stop the data path: a failed progress write is logged and retried.
- Local credentials in compose files and `docker/seaweedfs/s3.json` are throwaway placeholders
  for the local store. Never commit real secrets; `.env`, `*.tfvars`, `*.tfstate*` and `plans/`
  are git-ignored.

## Tests

| Location | Needs | How it runs |
|---|---|---|
| `tests/test_producer.py` | Python deps only | Host `pytest` (CI `lint-and-unit`). |
| `tests/test_edit_transforms.py` | PySpark and a JVM | Skipped on a host without PySpark (`pytest.importorskip`). Runs in the Spark image in CI. |
| End to end | Docker | `scripts/prove_exactly_once.py` |

Run the Spark tests in the image the way CI does. The Spark image has pytest but not the tests,
so mount them (and the recordings) in:

```bash
docker compose -f docker-compose.yml -f docker-compose.it.yml -p lakehouse-it build stream
docker compose -f docker-compose.yml -f docker-compose.it.yml -p lakehouse-it \
  run --rm --no-deps -v "$PWD/tests:/app/tests" -v "$PWD/data/recordings:/app/data/recordings" \
  spark pytest -q tests/test_edit_transforms.py
```

Orchestration modules have no committed unit tests; they were verified against a live stack
(see [project-roadmap.md](project-roadmap.md)).

## Commits

Conventional commits (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`), scoped to one change.
Messages explain the behavior or invariant, not a ticket or phase number.
