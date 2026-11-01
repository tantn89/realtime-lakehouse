# Deployment guide

Two ways to run the same pipeline: locally with Docker Compose, or on one EC2 instance with
Terraform. Design background: [system-architecture.md](system-architecture.md).

## Local run

Prerequisites: Docker with Compose 2.24.4 or later (the overlays use `!reset` and `!override`)
and 8 GB of memory for Docker. The full stack measured 4.8 GB at steady state, and Spark
Connect needs more while maintenance runs. See [Troubleshooting](#troubleshooting).

```bash
docker compose up -d --build
docker compose run --rm spark python -m wiki_lakehouse.smoke_check
```

The first command starts everything: Redpanda, topic creation, the producer, SeaweedFS, the
bucket, the Postgres-backed REST catalog, the three Spark streams, Spark Connect, and Dagster
(webserver and daemon). The schedules are on by default, so gold marts and checks run without
any action.

The producer needs a Wikimedia-compliant User-Agent (tool name plus contact). The compose file
has a default; set your own:

```bash
WIKIMEDIA_USER_AGENT="my-demo/0.1 (you@example.com)" docker compose up -d --build
```

### Ports

All published ports bind to `127.0.0.1` only, because nothing in the stack authenticates.

| Port | Service | Use |
|---|---|---|
| 3000 | Dagster UI | Jobs, schedules, asset checks. It can launch any job, hence loopback only. |
| 19092 | Redpanda (Kafka API) | External listener for host tools; containers use `redpanda:9092`. |
| 8333 | SeaweedFS S3 | Local S3 with fixed placeholder credentials. |
| 8181 | Iceberg REST catalog | Catalog API for host-side DuckDB or Spark. |

Spark Connect (`15002`) is deliberately not published: it runs arbitrary SQL without auth.

### Useful commands

| Task | Command |
|---|---|
| Stack works end to end (writes and reads an Iceberg table) | `docker compose run --rm spark python -m wiki_lakehouse.smoke_check` |
| Row counts, duplicates, latency, stream progress | `docker compose run --rm spark python -m wiki_lakehouse.lakehouse_status` |
| Prove exactly-once (crash test, own compose project, no ports) | `python scripts/prove_exactly_once.py` (add `--keep` to leave the stack up) |
| Benchmarks (writes `benchmarks/results.md` and `results.json`) | `python scripts/run_benchmarks.py [--seconds 45] [--rates 100,1000,5000,15000]` |
| Replay the recorded sample at a rate | `docker compose run --rm --no-deps producer replay data/recordings/sample.jsonl --rate 500 --loops 10 --shift-to-now --id-prefix demo-` |
| Follow a service | `docker compose logs -f stream` |
| Stop, keep data | `docker compose down` |
| Stop and delete all data | `docker compose down -v` |

The proof and benchmark scripts use standard-library Python and run their own isolated stacks
(`lakehouse-it`, `lakehouse-bench`), so they can run next to the dev stack. Expected results and
their meaning: [benchmarks/results.md](../benchmarks/results.md) and the docstrings of
`scripts/*.py`.

## AWS run

A short-lived demo: one `t3.xlarge` (4 vCPU, 16 GB) running the same compose stack, S3 for
storage, the Glue Data Catalog as the Iceberg catalog, Athena for queries. A measured run and its
cost are in [benchmarks/aws-run.md](../benchmarks/aws-run.md).

Prerequisites: Terraform, an AWS account and CLI profile (default profile name `lakehouse`; set
`aws_profile = null` in `terraform.tfvars` to use the default credential chain), and the Session
Manager plugin for the AWS CLI.

1. **Bundle the code.** The instance fetches a tarball from S3. `git archive` includes committed
   files only, so commit first:
   ```bash
   mkdir -p build
   git archive --format=tar.gz -o build/lakehouse.tar.gz HEAD
   ```
2. **Configure.** Copy `terraform/terraform.tfvars.example` to `terraform/terraform.tfvars`
   (git-ignored) and set at least `budget_email`. Optional: `region`, `instance_type`,
   `max_hours`, `budget_usd`. Defaults are in `terraform/variables.tf`.
3. **Apply.**
   ```bash
   terraform -chdir=terraform init
   terraform -chdir=terraform apply
   ```
   This creates the bucket (public access blocked, TLS-only), Glue databases `bronze`, `silver`
   and `ops`, an Athena workgroup, the IAM role, a security group with no inbound rules, the
   instance, and a monthly budget. The instance bootstraps itself
   (`terraform/user-data.sh.tftpl`, log at `/var/log/lakehouse-bootstrap.log`).
4. **Reach it through SSM.** No SSH key and no open port. `terraform output shell` prints the
   shell command; `terraform output dagster_ui` prints a port-forward to
   `http://localhost:3000`.
5. **Query with Athena.** Use the workgroup from `terraform output athena_workgroup`, for
   example `SELECT count(*) FROM silver.edits` once the streams have committed. The workgroup
   caps each query at 1 GB scanned.
6. **Tear down.**
   ```bash
   terraform -chdir=terraform destroy
   ```
   The bucket is `force_destroy`, so this deletes the data too.

### Cost controls

| Control | Behavior | Source |
|---|---|---|
| Self-terminating instance | A systemd timer powers the instance off `max_hours` (default 6) after boot; shutdown behavior is `terminate`, so compute and EBS stop billing. S3 and Glue data stay until `destroy`. The timer is absolute and persistent, so a reboot does not cancel it. | `terraform/main.tf`, `terraform/user-data.sh.tftpl` |
| Budget | Monthly budget (default $5) with alerts at 80% actual and 100% forecast, sent to `budget_email`. | `terraform/main.tf` |
| Athena cap | 1 GB scanned per query. | `terraform/main.tf` |

A budget alert does not stop anything; the timer and `destroy` do.

## Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| Containers exit or restart repeatedly on Docker Desktop | Not enough memory. The stack runs several JVMs (Spark streams and Spark Connect default to a 2 GB driver each, Redpanda is started with 1 GB). Raise the memory limit in Docker Desktop settings. A minimum has not been measured. |
| Apple Silicon or other arm64 host | Supported. The Spark image resolves the JDK path per architecture. |
| `docker compose ... config` fails on `!reset` or `!override` | Compose is too old; upgrade to Compose v2. |
| Producer exits at start | `WIKIMEDIA_USER_AGENT` is empty; set it. Exit code 1 later means Kafka did not confirm a delivery; compose restarts it from the saved position. |
| Port already in use | Another stack holds 3000, 19092, 8333 or 8181. Stop the old stack with `docker compose down` instead of changing ports. |
| Dagster daemon cannot load code on a fresh volume | Two processes migrated SQLite at once. The compose file makes the daemon wait for a healthy webserver; recreate with `docker compose down -v` if the volume predates that. |
| Gold refresh runs are skipped | A previous run is still active (single DuckDB writer). Check the run in the Dagster UI. |
| Terraform `apply` fails on the bundle | `build/lakehouse.tar.gz` is missing; see step 1, or set `bundle_path`. |
