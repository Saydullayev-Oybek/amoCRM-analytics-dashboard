# AGENTS.md

Guidance for Codex when working in this repository.

## What this is

An **incremental ETL pipeline** that pulls data from the **amoCRM (Kommo) v4 API**
and loads it **raw** (full JSON) into **PostgreSQL**, built on the
[dlt](https://dlthub.com) library. No transformation happens here — flattening
custom fields, pivoting, etc. is deferred to a downstream (dbt) layer.

## Commands

**Docker is the supported way to run this project** (README documents only that
path). A fresh clone has no `.venv` at all.

```bash
# --- Everything (Airflow + analytics-db + Metabase) ---
docker compose up -d --build     # UI: Airflow http://localhost:8080 (admin/admin), Metabase :3000
docker compose logs -f airflow-scheduler
docker compose down              # stop; add -v to also drop the data volumes

# --- Inside the containers (no host Python needed) ---
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt_project && dbt build --target docker"
docker compose exec airflow-scheduler airflow tasks test amocrm_etl dbt_build   # run ONE task standalone
docker compose exec airflow-scheduler airflow dags list-import-errors           # DAG parse check
docker compose exec analytics-db psql -U postgres -d amocrm                     # host psql not required
```

Optional host-side venv — only for running dlt/dbt manually outside Docker.
Python **3.12 or 3.13 only** (dlt does not support 3.14):

```bash
uv pip install --python .venv/bin/python -r dlt_pipeline/requirements.txt
./.venv/bin/python dlt_pipeline/pipeline.py   # whole pipeline, CLI entry point
./.venv/bin/dlt pipeline amocrm info          # dlt state / last load

# dbt MUST run from inside dbt_project/ — that's where profiles.yml is found
cd dbt_project && ../.venv/bin/dbt build      # run + test interleaved (target: dev)
cd dbt_project && ../.venv/bin/dbt debug      # show which profiles.yml / connection is used
```

There is no Python test suite; `dbt build` (schema + singleton tests) is the only
automated check.

## Configuration (git-ignored, never commit)

- `auth.json` — `{ "subdomain", "access_token" }` (long-lived amoCRM token).
- `postgres_config.json` — `{ host, port, database, username, password, schema }`;
  `schema` becomes the dlt `dataset_name`.
- `.example` templates exist for both. Secrets are read only from these files —
  never hard-code tokens/subdomains in code.
- **Config discovery** (`config.py`): looked up via `AMOCRM_CONFIG_DIR` env (used in
  Docker), else searched upward from CWD, else from the module dir (repo root). So
  the CLI works from any subdirectory.

## Architecture

```
dlt_pipeline/                # EL layer (dlt)
  amocrm/
    config.py   # read & validate auth.json + postgres_config.json; build PG conn string
    client.py   # RESTClient + rate-limit (≤6 req/s) + 429 retry + HAL pagination
    source.py   # @dlt.source with 11 resources (one per entity); RESOURCE_NAMES list
    runner.py   # reusable core: build_pipeline/build_source, run_table(), etl_run_log audit,
                #   auto-reset of local dlt state when the destination DB changes
  pipeline.py   # CLI entry point: load config, run whole source into postgres, print summary
  requirements.txt
dbt_project/                 # T layer (dbt) — star schema over raw_data
  profiles.yml        # gitignored. TWO targets: dev (localhost:5433) / docker (analytics-db:5432)
  models/staging/     # views, 1:1 translation of raw tables
  models/intermediate/  # currently empty (kept for future use)
  models/marts/       # fct_leads, dim_managers, dim_stages, mart_leads_{monthly,by_stage}
  tests/              # singleton tests (assert_lead_count_preserved)
dags/
  amocrm_dag.py       # Airflow DAG: one task per table, sequential, then dbt_build, */15 schedule
docker/Dockerfile     # apache/airflow image + dlt[postgres] + requests + dbt-postgres
docker/init-analytics-db.sql  # runs ONLY on a virgin analytics-db volume: creates metabase_ro
docker-compose.yaml   # Airflow (LocalExecutor) + analytics-db + metabase & metabase-db
```

Config files (`auth.json`, `postgres_config.json`) stay at the repo root.

Data flows: `dlt_pipeline/pipeline.py` → `amocrm_source(client)` (source.py) →
each resource paginates via `paginate()` (client.py) → dlt normalizes & loads into PostgreSQL.

Under Airflow: `dags/amocrm_dag.py` → `run_table(name)` (runner.py) →
`pipeline.run(source.with_resources(name))` — **one task per table**, run
sequentially so dlt state stays consistent and the 6 req/s limit is respected.

### Entities (11 resources)

- **Full reload:** `account` (replace), `users`, `pipelines`,
  `{leads,contacts,companies}_custom_fields` (merge).
- **Incremental on `updated_at` (merge/upsert):** `leads`, `contacts`,
  `companies`, `tasks`.
- **Incremental on `created_at`, append-only:** `events`.

dlt auto-splits nested arrays into child tables (e.g. `pipelines__statuses`) and
creates its own `_dlt_*` bookkeeping tables.

## Key conventions & gotchas

- **Comments and log messages are written in Uzbek.** Match this style when
  editing existing files.
- **Incremental watermark is managed by dlt's own state** — do not write manual
  watermark files/tables. Resources request `order[...]=asc` and pass
  `filter[updated_at][from]` (events: `filter[created_at][from]`) only when
  `last_value` is set.
- **Alpha filter caveat:** amoCRM's `filter[updated_at]` is an Alpha feature that
  must be enabled in account settings (`is_api_filter_enabled`). If off, the
  filter is *silently ignored* and every run does a full scan. `pipeline.py`
  checks this on startup and warns.
- **Rate limiting:** amoCRM allows 7 req/s; the client throttles to 6. 429s are
  retried honoring `Retry-After`. Note dlt's `paginate` sets a `raise_for_status`
  hook, so 429 can surface as an `HTTPError` inside `Session.send` — handled in
  `ThrottledSession.send`.
- **HTTP 204 (empty page):** list endpoints return 204 when data is exhausted;
  `ThrottledSession` rewrites the body to `{}` so the paginator stops cleanly.
- Pagination follows HAL `_links.next.href` via `JSONLinkPaginator`.
  Page limits: 250 for lists, 100 for `events`.
- **Deletes are NOT propagated:** `merge`/incremental only insert/update records
  that appear in the source. A lead deleted in amoCRM stays in the DB (stale row).
  To reflect deletes, do a periodic full reload or track deletion `events`
  (soft-delete in dbt) — not implemented yet.

## Orchestration (Airflow)

- **One Airflow task per table** (`load_<name>`), built by iterating
  `amocrm.source.RESOURCE_NAMES` at DAG-parse time (no client/I/O at parse).
  Tasks are **chained sequentially** — never parallel — for state safety and rate
  limiting. Each table gets its own status/log/retry in the UI = per-table monitoring.
- **Never** build the client or read config at DAG top level; only inside
  `run_table()` (runner.py), which runs at task execution time.
- **`dbt_build` is the last task in the chain** — a `BashOperator` running
  `dbt build --target docker` after all 11 loads succeed, so marts always match
  the data just loaded. `build` (not `run` + `test`) so a failing test blocks the
  models downstream of it. The `docker` target exists because inside the
  container analytics-db is `analytics-db:5432`, not `localhost:5433` — same
  split as `postgres_config.json`. `DBT_PROFILES_DIR` and the `./dbt_project`
  bind-mount are set in `docker-compose.yaml`.
- **`dbt_build` self-heals a missing `profiles.yml`:** the task copies
  `profiles.yml.example` over it (and logs that it did) before running dbt.
  `profiles.yml` is gitignored, so a fresh clone lacks it, and dbt's error
  (`Could not find profile named 'amocrm'`) names no file — the failure landed
  after all 11 loads with no hint. The example holds no secret; the same password
  sits in plain text in `docker-compose.yaml`. `auth.json` cannot be handled this
  way — it needs a real amoCRM token.
- **Audit table `etl_run_log`** (in the configured schema) records per-run,
  per-table `status` / `rows_loaded` / `load_id` / `error`. Created lazily by
  `ensure_audit_table()`. Query it to see which table failed and why.
- **Config in containers:** `AMOCRM_CONFIG_DIR` env points to the mounted config
  dir; `config.py` reads it. `PYTHONPATH` exposes the `amocrm` package;
  `DLT_DATA_DIR` holds dlt state/logs (bind-mounted at `./dlt_data`).
- **Two Postgres in compose:** `postgres` is Airflow's *own* metadata DB;
  `analytics-db` is the analytics PostgreSQL where amoCRM data lands. They are
  separate. `postgres_config.json` points at `analytics-db` (host = `analytics-db`,
  port 5432 inside the network); from the host connect via `localhost:5433`.
- **Auto state-reset:** `runner.build_pipeline()` fingerprints the destination
  (host+db+schema). If it differs from the last run, the local dlt state for the
  `amocrm` pipeline is wiped so a fresh full backfill runs — prevents
  "relation … does not exist" when pointing at a new/empty DB. The marker lives at
  `<DLT_DATA_DIR>/pipelines/.amocrm_destination`.
- **Full reload toggle:** set `AMOCRM_FULL_REFRESH=1` to force a from-scratch reload
  (runner passes dlt `refresh="drop_resources"`; CLI `pipeline.py` uses
  `"drop_sources"`). In Airflow, trigger the DAG with config `{"full_refresh": true}`
  (`_run` sets the env for that run) — no container restart needed. Off by default.
- **Schedule:** `*/15 * * * *`, `catchup=False`, `max_active_runs=1` (no overlap).
  Retries: 2 per task (on top of client.py's 429/connection retries).

## Dashboard (Metabase)

- **Two extra compose services:** `metabase` (UI on port 3000) and `metabase-db`
  (its own PostgreSQL app DB). A dedicated app DB — not the Airflow `postgres`
  service — because that volume is already initialised, so
  `/docker-entrypoint-initdb.d` scripts no longer fire; and not the default H2,
  which is unfit for backup.
- **Metabase connects as `metabase_ro`**, a read-only role on `analytics-db`
  (SELECT on `marts` + `staging` only; `raw_data` is deliberately not granted).
  Metabase exposes a SQL editor to every logged-in user, so it must never use
  the `postgres` superuser.
- **How `metabase_ro` gets created depends on the machine.**
  `docker/init-analytics-db.sql` is mounted into
  `/docker-entrypoint-initdb.d/`, so on a **fresh** `analytics-db` volume the
  role, the `staging`/`marts` schemas and the grants appear automatically. That
  script never runs on an **existing** volume (Postgres only executes init
  scripts on an empty data dir) — on the original dev machine the role was
  created by hand with the same SQL. If Metabase cannot connect after a
  `docker compose up` on an old volume, this is why; run the script's contents
  manually.
- **`alter default privileges` is load-bearing:** dbt *recreates* mart tables on
  every run, so plain `grant select on all tables` would be lost each time.
  Verified: after `dbt run` rebuilds `fct_leads`, `metabase_ro` can still read it.
- **Dashboards are NOT in git** — they live in the `metabase-db-data` volume.
  Back that volume up; Metabase's YAML serialization is a paid feature. If
  dashboard-as-code ever becomes a requirement, that means moving to Streamlit.
- Inside the compose network Metabase reaches the analytics DB as
  `analytics-db:5432` — the same address dbt's `docker` target uses.

## Reference

- User-facing docs: `README.md` (in Uzbek) — Docker-only setup and run steps.
- Design specs are **not tracked in git**. `docs/superpowers/` is gitignored: the
  marts spec contained real CRM figures and a real person's name, and this repo
  is public. Local copies may exist on a dev machine; do not re-add them.
