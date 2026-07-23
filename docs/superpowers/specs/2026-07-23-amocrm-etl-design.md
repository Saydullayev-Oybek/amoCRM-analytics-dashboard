# amoCRM → PostgreSQL Incremental ETL (dlt) — Design

**Date:** 2026-07-23
**Status:** Approved design → implementation plan next

## Goal

A Python pipeline that pulls all required entities from the amoCRM (Kommo)
CRM Platform API v4 **incrementally** and loads them into PostgreSQL using the
**dlt** (data load tool) library. Data is loaded **raw** (full JSON structure);
custom-field pivoting and other transforms are deferred to a later dbt layer.

## Credentials & configuration

- **`auth.json`** (gitignored) — read at startup:
  ```json
  { "subdomain": "your-subdomain", "access_token": "your-long-lived-token" }
  ```
  Base URL is derived as `https://{subdomain}.amocrm.ru`. No token or subdomain
  is ever hard-coded. An existing `auth.json` using legacy keys (`token`,
  `sub-domen`) is migrated to this format.
- **`postgres_config.json`** (gitignored) — PostgreSQL connection:
  ```json
  { "host": "...", "port": 5432, "database": "...", "username": "...", "password": "...", "schema": "amocrm" }
  ```
  Chosen over dlt `secrets.toml` to keep both config files in one consistent,
  discoverable JSON style (mirrors `auth.json`). The code builds the dlt
  `postgres` destination from this file.
- `auth.json.example` and `postgres_config.example.json` are committed as
  templates (no real secrets).

## API facts confirmed from documentation (QADAM 0)

Read: leads-api, contacts-api, companies-api, tasks-api, users-api,
leads_pipelines, custom-fields, events-and-notes, account-info, filters-api,
api-reference (index). `oauth-20` returned HTTP 404 (page moved) — auth pattern
confirmed from other pages.

**Global rules (Ограничения и рекомендации):**
- Auth: `Authorization: Bearer {access_token}`.
- Rate limit: **7 req/s per integration** (50/s per account). Exceeding → HTTP
  **429**; honor `Retry-After`. We default to **6 req/s** for headroom.
- Pagination: `page` + `limit`; response is HAL JSON with `_links.next.href`
  for the next page and `_embedded.{entity}` for records. Absent `next` (or an
  empty/204 page) ends iteration.

**Documentation gotchas flagged:**
1. **Alpha filter activation** — `filter[updated_at]` filtering is marked Alpha
   and must be enabled per account (`is_api_filter_enabled`). If disabled, the
   filter is silently ignored → every run becomes a full scan. Mitigation: on
   startup fetch `/api/v4/account?with=is_api_filter_enabled` and **warn loudly**
   if disabled.
2. **oauth-20 doc 404** — canonical auth page moved; pattern verified elsewhere.

## Architecture & file layout

```
ETL/
├── auth.json                    # gitignored
├── auth.json.example
├── postgres_config.json         # gitignored
├── postgres_config.example.json
├── amocrm/
│   ├── __init__.py
│   ├── config.py                # load + validate both JSON config files
│   ├── client.py                # reusable request layer (RESTClient wrapper)
│   └── source.py                # dlt source: all 11 resources
├── pipeline.py                  # entry point: build source → run → print stats
├── requirements.txt             # dlt[postgres], requests
├── .gitignore
└── README.md
```

### `amocrm/config.py`
- `load_auth()` → validates presence + type of `subdomain`, `access_token`;
  raises a clear, human-readable error if the file is missing/malformed.
- `load_postgres_config()` → validates required keys; builds the dlt
  `postgres` destination / connection string.

### `amocrm/client.py` — reusable request layer
Built on dlt's `RESTClient` (`dlt.sources.helpers.rest_client`) so dlt owns
pagination and incremental **state** (no manual watermark file/table):
- **Auth:** `BearerTokenAuth(access_token)`, `base_url=https://{subdomain}.amocrm.ru`.
- **Rate limit:** token-bucket limiter capped at ≤7 req/s (default 6) via a
  `requests` session hook — one shared limiter for the whole run.
- **429 retry:** backoff honoring `Retry-After`.
- **Pagination:** `JSONLinkPaginator` following `_links.next.href`; stops when
  `next` is absent.
- **`data_selector`** unwraps `_embedded.{entity}` per resource.

### `amocrm/source.py` — resources
One dlt resource per entity, all sharing the client layer. Full/lookup entities
pull completely; large entities pull incrementally via dlt `incremental`
mapping the cursor into the `filter[...][from]` query param.

## Resource matrix

| # | Resource | Endpoint | limit | Incremental cursor | write_disposition | primary_key |
|---|---|---|---|---|---|---|
| 1 | account | `/api/v4/account` | — (single obj) | — | replace | id |
| 2 | users | `/api/v4/users` | 250 | — (full) | merge | id |
| 3 | pipelines | `/api/v4/leads/pipelines` | — (no paging) | — | merge | id |
| 4 | leads_custom_fields | `/api/v4/leads/custom_fields` | 250 | — | merge | id |
| 5 | contacts_custom_fields | `/api/v4/contacts/custom_fields` | 250 | — | merge | id |
| 6 | companies_custom_fields | `/api/v4/companies/custom_fields` | 250 | — | merge | id |
| 7 | leads | `/api/v4/leads` | 250 | `filter[updated_at][from]` | merge | id |
| 8 | contacts | `/api/v4/contacts` | 250 | `filter[updated_at][from]` | merge | id |
| 9 | companies | `/api/v4/companies` | 250 | `filter[updated_at][from]` | merge | id |
| 10 | tasks | `/api/v4/tasks` | 250 | `filter[updated_at][from]` | merge | id |
| 11 | events | `/api/v4/events` | **100** | `filter[created_at][from]` | **append** | id |

- Incremental resources request `order[updated_at]=asc` (events:
  `order[created_at]=asc`) so pages advance monotonically and dlt's cursor
  tracks the high-water mark.
- **First run:** no `from` → full backfill. **Subsequent runs:** dlt injects
  `from = last_value` from its stored state.
- `events` is **append-only** — never upserted.
- `pipelines` embeds its statuses (`_embedded.statuses[]`); dlt normalizes the
  nested array into a child table automatically.

## Reliability / behavior (QADAM 3)

- Missing/malformed `auth.json` → clear error message, exit before any request.
- 429 → automatic retry with backoff (`Retry-After`).
- Alpha-filter check at startup → loud warning if disabled.
- After `pipeline.run()`, print per-resource load counts from `load_info`.
- First run = full backfill; later runs = incremental only (dlt state).

## Destination tables

dlt creates one table per resource in the configured schema (default `amocrm`):
`account`, `users`, `pipelines` (+ `pipelines__statuses` child), `leads`,
`contacts`, `companies`, `tasks`, `events`, `leads_custom_fields`,
`contacts_custom_fields`, `companies_custom_fields`. Nested arrays
(`custom_fields_values`, `_embedded`, tags, enums) become dlt child tables.
dlt also maintains `_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`.

## Environment

- New Python **3.12/3.13** virtualenv (dlt does not yet support 3.14; existing
  `my_env` is 3.14).
- `requirements.txt`: `dlt[postgres]`, `requests`.

## Out of scope (YAGNI)

- Custom-field pivoting / any transformation (deferred to dbt).
- `leads-unsorted` (not in the 11-entity list).
- OAuth code-exchange / token refresh (long-lived token only).
- Webhooks.
