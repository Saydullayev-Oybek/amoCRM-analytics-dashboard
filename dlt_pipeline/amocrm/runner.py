"""ETL yadrosi: dlt pipeline/source yasash, bitta table'ni ishga tushirish va
har run natijasini `etl_run_log` audit jadvaliga yozish.

CLI (pipeline.py) ham, Airflow DAG ham shu funksiyalarni chaqiradi — logika
bir joyda. Og'ir ishlar (client yasash, so'rov) faqat funksiya ichida bajariladi,
DAG parse paytida emas.
"""

from __future__ import annotations

import logging                          # log xabarlari uchun

import dlt                              # ETL freymvork
import psycopg2                         # PostgreSQL bilan bevosita ishlash (audit jadvali) — dlt[postgres] bilan keladi

from .client import build_client        # sozlangan RESTClient yasovchi
from .config import load_auth, load_postgres_config  # config o'qish
from .source import amocrm_source       # 11 resource'li dlt manbasi

log = logging.getLogger("amocrm")       # "amocrm" loggeri

# Audit jadval nomi (schema/dataset_name ichida yaratiladi).
AUDIT_TABLE = "etl_run_log"


def build_pipeline() -> dlt.Pipeline:
    """PostgreSQL'ga yozadigan dlt pipeline yaratadi (state shu pipeline nomida)."""
    pg = load_postgres_config()                                  # ulanish + dataset_name
    return dlt.pipeline(
        pipeline_name="amocrm",                                 # state shu nom bilan saqlanadi/tiklanadi
        destination=dlt.destinations.postgres(credentials=pg["credentials"]),  # nishon baza
        dataset_name=pg["dataset_name"],                        # schema
        progress="log",                                         # jarayonni logga chiqaradi
    )


def build_source():
    """auth.json'dan client yasab, amoCRM dlt manbasini qaytaradi."""
    auth = load_auth()                                          # subdomen + token
    client = build_client(auth)                                 # throttled RESTClient
    return amocrm_source(client)                                # barcha resource'li source


def ensure_audit_table() -> None:
    """`etl_run_log` jadvali yo'q bo'lsa yaratadi (schema bilan birga)."""
    pg = load_postgres_config()                                 # credentials + schema
    schema = pg["dataset_name"]
    ddl = f"""
        CREATE SCHEMA IF NOT EXISTS "{schema}";
        CREATE TABLE IF NOT EXISTS "{schema}"."{AUDIT_TABLE}" (
            id           BIGSERIAL PRIMARY KEY,
            dag_run_id   TEXT,                                  -- Airflow run identifikatori
            table_name   TEXT        NOT NULL,                  -- qaysi table (resource)
            status       TEXT        NOT NULL,                  -- success / failed
            rows_loaded  BIGINT,                                -- shu run'da yuklangan qatorlar soni
            load_id      TEXT,                                  -- dlt load package id
            error        TEXT,                                  -- xato matni (failed bo'lsa)
            logged_at    TIMESTAMPTZ NOT NULL DEFAULT now()     -- yozuv vaqti
        );
    """
    with psycopg2.connect(pg["credentials"]) as conn:          # bazaga ulanamiz
        with conn.cursor() as cur:                             # kursor ochamiz
            cur.execute(ddl)                                   # jadvalni yaratamiz (agar yo'q bo'lsa)
        conn.commit()                                          # o'zgarishni tasdiqlaymiz


def write_audit(
    table_name: str,
    status: str,
    dag_run_id: str = "",
    rows_loaded: int | None = None,
    load_id: str = "",
    error: str = "",
) -> None:
    """Bitta table run natijasini `etl_run_log`'ga yozadi."""
    pg = load_postgres_config()                                # credentials + schema
    schema = pg["dataset_name"]
    sql = f"""
        INSERT INTO "{schema}"."{AUDIT_TABLE}"
            (dag_run_id, table_name, status, rows_loaded, load_id, error)
        VALUES (%s, %s, %s, %s, %s, %s);
    """
    with psycopg2.connect(pg["credentials"]) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (dag_run_id, table_name, status, rows_loaded, load_id, error[:2000]))
        conn.commit()


def run_table(table_name: str, dag_run_id: str = "") -> int:
    """Bitta resource'ni (table'ni) yuklaydi va natijani audit jadvaliga yozadi.

    Muvaffaqiyatda yuklangan qatorlar sonini qaytaradi; xatoda audit yozib,
    xatoni qayta ko'taradi (Airflow task fail bo'lsin).
    """
    ensure_audit_table()                                       # audit jadval borligiga ishonch hosil qilamiz
    pipeline = build_pipeline()                                # dlt pipeline
    source = build_source()                                    # amoCRM source
    try:
        # Faqat bitta resource'ni ishga tushiramiz (qolganlari alohida task'larda).
        load_info = pipeline.run(source.with_resources(table_name))  # yuklash
        counts = {}                                            # jadval → qatorlar soni
        trace = pipeline.last_trace                            # oxirgi run tafsiloti
        if trace is not None and trace.last_normalize_info is not None:
            counts = dict(trace.last_normalize_info.row_counts)
        rows = int(counts.get(table_name, 0))                 # shu table bo'yicha son
        load_id = load_info.loads_ids[0] if load_info.loads_ids else ""  # dlt load id
        write_audit(                                           # muvaffaqiyatni yozamiz
            table_name, "success", dag_run_id=dag_run_id,
            rows_loaded=rows, load_id=load_id,
        )
        log.info("'%s' yuklandi: %d qator", table_name, rows)
        return rows
    except Exception as exc:                                   # har qanday xatoda
        write_audit(                                           # xatoni audit jadvaliga yozamiz
            table_name, "failed", dag_run_id=dag_run_id, error=str(exc),
        )
        log.error("'%s' yuklashda xato: %s", table_name, exc)
        raise                                                 # Airflow task fail bo'lishi uchun qayta ko'taramiz
