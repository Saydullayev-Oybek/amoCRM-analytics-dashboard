"""amoCRM → PostgreSQL ETL — ishga tushirish nuqtasi.

Ishlatish:
    python pipeline.py

Birinchi ishga tushirishda barcha ma'lumot to'liq tortiladi (backfill).
Keyingi ishga tushirishlarda faqat yangi/o'zgargan yozuvlar tortiladi
(dlt o'zining state mexanizmi orqali).
"""

from __future__ import annotations

import logging
import sys

import dlt

from amocrm.client import build_client
from amocrm.config import ConfigError, load_auth, load_postgres_config
from amocrm.source import amocrm_source

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("amocrm")


def check_alpha_filter(client) -> None:
    """`filter[updated_at]` (Alpha) yoqilganini tekshiradi va o'chiq bo'lsa ogohlantiradi.

    Bu filtr amoCRM'da Alpha bosqichida va hisob sozlamalarida yoqilishi kerak.
    O'chiq bo'lsa, filtr e'tiborsiz qoldiriladi va har safar to'liq skaner bo'ladi.
    """
    try:
        resp = client.get(
            "/api/v4/account", params={"with": "is_api_filter_enabled"}
        )
        resp.raise_for_status()
        enabled = resp.json().get("is_api_filter_enabled")
    except Exception as exc:  # noqa: BLE001 - tekshiruv pipeline'ni to'xtatmasin
        log.warning("Alpha-filtr holatini tekshirib bo'lmadi: %s", exc)
        return

    if enabled:
        log.info("is_api_filter_enabled = true — incremental filtr yoqilgan.")
    else:
        log.warning(
            "DIQQAT: is_api_filter_enabled o'chiq! filter[updated_at] e'tiborsiz "
            "qoldiriladi va har run to'liq skaner bo'lishi mumkin. "
            "amoCRM hisob sozlamalarida 'Alpha-фильтрация'ni yoqing."
        )


def print_load_summary(pipeline: dlt.Pipeline) -> None:
    """Har bir jadval uchun nechta yozuv yuklanganini konsolga chiqaradi."""
    trace = pipeline.last_trace
    row_counts = {}
    if trace is not None and trace.last_normalize_info is not None:
        row_counts = dict(trace.last_normalize_info.row_counts)

    log.info("=" * 48)
    log.info("Yuklangan yozuvlar (jadval bo'yicha):")
    if not row_counts:
        log.info("  (yangi/o'zgargan yozuv topilmadi)")
    for table in sorted(row_counts):
        if table.startswith("_dlt"):
            continue
        log.info("  %-30s %8d", table, row_counts[table])
    log.info("=" * 48)


def main() -> int:
    try:
        auth = load_auth()
        pg = load_postgres_config()
    except ConfigError as exc:
        log.error("Konfiguratsiya xatosi: %s", exc)
        return 1

    log.info("amoCRM hisobi: %s.amocrm.ru", auth["subdomain"])

    client = build_client(auth)
    check_alpha_filter(client)

    pipeline = dlt.pipeline(
        pipeline_name="amocrm",
        destination=dlt.destinations.postgres(credentials=pg["credentials"]),
        dataset_name=pg["dataset_name"],
        progress="log",
    )

    load_info = pipeline.run(amocrm_source(client))
    log.info("Yuklash yakunlandi: %s", load_info)
    print_load_summary(pipeline)
    return 0


if __name__ == "__main__":
    sys.exit(main())
