"""amoCRM → PostgreSQL ETL — ishga tushirish nuqtasi.

Ishlatish:
    python pipeline.py

Birinchi ishga tushirishda barcha ma'lumot to'liq tortiladi (backfill).
Keyingi ishga tushirishlarda faqat yangi/o'zgargan yozuvlar tortiladi
(dlt o'zining state mexanizmi orqali).
"""

from __future__ import annotations

import logging                              # log sozlash va xabar chiqarish uchun
import sys                                  # dasturdan chiqish kodini qaytarish uchun (exit code)

import dlt                                  # ETL freymvork (pipeline yaratish)

from amocrm.client import build_client      # sozlangan RESTClient yasovchi
from amocrm.config import ConfigError, load_auth, load_postgres_config  # konfiguratsiya o'qish
from amocrm.runner import _full_refresh_enabled  # AMOCRM_FULL_REFRESH env tekshiruvi
from amocrm.source import amocrm_source     # 11 ta resource'li dlt manbasi

logging.basicConfig(                        # loglarni bir marta global sozlaymiz
    level=logging.INFO,                     # INFO va undan yuqori darajadagi xabarlar
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",  # xabar ko'rinishi
    datefmt="%H:%M:%S",                     # vaqt formati (soat:daqiqa:sekund)
)
log = logging.getLogger("amocrm")           # shu modul uchun logger


def check_alpha_filter(client) -> None:
    """`filter[updated_at]` (Alpha) yoqilganini tekshiradi va o'chiq bo'lsa ogohlantiradi.

    Bu filtr amoCRM'da Alpha bosqichida va hisob sozlamalarida yoqilishi kerak.
    O'chiq bo'lsa, filtr e'tiborsiz qoldiriladi va har safar to'liq skaner bo'ladi.
    """
    try:
        resp = client.get(                                       # account'dan filtr holatini so'raymiz
            "/api/v4/account", params={"with": "is_api_filter_enabled"}
        )
        resp.raise_for_status()                                  # xatoni tekshiramiz
        enabled = resp.json().get("is_api_filter_enabled")       # true/false qiymatini olamiz
    except Exception as exc:  # noqa: BLE001 - tekshiruv pipeline'ni to'xtatmasin
        log.warning("Alpha-filtr holatini tekshirib bo'lmadi: %s", exc)  # tekshirish uzilsa faqat ogohlantiramiz
        return                                                   # va davom etamiz (bloklamaymiz)

    if enabled:                                                  # filtr yoqilgan bo'lsa
        log.info("is_api_filter_enabled = true — incremental filtr yoqilgan.")
    else:                                                        # filtr o'chiq bo'lsa
        log.warning(
            "DIQQAT: is_api_filter_enabled o'chiq! filter[updated_at] e'tiborsiz "
            "qoldiriladi va har run to'liq skaner bo'lishi mumkin. "
            "amoCRM hisob sozlamalarida 'Alpha-фильтрация'ni yoqing."
        )


def print_load_summary(pipeline: dlt.Pipeline) -> None:
    """Har bir jadval uchun nechta yozuv yuklanganini konsolga chiqaradi."""
    trace = pipeline.last_trace                                  # oxirgi run haqidagi ma'lumot
    row_counts = {}                                              # jadval → yozuvlar soni
    if trace is not None and trace.last_normalize_info is not None:  # ma'lumot mavjud bo'lsa
        row_counts = dict(trace.last_normalize_info.row_counts)  # normalize bosqichidagi sanoqlarni olamiz

    log.info("=" * 48)                                           # chiroyli ajratuvchi chiziq
    log.info("Yuklangan yozuvlar (jadval bo'yicha):")
    if not row_counts:                                           # hech narsa yuklanmagan bo'lsa
        log.info("  (yangi/o'zgargan yozuv topilmadi)")
    for table in sorted(row_counts):                             # jadval nomlari bo'yicha tartiblab
        if table.startswith("_dlt"):                             # dlt'ning ichki jadvallarini o'tkazib yuboramiz
            continue
        log.info("  %-30s %8d", table, row_counts[table])        # jadval nomi va yozuv sonini chiqaramiz
    log.info("=" * 48)                                           # yakuniy chiziq


def main() -> int:
    try:
        auth = load_auth()                                       # auth.json'ni o'qiymiz (subdomen + token)
        pg = load_postgres_config()                             # postgres_config.json'ni o'qiymiz
    except ConfigError as exc:                                   # konfiguratsiya xatosi bo'lsa
        log.error("Konfiguratsiya xatosi: %s", exc)             # xatoni chiqaramiz
        return 1                                                 # xato kodi bilan chiqamiz

    log.info("amoCRM hisobi: %s.amocrm.ru", auth["subdomain"])  # qaysi hisob bilan ishlayotganimizni ko'rsatamiz

    client = build_client(auth)                                 # sozlangan RESTClient yaratamiz
    check_alpha_filter(client)                                  # Alpha-filtr holatini tekshiramiz

    pipeline = dlt.pipeline(                                     # dlt pipeline'ini sozlaymiz
        pipeline_name="amocrm",                                 # pipeline nomi (state shu nom bilan saqlanadi)
        destination=dlt.destinations.postgres(credentials=pg["credentials"]),  # PostgreSQL manzili
        dataset_name=pg["dataset_name"],                        # schema (jadvallar qaysi schema'da bo'ladi)
        progress="log",                                         # jarayonni logga chiqarib turadi
    )

    # AMOCRM_FULL_REFRESH=1 bo'lsa: hamma jadval va holat tashlanib, noldan yuklanadi.
    refresh = "drop_sources" if _full_refresh_enabled() else None
    if refresh:
        log.warning("AMOCRM_FULL_REFRESH yoqilgan — hamma jadval to'liq qayta yuklanadi.")

    load_info = pipeline.run(amocrm_source(client), refresh=refresh)  # manbani ishga tushirib, yuklaymiz
    log.info("Yuklash yakunlandi: %s", load_info)              # natija haqida xabar
    print_load_summary(pipeline)                               # jadval bo'yicha sanoqni chiqaramiz
    return 0                                                    # muvaffaqiyatli tugadi (0 = OK)


if __name__ == "__main__":                                     # fayl to'g'ridan-to'g'ri ishga tushirilsa
    sys.exit(main())                                           # main()'ni chaqirib, chiqish kodini qaytaramiz
