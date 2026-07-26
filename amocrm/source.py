"""amoCRM dlt source: 11 ta entity, har biri alohida resource sifatida.

Umumiy so'rov/pagination/auth logikasi `client` moduldan olinadi.
Incremental holat (watermark) dlt'ning o'z state mexanizmi bilan boshqariladi —
qo'lda hech qanday watermark fayl/jadval yozilmaydi.
"""

from __future__ import annotations

import logging                                            # log xabarlari uchun

import dlt                                                # ETL freymvork (source/resource/incremental)
from dlt.sources.helpers.rest_client import RESTClient    # tur belgilash uchun (client argumenti)

from .client import paginate                              # sahifalab yozuv oqizadigan yordamchi

log = logging.getLogger("amocrm")                         # "amocrm" loggeri

# Ro'yxat endpointlari uchun standart sahifa hajmi (maksimum 250).
PAGE_LIMIT = 250
# events endpointi uchun maksimum 100.
EVENTS_PAGE_LIMIT = 100

# Airflow DAG parse paytida (client'siz, I/O'siz) har table uchun task yaratish
# uchun resource nomlari ro'yxati. Bu amocrm_source() qaytaradigan resource'lar
# tartibiga mos bo'lishi kerak. `events` hozircha o'chirilgan (izohda).
RESOURCE_NAMES = [
    "account",
    "users",
    "pipelines",
    "leads_custom_fields",
    "contacts_custom_fields",
    "companies_custom_fields",
    "leads",
    "contacts",
    "companies",
    "tasks",
    # "events",
]


@dlt.source(name="amocrm")                                # bu funksiya — "amocrm" nomli dlt manbasi
def amocrm_source(client: RESTClient):
    """Barcha entity'larni bitta dlt source sifatida qaytaradi."""

    # --- 1. account: bir marta, to'liq (bitta obyekt) ---
    @dlt.resource(name="account", write_disposition="replace", primary_key="id")  # har run'da to'liq almashtiriladi
    def account():
        resp = client.get(                                        # /account'ga bitta GET so'rov
            "/api/v4/account",
            params={"with": "is_api_filter_enabled"},             # Alpha-filtr holatini ham so'raymiz
        )
        resp.raise_for_status()                                   # xato (4xx/5xx) bo'lsa to'xtatamiz
        yield resp.json()                                         # bitta account obyektini qaytaramiz

    # --- 2. users: to'liq (incremental yo'q) ---
    @dlt.resource(name="users", write_disposition="merge", primary_key="id")  # id bo'yicha upsert
    def users():
        yield from paginate(                                      # barcha foydalanuvchilarni sahifalab olamiz
            client,
            "/api/v4/users",
            params={"limit": PAGE_LIMIT, "with": "role,group"},   # rol va guruh ma'lumoti bilan
            data_selector="_embedded.users",                      # yozuvlar JSON'ning shu yerida
        )

    # --- 3. pipelines: voronka va bosqichlar (to'liq, pagination yo'q) ---
    @dlt.resource(name="pipelines", write_disposition="merge", primary_key="id")  # id bo'yicha upsert
    def pipelines():
        resp = client.get("/api/v4/leads/pipelines")             # voronkalar ro'yxatini olamiz
        resp.raise_for_status()                                   # xatoni tekshiramiz
        yield from resp.json().get("_embedded", {}).get("pipelines", [])  # har bir voronkani qaytaramiz

    # --- 4-6. custom field ta'riflari (to'liq) ---
    def _custom_fields(name: str, entity_path: str):
        # Umumiy yasovchi: leads/contacts/companies uchun custom_fields resource'ini yaratadi.
        @dlt.resource(name=name, write_disposition="merge", primary_key="id")  # id bo'yicha upsert
        def resource():
            yield from paginate(                                  # custom field ta'riflarini sahifalab olamiz
                client,
                f"/api/v4/{entity_path}/custom_fields",           # masalan /api/v4/leads/custom_fields
                params={"limit": PAGE_LIMIT},
                data_selector="_embedded.custom_fields",          # ta'riflar shu yerda
            )

        return resource                                          # tayyor resource funksiyasini qaytaramiz

    leads_custom_fields = _custom_fields("leads_custom_fields", "leads")          # lead maydonlari
    contacts_custom_fields = _custom_fields("contacts_custom_fields", "contacts") # kontakt maydonlari
    companies_custom_fields = _custom_fields("companies_custom_fields", "companies")  # kompaniya maydonlari

    # --- 7-10. INCREMENTAL (updated_at bo'yicha, merge/upsert) ---
    def _incremental_entity(name: str, path: str, data_key: str):
        # Umumiy yasovchi: updated_at bo'yicha inkremental resource yaratadi.
        @dlt.resource(name=name, write_disposition="merge", primary_key="id")  # id bo'yicha upsert
        def resource(
            updated_at=dlt.sources.incremental("updated_at", initial_value=None)  # watermark: eng katta updated_at
        ):
            params = {
                "limit": PAGE_LIMIT,                              # sahifa hajmi
                "order[updated_at]": "asc",                       # eskidan yangiga tartiblab olamiz
            }
            # Birinchi ishga tushishda last_value=None → filtrsiz to'liq backfill.
            # Keyingi ishlarda dlt oldingi run'ning eng katta updated_at'ini beradi.
            # DIQQAT: to'liq server-tomon samara amoCRM Alpha-filtri yoqilganda ishlaydi;
            # ungacha ham dlt mijoz-tomonda takroriy yozuvlarni cheklaydi.
            if updated_at.last_value is not None:                 # oldingi run bo'lgan bo'lsa
                params["filter[updated_at][from]"] = int(updated_at.last_value)  # faqat undan keyingilarini so'raymiz
            yield from paginate(client, path, params, f"_embedded.{data_key}")   # yozuvlarni oqizamiz

        return resource                                          # tayyor resource'ni qaytaramiz

    leads = _incremental_entity("leads", "/api/v4/leads", "leads")            # bitimlar (lidlar)
    contacts = _incremental_entity("contacts", "/api/v4/contacts", "contacts")  # kontaktlar
    companies = _incremental_entity("companies", "/api/v4/companies", "companies")  # kompaniyalar
    tasks = _incremental_entity("tasks", "/api/v4/tasks", "tasks")            # vazifalar

    # --- 11. events: INCREMENTAL (created_at), APPEND-ONLY ---
    @dlt.resource(name="events", write_disposition="append", primary_key="id")  # faqat qo'shiladi (yangilanmaydi)
    def events(
        created_at=dlt.sources.incremental("created_at", initial_value=None)   # watermark: eng katta created_at
    ):
        params = {
            "limit": EVENTS_PAGE_LIMIT,                           # events uchun sahifa hajmi (100)
            "order[created_at]": "asc",                           # eskidan yangiga tartiblaymiz
        }
        if created_at.last_value is not None:                     # oldingi run bo'lgan bo'lsa
            params["filter[created_at][from]"] = int(created_at.last_value)  # faqat yangi hodisalarni so'raymiz
        yield from paginate(client, "/api/v4/events", params, "_embedded.events")  # hodisalarni oqizamiz

    return (                                                     # dlt'ga yuklanadigan barcha resource'lar ro'yxati
        account,
        users,
        pipelines,
        leads_custom_fields,
        contacts_custom_fields,
        companies_custom_fields,
        leads,
        contacts,
        companies,
        tasks,
        # events,                                                # hozircha o'chirilgan (ro'yxatdan chiqarilgan)
    )
