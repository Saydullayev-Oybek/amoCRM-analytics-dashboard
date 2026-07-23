"""amoCRM dlt source: 11 ta entity, har biri alohida resource sifatida.

Umumiy so'rov/pagination/auth logikasi `client` moduldan olinadi.
Incremental holat (watermark) dlt'ning o'z state mexanizmi bilan boshqariladi —
qo'lda hech qanday watermark fayl/jadval yozilmaydi.
"""

from __future__ import annotations

import logging

import dlt
from dlt.sources.helpers.rest_client import RESTClient

from .client import paginate

log = logging.getLogger("amocrm")

# Ro'yxat endpointlari uchun standart sahifa hajmi (maksimum 250).
PAGE_LIMIT = 250
# events endpointi uchun maksimum 100.
EVENTS_PAGE_LIMIT = 100


@dlt.source(name="amocrm")
def amocrm_source(client: RESTClient):
    """Barcha entity'larni bitta dlt source sifatida qaytaradi."""

    # --- 1. account: bir marta, to'liq (bitta obyekt) ---
    @dlt.resource(name="account", write_disposition="replace", primary_key="id")
    def account():
        resp = client.get(
            "/api/v4/account",
            params={"with": "is_api_filter_enabled"},
        )
        resp.raise_for_status()
        yield resp.json()

    # --- 2. users: to'liq (incremental yo'q) ---
    @dlt.resource(name="users", write_disposition="merge", primary_key="id")
    def users():
        yield from paginate(
            client,
            "/api/v4/users",
            params={"limit": PAGE_LIMIT, "with": "role,group"},
            data_selector="_embedded.users",
        )

    # --- 3. pipelines: voronka va bosqichlar (to'liq, pagination yo'q) ---
    @dlt.resource(name="pipelines", write_disposition="merge", primary_key="id")
    def pipelines():
        resp = client.get("/api/v4/leads/pipelines")
        resp.raise_for_status()
        yield from resp.json().get("_embedded", {}).get("pipelines", [])

    # --- 4-6. custom field ta'riflari (to'liq) ---
    def _custom_fields(name: str, entity_path: str):
        @dlt.resource(name=name, write_disposition="merge", primary_key="id")
        def resource():
            yield from paginate(
                client,
                f"/api/v4/{entity_path}/custom_fields",
                params={"limit": PAGE_LIMIT},
                data_selector="_embedded.custom_fields",
            )

        return resource

    leads_custom_fields = _custom_fields("leads_custom_fields", "leads")
    contacts_custom_fields = _custom_fields("contacts_custom_fields", "contacts")
    companies_custom_fields = _custom_fields("companies_custom_fields", "companies")

    # --- 7-10. INCREMENTAL (updated_at bo'yicha, merge/upsert) ---
    def _incremental_entity(name: str, path: str, data_key: str):
        @dlt.resource(name=name, write_disposition="merge", primary_key="id")
        def resource(
            updated_at=dlt.sources.incremental("updated_at", initial_value=None)
        ):
            params = {
                "limit": PAGE_LIMIT,
                "order[updated_at]": "asc",
            }
            # Birinchi ishga tushishda last_value=None → filtrsiz to'liq backfill.
            # Keyingi ishlarda dlt oldingi run'ning eng katta updated_at'ini beradi.
            if updated_at.last_value is not None:
                params["filter[updated_at][from]"] = int(updated_at.last_value)
            yield from paginate(client, path, params, f"_embedded.{data_key}")

        return resource

    leads = _incremental_entity("leads", "/api/v4/leads", "leads")
    contacts = _incremental_entity("contacts", "/api/v4/contacts", "contacts")
    companies = _incremental_entity("companies", "/api/v4/companies", "companies")
    tasks = _incremental_entity("tasks", "/api/v4/tasks", "tasks")

    # --- 11. events: INCREMENTAL (created_at), APPEND-ONLY ---
    @dlt.resource(name="events", write_disposition="append", primary_key="id")
    def events(
        created_at=dlt.sources.incremental("created_at", initial_value=None)
    ):
        params = {
            "limit": EVENTS_PAGE_LIMIT,
            "order[created_at]": "asc",
        }
        if created_at.last_value is not None:
            params["filter[created_at][from]"] = int(created_at.last_value)
        yield from paginate(client, "/api/v4/events", params, "_embedded.events")

    return (
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
        events,
    )
