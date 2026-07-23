"""Konfiguratsiya fayllarini (auth.json, postgres_config.json) o'qish va tekshirish.

Hech qanday token yoki subdomen kodning ichida qattiq yozilmaydi — barchasi
shu yerdagi JSON fayllardan o'qiladi.
"""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import quote_plus

AUTH_FILE = Path("auth.json")
POSTGRES_FILE = Path("postgres_config.json")


class ConfigError(Exception):
    """auth.json / postgres_config.json topilmaganda yoki noto'g'ri bo'lganda."""


def _read_json(path: Path, human_name: str) -> dict:
    if not path.exists():
        raise ConfigError(
            f"'{path}' fayli topilmadi. "
            f"'{human_name}.example' faylidan nusxa oling va to'ldiring."
        )
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:  # pragma: no cover - juda kam holat
        raise ConfigError(f"'{path}' faylini o'qib bo'lmadi: {exc}") from exc
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(
            f"'{path}' fayli noto'g'ri JSON formatida: {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise ConfigError(f"'{path}' fayli JSON obyekt (dict) bo'lishi kerak.")
    return data


def load_auth(path: Path = AUTH_FILE) -> dict:
    """Subdomain va access_token'ni o'qiydi.

    Spec formati: {"subdomain": ..., "access_token": ...}.
    Eski format (token / sub-domen) ham qabul qilinadi (moslashuvchanlik uchun).
    """
    data = _read_json(path, "auth.json")
    subdomain = data.get("subdomain") or data.get("sub-domen")
    token = data.get("access_token") or data.get("token")

    missing = []
    if not subdomain:
        missing.append("subdomain")
    if not token:
        missing.append("access_token")
    if missing:
        raise ConfigError(
            f"'{path}' da quyidagi kalit(lar) yetishmayapti yoki bo'sh: "
            f"{', '.join(missing)}. "
            f'Kutilgan format: {{"subdomain": "...", "access_token": "..."}}'
        )

    return {
        "subdomain": str(subdomain).strip(),
        "access_token": str(token).strip(),
    }


def base_url(subdomain: str) -> str:
    """amoCRM hisobining bazaviy URL manzili."""
    return f"https://{subdomain}.amocrm.ru"


def load_postgres_config(path: Path = POSTGRES_FILE) -> dict:
    """PostgreSQL ulanish ma'lumotlarini o'qiydi va dlt uchun tayyorlaydi.

    Qaytaradi: {"credentials": <connection string>, "dataset_name": <schema>}.
    """
    data = _read_json(path, "postgres_config.json")

    required = ["host", "port", "database", "username", "password"]
    missing = [k for k in required if data.get(k) in (None, "")]
    if missing:
        raise ConfigError(
            f"'{path}' da quyidagi kalit(lar) yetishmayapti: {', '.join(missing)}."
        )

    user = quote_plus(str(data["username"]))
    password = quote_plus(str(data["password"]))
    host = str(data["host"])
    port = int(data["port"])
    database = str(data["database"])
    dataset_name = str(data.get("schema") or "amocrm")

    credentials = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return {"credentials": credentials, "dataset_name": dataset_name}
