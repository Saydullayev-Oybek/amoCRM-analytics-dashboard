"""Konfiguratsiya fayllarini (auth.json, postgres_config.json) o'qish va tekshirish.

Hech qanday token yoki subdomen kodning ichida qattiq yozilmaydi — barchasi
shu yerdagi JSON fayllardan o'qiladi.
"""

from __future__ import annotations

import json                              # JSON fayllarni o'qish/tahlil qilish uchun
import os                                 # muhit o'zgaruvchilarini (env) o'qish uchun
from pathlib import Path                 # fayl yo'llari bilan qulay ishlash uchun
from urllib.parse import quote_plus      # parol/login'dagi maxsus belgilarni URL uchun xavfsizlashtirish

# Config fayllar nomlari (aniq joyi ish vaqtida topiladi — _resolve_config_path).
AUTH_FILENAME = "auth.json"
POSTGRES_FILENAME = "postgres_config.json"


def _resolve_config_path(filename: str) -> Path:
    """Config faylni topadi — qaysi papkadan ishga tushirilishidan qat'i nazar.

    Qidiruv tartibi:
      1. AMOCRM_CONFIG_DIR env (Airflow/Docker uchun) — eng yuqori ustuvorlik.
      2. Joriy ish papkasidan yuqoriga qarab (CWD → ota-papkalar → /).
      3. Modul (config.py) joyidan yuqoriga qarab — repo ildizini topish uchun.
    Hech qayerdan topilmasa, oddiy nom qaytariladi (xato xabari aniq bo'lsin).
    """
    env_dir = os.environ.get("AMOCRM_CONFIG_DIR")           # 1) env ustuvor
    if env_dir:
        return Path(env_dir) / filename

    cwd = Path.cwd()                                        # 2) joriy papkadan yuqoriga
    for base in [cwd, *cwd.parents]:
        candidate = base / filename
        if candidate.exists():
            return candidate

    here = Path(__file__).resolve()                         # 3) modul joyidan yuqoriga (repo ildizi)
    for base in here.parents:
        candidate = base / filename
        if candidate.exists():
            return candidate

    return Path(filename)                                  # 4) topilmadi — nom bilan qaytaramiz


class ConfigError(Exception):
    """auth.json / postgres_config.json topilmaganda yoki noto'g'ri bo'lganda."""
    # Maxsus xato turi — konfiguratsiya muammolarini boshqa xatolardan ajratish uchun.


def _read_json(path: Path, human_name: str) -> dict:
    # Berilgan yo'ldan JSON faylni o'qib, dict qaytaradi; muammoda ConfigError beradi.
    if not path.exists():                              # fayl umuman mavjud emasmi
        raise ConfigError(
            f"'{path}' fayli topilmadi. "
            f"'{human_name}.example' faylidan nusxa oling va to'ldiring."
        )
    try:
        raw = path.read_text(encoding="utf-8")         # fayl matnini o'qiymiz (UTF-8)
    except OSError as exc:  # pragma: no cover - juda kam holat
        raise ConfigError(f"'{path}' faylini o'qib bo'lmadi: {exc}") from exc  # o'qishda tizim xatosi
    try:
        data = json.loads(raw)                          # matnni JSON'ga aylantiramiz
    except json.JSONDecodeError as exc:                 # JSON sintaksisi buzuq bo'lsa
        raise ConfigError(
            f"'{path}' fayli noto'g'ri JSON formatida: {exc}"
        ) from exc
    if not isinstance(data, dict):                      # yuqori daraja obyekt (dict) emasmi
        raise ConfigError(f"'{path}' fayli JSON obyekt (dict) bo'lishi kerak.")
    return data                                         # tekshirilgan dict'ni qaytaramiz


def load_auth(path: Path | None = None) -> dict:
    """Subdomain va access_token'ni o'qiydi.

    Spec formati: {"subdomain": ..., "access_token": ...}.
    Eski format (token / sub-domen) ham qabul qilinadi (moslashuvchanlik uchun).
    """
    if path is None:                                            # yo'l berilmasa — avtomatik topamiz
        path = _resolve_config_path(AUTH_FILENAME)
    data = _read_json(path, "auth.json")                        # auth.json'ni dict sifatida o'qiymiz
    subdomain = data.get("subdomain") or data.get("sub-domen")  # yangi yoki eski kalitdan subdomen
    token = data.get("access_token") or data.get("token")       # yangi yoki eski kalitdan token

    missing = []                                                # yetishmayotgan kalitlar ro'yxati
    if not subdomain:                                           # subdomen bo'sh/yo'q bo'lsa
        missing.append("subdomain")
    if not token:                                               # token bo'sh/yo'q bo'lsa
        missing.append("access_token")
    if missing:                                                 # birortasi yetishmasa
        raise ConfigError(
            f"'{path}' da quyidagi kalit(lar) yetishmayapti yoki bo'sh: "
            f"{', '.join(missing)}. "
            f'Kutilgan format: {{"subdomain": "...", "access_token": "..."}}'
        )

    return {                                                    # tozalangan (probelsiz) qiymatlarni qaytaramiz
        "subdomain": str(subdomain).strip(),
        "access_token": str(token).strip(),
    }


def base_url(subdomain: str) -> str:
    """amoCRM hisobining bazaviy URL manzili."""
    return f"https://{subdomain}.amocrm.ru"                     # masalan https://mycompany.amocrm.ru


def load_postgres_config(path: Path | None = None) -> dict:
    """PostgreSQL ulanish ma'lumotlarini o'qiydi va dlt uchun tayyorlaydi.

    Qaytaradi: {"credentials": <connection string>, "dataset_name": <schema>}.
    """
    if path is None:                                            # yo'l berilmasa — avtomatik topamiz
        path = _resolve_config_path(POSTGRES_FILENAME)
    data = _read_json(path, "postgres_config.json")             # postgres_config.json'ni o'qiymiz

    required = ["host", "port", "database", "username", "password"]   # majburiy kalitlar
    missing = [k for k in required if data.get(k) in (None, "")]      # bo'sh yoki yo'qlarini yig'amiz
    if missing:                                                 # birortasi yetishmasa
        raise ConfigError(
            f"'{path}' da quyidagi kalit(lar) yetishmayapti: {', '.join(missing)}."
        )

    user = quote_plus(str(data["username"]))                    # login'ni URL uchun xavfsizlashtirish
    password = quote_plus(str(data["password"]))                # parolni URL uchun xavfsizlashtirish
    host = str(data["host"])                                    # server manzili (masalan localhost)
    port = int(data["port"])                                    # port (masalan 5432)
    database = str(data["database"])                            # baza nomi
    dataset_name = str(data.get("schema") or "amocrm")          # schema → dlt dataset_name (standart "amocrm")

    credentials = f"postgresql://{user}:{password}@{host}:{port}/{database}"  # to'liq ulanish satri
    return {"credentials": credentials, "dataset_name": dataset_name}          # dlt kutgan formatda qaytaramiz
