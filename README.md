# amoCRM analitika pipeline

amoCRM (Kommo) dan ma'lumotni olib, tozalab, brauzerda dashboard qilib
ko'rsatadi. Hammasi Docker ichida ishlaydi.

## Nima qiladi

```
amoCRM API  →  PostgreSQL (xom)  →  PostgreSQL (tozalangan)  →  Dashboard
                    dlt                    dbt                   Metabase
```

- **dlt** — amoCRM'dan 11 ta jadval tortadi va xom holda bazaga yozadi.
  Birinchi marta hammasini, keyin faqat o'zgarganini.
- **dbt** — xom jadvallardan tushunarli jadvallar yasaydi: bitimlar, menejerlar,
  voronka bosqichlari, oylik hisobot.
- **Metabase** — shu jadvallar ustida brauzerdan ochiladigan dashboard.
- **Airflow** — hammasini har 15 daqiqada avtomatik ishlatib turadi.

## Ishga tushirish

Kerak: **Docker Desktop**, **git**, va amoCRM access token.

**1. Loyihani oling**

```bash
git clone https://github.com/Saydullayev-Oybek/amoCRM-analytics-dashboard
cd amoCRM-analytics-dashboard
```

**2. Config fayllarni yarating**

```bash
cp auth.json.example auth.json
cp postgres_config.json.example postgres_config.json
cp dbt_project/profiles.yml.example dbt_project/profiles.yml
```

`auth.json` ga amoCRM ma'lumotingizni yozing:

```json
{
  "subdomain": "sizning-subdomen",
  "access_token": "sizning-tokeningiz"
}
```

Qolgan ikkitasi tayyor — tegmasangiz ham bo'ladi.

> ⚠️ Bu qadamni keyingisidan **oldin** bajaring. Fayllar bo'lmasa Docker ularning
> o'rniga papka yaratib qo'yadi va konteyner ishga tushmaydi.

**3. Ko'taring**

```bash
docker compose up -d --build
```

Birinchi marta 5–10 daqiqa ketadi.

**4. Pipeline'ni yoqing**

http://localhost:8080 (`admin` / `admin`) → `amocrm_etl` DAG'ini toping va yoqing.
Sukut bo'yicha u pauzada turadi.

**5. Dashboard'ni sozlang**

http://localhost:3000 → admin akkaunt yarating → bazani ulang:

| Maydon | Qiymat |
|---|---|
| Host | `analytics-db` |
| Port | `5432` |
| Database | `amocrm` |
| Username | `metabase_ro` |
| Password | `metabase_ro_2026` |

Tayyor. Endi dashboard yasashingiz mumkin.

## Manzillar

| Manzil | Nima |
|---|---|
| http://localhost:8080 | Airflow — pipeline holati |
| http://localhost:3000 | Metabase — dashboard |
| `localhost:5433` | PostgreSQL — DBeaver/psql uchun |

Jamoa ichki tarmoqdan kirishi uchun `localhost` o'rniga kompyuteringiz IP'sini
bering (`ipconfig getifaddr en0`).

## Foydali buyruqlar

```bash
docker compose ps                            # holat
docker compose logs -f airflow-scheduler     # pipeline loglari
docker compose down                          # to'xtatish
docker compose down -v                       # ma'lumot bilan birga o'chirish

# bazaga ulanish
docker compose exec analytics-db psql -U postgres -d amocrm
```

**To'liq qayta yuklash:** Airflow'da DAG'ni "Trigger DAG w/ config" orqali
`{"full_refresh": true}` bilan ishga tushiring.

## Batafsil

Texnik tafsilotlar: [CLAUDE.md](CLAUDE.md)
