# amoCRM → PostgreSQL ETL (dlt)

amoCRM (Kommo) CRM Platform API v4'dan ma'lumotni **incremental** tarzda tortib,
**PostgreSQL**'ga yuklaydigan Python pipeline. [dlt](https://dlthub.com) kutubxonasi
ustiga qurilgan. Ma'lumot **xom** (to'liq JSON) holda yuklanadi — custom fieldlarni
pivot qilish yoki boshqa transformatsiya keyingi (dbt) bosqichga qoldiriladi.

## Talablar

- Python **3.12 yoki 3.13** (dlt hozircha 3.14'ni qo'llab-quvvatlamaydi)
- Ishlaydigan PostgreSQL baza
- amoCRM long-lived access token

## O'rnatish

```bash
# 3.13 virtual muhit (uv bilan)
uv venv --python 3.13 .venv
uv pip install --python .venv/bin/python -r requirements.txt

# yoki standart pip bilan
python3.13 -m venv .venv
./.venv/bin/pip install -r requirements.txt
```

## Sozlash

### 1. `auth.json`

`auth.json.example`'dan nusxa oling va to'ldiring:

```json
{
  "subdomain": "sizning-subdomen",
  "access_token": "sizning-long-lived-token"
}
```

- `subdomain` — `https://<subdomain>.amocrm.ru` dagi qism.
- `access_token` — amoCRM'da yaratilgan uzoq muddatli (long-lived) token.
- Bu fayl `.gitignore`'da — Git'ga hech qachon yuklanmaydi.

### 2. `postgres_config.json`

`postgres_config.example.json`'dan nusxa oling:

```json
{
  "host": "localhost",
  "port": 5432,
  "database": "amocrm",
  "username": "postgres",
  "password": "sizning-parolingiz",
  "schema": "amocrm"
}
```

`schema` — PostgreSQL'da jadvallar yaratiladigan schema nomi (dlt `dataset_name`).
Bu fayl ham `.gitignore`'da.

## Ishga tushirish

```bash
./.venv/bin/python pipeline.py
```

- **Birinchi run** — to'liq backfill (barcha ma'lumot tortiladi).
- **Keyingi run'lar** — faqat yangi/o'zgargan yozuvlar (dlt state orqali).

Har run oxirida har bir jadval uchun nechta yozuv yuklangani konsolga chiqadi.

## Entity'lar

| Jadval | Endpoint | Rejim | write_disposition |
|---|---|---|---|
| `account` | `/api/v4/account` | to'liq (1 obyekt) | replace |
| `users` | `/api/v4/users` | to'liq | merge |
| `pipelines` | `/api/v4/leads/pipelines` | to'liq | merge |
| `leads_custom_fields` | `/api/v4/leads/custom_fields` | to'liq | merge |
| `contacts_custom_fields` | `/api/v4/contacts/custom_fields` | to'liq | merge |
| `companies_custom_fields` | `/api/v4/companies/custom_fields` | to'liq | merge |
| `leads` | `/api/v4/leads` | **incremental** (`updated_at`) | merge |
| `contacts` | `/api/v4/contacts` | **incremental** (`updated_at`) | merge |
| `companies` | `/api/v4/companies` | **incremental** (`updated_at`) | merge |
| `tasks` | `/api/v4/tasks` | **incremental** (`updated_at`) | merge |
| `events` | `/api/v4/events` | **incremental** (`created_at`) | **append** |

`pipelines` ichidagi bosqichlar (`_embedded.statuses`) va boshqa ichma-ich massivlar
(`custom_fields_values`, teglar, `enums`) dlt tomonidan avtomatik **bola jadvallarga**
(masalan `pipelines__statuses`) ajratiladi. dlt qo'shimcha texnik jadvallar ham
yaratadi: `_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`.

## Incremental qanday ishlaydi

- Incremental entity'lar `order[updated_at]=asc` (events uchun `order[created_at]=asc`)
  bilan so'raladi, shunda sahifalar bir tekis o'sadi.
- dlt cursor maydonining eng katta qiymatini o'z **state**'ida saqlaydi (qo'lda
  watermark fayl/jadval yozilmaydi).
- Keyingi run'da dlt shu qiymatni `filter[updated_at][from]` (events: `filter[created_at][from]`)
  parametriga qo'yadi, natijada faqat yangi/o'zgarganlar tortiladi.
- `events` — **append-only**: hech qachon UPSERT qilinmaydi, faqat qo'shiladi.

### Incrementalni tekshirish

1. `pipeline.py`'ni ishga tushiring — to'liq backfill bo'ladi.
2. amoCRM'da bitta lead'ni o'zgartiring (yoki yangi qo'shing).
3. `pipeline.py`'ni qayta ishga tushiring — konsol yakunida faqat o'sha bitta
   (yoki bir nechta) o'zgargan yozuv ko'rsatilishi kerak.
4. dlt state'ni ko'rish: `./.venv/bin/dlt pipeline amocrm info`.

## ⚠️ Muhim eslatma — Alpha filtr

amoCRM'da `filter[updated_at]` filtri **Alpha** bosqichida bo'lib, hisob
sozlamalarida yoqilishi kerak (`is_api_filter_enabled`). Agar o'chiq bo'lsa, filtr
**jimgina e'tiborsiz** qoldiriladi va har run to'liq skaner bo'ladi. Pipeline ishga
tushganda buni tekshiradi va o'chiq bo'lsa konsolga ogohlantirish chiqaradi.

## Loyiha tuzilishi

```
amocrm/
  config.py    # auth.json + postgres_config.json o'qish/tekshirish
  client.py    # RESTClient + rate-limit (≤7 req/s) + 429 retry + pagination
  source.py    # 11 ta dlt resource
pipeline.py    # ishga tushirish nuqtasi
```
