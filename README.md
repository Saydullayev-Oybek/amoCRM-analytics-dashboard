# amoCRM → PostgreSQL ETL

amoCRM (Kommo) dan ma'lumotni olib, **PostgreSQL** bazasiga yuklaydigan pipeline.
[dlt](https://dlthub.com) kutubxonasi ustiga qurilgan.

**Nima qiladi:**
- amoCRM'dagi leadlar, kontaktlar, kompaniyalar, vazifalar va boshqalarni tortadi.
- Ularni PostgreSQL'ga **xom (JSON) holda** yuklaydi (tozalash/transformatsiya keyin — dbt bosqichida).
- Birinchi marta hammasini, keyin esa **faqat o'zgarganini** yuklaydi.

---

## 1. Talablar

- Python **3.12 yoki 3.13** (3.14 hali qo'llab-quvvatlanmaydi)
- Ishlaydigan PostgreSQL baza
- amoCRM access token

## 2. O'rnatish

```bash
# virtual muhit yaratish va paketlarni o'rnatish
uv venv --python 3.13 .venv
uv pip install --python .venv/bin/python -r dlt_pipeline/requirements.txt
```

## 3. Sozlash

Ikkita config fayl yaratasiz (ikkalasi ham `.gitignore`'da — Git'ga tushmaydi).

**`auth.json`** — amoCRM ulanishi (`auth.json.example`dan nusxa oling):

```json
{
  "subdomain": "sizning-subdomen",
  "access_token": "sizning-tokeningiz"
}
```
> `subdomain` — `https://SUBDOMEN.amocrm.ru` dagi qism.

**`postgres_config.json`** — baza ulanishi (`postgres_config.example.json`dan nusxa oling):

```json
{
  "host": "localhost",
  "port": 5432,
  "database": "amocrm",
  "username": "postgres",
  "password": "parolingiz",
  "schema": "amocrm"
}
```
> `schema` — jadvallar yaratiladigan schema nomi.

## 4. Ishga tushirish

```bash
./.venv/bin/python dlt_pipeline/pipeline.py
```

- **Birinchi run** — hamma ma'lumot tortiladi (to'liq).
- **Keyingi run'lar** — faqat yangi yoki o'zgargan yozuvlar.

Har run oxirida qaysi jadvalga nechta yozuv yuklangani ko'rinadi.

---

## Qaysi jadvallar yuklanadi

| Jadval | Nima | Yuklash usuli |
|---|---|---|
| `leads` | Bitimlar (lidlar) | faqat o'zgargani |
| `contacts` | Kontaktlar | faqat o'zgargani |
| `companies` | Kompaniyalar | faqat o'zgargani |
| `tasks` | Vazifalar | faqat o'zgargani |
| `events` | Hodisalar (log) | faqat yangisi qo'shiladi |
| `account` | Hisob ma'lumoti | har run to'liq |
| `users` | Foydalanuvchilar | har run to'liq |
| `pipelines` | Voronka va bosqichlar | har run to'liq |
| `*_custom_fields` | Maxsus maydon ta'riflari | har run to'liq |

> **"Faqat o'zgargani"** — katta jadvallar (leads va h.k.) har safar butunlay
> emas, faqat oxirgi run'dan keyin o'zgargan yozuvlar yuklanadi. Buni dlt o'zi
> avtomatik boshqaradi (qo'lda hech narsa qilish shart emas).

> **"Har run to'liq"** — kichik jadvallar har safar to'liq qayta yuklanadi
> (ular kichik, shuning uchun tez).

Katta jadvallar `id` bo'yicha **yangilanadi** (dublikat bo'lmaydi). dlt qo'shimcha
ichki jadvallar ham yaratadi (`_dlt_*`) — bularga tegmang.

---

## Avtomatlashtirish (Airflow + Docker)

Pipeline'ni **har 15 daqiqada avtomatik** ishlatish va har bir jadvalni
alohida kuzatish uchun Airflow sozlangan.

```bash
docker compose up -d --build     # ishga tushirish
docker compose down              # to'xtatish
```

- **UI:** http://localhost:8080 (login: `admin`, parol: `admin`)
- `amocrm_etl` degan DAG'ni topib **yoqing** — keyin har 15 daqiqada o'zi ishlaydi.
- Qo'lda sinash uchun "Trigger DAG" tugmasini bosing.

**Kuzatuv:**
- Har jadval alohida task (`load_leads`, `load_contacts`, ...). UI'da qaysi biri
  ishladi (yashil) yoki xato berdi (qizil) — darrov ko'rinadi.
- Har run natijasi bazadagi `etl_run_log` jadvaliga ham yoziladi:
  ```sql
  SELECT * FROM public.etl_run_log ORDER BY logged_at DESC;
  ```
  Bu yerda qaysi jadval nechta yozuv yuklagani va nima xato bergani ko'rinadi.

### Analitika bazasi (`analytics-db`)

Compose ichida **ikkita** PostgreSQL bor:
- `postgres` — Airflow'ning o'z metadata bazasi (tegilmaydi).
- **`analytics-db`** — amoCRM ma'lumoti shu yerga yig'iladi.

`postgres_config.json` `analytics-db`ga sozlangan (`host: analytics-db`, port 5432).
Ma'lumotni **host kompyuterdan** ko'rish uchun `localhost:5433` orqali ulaning:

```bash
# eng oson — konteyner ichidagi psql (host'da psql o'rnatilmagan bo'lsa ham)
docker compose exec analytics-db psql -U postgres -d amocrm -c "\dt public.*"

# yoki DBeaver / Power BI: host=localhost, port=5433, db=amocrm, user=postgres
```

> **Auto state-reset:** boshqa/yangi bazaga o'tsangiz, dlt eski state'ni o'zi
> tozalab, toza backfill qiladi (`runner.py`) — "relation does not exist" xatosi
> chiqmaydi.

### ⚠️ O'chirish (delete) haqida

amoCRM'da lead/kontakt **o'chirsangiz**, u bazada **qolib ketadi** (incremental+merge
o'chirishlarni aks ettirmaydi). Joriy holat aniq kerak bo'lsa: vaqti-vaqti to'liq
reload yoki `events` orqali o'chirishni kuzatish (hozircha amalga oshirilmagan).

### To'liq qayta yuklash (full reload)

Hamma ma'lumotni noldan qayta yuklamoqchi bo'lsangiz:

- **Airflow'da (eng oson):** DAG'ni **"Trigger DAG w/ config"** orqali quyidagi config
  bilan ishga tushiring — konteynerni qayta ishga tushirish shart emas:
  ```json
  {"full_refresh": true}
  ```
  Shu run'da har jadval tashlanib, noldan yuklanadi. Keyingi oddiy runlar yana
  incremental bo'ladi.

- **CLI'da:**
  ```bash
  AMOCRM_FULL_REFRESH=1 ./.venv/bin/python dlt_pipeline/pipeline.py
  ```

- **Qo'lda (to'liq tozalash):** bazani `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`
  qilib, `rm -rf dlt_data/pipelines/amocrm` bilan state'ni o'chirib, keyin ishga tushiring.

---

## Dashboard (Metabase)

`docker compose up -d` dan keyin dashboard shu manzilda:

| Kim | Manzil |
|---|---|
| Siz (shu kompyuter) | http://localhost:3000 |
| Jamoa (ichki tarmoq) | `http://<host-IP>:3000` — IP'ni bilish: `ipconfig getifaddr en0` |

**Birinchi marta (bir martalik sozlash):**

1. `http://localhost:3000` — Metabase admin akkauntingizni yarating (email + parol).
2. Ma'lumot bazasini ulash: **Add your data** → PostgreSQL:

   | Maydon | Qiymat |
   |---|---|
   | Host | `analytics-db` |
   | Port | `5432` |
   | Database | `amocrm` |
   | Username | `metabase_ro` |
   | Password | `metabase_ro_2026` |

   > `localhost` emas, **`analytics-db`** — Metabase konteyner ichida ishlaydi.

3. Jamoa a'zolarini qo'shish: **Admin → People → Invite someone**.

**Nega `metabase_ro`, `postgres` emas:** Metabase'da SQL editor bor, ya'ni kirgan
odam ixtiyoriy so'rov yoza oladi. `metabase_ro` faqat `marts` va `staging` dan
**o'qiy** oladi — `raw_data` ko'rinmaydi, `delete`/`drop` ishlamaydi.

Foydalanuvchini qayta yaratish kerak bo'lsa (parolni almashtirish uchun):

```bash
docker compose exec analytics-db psql -U postgres -d amocrm -c "
  alter user metabase_ro with password 'yangi-parol';"
```

### Dashboard qanday yasaladi

Metabase'da mantiq: avval **Question** (bitta grafik), keyin ularni **Dashboard**ga
yig'asiz.

**1. Jami bitimlar (raqam)**

`+ New → Question → Raw data → Marts → Fct Leads` → **Summarize: Count of rows**
→ vizualizatsiya **Number** → Save.

**2. Oylik dinamika (chiziq)**

`+ New → Question → Marts → Mart Leads Monthly` → vizualizatsiya **Line**
(X: `Created Month`, Y: `Lead Count`) → `Add series` bilan `Cumulative Count`
qo'shiladi → Save.

> Bu jadval **allaqachon oy bo'yicha yig'ilgan** — Summarize qo'shmang, aks holda
> raqamlar ikki marta yig'iladi.

**3. Bosqich bo'yicha taqsimot (ustun)**

`+ New → Question → Marts → Mart Leads By Stage` → vizualizatsiya **Bar**
(X: `Stage Name`, Y: `Lead Count`) → Save.

> ⚠️ Notebook editorda **Sort → `Stage Sort` ascending** qadamini qo'shing.
> Aks holda Metabase ustunlarni alifbo yoki kattalik bo'yicha tartiblaydi va
> voronka aralashib ketadi. `stage_sort` ustuni aynan shuning uchun mart'da bor.

**4. Yig'ish:** `+ New → Dashboard` → o'ngdagi **+** orqali uchala savolni qo'shing
→ o'lchamini sozlab **Save**.

**5. Matn kartochkasi** (tahrirlash rejimida `+ → Text`) — buni albatta qo'shing:

> ⚠️ Baza 2026-07-22 da import qilingan. Bitim summasi (`price`) hech qachon
> to'ldirilmagan — barchasida 0. Tizimda bitta menejer bor. Bitimlar bosqichlar
> bo'ylab hech qachon ko'chmagan, shuning uchun "bosqich taqsimoti" sotuv
> konversiyasi emas, importning holati.

Kontekstsiz grafik — chalg'ituvchi grafik. Jamoa raqamlarni noto'g'ri o'qimasligi
uchun bu izoh dashboard'ning o'zida turishi kerak.

**SQL bilan ham bo'ladi:** `+ New → SQL query` — ko'p hollarda GUI'dan tezroq.

```sql
select created_month, lead_count, cumulative_count
from marts.mart_leads_monthly
order by created_month
```

**Jadvallar ko'rinmasa:** Admin → Databases → Sync database schema now. Metabase
sxemani vaqti-vaqti skanerlaydi, yangi dbt modeli darhol chiqmasligi mumkin.

### Zaxira (backup)

Dashboardlar git'da emas, `metabase-db-data` volume'ida saqlanadi:

```bash
docker run --rm -v etl_metabase-db-data:/data -v "$PWD":/backup alpine \
  tar czf /backup/metabase-backup.tar.gz -C /data .
```

---

## Loyiha tuzilishi

```
dlt_pipeline/            # amoCRM'dan olish (dlt)
  amocrm/
    config.py    # config fayllarni o'qish
    client.py    # amoCRM API bilan ishlash (rate-limit, retry, pagination)
    source.py    # qaysi jadvallar olinishi
    runner.py    # ishga tushirish yadrosi + etl_run_log
  pipeline.py    # qo'lda ishga tushirish nuqtasi
  requirements.txt
dbt_project/             # tozalash/transformatsiya (dbt) — star schema
  models/staging/  # xom jadvallarning 1:1 tarjimasi (view)
  models/marts/    # fct_leads, dim_managers, dim_stages, mart_leads_*
  tests/           # qo'lda yozilgan testlar
dags/
  amocrm_dag.py    # Airflow jadvali (har 15 daqiqa) + oxirida dbt_build
docker/, docker-compose.yaml   # Airflow + Metabase konteynerlari
```

Config fayllar (`auth.json`, `postgres_config.json`) repo ildizida turadi.

---

## ⚠️ Bitta muhim eslatma — tezlik

amoCRM'da `filter[updated_at]` degan filtr **Alpha** bosqichida va hisob
sozlamalarida yoqilishi kerak. **O'chiq bo'lsa** pipeline baribir to'g'ri ishlaydi
(baza toza qoladi), lekin **sekinroq** bo'ladi — chunki amoCRM har safar hamma
ma'lumotni qaytaradi, dlt esa keraksizini o'zi tashlaydi.

Pipeline ishga tushganda bu filtr o'chiq bo'lsa konsolda ogohlantirish chiqaradi.
Tezlashtirish uchun amoCRM qo'llab-quvvatlash xizmatidan uni yoqishni so'rang.

---

## Foydali buyruqlar

```bash
# oxirgi yuklash / dlt holatini ko'rish
./.venv/bin/dlt pipeline amocrm info

# Airflow scheduler loglarini kuzatish
docker compose logs -f airflow-scheduler
```
