# amoCRM analitika pipeline

amoCRM (Kommo) dan ma'lumotni olib, tozalab, brauzerda ko'rsatadigan to'liq
analitika quvuri. Hammasi Docker ichida ishlaydi — bitta buyruq bilan ko'tariladi.

## Loyiha haqida

Uchta qatlamdan iborat:

```
amoCRM API
    │
    ▼  ①  EL  — dlt
raw_data schema          xom JSON, hech narsa o'zgartirilmaydi
    │
    ▼  ②  T   — dbt
staging + marts          tozalangan, star schema (fakt + o'lchovlar)
    │
    ▼  ③  BI  — Metabase
dashboard                brauzerda, jamoa ko'radi
```

**① EL — ma'lumotni olish ([dlt](https://dlthub.com))**
amoCRM'dan 11 ta jadval tortiladi va PostgreSQL'ga **xom holda** yoziladi. Birinchi
run'da hammasi, keyin esa **faqat o'zgargani**. Hech qanday transformatsiya yo'q —
bu ataylab, chunki xom nusxa saqlanib qolsa, keyinchalik mantiqni o'zgartirib
qayta hisoblash mumkin.

**② T — transformatsiya ([dbt](https://getdbt.com))**
Xom jadvallar ustidan star schema quriladi: `fct_leads` (fakt) + `dim_managers`,
`dim_stages` (o'lchovlar), ustiga tayyor hisobotlar — `mart_leads_monthly`,
`mart_leads_by_stage`. 32 ta avtomatik test har run'da tekshiradi.

**③ BI — ko'rish (Metabase)**
Marts ustida brauzerdan ochiladigan dashboard. Jamoa ichki tarmoqdan kiradi.

**Orkestratsiya (Airflow)** — har 15 daqiqada: 11 ta jadval ketma-ket yuklanadi,
so'ng `dbt build` ishlaydi. Har jadval alohida task, ya'ni qaysi biri yiqilgani
UI'da darhol ko'rinadi.

**Ko'tarilgandan keyin nima olasiz:**

| Manzil | Nima |
|---|---|
| http://localhost:8080 | Airflow — pipeline holati (`admin` / `admin`) |
| http://localhost:3000 | Metabase — dashboard |
| `localhost:5433` | PostgreSQL — DBeaver/psql bilan ulanish uchun |

---

## Ishga tushirish (Docker)

Python o'rnatish, virtual muhit yaratish shart emas — hammasi konteynerlar ichida.

### 0. Kerakli narsalar

- **Docker Desktop** (yoki Docker Engine + Compose)
- **git**
- amoCRM access token va subdomen

### 1. Loyihani olish

```bash
git clone https://github.com/Saydullayev-Oybek/amoCRM-analytics-dashboard
cd amoCRM-analytics-dashboard
```

### 2. Uchta config fayl

Bu fayllar `.gitignore`'da — sir bo'lgani uchun clone bilan **kelmaydi**, o'zingiz
yaratasiz:

```bash
cp auth.json.example auth.json
cp postgres_config.json.example postgres_config.json
cp dbt_project/profiles.yml.example dbt_project/profiles.yml
```

> ⚠️ **Buni `docker compose up` dan OLDIN qiling.** `auth.json` va
> `postgres_config.json` compose'da **fayl** sifatida mount qilingan. Fayl mavjud
> bo'lmasa, Docker o'sha nomda **papka** yaratib qo'yadi va konteyner ishga
> tushmaydi. Bu eng ko'p uchraydigan xato.

Endi ichini to'ldiring:

**`auth.json`** — amoCRM kaliti:

```json
{
  "subdomain": "sizning-subdomen",
  "access_token": "sizning-tokeningiz"
}
```

> `subdomain` — `https://SUBDOMEN.amocrm.ru` dagi qism.

**`postgres_config.json`** — Docker uchun tayyor, faqat parolni tekshiring:

```json
{
  "host": "analytics-db",
  "port": 5432,
  "database": "amocrm",
  "username": "postgres",
  "password": "admin",
  "schema": "raw_data"
}
```

> `host` — `localhost` **emas**, `analytics-db`. Pipeline konteyner ichida
> ishlaydi, u bazani compose tarmog'idagi nomi orqali ko'radi.

**`dbt_project/profiles.yml`** — parollar `postgres_config.json` bilan bir xil
bo'lsin. Ikkita target bor: `dev` (host'dan) va `docker` (Airflow ichidan) —
namunada ikkalasi ham tayyor.

### 3. Ko'tarish

```bash
docker compose up -d --build
```

Birinchi marta 5–10 daqiqa (image build + Metabase yuklab olinadi). Holatni
ko'rish:

```bash
docker compose ps        # hammasi "healthy" bo'lishi kerak
```

### 4. Pipeline'ni yoqish

http://localhost:8080 → login `admin` / `admin` → `amocrm_etl` DAG'ini toping va
chapdagi tugma bilan **yoqing**.

Sukut bo'yicha u pauzada turadi (`DAGS_ARE_PAUSED_AT_CREATION`), shuning uchun bu
qadam shart. Yoqilgandan keyin har 15 daqiqada o'zi ishlaydi. Darhol sinash uchun
**Trigger DAG** tugmasini bosing.

Birinchi run uzoq davom etadi — butun amoCRM tarixini tortadi.

### 5. Dashboard

http://localhost:3000 → admin akkaunt yarating → bazani ulang:

| Maydon | Qiymat |
|---|---|
| Host | `analytics-db` |
| Port | `5432` |
| Database | `amocrm` |
| Username | `metabase_ro` |
| Password | `metabase_ro_2026` |

`metabase_ro` — **faqat-o'qish** roli, baza birinchi ko'tarilganda
[docker/init-analytics-db.sql](docker/init-analytics-db.sql) uni avtomatik
yaratadi. Qo'lda SQL yozish kerak emas.

### To'xtatish

```bash
docker compose down          # to'xtatish (ma'lumot saqlanib qoladi)
docker compose down -v       # ma'lumot bilan BIRGA o'chirish (ehtiyot bo'ling)
```

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

## Avtomatlashtirish (Airflow)

DAG `amocrm_etl` har **15 daqiqada** ishlaydi: 11 ta jadval **ketma-ket**
yuklanadi, so'ng oxirida `dbt_build` taski marts'ni qayta quradi.

Ketma-ket — parallel emas: dlt state'i buzilmasligi va amoCRM'ning 7 so'rov/soniya
chegarasi hurmat qilinishi uchun.

**Kuzatuv:**

- Har jadval alohida task (`load_leads`, `load_contacts`, ...). UI'da qaysi biri
  ishladi (yashil) yoki xato berdi (qizil) — darrov ko'rinadi.
- `dbt_build` zanjirning oxirida. Bitta jadval yuklanmasa u umuman ishlamaydi —
  ya'ni marts yarim ma'lumot ustiga qurilmaydi.
- Har run natijasi bazadagi `etl_run_log` jadvaliga ham yoziladi:
  ```sql
  select * from raw_data.etl_run_log order by logged_at desc;
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

- **Qo'lda (to'liq tozalash):** bazadagi schema'ni tashlab, dlt state'ini o'chiring:
  ```bash
  docker compose exec analytics-db psql -U postgres -d amocrm \
    -c "drop schema raw_data cascade;"
  rm -rf dlt_data/pipelines/amocrm
  ```
  Keyin DAG'ni ishga tushiring.

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

### Qaysi jadvaldan nima chiqadi

| Ko'rsatkich | Manba jadval |
|---|---|
| Jami bitimlar | `marts.fct_leads` |
| Oylik dinamika + kumulyativ o'sish | `marts.mart_leads_monthly` |
| Bosqich bo'yicha taqsimot | `marts.mart_leads_by_stage` |

> ⚠️ **Raqamlarni to'g'ri o'qish uchun:** baza 2026-07-22 da import qilingan,
> `price` hech qachon to'ldirilmagan (barcha bitimda 0), bitta menejer bor, va
> bitimlar bosqichlar bo'ylab hech qachon ko'chmagan — "bosqich taqsimoti" sotuv
> konversiyasi **emas**, importning holati.

**Jadvallar Metabase'da ko'rinmasa:** Admin → Databases → Sync database schema now.

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

Hammasi Docker orqali — host'ga hech narsa o'rnatish shart emas.

```bash
# holat
docker compose ps
docker compose logs -f airflow-scheduler     # pipeline loglari
docker compose logs -f metabase              # dashboard loglari

# bazaga ulanish (host'da psql bo'lmasa ham ishlaydi)
docker compose exec analytics-db psql -U postgres -d amocrm

# marts'ni qo'lda qayta qurish (DAG'ni kutmasdan)
docker compose exec airflow-scheduler \
  bash -c "cd /opt/airflow/dbt_project && dbt build --target docker"

# qaysi jadval qachon va nechta yozuv yuklagani
docker compose exec analytics-db psql -U postgres -d amocrm \
  -c "select * from raw_data.etl_run_log order by logged_at desc limit 20;"

# qayta ishga tushirish (kod o'zgargandan keyin)
docker compose up -d --build
```
