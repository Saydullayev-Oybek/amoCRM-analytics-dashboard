# amoCRM dbt marts — star schema design

**Sana:** 2026-08-02
**Holat:** Tasdiqlangan design → keyingi qadam: implementation plan
**Oldingi spec:** `2026-07-23-amocrm-etl-design.md` (EL qatlami)

## Maqsad

`raw_data` schema'sidagi xom amoCRM jadvallari ustiga **star schema** qurish:
bitta fakt jadvali + o'lchov (dimension) jadvallari, va ular ustida ikkita
agregat mart. Maqsad — bitimlar sonini menejer, voronka bosqichi va oy kesimida
o'lchash.

Vizualizatsiya bu spec'ga kirmaydi. Hozircha faqat dbt jadvallar tayyorlanadi;
BI vositasi (Power BI) keyinroq shu jadvallarga ulanadi.

## Ma'lumotning holati (2026-08-02 da o'lchangan)

Design shu faktlarga tayanadi — ular o'lchamni va nimani qurmaslik kerakligini
belgilagan:

| Tekshiruv | Natija |
|---|---|
| Tirik bitimlar (`not is_deleted`) | 8 380 |
| Foydalanuvchilar | **1** (Мухаммадазиз) |
| Voronkalar | **1**, 12 bosqich |
| Bitim bor bosqichlar | **3** (6901 / 917 / 562) |
| `price` > 0 bo'lgan bitimlar | **0** — barchasi nol |
| Won/Lost (status 142/143) dagi bitimlar | **0** |
| `sale_field_changed` eventlari | **0** (jami 144 016 event ichida) |
| `lead_status_changed` eventlari | **0** |
| `utm_source` to'ldirilgan | 59 / 8380 (0.7%) |
| Teglar | 8377 tasida bitta teg: `импорт_22072026_2122` |
| `created_at` oralig'i | 2025-02-20 → 2026-07-27 |

**Xulosa:** bu ishlab turgan sotuv jarayonining ma'lumoti emas, balki boshqa
tizimdan 2026-07-22 da ko'chirilgan skelet yozuvlar. Bitim nomlari
avtomatik (`Lead #42005127`), `updated_at` barchasida import sanasi.

Shundan kelib chiqib:

- **`price` ishlatilmaydi.** U ETL xatosi emas — amoCRM'ning o'zida hech qachon
  to'ldirilmagan (uch mustaqil dalil: `price`=0 va `NULL` emas;
  `price_with_minor_units` ham 0; `sale_field_changed` eventi umuman yo'q).
  Summa, o'rtacha chek, daromad metrikalari qurilmaydi.
- **Konversiya / win rate qurilmaydi** — 142/143 da bitta ham bitim yo'q.
- **UTM atributsiyasi qurilmaydi** — to'ldirilganlik 1% dan past.
- **`mart_leads_by_stage` voronka emas.** `lead_status_changed` eventi yo'q, ya'ni
  bitimlar bosqichlar bo'ylab hech qachon harakatlanmagan. Jadval importning
  taqsimotini ko'rsatadi; undan sotuv xulosasi chiqarilmasligi kerak.

Ma'lumot to'ldirila boshlagach bu cheklovlar yo'qoladi — struktura o'zgartirishsiz
ishlaydi.

## Arxitektura

Mavjud `staging` qatlami o'zgarishsiz qoladi. Yangi modellar `marts` schema'siga
tushadi.

```
raw_data (dlt)
   └── staging/          [view]   — 1:1 tarjima, tegilmaydi
         ├── stg_amocrm__leads
         ├── stg_amocrm__users
         └── stg_amocrm__pipeline_statuses
                │
                ▼
        marts/            [table]
         ├── fct_leads          ← stg_amocrm__leads
         ├── dim_managers       ← stg_amocrm__users
         ├── dim_stages         ← stg_amocrm__pipeline_statuses
         ├── mart_leads_monthly     ← fct_leads
         └── mart_leads_by_stage    ← fct_leads + dim_*
```

Nomlash konvensiyasi: star jadvallari `fct_` / `dim_`, agregatlar `mart_`.

Materializatsiya [dbt_project.yml](../../../dbt_project/dbt_project.yml) da
allaqachon sozlangan (`marts: +materialized: table`) — o'zgartirilmaydi.

## Modellar

### `fct_leads`

**Grain:** bitta tirik bitim = bitta qator (hozir 8 380).
**Manba:** `stg_amocrm__leads`. Join yo'q, agregatsiya yo'q, filtr yo'q
(o'chirilganlar staging'da allaqachon chiqarib tashlangan).

| Ustun | Tur | Izoh |
|---|---|---|
| `lead_id` | bigint | birlamchi kalit |
| `manager_id` | bigint | → `dim_managers.manager_id` |
| `pipeline_id` | bigint | → `dim_stages` (kalitning 1-qismi) |
| `status_id` | bigint | → `dim_stages` (kalitning 2-qismi) |
| `lead_name` | text | hozir `Lead #NNNN` |
| `created_at` | timestamp | |
| `updated_at` | timestamp | hozir barchasida import sanasi |

O'lchov ustuni yo'q — fakt sanaladigan hodisaning o'zi, metrika `count(*)`.
`price` olib tashlanadi.

### `dim_managers`

**Grain:** bitta menejer = bitta qator (hozir 1).
**Manba:** `stg_amocrm__users`.

`manager_id`, `manager_name`, `email`.

Yetim `manager_id` yo'qligi tekshirilgan (0 ta), shuning uchun "Noma'lum"
zaxira a'zosi qo'shilmaydi — o'rniga `relationships` testi buzilishni
darhol ushlaydi.

### `dim_stages`

**Grain:** bitta voronka bosqichi = bitta qator (hozir 12).
**Manba:** `stg_amocrm__pipeline_statuses`.

| Ustun | Izoh |
|---|---|
| `stage_key` | surrogat: `pipeline_id || '-' || status_id` |
| `pipeline_id` | tabiiy kalitning 1-qismi |
| `status_id` | tabiiy kalitning 2-qismi |
| `stage_name` | bosqich nomi |
| `stage_sort` | voronka ichidagi tartib |
| `stage_type` | amoCRM status turi |
| `is_won` | `status_id = 142` |
| `is_lost` | `status_id = 143` |

**Kalit nega ikki ustunli:** amoCRM'da `142`/`143` **har bir voronkada
takrorlanadi**. Hozir voronka bitta bo'lgani uchun `status_id` yakka o'zi ham
unikal, lekin ikkinchi voronka qo'shilishi bilan bu buziladi va join
qatorlarni ko'paytirib yuboradi (har bitim ikki marta sanaladi). Shuning uchun
`fct_leads` bilan join **`pipeline_id` va `status_id` ikkalasi bo'yicha**
ketadi.

`is_won` / `is_lost` aynan shu yerda, `fct_leads` da emas — "yutuq" bitimning
emas, bosqichning xossasi. Fakt toza qoladi, mantiq bitta joyda.

### `mart_leads_monthly`

**Grain:** bitta oy = bitta qator.
**Manba:** `fct_leads`.

`created_month` (`date_trunc('month', created_at)`), `lead_count`,
`cumulative_count` (oylar bo'yicha yig'indi, oyning o'sish egri chizig'i uchun).

Bo'sh oylar to'ldirilmaydi — hozir 2026-05 va 2026-06 da bitim yo'q va ular
qatorsiz qoladi. Uzluksiz o'q kerak bo'lsa, uni BI qatlami hal qiladi
(dbt tomonida sana o'lchovi qurish hozircha ortiqcha).

### `mart_leads_by_stage`

Mavjud model **qayta yoziladi**: manba `int_leads_enriched` o'rniga
`fct_leads` + `dim_managers` + `dim_stages`.

Chiqish ustunlari: `manager_name`, `stage_name`, `stage_sort`, `lead_count`.
`total_price` **olib tashlanadi** (model va
[_marts__models.yml](../../../dbt_project/models/marts/_marts__models.yml) dan).

Join `dim_stages` ga `(pipeline_id, status_id)` bo'yicha.

## Mavjud modellar bilan qarorlar

| Model | Qaror | Sabab |
|---|---|---|
| `staging/*` | Tegilmaydi | Manbaning 1:1 tarjimasi. `price` o'sha yerda qolaveradi — zarari yo'q, kerak bo'lsa tayyor. |
| `int_leads_enriched` | **O'chiriladi** | Faqat `mart_leads_by_stage` ni oziqlantirardi; o'rnini fct+dim egalladi. `is_won`/`is_lost` `dim_stages` ga ko'chdi. |
| `mart_leads_by_stage` | Qayta yoziladi | Yuqoriga qarang. |
| `intermediate` papkasi | Bo'sh qoladi | `dbt_project.yml` dagi konfiguratsiyasi turaveradi — keyin kerak bo'ladi. |

## Testlar

| Model | Test |
|---|---|
| `fct_leads.lead_id` | `unique`, `not_null` |
| `fct_leads.manager_id` | `not_null`, `relationships` → `dim_managers.manager_id` |
| `dim_managers.manager_id` | `unique`, `not_null` |
| `dim_stages.stage_key` | `unique`, `not_null` |
| `mart_leads_monthly.created_month` | `unique`, `not_null` |
| `mart_leads_by_stage.{manager_name,stage_name,lead_count}` | `not_null` |

Qo'shimcha — **bitimlar sonining saqlanishi** (singleton test, `tests/`
papkasida SQL fayl):

```
sum(mart_leads_by_stage.lead_count)  ==  count(fct_leads)
```

`fct_leads` ning o'zida join yo'q, shuning uchun u qatorni ko'paytira olmaydi —
haqiqiy xavf `mart_leads_by_stage` dagi `dim_stages` join'ida. Agar ikkinchi
voronka qo'shilib, join `(pipeline_id, status_id)` o'rniga faqat `status_id`
bo'yicha ketib qolsa, `142`/`143` takrorlanishi har bitimni ikki marta
sanaydi — bu test aynan shuni ushlaydi.

Mavjud staging testlari o'zgarishsiz qoladi, shu jumladan
`unique_stg_amocrm__pipeline_statuses_status_id`. U ikkinchi voronka
qo'shilganda yiqiladi — bu kutilgan va foydali signal; o'sha paytda staging
kaliti ham `(pipeline_id, status_id)` ga o'tkaziladi.

## Ishga tushirish

```bash
cd dbt_project
dbt run    # modellarni quradi
dbt test   # testlarni ishlatadi
```

Airflow DAG'iga ulash bu spec'ga kirmaydi — alohida ish.

## Ko'lamdan tashqarida

Ataylab kiritilmadi:

- **Ma'lumot sifati marti** (`price` to'ldirilganligi, UTM %, ETL holati) —
  taklif qilindi, kiritilmadi. Kerak bo'lsa bitta model.
- **Sana o'lchovi (`dim_date`)** — vaqt dinamikasi metrikasi tanlanmagan, oylik
  agregat yetarli.
- **`tasks`, `contacts`, `companies`, `events`** modellari — staging'da yo'q.
  `events` keyinchalik bosqich harakati tarixini berishi mumkin (hozir bunday
  event yo'q).
- **Turg'un bitimlar jadvali** — "N kundan beri qimirlamagan" javob har kuni
  o'zgaradi, uni jadvalga yozib qo'yish uni ertaga yolg'onga aylantiradi.
  `fct_leads.updated_at` bor, hisob so'rov paytida qilinadi.
- **O'chirilgan bitimlarni kuzatish** — EL qatlamining ma'lum cheklovi
  (`CLAUDE.md` ga qarang), bu yerda hal qilinmaydi.
