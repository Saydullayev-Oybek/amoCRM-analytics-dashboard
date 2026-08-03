-- analytics-db BIRINCHI marta yaratilganda (volume bo'sh bo'lganda) bir marta ishlaydi.
-- Keyingi `docker compose up` larda ishlamaydi — Postgres init skriptlari faqat
-- bo'sh data katalogida bajariladi.
--
-- Vazifasi: Metabase uchun FAQAT-O'QISH foydalanuvchisini tayyorlash.
-- Metabase'da SQL editor bor — kirgan har kim ixtiyoriy so'rov yoza oladi.
-- Shuning uchun u `postgres` superuser bilan emas, shu rol bilan ulanadi.

create user metabase_ro with password 'metabase_ro_2026';
grant connect on database amocrm to metabase_ro;

-- Schema'larni oldindan yaratamiz: grant berish uchun ular MAVJUD bo'lishi kerak,
-- lekin dbt hali ishlamagan. dbt keyin "create schema if not exists" qiladi,
-- ya'ni to'qnashuv bo'lmaydi.
create schema if not exists staging;
create schema if not exists marts;

grant usage on schema staging, marts to metabase_ro;
grant select on all tables in schema staging, marts to metabase_ro;

-- MUHIM: dbt har `run` da mart jadvallarini QAYTA yaratadi. Faqat yuqoridagi
-- `grant select on all tables` bo'lsa, ruxsat har `dbt run` dan keyin yo'qoladi.
-- `default privileges` esa bundan keyin YARATILADIGAN jadvallarga ham amal qiladi.
alter default privileges in schema staging, marts grant select on tables to metabase_ro;

-- `raw_data` ataylab berilmaydi — dashboard marts ustida ishlaydi, xom JSON'da emas.
