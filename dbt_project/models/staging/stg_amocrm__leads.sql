-- Leads (bitimlar) staging: kerakli ustunlarni tanlash, nomlash va tiplash.
-- Sana maydonlari Unix epoch (son) -> timestamp.

with source as (
    select * from {{ source('amocrm', 'leads') }}
)

select
    id                          as lead_id,          -- bitim id
    name                        as lead_name,        -- bitim nomi
    price,                                           -- bitim summasi
    responsible_user_id         as manager_id,       -- mas'ul menejer
    status_id,                                       -- voronka bosqichi id
    pipeline_id,                                     -- voronka id
    to_timestamp(created_at)    as created_at,       -- yaratilgan vaqt
    to_timestamp(updated_at)    as updated_at,       -- oxirgi o'zgargan vaqt
    is_deleted                                       -- o'chirilganmi (API flagi)
from source
where not is_deleted                                 -- o'chirilgan bitimlarni chiqarib tashlaymiz
