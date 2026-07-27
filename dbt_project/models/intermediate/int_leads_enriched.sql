-- Boyitilgan bitimlar: leads + menejer ismi + bosqich nomi.
-- Bu yerda hosila belgilar (won/lost/created_month) hisoblanadi — marts shundan foydalanadi.

with leads as (
    select * from {{ ref('stg_amocrm__leads') }}
),

users as (
    select * from {{ ref('stg_amocrm__users') }}
),

statuses as (
    select * from {{ ref('stg_amocrm__pipeline_statuses') }}
)

select
    l.lead_id,
    l.manager_id,
    coalesce(u.manager_name, 'Noma''lum')   as manager_name,   -- menejer ismi
    l.pipeline_id,
    l.status_id,
    s.status_name,                                             -- bosqich nomi
    s.status_sort,                                             -- voronka tartibi
    l.price,
    l.created_at,
    date_trunc('month', l.created_at)        as created_month, -- yaratilgan oy
    (l.status_id = 142)                      as is_won,        -- won (hozircha hech biri emas)
    (l.status_id = 143)                      as is_lost        -- lost
from leads l
left join users u    on l.manager_id = u.user_id
left join statuses s on l.status_id = s.status_id
