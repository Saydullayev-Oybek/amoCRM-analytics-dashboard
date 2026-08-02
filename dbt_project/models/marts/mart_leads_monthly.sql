-- MART: oy bo'yicha yaratilgan bitimlar soni + kumulyativ o'sish.
-- Bu hozirgi ma'lumotdagi eng ma'noli kesim (2025-02 dan 2026-07 gacha).
-- Bitimi yo'q oylar qatorsiz qoladi (masalan 2026-05, 2026-06) — uzluksiz
-- vaqt o'qi kerak bo'lsa, uni BI qatlami hal qiladi.

with oylik as (

    select
        date_trunc('month', created_at) as created_month,
        count(*)                        as lead_count
    from {{ ref('fct_leads') }}
    group by 1

)

select
    created_month,
    lead_count,
    sum(lead_count) over (order by created_month) as cumulative_count  -- to'plangan yig'indi
from oylik
order by created_month
