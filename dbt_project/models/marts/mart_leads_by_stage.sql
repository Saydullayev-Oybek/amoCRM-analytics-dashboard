-- MART: menejer × voronka bosqichi bo'yicha bitimlar soni (voronka hisoboti).
-- Dashboard shu jadvaldan o'qiydi. status_sort bo'yicha tartiblangan (voronka tartibi).

select
    manager_name,                                    -- menejer
    status_name             as stage_name,           -- voronka bosqichi
    status_sort             as stage_sort,           -- voronka tartibi (grafik uchun)
    count(*)                as lead_count,           -- shu bosqichdagi bitimlar soni
    sum(price)              as total_price           -- shu bosqichdagi umumiy summa
from {{ ref('int_leads_enriched') }}
group by manager_name, status_name, status_sort
order by manager_name, status_sort
