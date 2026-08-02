-- MART: menejer × voronka bosqichi bo'yicha bitimlar soni.
-- Manba: fct_leads + dim'lar (avval int_leads_enriched edi).
--
-- DIQQAT: bu voronka HARAKATI emas. Bazada lead_status_changed eventi umuman
-- yo'q, ya'ni bitimlar bosqichlar bo'ylab hech qachon ko'chmagan — jadval
-- importning taqsimotini ko'rsatadi, sotuv konversiyasini emas.
--
-- dim_stages'ga join IKKALA ustun bo'yicha — sababi dim_stages.sql izohida.

select
    coalesce(m.manager_name, 'Noma''lum') as manager_name,   -- menejer
    s.stage_name,                                            -- voronka bosqichi
    s.stage_sort,                                            -- voronka tartibi (grafik uchun)
    count(*)                              as lead_count      -- shu bosqichdagi bitimlar soni
from {{ ref('fct_leads') }} f
left join {{ ref('dim_managers') }} m
    on f.manager_id = m.manager_id
left join {{ ref('dim_stages') }} s
    on  f.pipeline_id = s.pipeline_id
    and f.status_id   = s.status_id
group by 1, 2, 3
order by manager_name, s.stage_sort
