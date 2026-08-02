-- Bitimlar sonining saqlanishi.
-- mart_leads_by_stage dagi lead_count yig'indisi fct_leads qator soniga
-- TENG bo'lishi shart.
--
-- Nima ushlaydi: dim_stages join'i qatorlarni ko'paytirib yuborishini.
-- Ikkinchi voronka qo'shilgan kunda, agar join (pipeline_id, status_id)
-- o'rniga faqat status_id bo'yicha ketsa, 142/143 takrorlanishi har bitimni
-- ikki marta sanaydi — o'shanda shu test qizil beradi.
--
-- dbt qoidasi: 0 qator qaytsa — test o'tdi.

with mart as (
    select sum(lead_count) as cnt from {{ ref('mart_leads_by_stage') }}
),

fakt as (
    select count(*) as cnt from {{ ref('fct_leads') }}
)

select
    mart.cnt as mart_dagi_soni,
    fakt.cnt as fakt_dagi_soni
from mart
cross join fakt
where mart.cnt <> fakt.cnt
