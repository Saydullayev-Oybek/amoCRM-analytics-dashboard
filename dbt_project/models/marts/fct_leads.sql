-- FAKT: bitimlar. Grain — bitta tirik bitim = bitta qator.
-- Join yo'q, agregatsiya yo'q, filtr yo'q (o'chirilganlar staging'da chiqarilgan).
-- O'lchov ustuni ataylab yo'q: fakt sanaladigan hodisaning o'zi, metrika — count(*).
-- price olib tashlangan — amoCRM'da hech qachon to'ldirilmagan (spec'ga qarang).

select
    lead_id,        -- birlamchi kalit
    manager_id,     -- -> dim_managers
    pipeline_id,    -- -> dim_stages (kalitning 1-qismi)
    status_id,      -- -> dim_stages (kalitning 2-qismi)
    lead_name,      -- hozir "Lead #NNNN" (import qo'ygan avtomatik nom)
    created_at,
    updated_at      -- hozir barchasida import sanasi (2026-07-22)
from {{ ref('stg_amocrm__leads') }}
