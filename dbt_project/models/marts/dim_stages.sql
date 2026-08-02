-- O'LCHOV: voronka bosqichlari.
-- Grain — bitta voronka bosqichi = bitta qator.
--
-- DIQQAT — kalit ikki ustunli: (pipeline_id, status_id).
-- amoCRM'da 142 ("Успешно реализовано") va 143 ("Закрыто и не реализовано")
-- HAR BIR voronkada takrorlanadi. Hozir voronka bitta bo'lgani uchun status_id
-- yakka o'zi ham unikal, lekin ikkinchi voronka qo'shilishi bilan bu buziladi.
-- Shuning uchun fct_leads bilan join IKKALA ustun bo'yicha ketishi shart —
-- aks holda har bitim ikki marta sanaladi.

select
    pipeline_id::text || '-' || status_id::text as stage_key,  -- surrogat kalit (unique testi uchun)
    pipeline_id,
    status_id,
    status_name  as stage_name,   -- bosqich nomi
    status_sort  as stage_sort,   -- voronka ichidagi tartib (kichikdan kattaga)
    status_type  as stage_type,   -- amoCRM status turi
    (status_id = 142) as is_won,  -- yutilgan bosqichmi
    (status_id = 143) as is_lost  -- yo'qotilgan bosqichmi
from {{ ref('stg_amocrm__pipeline_statuses') }}
