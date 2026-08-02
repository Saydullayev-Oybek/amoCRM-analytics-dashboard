-- O'LCHOV: menejerlar (amoCRM foydalanuvchilari).
-- Grain — bitta menejer = bitta qator.
-- "Noma'lum" zaxira a'zosi qo'shilmaydi: yetim manager_id yo'qligi tekshirilgan,
-- va fct_leads'dagi relationships testi paydo bo'lsa darhol ushlaydi.

select
    user_id as manager_id,   -- birlamchi kalit
    manager_name,
    email
from {{ ref('stg_amocrm__users') }}
