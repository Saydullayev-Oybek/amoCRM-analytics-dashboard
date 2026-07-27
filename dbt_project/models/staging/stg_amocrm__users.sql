-- Users (menejerlar) staging: id va ism.

select
    id      as user_id,        -- menejer id
    name    as manager_name,   -- menejer ismi
    email
from {{ source('amocrm', 'users') }}
