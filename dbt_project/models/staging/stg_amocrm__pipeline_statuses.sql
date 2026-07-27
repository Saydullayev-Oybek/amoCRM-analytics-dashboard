-- Voronka bosqichlari (statuslar) staging: bosqich nomi va tartibi.
-- amoCRM'da 142 = "Успешно реализовано" (won), 143 = "Закрыто и не реализовано" (lost).

select
    id              as status_id,       -- bosqich id
    name            as status_name,     -- bosqich nomi
    sort            as status_sort,     -- voronka ichidagi tartib (kichikdan kattaga)
    pipeline_id,                        -- qaysi voronka
    type            as status_type      -- bosqich turi
from {{ source('amocrm', 'pipelines___embedded__statuses') }}
