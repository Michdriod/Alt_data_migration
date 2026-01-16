with src as (
    select * from {{ source('legacy_core', 'customer_cif') }}
)

select * from src