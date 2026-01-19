with src as (
    select * from {{ source('legacy_core', 'casa') }}
)

select * from src