{{
    config(
        materialized='table'
    )
}}


select 
    
from {{ ref('xform_customers') }}