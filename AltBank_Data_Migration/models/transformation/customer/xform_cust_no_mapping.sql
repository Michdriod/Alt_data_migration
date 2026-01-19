{{
    config(
        materialized='view'
    )
}}


select customer_uuid, cust_no from {{ ref('xform_customers') }}