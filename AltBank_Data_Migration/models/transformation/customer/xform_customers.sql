{{ config(
  pre_hook = [
    "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""
  ]
) }}

with src as (
	select * from {{ ref('stg_customers') }}
),
base as (
	select
		s.*
	from src s
)
select
	-- Deterministic UUID v5 derived from cust_id (requires uuid-ossp)
	uuid_generate_v5(uuid_ns_url(), base.cust_id::text) as customer_uuid,

	{{ generate_select_from_mapping('CUSTOMER','customer','base') }}

from base as base