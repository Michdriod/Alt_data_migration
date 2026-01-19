{{ config(
	pre_hook = [
		"CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""
	],
	materialized='view'
) }}

with src as (
	select * from {{ ref('stg_casa') }}
),
base as (
	select s.* from src s
),
cust_map as (
	select customer_uuid, cust_no from {{ ref('xform_cust_no_mapping') }}
),
projected as (
	select
		-- Generate random UUID v4 for accounts
		uuid_generate_v4() as account_uuid,

		-- Lookup customer_uuid via cust_no mapping view
		cm.customer_uuid as customer_uuid,

		-- Derived flags not covered by the macro
		(base.restriction_code = 3 or upper(coalesce(base.status_code, '')) = 'F') as is_fully_frozen,

		-- Columns generated from mapping seed
		{{ generate_select_from_mapping('CASA','CASA_ACCOUNT','base') }},

		-- System timestamps
		now() as created_at,
		now() as updated_at
	from base as base
	left join cust_map cm
	  on cm.cust_no = base.cust_id
)
select
	projected.*,
	-- Available balance computed from macro-projected decimals
	(coalesce(projected.ledger_balance, 0)::numeric - coalesce(projected.lien_amount, 0)::numeric)::numeric(38,2) as available_balance
from projected
