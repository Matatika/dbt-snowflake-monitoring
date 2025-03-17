-- <1000 rows, will be more expensive to materialize incrementally with multiple SQL statements
{{ config(materialized='table') }}

select
    date,
    organization_name,
    contract_number,
    account_name,
    account_locator,
    region,
    service_level,
    usage_type,
    currency,
    effective_rate,
    service_type
from {{ source('snowflake_organization_usage', 'rate_sheet_daily') }}
{% if var('raw_config_rate_sheet_daily_enabled', false) %}
union all
select
    date,
    organization_name,
    contract_number,
    account_name,
    account_locator,
    region,
    service_level,
    usage_type,
    currency,
    effective_rate,
    service_type
from {{ source('raw_config', 'rate_sheet_daily') }}
{% endif %}
order by date
