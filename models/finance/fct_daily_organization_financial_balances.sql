{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='surrogate_key',
        cluster_by=['date_day'],
    )
}}


select
    {{ dbt_utils.generate_surrogate_key([
            'ORGANIZATION_ID',
            'cast(convert_timezone(\'UTC\', CREATED_AT) as date)'
    ]) }} as surrogate_key,
    ORGANIZATION_ID,
    cast(convert_timezone('UTC', CREATED_AT) as date) as date_day,
    round(sum({{ compute_contribution_to_financial_balance('status', 'amount_usd') }}), 2) as amount_usd_daily_contribution_financial_balance,
    round(sum(amount_usd_daily_contribution_financial_balance)
                     over (partition by ORGANIZATION_ID order by date_day), 2) as financial_balance_usd
from {{ ref('stg_invoices') }}

{% if is_incremental() %}
-- Discussion to have about  late arriving data:
-- It makes sense for this model to be incremental since the granularity is day X organization
-- However we need to take into consideration the fact that some invoices could be created late: I arbitrarily chose
-- to consider that invoices could arrive with a 48h delay. A full refresh at an appropriate frequency could be necessary
where date_day >= (select least(max(date_day), convert_timezone('UTC', current_timestamp) - interval '48 hours') from {{ this }})
{% endif %}
group by 1, 2, 3
