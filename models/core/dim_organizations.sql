{{
    config(
        cluster_by=['organization_id']
    )
}}

with organization_current_financial_balance as (
    select ORGANIZATION_ID,
       FINANCIAL_BALANCE_USD as current_financial_balance_usd
from {{ ref('fct_daily_organization_financial_balances') }}
qualify row_number() over (partition by ORGANIZATION_ID order by  date_day desc ) = 1
)

select o.ORGANIZATION_ID,
       o.LEGAL_ENTITY_COUNTRY_CODE,

       o.COUNT_TOTAL_CONTRACTS_ACTIVE,
       o.FIRST_PAYMENT_DATE,
       o.LAST_PAYMENT_DATE,

       stats.FIRST_INVOICE_CREATED_AT,
       stats.LAST_INVOICE_CREATED_AT,

       stats.TOTAL_NB_INVOICES,
       stats.TOTAL_NB_PAID_INVOICES,
       stats.TOTAL_INVOICED_AMOUNT_USD,

       stats.NB_INVOICES_L30D,
       stats.NB_PAID_INVOICES_L30D,
       stats.INVOICED_AMOUNT_USD_L30D,

       stats.LAST_INVOICE_ID,
       stats.LAST_INVOICE_STATUS,
       stats.LAST_INVOICE_PAYMENT_METHOD,
       stats.LAST_INVOICE_AMOUNT_USD,

       curr_fb.current_financial_balance_usd,

       o.CREATED_AT

from {{ ref('stg_organizations') }} as o
         left join {{ ref('intermediate_organization_invoices_stats') }} as stats on o.ORGANIZATION_ID = stats.ORGANIZATION_ID
         left join organization_current_financial_balance as curr_fb on o.ORGANIZATION_ID = curr_fb.ORGANIZATION_ID
