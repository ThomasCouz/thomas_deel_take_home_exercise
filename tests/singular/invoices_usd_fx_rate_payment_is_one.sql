{{ config(
    severity = 'error',
    store_failures = true,
) }}

select invoice_id, PAYMENT_CURRENCY, fx_rate_payment
from {{ ref('stg_invoices') }}
where (PAYMENT_CURRENCY in ('USD', 'USDC') and fx_rate_payment != 1)
