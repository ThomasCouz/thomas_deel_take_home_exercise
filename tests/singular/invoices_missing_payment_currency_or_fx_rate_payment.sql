{{ config(
    severity = 'error',
    store_failures = true,
) }}

select INVOICE_ID, PAYMENT_AMOUNT, PAYMENT_CURRENCY, fx_rate_payment
from {{ ref('stg_invoices') }}
where PAYMENT_AMOUNT is not null and (PAYMENT_CURRENCY is null or fx_rate_payment is null)
