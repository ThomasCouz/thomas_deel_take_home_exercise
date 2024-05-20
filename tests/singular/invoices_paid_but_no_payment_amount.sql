{{ config(
    severity = 'warn',
    store_failures = true,
) }}

select INVOICE_ID, STATUS, PAYMENT_AMOUNT
from {{ ref('stg_invoices') }}
where STATUS = 'paid'
and not HAS_PAYMENT
-- we assume that some intenal Deel invoices that should not trigger a payment (to be confirmed)
and coalesce(PAYMENT_METHOD, '') != 'internal'
