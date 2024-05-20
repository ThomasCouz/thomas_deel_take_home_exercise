-- To be discussed with business stakeholder: to what extend do we need to mark this test as failed ?
-- It might be ok for the test not to fail if a low number of invoices are caught ?
{{ config(
    severity = 'error',
    store_failures = true,
) }}

{% set threshold_pct = 99.9 %}

with details as (
    select INVOICE_ID,
           STATUS,
           PAYMENT_METHOD,
           has_payment,
           (not has_payment and PAYMENT_METHOD is not null) or
           (has_payment and PAYMENT_METHOD is null) as is_invoice_payment_to_check
    from {{ ref('stg_invoices') }}
    -- we assume that some intenal Deel invoices that should not trigger a payment (to be confirmed)
    where coalesce(PAYMENT_METHOD, '') != 'internal'
),
    summary as (
        select count(*)                                                       as nb_invoices_reference,
               count_if(is_invoice_payment_to_check)                          as nb_invoices_payment_to_check,
               round(nb_invoices_payment_to_check / nb_invoices_reference, 4) as pct_invoices_payment_to_check
        from details
    )

select *
from summary
where pct_invoices_payment_to_check > 1 - {{ threshold_pct }} / 100
