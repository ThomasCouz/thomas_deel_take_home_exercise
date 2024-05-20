{{ config(
    severity = 'error',
    store_failures = true,
) }}

select ORGANIZATION_ID, FIRST_PAYMENT_DATE, LAST_PAYMENT_DATE
from {{ ref('stg_organizations') }}
where LAST_PAYMENT_DATE < FIRST_PAYMENT_DATE
