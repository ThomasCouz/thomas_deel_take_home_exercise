select try_to_number(ORGANIZATION_ID)              as organization_id,
       try_to_number(LEGAL_ENTITY_COUNTRY_CODE)    as legal_entity_country_code,
       try_to_number(COUNT_TOTAL_CONTRACTS_ACTIVE) as count_total_contracts_active,
       try_to_date(FIRST_PAYMENT_DATE)             as first_payment_date,
       try_to_date(LAST_PAYMENT_DATE)              as last_payment_date,
       try_to_timestamp(CREATED_DATE)              as created_at
from {{ source('raw_data_deel', 'raw_organizations') }}
