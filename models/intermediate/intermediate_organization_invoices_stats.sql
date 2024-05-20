with organization_last_invoice_infos as (
    select ORGANIZATION_ID, INVOICE_ID, STATUS, PAYMENT_METHOD, AMOUNT_USD
    from {{ ref('stg_invoices') }}
    qualify row_number() over (partition by ORGANIZATION_ID order by CREATED_AT desc ) = 1
),

     organizataion_invoice_stats as (
         select ORGANIZATION_ID,
                min(CREATED_AT)           as first_invoice_created_at,
                max(CREATED_AT)           as last_invoice_created_at,
                count(*)                  as total_nb_invoices,
                count_if(STATUS = 'paid') as total_nb_paid_invoices,
                round(sum(AMOUNT_USD), 2) as total_invoiced_amount_usd,
                count_if(convert_timezone('UTC', CREATED_AT)
                    > convert_timezone('UTC', current_timestamp) - interval '30 days') as nb_invoices_l30d,
                count_if(convert_timezone('UTC', CREATED_AT)
                    > convert_timezone('UTC', current_timestamp) - interval '30 days' and STATUS = 'paid')  as nb_paid_invoices_l30d,
                round(sum(iff(convert_timezone('UTC', CREATED_AT)
                    > convert_timezone('UTC', current_timestamp) - interval '30 days', AMOUNT_USD, null)), 2) as invoiced_amount_usd_l30d
         from {{ ref('stg_invoices') }}
         group by 1
     )

select organizataion_invoice_stats.ORGANIZATION_ID,
       organizataion_invoice_stats.first_invoice_created_at,
       organizataion_invoice_stats.last_invoice_created_at,

       organizataion_invoice_stats.total_nb_invoices,
       organizataion_invoice_stats.total_nb_paid_invoices,
       organizataion_invoice_stats.total_invoiced_amount_usd,

       organizataion_invoice_stats.nb_invoices_l30d,
       organizataion_invoice_stats.nb_paid_invoices_l30d,
       organizataion_invoice_stats.invoiced_amount_usd_l30d,

       organization_last_invoice_infos.INVOICE_ID     as last_invoice_id,
       organization_last_invoice_infos.STATUS         as last_invoice_status,
       organization_last_invoice_infos.PAYMENT_METHOD as last_invoice_payment_method,
       organization_last_invoice_infos.AMOUNT_USD     as last_invoice_amount_usd

from organizataion_invoice_stats
         left join organization_last_invoice_infos
                   on organizataion_invoice_stats.ORGANIZATION_ID = organization_last_invoice_infos.ORGANIZATION_ID


