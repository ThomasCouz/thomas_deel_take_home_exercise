select ORGANIZATION_ID,
       DATE_DAY,
       FINANCIAL_BALANCE_USD,
       lag(financial_balance_usd)
           over (partition by ORGANIZATION_ID order by DATE_DAY) as previous_financial_balance_usd,
        iff(previous_financial_balance_usd != 0,
           round((financial_balance_usd - previous_financial_balance_usd) / previous_financial_balance_usd, 4),
           null)                                                 as pct_change_financial_balance
from {{ ref('fct_daily_organization_financial_balances') }}
-- the alert should be sent only for changes of more than 50% vs previous financial balance
qualify abs(pct_change_financial_balance) > 0.5
order by DATE_DAY desc
