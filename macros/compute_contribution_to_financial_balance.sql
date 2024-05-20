-- Discussion to have about how an invoice should impact the customer's financial balance depending on its
-- status:
-- We need an explicit and agreed definition of the financial balance to make sure we implement it correctly.
-- This definition should be properly documented (with dbt docs for example)

{% macro compute_contribution_to_financial_balance(status, amount_usd) %}
    case
        when {{ status }} in ('pending', 'awaiting_payment', 'open') then {{ amount_usd }}
        when {{ status }} in ('credited', 'refunded') then -{{ amount_usd }}
        else 0
    end
{% endmacro %}