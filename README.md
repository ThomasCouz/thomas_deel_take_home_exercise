### Choices and assumptions
- I chose to focus primarily on the **data modelisation** and the **alerting function**
- When developing models and the alerting function I prioritized simplicity, reliability and efficiency
- I chose to use dbt and snowflake for this exercise
- I understand that the data ingestion is not the primary focus of the exercise so I just loaded the csv files in snowflake manually in two tables: `raw_organizations` and `raw_invoices`
- All tables and views are stored in a single database and schema for the exercise (the database and schema are declared as dbt variables in the `dbt_project.yml` file). If the content of this exercice would be used in production I would consider storing views/tables in different databases/schemas 


### How to run
- add `.env` file and enter the following information: 
  ```
  ACCOUNT=<snowflake_account>
  USER=<snowflake_user>
  PASSWORD=<snowflake_password>
  ROLE=<snowflake_role>
  WAREHOUSE=<snowflake_warehouse>
  DATABASE=<snowflake_database>
  SCHEMA=<snowflake_schema>
  SLACK_API_TOKEN=<slack_api_token>
  SLACK_CHANNEL_ID=<slack_channel>
  ```
- run `sh init.sh` to install requirements and dbt packages and to build models
- run `python alerting_function.py` to send to slack the alerts for yesterday's financial balances
- run `python alerting_function.py 2024-04-01 2024-04-04` to send to slack the alerts for financial balances between 2024-04-01 and 2024-04-04 (included).

  Alerts will be sent in the Slack channel filled in the `.env` file


### Discussion about the definition of an organization's financial balance
- From my understanding an organization's financial balance is the amount they owe to Deel at a given point in 
time, based on the status of their invoices.
- For some statuses the amount is due do Deel, for other statuses the amount is due to the customer, and for some other the amound is not due anymore.
- Therefore, there is some business logic to understand in order to build a financial balance concept/metric. **The first thing I would do is to validate with adequate stakeholders how the financial balance should be computed.**
- In the meantime I made the decision to account for invoices as such: 
  - `pending`, `awaiting_payment`, `open` invoices contribute positively to the financial balance
  - `credited`, `refunded` invoices contribute negatively to the financial balance
  - other statuses do not contribute to the financial balance
  
  The logic is encapsulated in the `compute_contribution_to_financial_balance` macro.


### Models architecture
![dbt_dag.png](images%2Fdbt_dag.png)
- `stg_invoices` and `stg_organizations` are staging models built on top of raw data
- `fct_daily_organization_financial_balances` is a day X organization granularity model that stores all the historical financial balances for an organization.
- `intermediate_organization_invoices_stats` is an intermediate model (not exposed externally) that gather invoices information at the organization level
- `dim_organizations` is the main organization table 
- `fct_alerts_organization_financial_balance_moves_50_pct` gathers all the occurrences of the alert we want to send. I chose to create a dbt model for it so that all the business logic remains in dbt (and the python alerting function is used only to send the content)  

### Tests
- appropriate tests have been added to various columns (primary_key, not_null, accepted_values...)
- In addition other singular test have been added to assess data quality: 
  - `invoices_paid_but_no_payment_amount`: some invoices have a `paid` status but do not have a `payment_amount` (even when we exclude internal invoices). **This is something odd to me, I would like to investigate this** 
  - `invoices_payment_method_should_match_has_payment`: I'm exepecting that for a given invoice, either it has a payment method and a payment amount or it does not have a payment method nor a payment amount. **There are some edge cases that I don't understand and that I would like to investigate** - however I would not say it is a breaking issue under a given threshbold
  - `invoices_missing_payment_currency_or_fx_rate_payment`: for every invoice that has a payment amount, the columns `payment_currency` and `fx_rate_payment` should not be null
  - `invoices_usd_fx_rate_is_one`: data quality check to validate that fx rate for USD is 1
  - `invoices_usd_fx_rate_payment_is_one`: data quality check to validate that fx rate payment for USD is 1
  - `organizations_first_is_before_last_payment_date`: Organization's first payment date should always be before their last payment date. If not it means that there is a bug with the data producer of this table



### About models performance 
- Should `intermediate_organization_invoices_stats` be a table instead of a view ? I usually keep intermediate models as views, however if this model is referenced several times in downstream models it might make sense to materialize it as a table
- `dim_organizations`: added a partition on `organization_id` since this table will probably be used a lot in joins
- `fct_daily_organization_financial_balances`: this might be the table that should be monitored in terms of performance because of the volume of data + the aggregation. 
  - I added a partition on `date_day` as this column will often be used for filtering/ordering
  - This table is reloaded incrementally as most of the time data on past days is not expected to change.
  However, we need to take into consideration the fact that some invoices could be created late (late arriving data), for instance if there is an issue 
with the ingestion of the data. **I arbitrarily chose to check for late arriving invoices in the past 48h**, however this point would need some further discussion 
  with both the team wo is responsible for the ingestion and the teams that uses the data in downstream models in order to find the appropriate 
  balance between model efficiency and data quality. A full refresh of this model could be necessary (every week ? every month ?) to make sure we end up 
  catching "very" late arriving invoices





### About the alerting python function
- As I understand that the function/output is expected to be simple (Simplicity is preferred), I deliberately chose not to go with an Airflow dag.
I find the code to be more readable and testable with "simple" python functions, and I think it is more suitable for this kind of exercise
- The python functions provided can be passed to some PythonOperator and a simple dag could be created from them. I chose not to spend time on this as I understand that it is not the main focus of the exercise.
However, if this use case were to go to production I would use more reliable systems and probably go with an orchestration tool (eg. airflow dag)
- The alerting function can be called in 2 different ways: 
  - **without argument**: only the data from the day before will be returned. This is idempotent (same results if called multiple times in a row) **however the results will change from one day to another**
  - **with arguments** (start_date and end_date): we explicitly indicate what for which date range we want to be alerted. This is idempotent and the results will not change from one day to another (except if dates are in the future)
  
  If such a system would be deployed in production I would prefer the second option because it allows us to "replay" in a determinist way what happened in the past

Example of message received in Slack
![slack_screen.png](images%2Fslack_screen.png)


### Possible improvements 
I had a few ideas that I did not manage to implement within the time I allocated to this exercice:  
- every time the alerting function is called and the alert is successfully sent to slack, store the content of the alert so that the next time the function is called we can compare the content of the alert and not send the same content multiple times
- if the csv is empty we do not want to send the alert (or we might want to send a different type of message ?)
- adopt best practices for dbt project: precommit with linter & sqlfluff, cicd, [dbt-project-evaluator](https://github.com/dbt-labs/dbt-project-evaluator), [dbt-codegen](https://github.com/dbt-labs/dbt-codegen) ...
- use proper dbt docs (instead of descriptions)
- use dbt selectors (to reload models in a reliable way)
- use groups and model access (in order to control which models are exposed)
- use models versions (if there is a need)
- use data contract with data producers + gather additional infos on some columns (in order to document them properly)
- work on the display of the alert to make it more "shiny" 