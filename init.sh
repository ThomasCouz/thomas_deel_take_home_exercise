pip install -r requirements.txt
dbt clean
dbt deps
dbt debug
dbt compile
dbt build --full-refresh