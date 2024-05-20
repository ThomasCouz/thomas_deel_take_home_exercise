{% test primary_key(model, column_name) %}

with 

null_rows as (
    select 
        {{ column_name }},
        'null' as problem 
    from {{ model }}
    where {{ column_name }} is null
),

not_unique_rows as (
    select
        {{ column_name }},
        'not unique' as problem
    from (
        select
            {{ column_name }},
            count(*)
        from {{ model }}
        where {{ column_name }} is not null
        group by 1
        having count(*) > 1
    )
)

select * from null_rows
union all
select * from not_unique_rows

{% endtest %}