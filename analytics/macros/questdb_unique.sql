{% test questdb_unique(model, column_name) %}

select *
from (
    select
        {{ column_name }},
        count(*) as cnt
    from {{ model }}
    group by {{ column_name }}
) AS sub
where cnt > 1

{% endtest %}
