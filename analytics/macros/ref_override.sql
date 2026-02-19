{% macro ref(model_name) %}
   {% do return(builtins.ref(model_name).include(database=false, schema=false)) %}
{% endmacro %}

{% macro source(source_name, table_name) %}
   {% do return(builtins.source(source_name, table_name).include(database=false, schema=false)) %}
{% endmacro %}
