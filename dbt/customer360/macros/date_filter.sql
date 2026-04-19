{% macro date_filter(column_name, date_var) %}
    {{ column_name }} = CAST('{{ date_var }}' AS DATE)
{% endmacro %}