{% macro generate_date_spine(start_date, end_date) %}
    {%- if execute -%}
        {# Create a date spine using GENERATE_DATE_ARRAY (BigQuery-specific) #}
        SELECT date
        FROM UNNEST(
            GENERATE_DATE_ARRAY(
                CAST('{{ start_date }}' AS DATE),
                CAST('{{ end_date }}' AS DATE)
            )
        ) AS date
    {%- endif -%}
{% endmacro %}
