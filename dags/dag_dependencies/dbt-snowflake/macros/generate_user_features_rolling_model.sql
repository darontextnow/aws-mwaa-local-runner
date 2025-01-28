{%- macro generate_user_features_rolling_model(model) -%}

{{
    config(
        full_refresh = false,
        materialized='incremental',
        unique_key='report_date'
    )
}}

SELECT
    {{ var('ds') }}::DATE AS report_date,
    username

    {%- set columns = adapter.get_columns_in_relation(model) -%}
    {% for column in columns %}

        {%- set col_name = column.name -%}

        {% if column.is_numeric() %}

            ,{{ agg_avg_days_between(col_name) }}
            ,{{ agg_days_from_last(col_name, 'report_date') }}

            {% for w in [0, 3, 7, 28] %}

                ,{{ agg_window_sum(col_name, w, 'report_date') }}
                ,{{ agg_window_count(col_name, w, 'report_date') }}

            {% endfor %}

        {% elif column.is_string() and col_name not in ['USERNAME', 'USER_SET_ID'] %}

            ,{{ agg_unique_count(col_name) }}
            ,{{ agg_mode(col_name) }}
            ,{{ agg_last(col_name) }}
            ,{{ agg_days_from_last_text(col_name, 'report_date') }}

        {% endif %}

    {% endfor %}

FROM {{ model }}
WHERE (date_utc BETWEEN DATEADD(DAY, -28, {{ var('ds') }}::DATE) AND {{ var('ds') }}::DATE)
GROUP BY 1, 2

{%- endmacro -%}