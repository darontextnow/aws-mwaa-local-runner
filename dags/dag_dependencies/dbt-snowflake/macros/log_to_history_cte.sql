{% macro log_to_history_cte(log_table, id_column, state_columns, ts_column, aux_columns=[], order_by=[]) %}

    {# aux_columns are extra columns that we want to keep in the final CTE #}
    {% if aux_columns | length > 0 %}
        {% set aux_columns = aux_columns | join(', ') ~ ',' %}
    {% else %}
        {% set aux_columns = '' %}
    {% endif %}

    {# if there are ties in the ts_column, order_by is used to break the ties #}
    {% if order_by | length > 0 %}
        {% set order_by = order_by | join(', ') %}
    {% else %}
        {% set order_by = ts_column %}
    {% endif %}

-- if you are familiar with Vertica, the first two CTEs simulate Vertica's CONDITIONAL_CHANGE_EVENT(state_columns)

hist AS (
    -- for each item, find out if the state variable(s) has changed
    SELECT
        {{ id_column }},
        {{ state_columns | join(', ') }},
        {{ aux_columns }}
        CASE WHEN
            {% for sv in state_columns %}
                {{ sv }} != lag({{ sv }}) OVER (PARTITION BY {{ id_column }} ORDER BY {{ order_by }})
                {% if not loop.last %}
                OR
                {% endif %}
            {% endfor %}
            THEN 1 ELSE 0
        END AS state_changed,
        {{ order_by }}
    FROM {{ log_table }}
),

hist_with_time_blocks AS (
    -- for each item, if state variable(s) are unchanged, they belong to the same block_id
    -- if any of state variable(s) changed, a new block_id is assigned
    -- calculate from_ts and to_s between which state variable(s) are valid
    SELECT
        {{ id_column }},
        {{ state_columns | join(', ') }},
        {{ aux_columns }}
        SUM(state_changed) OVER (
            PARTITION BY {{ id_column }}
            ORDER BY {{ order_by }}
            ROWS UNBOUNDED PRECEDING
        ) AS block_id,
        {{ order_by }},
        {{ ts_column }} AS from_ts,
        LEAD({{ ts_column }}) OVER (PARTITION BY {{ id_column }} ORDER BY {{ order_by }}) AS to_ts
    FROM hist
),

log_to_history AS (
    -- squash rows with the same item_id / block_id, this should dramatically reduce number of rows
    SELECT DISTINCT
        {{ id_column }},
        {{ state_columns | join(', ') }},
        {{ aux_columns }}
        MIN(from_ts) OVER (
            PARTITION BY {{ id_column }}, block_id
            ORDER BY {{ order_by }} ROWS UNBOUNDED PRECEDING
        ) AS from_ts,
        LAST_VALUE(to_ts) OVER (
            PARTITION BY {{ id_column }}, block_id
            ORDER BY from_ts, to_ts NULLS LAST
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS to_ts
    FROM hist_with_time_blocks
)

{%- endmacro %}
