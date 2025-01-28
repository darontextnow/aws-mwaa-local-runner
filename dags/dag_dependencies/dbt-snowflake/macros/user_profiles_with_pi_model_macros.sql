{%- macro clean_varchar_col(col) -%}
    NULLIF(TRIM({{ col }}), '')
{%- endmacro -%}

{%- macro get_pp_col_ts(pp_col) -%}
    --pp columns are arrays [created_at, value]. So, return created_at
    {{ pp_col }}[0]::TIMESTAMP_NTZ
{%- endmacro -%}

{%- macro get_pp_col_val(pp_col) -%}
    --pp columns are arrays [created_at, value]. So, return value. Don't cast value here as can be multiple data-types.
    {{ pp_col }}[1]
{%- endmacro -%}

{%- macro get_min_ts_column(pp_upd_col, pp_alc_col) -%}
    --return the min timestamp from these columns
    LEAST_IGNORE_NULLS(r.created_at, u.timestamp, {{ get_pp_col_ts(pp_upd_col) }}, {{ get_pp_col_ts(pp_alc_col) }})
{%- endmacro -%}

{%- macro get_max_ts_column(pp_upd_col, pp_alc_col) -%}
    --return the max timestamp from these columns
    GREATEST_IGNORE_NULLS(r.created_at, u.timestamp, {{ get_pp_col_ts(pp_upd_col) }}, {{ get_pp_col_ts(pp_alc_col) }})
{%- endmacro -%}

{%- macro generate_min_val_from_4_cols(reg_col, users_col, pp_upd_col, pp_alc_col) -%}

    CASE
        WHEN ({{ get_min_ts_column(pp_upd_col, pp_alc_col) }} = r.created_at)
            AND ({{ clean_varchar_col(reg_col) }} IS NOT NULL) THEN {{ clean_varchar_col(reg_col) }}
        WHEN ({{ get_min_ts_column(pp_upd_col, pp_alc_col) }} = u.timestamp)
            AND ({{ clean_varchar_col(users_col) }} IS NOT NULL) THEN {{ clean_varchar_col(users_col) }}
        WHEN ({{ get_min_ts_column(pp_upd_col, pp_alc_col) }} = {{ get_pp_col_ts(pp_upd_col) }})
            AND ({{ get_pp_col_val(pp_upd_col) }} IS NOT NULL) THEN {{ get_pp_col_val(pp_upd_col) }}
        WHEN ({{ get_min_ts_column(pp_upd_col, pp_alc_col) }} = {{ get_pp_col_ts(pp_alc_col) }})
            AND ({{ get_pp_col_val(pp_alc_col) }} IS NOT NULL) THEN {{ get_pp_col_val(pp_alc_col) }}
        ELSE COALESCE({{ clean_varchar_col(reg_col) }}, {{ clean_varchar_col(users_col) }}, {{ get_pp_col_val(pp_upd_col) }}, {{ get_pp_col_val(pp_alc_col) }})
    END

{%- endmacro -%}

{%- macro generate_max_val_from_4_cols(reg_col, users_col, pp_upd_col, pp_alc_col) -%}

    CASE
        WHEN ({{ get_max_ts_column(pp_upd_col, pp_alc_col) }} = r.created_at)
            AND ({{ clean_varchar_col(reg_col) }} IS NOT NULL) THEN {{ clean_varchar_col(reg_col) }}
        WHEN ({{ get_max_ts_column(pp_upd_col, pp_alc_col) }} = u.timestamp)
            AND ({{ clean_varchar_col(users_col) }} IS NOT NULL) THEN {{ clean_varchar_col(users_col) }}
        WHEN ({{ get_max_ts_column(pp_upd_col, pp_alc_col) }} = {{ get_pp_col_ts(pp_upd_col) }})
            AND ({{ get_pp_col_val(pp_upd_col) }} IS NOT NULL) THEN {{ get_pp_col_val(pp_upd_col) }}
        WHEN ({{ get_max_ts_column(pp_upd_col, pp_alc_col) }} = {{ get_pp_col_ts(pp_alc_col) }})
            AND ({{ get_pp_col_val(pp_alc_col) }} IS NOT NULL) THEN {{ get_pp_col_val(pp_alc_col) }}
        ELSE COALESCE({{ get_pp_col_val(pp_upd_col) }}, {{ get_pp_col_val(pp_alc_col) }}, {{ clean_varchar_col(users_col) }}, {{ clean_varchar_col(reg_col) }})
    END

{%- endmacro -%}

{%- macro generate_array_from_4_cols(reg_col, users_col, pp_upd_col, pp_alc_col) -%}

        ARRAY_DISTINCT(ARRAY_CONSTRUCT_COMPACT(
            {{ clean_varchar_col(reg_col) }},
            {{ clean_varchar_col(users_col) }},
            {{ get_pp_col_val(pp_upd_col) }},
            {{ get_pp_col_val(pp_alc_col) }}
        ))

{%- endmacro -%}

{%- macro generate_sourced_from_val(reg_col, users_col, pp_upd_col, pp_alc_col) -%}

    CASE
        WHEN ({{ get_max_ts_column(pp_upd_col, pp_alc_col) }} = r.created_at)
            AND ({{ clean_varchar_col(reg_col) }} IS NOT NULL) THEN 'registrations'
        WHEN ({{ get_max_ts_column(pp_upd_col, pp_alc_col) }} = u.timestamp)
            AND ({{ clean_varchar_col(users_col) }} IS NOT NULL) THEN 'users'
        WHEN ({{ get_max_ts_column(pp_upd_col, pp_alc_col) }} = {{ get_pp_col_ts(pp_upd_col) }})
            AND ({{ get_pp_col_val(pp_upd_col) }} IS NOT NULL) THEN 'userinformationupdate'
        WHEN ({{ get_max_ts_column(pp_upd_col, pp_alc_col) }} = {{ get_pp_col_ts(pp_alc_col) }})
            AND ({{ get_pp_col_val(pp_alc_col) }} IS NOT NULL) THEN 'applifecyclechanged'
        WHEN {{ get_pp_col_val(pp_upd_col) }} IS NOT NULL THEN 'userinformationupdate'
        WHEN {{ get_pp_col_val(pp_alc_col) }} IS NOT NULL THEN 'applifecyclechanged'
        WHEN {{ clean_varchar_col(users_col) }} IS NOT NULL THEN 'users'
        WHEN {{ clean_varchar_col(reg_col) }} IS NOT NULL THEN 'registrations'
        ELSE NULL
    END

{%- endmacro -%}

{%- macro generate_ppr_location_data_dicts(location_source) -%}

    {% set loc_dict = "{
        'area_code': NULLIF(TRIM(\"payload.location.area_code\"), ''),
        'city': NULLIF(TRIM(\"payload.location.city\"), ''),
        'continent_code': NULLIF(TRIM(\"payload.location.continent_code\"), ''),
        'country_code': NULLIF(TRIM(\"payload.location.country_code\"), ''),
        'state_code': NULLIF(TRIM(\"payload.location.state_code\"), ''),
        'zip_code': NULLIF(TRIM(\"payload.location.zip_code\"), ''),
        'latitude': NULLIF(TRIM(\"payload.location.coordinates.latitude\"), ''),
        'longitude': NULLIF(TRIM(\"payload.location.coordinates.longitude\"), '')
    }" %}

    CASE WHEN "payload.location.location_source" = '{{ location_source }}' AND {{ loc_dict }} <> {} THEN {{ loc_dict }} END

{%- endmacro -%}

{%- macro generate_incremental_load_column_expressions_retaining_previous_values(model) -%}

    {%- set columns = adapter.get_columns_in_relation(model) -%}
    {%- for column in columns -%}

        {% set col_name = column.name %}

        {%- if column.dtype == 'ARRAY' -%}
            ARRAY_DISTINCT(ARRAY_CAT(NVL(updated_data.{{ col_name }}, []), NVL(previous_data.{{ col_name }}, []))) AS {{ col_name }},
        {%- else -%}
            {%- if col_name.startswith('FIRST_') and col_name != 'FIRST_FIRST_NAME' -%}
                COALESCE(previous_data.{{ col_name }}, updated_data.{{ col_name }}) AS {{ col_name }},
            {%- else -%}
                COALESCE(updated_data.{{ col_name }}, previous_data.{{ col_name }}) AS {{ col_name }},
            {%- endif -%}
        {%- endif -%}

    {%- endfor -%}

{%- endmacro -%}
