{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}


WITH deduplicated_installs AS (
    SELECT
        client_type,
        user_set_id,
        adjust_id,
        installed_at,
        new_user_install,
        old_user_install,
        country_code,
        is_organic
    FROM {{ ref('growth_installs_dau_generation') }}
    JOIN {{ ref('installs') }} AS i USING (client_type, adjust_id, installed_at)
    WHERE (user_set_id NOT IN (SELECT set_uuid FROM {{ ref('bad_sets') }}))

    {% if is_incremental() %}
        AND (i.installed_at >= {{ var('ds') }}::DATE - INTERVAL '30 DAYS')
    {% endif %}

    QUALIFY ROW_NUMBER() OVER (PARTITION BY client_type, adjust_id ORDER BY installed_at) = 1
)

SELECT
    *,
    installed_at::date as date_utc,
    CASE WHEN country_code IN ('US', 'CA') THEN country_code ELSE 'INTL' END AS geo,
    {% if is_incremental() %} zeroifnull(prev_install_number) + {% endif %} row_number() OVER (PARTITION BY user_set_id, client_type ORDER BY installed_at) AS install_number,
    lead(adjust_id) OVER (PARTITION BY client_type, user_set_id ORDER BY installed_at) AS next_adjust_id,
    lead(installed_at) OVER (PARTITION BY client_type, user_set_id ORDER BY installed_at) AS next_installed_at

FROM deduplicated_installs AS n

{% if is_incremental() %}

LEFT JOIN (
    SELECT
        client_type,
        user_set_id,
        MAX(install_number) AS prev_install_number
    FROM {{ this }}
    WHERE (installed_at >= {{ var('ds') }}::DATE - INTERVAL '30 DAYS')
    GROUP BY client_type, user_set_id

) AS existing USING (client_type, user_set_id)

{% endif %}
