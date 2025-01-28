{# This model is intended for manual runs for LTV model refresh hence enabled is set to false #}
{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE',
        enabled=false
    )
}}

{% set query %}

    -- result table for storing attributed DAUs which will be populated in the anonymous-block below
    CREATE OR REPLACE TEMPORARY TABLE attribution_results (
        client_type STRING,
        user_set_id STRING,
        adjust_id STRING,
        install_date DATE,
        date_utc DATE,
        dau FLOAT
    );


    -- calculating next install date etc, to be used in the loop below
    CREATE OR REPLACE TEMPORARY TABLE installs_strata AS
    SELECT
        client_type,
        adjust_id,
        install_date,
        user_set_id,
        install_number,
        strata,
        LEAD(install_date) OVER (PARTITION BY client_type, user_set_id ORDER BY install_number) AS next_install_date
    FROM {{ ref('ltv_numbered_installs') }}
    JOIN {{ ref('ltv_installs_strata') }} USING (client_type, adjust_id, install_number)
    WHERE install_date <= :end_date;


    EXECUTE IMMEDIATE $$
    DECLARE
        start_date DATE;
        end_date DATE;
    BEGIN

        LET c1 CURSOR FOR SELECT MAX(date_utc) - INTERVAL '30 days' FROM {{ this }};
        OPEN c1;
        FETCH c1 INTO start_date;
        CLOSE c1;

        end_date := {{ var('current_date') }} - INTERVAL '30 DAYS';

        -- working table
        CREATE OR REPLACE TEMPORARY TABLE temp_remaining_dau AS
        SELECT
            ad.client_type,
            ad.user_set_id,
            ad.date_utc,
            ad.dau
        FROM {{ ref('dau_user_set_active_days') }} ad
        WHERE
            ad.dau > 0
            AND ad.date_utc >= :start_date
            AND ad.date_utc < :end_date;

        -- loop through install numbers
        -- We start from new user installs (install_number = 1) and compile their DAU from dau_user_set_active_days.
        -- Some fraction of their DAU will be attributed to these new installs.
        -- Any leftover (unattributed_dau) will be available for the next round of split.
        FOR install_number IN 1 TO 25 DO

            CREATE OR REPLACE TEMPORARY TABLE split_result AS
            WITH installs AS (
                SELECT
                    *
                FROM installs_strata
                WHERE
                    install_number = :install_number
                    AND install_date < :end_date
            ),
            strata_size AS (
                SELECT
                    strata,
                    COUNT(1) s
                FROM installs
                GROUP BY strata
            )

            SELECT
                i.*,
                dates.date_utc,
                dau,
                -- calcations at partition level
                SUM(dau) OVER (PARTITION BY strata, dates.date_utc) AS total_dau,
                SUM(CASE WHEN ad.date_utc < coalesce(i.next_install_date, '9999-12-31'::DATE) THEN dau ELSE 0 END) OVER (PARTITION BY strata, dates.date_utc) AS blue_dau,
                SUM(CASE WHEN dates.date_utc < coalesce(i.next_install_date, '9999-12-31'::DATE) THEN 1 ELSE 0 END) OVER (PARTITION BY strata, dates.date_utc) AS strata_blue_size,
                ZEROIFNULL(blue_dau / NULLIFZERO(strata_blue_size)) AS ret_without_reinstall,
                strata_size.s AS strata_size,
                LEAST(ret_without_reinstall * strata_size.s, total_dau) AS dau_for_current_install_number,
                total_dau - dau_for_current_install_number AS dau_for_next_install_number,
                dau_for_next_install_number / NULLIFZERO(total_dau - blue_dau) AS factor,
                -- attribute a portion of future active days to the original install
                CASE WHEN dates.date_utc >= next_install_date THEN dau * (1 - factor) ELSE dau END AS attributed_dau,
                -- any leftover is handled in the next iteration
                dau - attributed_dau AS unattributed_dau
            FROM installs i
            JOIN strata_size USING (strata)
            JOIN {{ source('support', 'dates') }} ON
                dates.date_utc >= :start_date AND
                dates.date_utc < :end_date AND
                DATEDIFF(day, i.install_date, dates.date_utc) BETWEEN 0 AND 730
            LEFT JOIN temp_remaining_dau ad ON  -- must use left JOIN here, otherwise strata_blue_size will be wrong
                i.client_type = ad.client_type AND
                i.user_set_id = ad.user_set_id AND
                ad.date_utc = dates.date_utc AND
                ad.dau > 0 AND
                DATEDIFF(day, i.install_date, ad.date_utc) BETWEEN 0 AND 730;

            MERGE INTO temp_remaining_dau r
            USING split_result s
            ON  r.client_type = s.client_type AND
                r.user_set_id = s.user_set_id AND
                r.date_utc = s.date_utc
            WHEN MATCHED AND s.unattributed_dau > 0 THEN UPDATE SET r.dau = s.unattributed_dau
            WHEN MATCHED AND ZEROIFNULL(s.unattributed_dau) = 0 THEN DELETE;

            INSERT INTO attribution_results
            SELECT
                client_type,
                user_set_id,
                adjust_id,
                install_date,
                date_utc,
                attributed_dau
            FROM split_result s
            WHERE s.attributed_dau > 0;

        END FOR; -- install_number

    END;
    $$

{% endset %}

{% if execute and flags.WHICH in ["run", "build"] %}
    {% do run_query(query) %}
{% endif %}

SELECT * FROM attribution_results