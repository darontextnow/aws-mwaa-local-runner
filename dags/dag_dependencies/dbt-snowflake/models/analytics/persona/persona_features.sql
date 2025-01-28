{{
    config(
        tags=['weekly'],
        materialized='table',
        snowflake_warehouse='PROD_WH_SMALL'
    ) 
}}

-- * File: persona_features_table.sql
-- * Author: Hao Zhang
-- * Purpose:
--   This query generates a unified table with enriched user persona features 
--   for modeling and analysis. The table aggregates data on user devices, 
--   locations, demographics, household income, interests, and messaging behaviors.
--
-- * Step Overview:
--   - Step 1: Enrich user data with ZIP code-based latitude/longitude projections and timezone information.
--   - Step 2: Join device data to include device counts, prices, and usage details.
--   - Step 3: Add household income and demographic details.
--   - Step 4: Include user-reported data like age, gender, and ethnicity.
--   - Step 5: User Trust Score
--   - Step 6: QSR and domain-specific features
--   - Step 7: Email domain features
--   - Step 8: Messaging behavior features

SELECT
    a.user_id_hex,
    
    -- Step 1: Geographic projections and timezone conversion
    COS(RADIANS(zip_latitude)) * COS(RADIANS(zip_longitude)) AS zip_x,
    COS(RADIANS(zip_latitude)) * SIN(RADIANS(zip_longitude)) AS zip_y,
    SIN(RADIANS(zip_latitude)) AS zip_z,
    -- Timezone conversion
    CASE
        WHEN RIGHT(CONVERT_TIMEZONE(timezone, '2024-09-01 00:00:00'), 1) = 'Z' THEN 0
        ELSE TRY_CAST(RIGHT(CONVERT_TIMEZONE(timezone, '2024-09-01 00:00:00'), 5) AS INT)
    END / 100 / 4 + 2 AS timezone,

    -- Step 2: Device counts and usage features
    num_devices_12mo,
    num_phones_12mo,
    num_tablets_12mo,
    num_other_devices_12mo,
    max_concurrent_devices,
    max_device_price_12mo,
    CASE
        WHEN newest_device_age_in_months > 60 THEN 60
        WHEN newest_device_age_in_months < 0 THEN 0
        ELSE newest_device_age_in_months
    END AS newest_device_age_in_months,
    is_ios_user,
    has_lifeline_device,

    -- Step 3: Demographics and household income
    hhi_mean AS zip_hhi_mean,
    hhi_median AS zip_hhi_median,
    latest_homevalue AS zip_home_value,
    total_population AS zip_total_population,
    total_urban_pop / (total_urban_pop + total_rural_pop) AS zip_urban_pop_pct,

    -- Household income
    CASE
        WHEN household_income = 'HOUSEHOLD_INCOME_UNDER_10000' THEN 1
        WHEN household_income IN (
            'HOUSEHOLD_INCOME_UNKNOWN',
            'HOUSEHOLD_INCOME_PREFER_NOT_DISCLOSE'
        ) THEN hhi_10k_less / 100 ELSE 0
    END AS household_income_under_10000,
    CASE
        WHEN household_income = 'HOUSEHOLD_INCOME_FROM_10000_TO_14999' THEN 1
        WHEN household_income IN (
            'HOUSEHOLD_INCOME_UNKNOWN',
            'HOUSEHOLD_INCOME_PREFER_NOT_DISCLOSE'
        ) THEN hhi_10k_15k / 100 ELSE 0
    END AS household_income_from_10000_to_14999,
    CASE
        WHEN household_income = 'HOUSEHOLD_INCOME_FROM_15000_TO_19999' THEN 1
        WHEN household_income IN (
            'HOUSEHOLD_INCOME_UNKNOWN',
            'HOUSEHOLD_INCOME_PREFER_NOT_DISCLOSE'
        ) THEN hhi_15k_25k / 100 ELSE 0
    END AS household_income_from_15000_to_19999,
    CASE
        WHEN household_income = 'HOUSEHOLD_INCOME_FROM_20000_TO_39999' THEN 1
        WHEN household_income IN (
            'HOUSEHOLD_INCOME_UNKNOWN',
            'HOUSEHOLD_INCOME_PREFER_NOT_DISCLOSE'
        ) THEN hhi_25k_35k / 100 ELSE 0
    END AS household_income_from_20000_to_39999,
    CASE
        WHEN household_income = 'HOUSEHOLD_INCOME_FROM_40000_TO_49999' THEN 1
        WHEN household_income IN (
            'HOUSEHOLD_INCOME_UNKNOWN',
            'HOUSEHOLD_INCOME_PREFER_NOT_DISCLOSE'
        ) THEN hhi_35k_50k / 100 ELSE 0
    END AS household_income_from_40000_to_49999,
    CASE
        WHEN household_income = 'HOUSEHOLD_INCOME_FROM_50000_TO_99999' THEN 1
        WHEN household_income IN (
            'HOUSEHOLD_INCOME_UNKNOWN', 
            'HOUSEHOLD_INCOME_PREFER_NOT_DISCLOSE'
        ) THEN (hhi_50k_75k + hhi_75k_100k) / 100 ELSE 0
    END AS household_income_from_50000_to_99999,
    CASE
        WHEN household_income = 'HOUSEHOLD_INCOME_FROM_100000_TO_149999' THEN 1
        WHEN household_income IN (
            'HOUSEHOLD_INCOME_UNKNOWN',
            'HOUSEHOLD_INCOME_PREFER_NOT_DISCLOSE'
        ) THEN hhi_100k_150k / 100 ELSE 0
    END AS household_income_from_100000_to_149999,
    CASE
        WHEN household_income = 'HOUSEHOLD_INCOME_FROM_150000_AND_ABOVE' THEN 1
        WHEN household_income IN (
            'HOUSEHOLD_INCOME_UNKNOWN',
            'HOUSEHOLD_INCOME_PREFER_NOT_DISCLOSE'
        ) THEN (hhi_150k_200k + hhi_200k_more) / 100 ELSE 0
    END AS household_income_from_150000_and_above,

    -- Step 4: Self-reported
    -- Step 4.1: Self-reported demographics
    CASE gender WHEN 'M' THEN 1 WHEN 'F' THEN 0 ELSE NULL END AS gender,
    median_age AS zip_median_age,
    CASE age_range
        WHEN 'UNDER_18' THEN 14
        WHEN 'FROM_18_TO_24' THEN 21
        WHEN 'FROM_25_TO_34' THEN 30
        WHEN 'FROM_35_TO_44' THEN 40
        WHEN 'FROM_45_TO_54' THEN 50
        WHEN 'FROM_55_TO_64' THEN 60
        WHEN 'FROM_65_AND_ABOVE' THEN 70
    END AS age_range,

    CASE WHEN ethnicity = 'aian' THEN 1 WHEN ethnicity IS NULL THEN NULL ELSE 0 END AS ethnicity_aian,
    CASE WHEN ethnicity = 'hispanic' THEN 1 WHEN ethnicity IS NULL THEN NULL ELSE 0 END AS ethnicity_hispanic,
    CASE WHEN ethnicity = 'black' THEN 1 WHEN ethnicity IS NULL THEN NULL ELSE 0 END AS ethnicity_black,
    CASE WHEN ethnicity = 'white' THEN 1 WHEN ethnicity IS NULL THEN NULL ELSE 0 END AS ethnicity_white,
    CASE WHEN ethnicity = 'aapi' THEN 1 WHEN ethnicity IS NULL THEN NULL ELSE 0 END AS ethnicity_aapi,

    -- Step 4.2: Self-reported use case
    use_case_backup,
    use_case_business,
    use_case_buying_or_selling,
    use_case_dating,
    use_case_for_work,
    use_case_job_hunting,
    use_case_long_distance_calling,
    use_case_other,
    use_case_primary,

    -- Step 4.3: Self-reported interests
    CASE WHEN interests_automotive = 0 AND interests_dating = 0 AND interests_entertainment = 0
            AND interests_finance = 0 AND interests_food = 0 AND interests_health_and_fitness = 0
            AND interests_news = 0 AND interests_retail = 0 AND interests_travel = 0 THEN NULL 
        ELSE interests_automotive 
    END AS interests_automotive,
    CASE WHEN interests_automotive = 0 AND interests_dating = 0 AND interests_entertainment = 0
            AND interests_finance = 0 AND interests_food = 0 AND interests_health_and_fitness = 0
            AND interests_news = 0 AND interests_retail = 0 AND interests_travel = 0 THEN NULL
        ELSE interests_dating 
    END AS interests_dating,
    CASE WHEN interests_automotive = 0 AND interests_dating = 0 AND interests_entertainment = 0
            AND interests_finance = 0 AND interests_food = 0 AND interests_health_and_fitness = 0
            AND interests_news = 0 AND interests_retail = 0 AND interests_travel = 0 THEN NULL
        ELSE interests_entertainment
    END AS interests_entertainment,
    CASE WHEN interests_automotive = 0 AND interests_dating = 0 AND interests_entertainment = 0
             AND interests_finance = 0 AND interests_food = 0 AND interests_health_and_fitness = 0
             AND interests_news = 0 AND interests_retail = 0 AND interests_travel = 0
        THEN NULL 
        ELSE interests_finance 
    END AS interests_finance,
    CASE WHEN interests_automotive = 0 AND interests_dating = 0 AND interests_entertainment = 0
            AND interests_finance = 0 AND interests_food = 0 AND interests_health_and_fitness = 0
            AND interests_news = 0 AND interests_retail = 0 AND interests_travel = 0 THEN NULL
        ELSE interests_food 
    END AS interests_food,
    CASE WHEN interests_automotive = 0 AND interests_dating = 0 AND interests_entertainment = 0
            AND interests_finance = 0 AND interests_food = 0 AND interests_health_and_fitness = 0
            AND interests_news = 0 AND interests_retail = 0 AND interests_travel = 0 THEN NULL
        ELSE interests_health_and_fitness 
    END AS interests_health_and_fitness,
    CASE WHEN interests_automotive = 0
            AND interests_dating = 0
            AND interests_entertainment = 0
            AND interests_finance = 0
            AND interests_food = 0
            AND interests_health_and_fitness = 0
            AND interests_news = 0
            AND interests_retail = 0
            AND interests_travel = 0 THEN NULL
        ELSE interests_news 
    END AS interests_news,
    CASE WHEN interests_automotive = 0 AND interests_dating = 0 AND interests_entertainment = 0
            AND interests_finance = 0 AND interests_food = 0 AND interests_health_and_fitness = 0
            AND interests_news = 0 AND interests_retail = 0 AND interests_travel = 0 THEN NULL 
        ELSE interests_retail 
    END AS interests_retail,
    CASE WHEN interests_automotive = 0 AND interests_dating = 0 AND interests_entertainment = 0
            AND interests_finance = 0 AND interests_food = 0 AND interests_health_and_fitness = 0
            AND interests_news = 0 AND interests_retail = 0 AND interests_travel = 0 THEN NULL 
        ELSE interests_travel
    END AS interests_travel,

    -- Step 5: User Trust score
    likelihood_of_disable,

    -- Step 6: QSR and domain-specific features
    qsr_near_zip AS qsr_store_count_near_ip,
    zips_visited AS qsr_zip_visited_ip,
    breakfast_qsr_count AS qsr_breakfast_count_ip,
    lunch_qsr_count AS qsr_lunch_count_ip,
    dinner_qsr_count AS qsr_dinner_count_ip,
    otherhours_qsr_count AS qsr_otherhours_count_ip,

    -- Step 7: Email domain features
    CASE WHEN is_education_domain = 'EDUCATION_DOMAIN' THEN 1 ELSE 0 END AS is_education_domain,

    -- Step 8: Messaging behavior features (quantiles)
    CASE 
        WHEN no_mms_sent <= APPROX_PERCENTILE(no_mms_sent, 0.25) OVER () THEN 1
        WHEN no_mms_sent <= APPROX_PERCENTILE(no_mms_sent, 0.5) OVER () THEN 2
        WHEN no_mms_sent <= APPROX_PERCENTILE(no_mms_sent, 0.75) OVER () THEN 3
        ELSE 4
    END / 3 - 1 / 3 AS outbound_mms_quartile,
    -- the raw counts are not useful, I'm bucketing messaging behaviors into quartiles
    CASE 
        WHEN outbound_messages_weekday_90d <= APPROX_PERCENTILE(outbound_messages_weekday_90d, 0.25) OVER () THEN 1
        WHEN outbound_messages_weekday_90d <= APPROX_PERCENTILE(outbound_messages_weekday_90d, 0.5) OVER () THEN 2
        WHEN outbound_messages_weekday_90d <= APPROX_PERCENTILE(outbound_messages_weekday_90d, 0.75) OVER () THEN 3
        ELSE 4 
    END / 3 - 1 / 3 AS outbound_messages_weekday_90d_quartile,

    CASE
        WHEN outbound_messages_weekend_90d <= APPROX_PERCENTILE(outbound_messages_weekend_90d, 0.25) OVER () THEN 1
        WHEN outbound_messages_weekend_90d <= APPROX_PERCENTILE(outbound_messages_weekend_90d, 0.5) OVER () THEN 2
        WHEN outbound_messages_weekend_90d <= APPROX_PERCENTILE(outbound_messages_weekend_90d, 0.75) OVER () THEN 3
        ELSE 4
    END / 3 - 1 / 3 AS outbound_messages_weekend_90d_quartile,

    CASE
        WHEN outbound_messages_morning_90d <= APPROX_PERCENTILE(outbound_messages_morning_90d, 0.25) OVER () THEN 1
        WHEN outbound_messages_morning_90d <= APPROX_PERCENTILE(outbound_messages_morning_90d, 0.5) OVER () THEN 2
        WHEN outbound_messages_morning_90d <= APPROX_PERCENTILE(outbound_messages_morning_90d, 0.75) OVER () THEN 3
        ELSE 4
    END / 3 - 1 / 3 AS outbound_messages_morning_90d_quartile,

    CASE
        WHEN outbound_messages_afternoon_90d <= APPROX_PERCENTILE(outbound_messages_afternoon_90d, 0.25) OVER () THEN 1
        WHEN outbound_messages_afternoon_90d <= APPROX_PERCENTILE(outbound_messages_afternoon_90d, 0.5) OVER () THEN 2
        WHEN outbound_messages_afternoon_90d <= APPROX_PERCENTILE(outbound_messages_afternoon_90d, 0.75) OVER () THEN 3
        ELSE 4
    END / 3 - 1 / 3 AS outbound_messages_afternoon_90d_quartile,

    CASE
        WHEN outbound_messages_evening_90d <= APPROX_PERCENTILE(outbound_messages_evening_90d, 0.25) OVER () THEN 1
        WHEN outbound_messages_evening_90d <= APPROX_PERCENTILE(outbound_messages_evening_90d, 0.5) OVER () THEN 2
        WHEN outbound_messages_evening_90d <= APPROX_PERCENTILE(outbound_messages_evening_90d, 0.75) OVER () THEN 3
        ELSE 4
    END / 3 - 1 / 3 AS outbound_messages_evening_90d_quartile,

    CASE
        WHEN outbound_messages_other_time_90d <= APPROX_PERCENTILE(outbound_messages_other_time_90d, 0.25) OVER () THEN 1
        WHEN outbound_messages_other_time_90d <= APPROX_PERCENTILE(outbound_messages_other_time_90d, 0.5) OVER () THEN 2
        WHEN outbound_messages_other_time_90d <= APPROX_PERCENTILE(outbound_messages_other_time_90d, 0.75) OVER () THEN 3
        ELSE 4
    END / 3 - 1 / 3 AS outbound_messages_other_times_90d_quartile,

    CASE WHEN inbound_messages_weekday_90d <= APPROX_PERCENTILE(inbound_messages_weekday_90d, 0.25) OVER () THEN 1
        WHEN inbound_messages_weekday_90d <= APPROX_PERCENTILE(inbound_messages_weekday_90d, 0.5) OVER () THEN 2
        WHEN inbound_messages_weekday_90d <= APPROX_PERCENTILE(inbound_messages_weekday_90d, 0.75) OVER () THEN 3
        ELSE 4
    END / 3 - 1 / 3 AS inbound_messages_weekday_90d_quartile,

    CASE
        WHEN inbound_messages_weekend_90d <= APPROX_PERCENTILE(inbound_messages_weekend_90d, 0.25) OVER () THEN 1
        WHEN inbound_messages_weekend_90d <= APPROX_PERCENTILE(inbound_messages_weekend_90d, 0.5) OVER () THEN 2
        WHEN inbound_messages_weekend_90d <= APPROX_PERCENTILE(inbound_messages_weekend_90d, 0.75) OVER () THEN 3
        ELSE 4
    END / 3 - 1 / 3 AS inbound_messages_weekend_90d_quartile,

    CASE
        WHEN inbound_messages_morning_90d <= APPROX_PERCENTILE(inbound_messages_morning_90d, 0.25) OVER () THEN 1
        WHEN inbound_messages_morning_90d <= APPROX_PERCENTILE(inbound_messages_morning_90d, 0.5) OVER () THEN 2
        WHEN inbound_messages_morning_90d <= APPROX_PERCENTILE(inbound_messages_morning_90d, 0.75) OVER () THEN 3
        ELSE 4
    END / 3 - 1 / 3 AS inbound_messages_morning_90d_quartile,

    CASE
        WHEN inbound_messages_afternoon_90d <= APPROX_PERCENTILE(inbound_messages_afternoon_90d, 0.25) OVER () THEN 1
        WHEN inbound_messages_afternoon_90d <= APPROX_PERCENTILE(inbound_messages_afternoon_90d, 0.5) OVER () THEN 2
        WHEN inbound_messages_afternoon_90d <= APPROX_PERCENTILE(inbound_messages_afternoon_90d, 0.75) OVER () THEN 3
        ELSE 4 
    END / 3 - 1 / 3 AS inbound_messages_afternoon_90d_quartile,

     CASE WHEN inbound_messages_evening_90d <= APPROX_PERCENTILE(inbound_messages_evening_90d, 0.25) OVER () THEN 1
        WHEN inbound_messages_evening_90d <= APPROX_PERCENTILE(inbound_messages_evening_90d, 0.5) OVER () THEN 2
        WHEN inbound_messages_evening_90d <= APPROX_PERCENTILE(inbound_messages_evening_90d, 0.75) OVER () THEN 3
        ELSE 4
    END / 3 - 1 / 3 AS inbound_messages_evening_90d_quartile,

    CASE WHEN inbound_messages_other_time_90d <= APPROX_PERCENTILE(inbound_messages_other_time_90d, 0.25) OVER () THEN 1
        WHEN inbound_messages_other_time_90d <= APPROX_PERCENTILE(inbound_messages_other_time_90d, 0.5) OVER () THEN 2
        WHEN inbound_messages_other_time_90d <= APPROX_PERCENTILE(inbound_messages_other_time_90d, 0.75) OVER () THEN 3
    ELSE 4 END / 3 - 1 / 3 AS inbound_messages_other_times_90d_quartile,

FROM {{ ref('persona_universe') }} AS a
LEFT JOIN {{ ref('persona_device_count') }} AS b ON (a.username = b.username)
LEFT JOIN {{ ref('persona_general_location') }} AS c ON (a.username = c.username)
LEFT JOIN {{ ref('persona_self_reported') }} AS d ON (a.username = d.username and a.user_id_hex = d.user_id_hex)
LEFT JOIN {{ ref('persona_census') }} AS e ON (a.username = e.username)
LEFT JOIN {{ ref('persona_message_p1') }} AS f ON (a.user_id_hex = f.user_id_hex)
LEFT JOIN {{ ref('persona_message_p2') }} AS g ON (a.username = g.username and a.user_id_hex = g.user_id_hex)
LEFT JOIN {{ ref('persona_email_domain') }} AS h ON (a.username = h.username)
LEFT JOIN {{ ref('persona_trust_score') }} AS i ON (a.username = i.username)
LEFT JOIN {{ ref('persona_device_price') }} AS j ON (a.user_id_hex = j.user_id_hex)
LEFT JOIN {{ ref('persona_qsr_store_cnt') }} AS k ON (a.username = k.username)
