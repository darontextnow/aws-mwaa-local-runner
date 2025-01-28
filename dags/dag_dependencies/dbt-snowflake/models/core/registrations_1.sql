{#
    Run this model every 4 hours
    This file contains 4 steps
        1. Set up staging tables
        2. Update attribution data
        3. UPDATE data from stage for existing users in core.registrations
        4. INSERT new users from stage into core.registrations
    NOTE: This logic MUST ensure no duplicate usernames are added to core.registrations table.
#}

{{
    config(
        tags=['4h'],
        alias='registrations',
        materialized='incremental',
        incremental_strategy='append',
        unique_key='username',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

{% set pre_exec_query %}
    USE WAREHOUSE PROD_WH_MEDIUM;  --snowflake_warehouse from config above doesn't apply to queries ran by macro.
    -- Step 1: Create staging tables
    CREATE TEMP TABLE to_process AS
    SELECT
        created_at,
        http_response_status,
        error_code,
        fr.username,
        fr.client_type,
        client_version,
        country_code,
        city_name,
        latitude,
        longitude,
        client_ip,
        phone_num,
        email,
        fbid,
        gender,
        dob,
        first_name,
        last_name,
        language,
        timezone,
        idfa,
        idfv,
        gps_adid,
        android_id,
        mac_address,
        adid,
        tracker,
        tracker_name,
        is_organic,
        device_type,
        installed_at AS device_installed_at,
        utm_source,
        utm_medium,
        utm_term,
        utm_content,
        utm_campaign,
        integrity_sess_attested,
        integrity_sess_exists,
        integrity_sess_valid,
        google_signin,
        provider
    FROM (
        SELECT
            created_at,
            http_response_status,
            error_code,
            username,
            client_type,
            client_version,
            country_code,
            city_name,
            latitude,
            longitude,
            client_ip,
            phone_num,
            email,
            fbid,
            gender,
            dob,
            first_name,
            last_name,
            language,
            timezone,
            COALESCE(idfa, '') AS idfa,
            COALESCE(idfv, '') AS idfv,
            COALESCE(gaid, '') AS gps_adid,
            COALESCE(android_id, '') AS android_id,
            mac_address,
            utm_params:utm_source::VARCHAR AS utm_source,
            utm_params:utm_medium::VARCHAR AS utm_medium,
            utm_params:utm_term::VARCHAR AS utm_term,
            utm_params:utm_content::VARCHAR AS utm_content,
            utm_params:utm_campaign::VARCHAR AS utm_campaign,
            integrity_sess_attested,
            integrity_sess_exists,
            integrity_sess_valid,
            google_signin,
            provider
        FROM {{ source('firehose', 'registrations') }}
        WHERE
            created_at >= {{ var('data_interval_start') }}::TIMESTAMP - INTERVAL '6 hours'
            AND created_at < {{ var('data_interval_end') }}::TIMESTAMP
            AND http_response_status <= 299
            AND COALESCE(CONTAINS(TAGS, 'nonprod'), FALSE) = FALSE
            AND created_at IS NOT NULL
            AND TRIM(NVL(username, '')) <> ''
            AND TRIM(NVL(client_type, '')) <> ''
        QUALIFY ROW_NUMBER() OVER (PARTITION BY username ORDER BY created_at) = 1
    ) fr
    LEFT JOIN (
        SELECT DISTINCT username, adid, installed_at, client_type,tracker, tracker_name, is_organic, device_type
        FROM {{ source('adjust', 'registrations') }}
        JOIN {{ source('adjust', 'apps') }} USING (app_name)
        WHERE
            TRIM(NVL(username, '')) <> ''
            AND created_at >= {{ var('data_interval_start') }}::TIMESTAMP - INTERVAL '6 hours'
            AND created_at <  {{ var('data_interval_end') }}::TIMESTAMP
    ) ar ON
        (fr.username = ar.username)
        AND (fr.client_type = ar.client_type)
        AND (fr.created_at >= ar.installed_at - INTERVAL '2 hours');

    -- Step 2: update attribution data.
    -- As a buffer, registration timestamp can be up to one day
    -- earlier than attribution timestamp.
    -- Priority for matching:
    -- * attribution from adjust
    -- * last reattribution > original attribution
    -- * adid > device ids
    -- * idfv > idfa
    -- * android_id > google advertising id

    -- Step 2a: reattribution
    CREATE TEMP TABLE core.staging AS
    SELECT
        p.*,
        r.created_at AS reattributed_at,
        r.created_at::DATE AS cohort_utc
    FROM to_process p
    JOIN {{ source('adjust', 'apps') }} a ON (p.client_type = a.client_type)
    JOIN {{ ref('reattribution_with_pi') }} r ON (p.adid = r.adid) AND (a.app_name = r.app_name)
    WHERE (p.created_at >= r.created_at - INTERVAL '2 hours')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.adid ORDER BY r.created_at DESC) = 1;

    -- Note: MUST DO SEPARATE INSERTS here to ensure no duplicates are inserted
    --insert where idfv = idfv
    INSERT INTO core.staging
    SELECT
        p.created_at, p.http_response_status, p.error_code, p.username, p.client_type, p.client_version,
        p.country_code, p.city_name, p.latitude, p.longitude, p.client_ip, p.phone_num, p.email, p.fbid,
        p.gender, p.dob, p.first_name, p.last_name, p.language, p.timezone,
        p.idfa, p.idfv, p.gps_adid, p.android_id, p.mac_address,
        r.adid, r.tracker, r.tracker_name, r.is_organic, r.device_type, r.installed_at,
        utm_source, utm_medium, utm_term, utm_content, utm_campaign,
        integrity_sess_attested, integrity_sess_exists, integrity_sess_valid, google_signin, provider,
        r.created_at AS reattributed_at,
        r.created_at::DATE AS cohort_utc
    FROM to_process p
    JOIN {{ source('adjust', 'apps') }} a ON (p.client_type = a.client_type)
    JOIN {{ ref('reattribution_with_pi') }} r ON (a.app_name = r.app_name) AND (p.idfv = r.idfv)
    WHERE
        (p.created_at >= r.created_at - INTERVAL '2 hours')
        AND (p.idfv <> '')
        AND (username NOT IN (SELECT username FROM core.staging))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.adid ORDER BY  r.created_at DESC) = 1;

    --this time insert where idfa = idfa
    INSERT INTO core.staging
    SELECT
        p.created_at, p.http_response_status, p.error_code, p.username, p.client_type, p.client_version,
        p.country_code, p.city_name, p.latitude, p.longitude, p.client_ip, p.phone_num, p.email, p.fbid,
        p.gender, p.dob, p.first_name, p.last_name, p.language, p.timezone,
        p.idfa, p.idfv, p.gps_adid, p.android_id, p.mac_address,
        r.adid, r.tracker, r.tracker_name, r.is_organic, r.device_type, r.installed_at,
        utm_source, utm_medium, utm_term, utm_content, utm_campaign,
        integrity_sess_attested, integrity_sess_exists, integrity_sess_valid, google_signin, provider,
        r.created_at AS reattributed_at,
        r.created_at::DATE AS cohort_utc
    FROM to_process p
    JOIN {{ source('adjust', 'apps') }} a ON (p.client_type = a.client_type)
    JOIN {{ ref('reattribution_with_pi') }} r ON (a.app_name = r.app_name) AND (p.idfa = r.idfa)
    WHERE
        (p.created_at >= r.created_at - INTERVAL '2 hours')
        AND (p.idfa <> '')
        AND (username NOT IN (SELECT username FROM core.staging))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.adid ORDER BY  r.created_at DESC) = 1;

    --this time insert where android_id = android_id
    INSERT INTO core.staging
    SELECT
        p.created_at, p.http_response_status, p.error_code, p.username, p.client_type, p.client_version,
        p.country_code, p.city_name, p.latitude, p.longitude, p.client_ip, p.phone_num, p.email, p.fbid,
        p.gender, p.dob, p.first_name, p.last_name, p.language, p.timezone,
        p.idfa, p.idfv, p.gps_adid, p.android_id,
        p.mac_address, r.adid, r.tracker, r.tracker_name, r.is_organic, r.device_type, r.installed_at,
        utm_source, utm_medium, utm_term, utm_content, utm_campaign,
        integrity_sess_attested, integrity_sess_exists, integrity_sess_valid, google_signin, provider,
        r.created_at AS reattributed_at,
        r.created_at::DATE AS cohort_utc
    FROM to_process p
    JOIN {{ source('adjust', 'apps') }} a ON (p.client_type = a.client_type)
    JOIN {{ ref('reattribution_with_pi') }} r ON (a.app_name = r.app_name) AND (p.android_id = r.android_id)
    WHERE
        (p.created_at >= r.created_at - INTERVAL '2 hours')
        AND (p.android_id <> '')
        AND (username NOT IN (SELECT username FROM core.staging))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.adid ORDER BY  r.created_at DESC) = 1;

    --this time insert where gps_adid = gps_adid
    INSERT INTO core.staging
    SELECT
        p.created_at, p.http_response_status, p.error_code, p.username, p.client_type, p.client_version,
        p.country_code, p.city_name, p.latitude, p.longitude, p.client_ip, p.phone_num, p.email, p.fbid,
        p.gender, p.dob, p.first_name, p.last_name, p.language, p.timezone,
        p.idfa, p.idfv, p.gps_adid, p.android_id,
        p.mac_address, r.adid, r.tracker, r.tracker_name, r.is_organic, r.device_type, r.installed_at,
        utm_source, utm_medium, utm_term, utm_content, utm_campaign,
        integrity_sess_attested, integrity_sess_exists, integrity_sess_valid, google_signin, provider,
        r.created_at AS reattributed_at,
        r.created_at::DATE AS cohort_utc
    FROM to_process p
    JOIN {{ source('adjust', 'apps') }} a ON (p.client_type = a.client_type)
    JOIN {{ ref('reattribution_with_pi') }} r ON (a.app_name = r.app_name) AND (p.gps_adid = r.gps_adid)
    WHERE
        (p.created_at >= r.created_at - INTERVAL '2 hours')
        AND (p.gps_adid <> '')
        AND (username NOT IN (SELECT username FROM core.staging))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.adid ORDER BY  r.created_at DESC) = 1;

    -- Step 2b: installs
    INSERT INTO core.staging
    SELECT
        p.*,
        NULL::TIMESTAMP AS reattributed_at,
        i.installed_at::DATE AS cohort_utc
    FROM to_process p
    JOIN {{ source('adjust', 'apps') }} a ON (p.client_type = a.client_type)
    JOIN {{ ref('installs_with_pi') }} i ON (p.adid = i.adid) AND (a.app_name = i.app_name)
    WHERE
        (p.created_at >= i.installed_at - INTERVAL '1 DAY')
        AND (username NOT IN (SELECT username FROM core.staging));

    INSERT INTO core.staging
    SELECT
        p.created_at, p.http_response_status, p.error_code, p.username, p.client_type, p.client_version,
        p.country_code, p.city_name, p.latitude, p.longitude, p.client_ip, p.phone_num, p.email, p.fbid,
        p.gender, p.dob, p.first_name, p.last_name, p.language, p.timezone,
        p.idfa, p.idfv, p.gps_adid, p.android_id,
        p.mac_address, i.adid, i.tracker, i.tracker_name, i.is_organic, i.device_type, i.installed_at,
        utm_source, utm_medium, utm_term, utm_content, utm_campaign,
        integrity_sess_attested, integrity_sess_exists, integrity_sess_valid, google_signin, provider,
        NULL::TIMESTAMP AS reattributed_at,
        i.installed_at::DATE AS cohort_utc
    FROM to_process p
    JOIN {{ source('adjust', 'apps') }} a ON (p.client_type = a.client_type)
    JOIN {{ ref('installs_with_pi') }} i ON (a.app_name = i.app_name) AND (p.idfv = i.idfv)
    WHERE
        (p.created_at >= i.installed_at - INTERVAL '1 DAY')
        AND (p.idfv <> '')
        AND (username NOT IN (SELECT username FROM core.staging));

    INSERT INTO core.staging
    SELECT
        p.created_at, p.http_response_status, p.error_code, p.username, p.client_type, p.client_version,
        p.country_code, p.city_name, p.latitude, p.longitude, p.client_ip, p.phone_num, p.email, p.fbid,
        p.gender, p.dob, p.first_name, p.last_name, p.language, p.timezone,
        p.idfa, p.idfv, p.gps_adid, p.android_id,
        p.mac_address, i.adid, i.tracker, i.tracker_name, i.is_organic, i.device_type, i.installed_at,
        utm_source, utm_medium, utm_term, utm_content, utm_campaign,
        integrity_sess_attested, integrity_sess_exists, integrity_sess_valid, google_signin, provider,
        NULL::TIMESTAMP AS reattributed_at,
        i.installed_at::DATE AS cohort_utc
    FROM to_process p
    JOIN {{ source('adjust', 'apps') }} a ON (p.client_type = a.client_type)
    JOIN {{ ref('installs_with_pi') }} i ON (a.app_name = i.app_name) AND (p.idfa = i.idfa)
    WHERE
        (p.created_at >= i.installed_at - INTERVAL '1 DAY')
        AND (p.idfa <> '')
        AND (username NOT IN (SELECT username FROM core.staging));

    INSERT INTO core.staging
    SELECT
        p.created_at, p.http_response_status, p.error_code, p.username, p.client_type, p.client_version,
        p.country_code, p.city_name, p.latitude, p.longitude, p.client_ip, p.phone_num, p.email, p.fbid,
        p.gender, p.dob, p.first_name, p.last_name, p.language, p.timezone,
        p.idfa, p.idfv, p.gps_adid, p.android_id,
        p.mac_address, i.adid, i.tracker, i.tracker_name, i.is_organic, i.device_type, i.installed_at,
        utm_source, utm_medium, utm_term, utm_content, utm_campaign,
        integrity_sess_attested, integrity_sess_exists, integrity_sess_valid, google_signin, provider,
        NULL::TIMESTAMP AS reattributed_at,
        i.installed_at::DATE AS cohort_utc
    FROM to_process p
    JOIN {{ source('adjust', 'apps') }} a ON (p.client_type = a.client_type)
    JOIN {{ ref('installs_with_pi') }} i ON (a.app_name = i.app_name) AND (p.android_id = i.android_id)
    WHERE
        (p.created_at >= i.installed_at - INTERVAL '1 DAY')
        AND (p.android_id <> '')
        AND (username NOT IN (SELECT username FROM core.staging));

    INSERT INTO core.staging
    SELECT
        p.created_at, p.http_response_status, p.error_code, p.username, p.client_type, p.client_version,
        p.country_code, p.city_name, p.latitude, p.longitude, p.client_ip, p.phone_num, p.email, p.fbid,
        p.gender, p.dob, p.first_name, p.last_name, p.language, p.timezone,
        p.idfa, p.idfv, p.gps_adid, p.android_id,
        p.mac_address, i.adid, i.tracker, i.tracker_name, i.is_organic, i.device_type, i.installed_at,
        utm_source, utm_medium, utm_term, utm_content, utm_campaign,
        integrity_sess_attested, integrity_sess_exists, integrity_sess_valid, google_signin, provider,
        NULL::TIMESTAMP AS reattributed_at,
        i.installed_at::DATE AS cohort_utc
    FROM to_process p
    JOIN {{ source('adjust', 'apps') }} a ON (p.client_type = a.client_type)
    JOIN {{ ref('installs_with_pi') }} i ON (a.app_name = i.app_name) AND (p.gps_adid = i.gps_adid)
    WHERE
        (p.created_at >= i.installed_at - INTERVAL '1 DAY')
        AND (p.gps_adid <> '')
        AND (username NOT IN (SELECT username FROM core.staging));

    -- Step 2c: other platforms
    INSERT INTO core.staging
    SELECT
        created_at, http_response_status, error_code, username, client_type, client_version,
        country_code, city_name, latitude, longitude, client_ip, phone_num, email, fbid,
        gender, dob, first_name, last_name, language, timezone,
        idfa, idfv, gps_adid, android_id, mac_address, adid,
        UPPER(LEFT(MD5(
           utm_source
           || COALESCE(utm_medium, '')
           || COALESCE(utm_term, '')
           || COALESCE(utm_content, '')
           || COALESCE(utm_campaign, '')
        ), 6)) AS tracker,
        tracker_name,
        COALESCE(is_organic, utm_source IS NULL) AS is_organic,
        device_type, device_installed_at,
        utm_source, utm_medium, utm_term, utm_content, utm_campaign,
        integrity_sess_attested, integrity_sess_exists, integrity_sess_valid, google_signin, provider,
        NULL::TIMESTAMP AS reattributed_at,
        CASE WHEN client_type NOT ILIKE '%ANDROID%'
            AND client_type NOT ILIKE '%IOS%'
            AND client_type NOT ILIKE '%AMAZON%'
            THEN created_at::DATE
        END AS cohort_utc
    FROM to_process
    WHERE (username NOT IN (SELECT username FROM core.staging));

    -- Step 3: update old rows and insert new rows

    -- Update: set missing attribution data in the destination table
    -- or in case of reattribution, update only when the reattribution timestamp is
    -- closer than what we already have in the destination table
    UPDATE {{ this }}
    SET adid = s.adid,
        tracker = s.tracker,
        tracker_name = s.tracker_name,
        is_organic = s.is_organic,
        device_type = s.device_type,
        device_installed_at = s.device_installed_at,
        reattributed_at = s.reattributed_at,
        cohort_utc = s.cohort_utc
    FROM core.staging s
    WHERE
        (registrations.username = s.username)
        AND (
          (registrations.adid IS NULL AND s.adid IS NOT NULL) OR
          (registrations.is_organic AND NOT s.is_organic) OR
          (s.reattributed_at > COALESCE(registrations.reattributed_at, registrations.device_installed_at))
        )
    ;
{% endset %}

{% if execute and flags.WHICH in ["run", "build"] %}
    {% do run_query(pre_exec_query) %}
{% endif %}

SELECT
    created_at, http_response_status, error_code, username, client_type, client_version,
    country_code, city_name,
    latitude::DECIMAL(11,9) AS latitude,
    longitude::DECIMAL(12,9) AS longitude,
    idfa, idfv,
    gps_adid AS gaid,
    android_id,
    mac_address,
    NULL::VARCHAR(32) AS mac_md5,
	NULL::VARCHAR(200) AS win_naid,
	NULL::VARCHAR(200) AS win_adid,
    client_ip, phone_num, email, fbid, gender, dob, first_name, last_name, language, timezone, adid,
    NULL AS click_time,
    tracker, tracker_name, is_organic, device_type, device_installed_at, reattributed_at, cohort_utc,
    utm_source, utm_medium, utm_term, utm_content, utm_campaign,
    integrity_sess_attested, integrity_sess_exists, integrity_sess_valid,
    NULL::VARCHAR(16) AS delayed_reg_state,
    google_signin,
    provider
FROM core.staging
WHERE username NOT IN (SELECT username FROM {{ this }})  -- no duplicate usernames!!
QUALIFY ROW_NUMBER() OVER (PARTITION BY username ORDER BY created_at) = 1
