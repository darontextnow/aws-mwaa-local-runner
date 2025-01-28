
/* FIXME:
    PORTOUTS are not currently in phone_number_logs, therefore the corresponding
    records will have `unassigned_at` incorrectly reported as NULL
*/


{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='assigment_id'
    )
}}

WITH current_chunk AS (
    SELECT
        username,
        phone_number,
        event,
        created_at,
        LEAD(event) OVER (PARTITION BY username, phone_number ORDER BY created_at) AS next_event,
        LEAD(created_at) OVER (PARTITION BY username, phone_number ORDER BY created_at) AS next_event_ts,
        LAG(event) OVER (PARTITION BY username, phone_number ORDER BY created_at) AS prev_event
    FROM {{ ref('phone_number_assignments') }}

{% if is_incremental() %}
    WHERE (created_at >= {{ var('data_interval_start') }}::DATE - INTERVAL '7 DAYS')
{% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['username', 'phone_number', 'created_at']) }} AS assigment_id,
    username,
    phone_number,
    created_at as assigned_at,
    next_event_ts AS unassigned_at
FROM current_chunk
WHERE
    (event IN ('ASSIGN', 'PORTIN'))
    AND (NVL(next_event, 'UNASSIGN') IN ('UNASSIGN', 'PORTOUT'))

{% if is_incremental() %}

    -- When running in incremental model, we may need to update existing rows in the table with
    -- null unassigned_at, when the following conditions are both true
    --  1. The first record in current_chunk is an unassign
    --  2. There is a corresponding entry in existing table with the same user_id/phone_number with unassigned_at = NULL

    UNION ALL SELECT
        {{ dbt_utils.generate_surrogate_key(['username', 'phone_number', 'assigned_at']) }} AS assigment_id,
        username,
        phone_number,
        carry_over.assigned_at,
        current_chunk.created_at AS unassigned_at
    FROM current_chunk
    JOIN {{ this }} AS carry_over USING (username, phone_number)
    WHERE
        (event IN ('UNASSIGN', 'PORTOUT'))
        AND (prev_event IS NULL)
        AND (carry_over.unassigned_at IS NULL)
        AND (carry_over.assigned_at < current_chunk.created_at)
{% endif %}
