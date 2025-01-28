{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='user_set_id'
    )
}}

/*

The script is designed to be run incrementally and at any possible time intervals. It collects
all rfm_segment states for every user from the last update and only cares about the last two:
the latest being the current segment, the penultimate one previous segment. The last part of the
query merits some extra explanation on the incremental part:
  1. We only update the table when one of the two conditions is fulfilled. First is when the
     newly collected current_segment differs from the existing current_segment. Second is when
     the newly collected previous_segment differs from the existing previous_segment AND when
     the new one is not null.
  2. Updating user based on the first condition is a no-brainer. However, note the NVL call on
     previous_segment: it is very possible for the segment to change and stick, hence causing
     the incremental query to not pick up any new previous_segment in its time frame. When we
     update without the NVL call, we will put in an empty previous_segment instead of making the
     existing current_segment the new previous_segment. This NVL call is very important.
  3. Updating user based on the second condition is also pretty self-explanatory, with one caveat.
     Suppose we collected no new previous_segment in the incremental time frame, and his/her new
     current_segment is the same as the existing current_segment, this means that there was no
     state change. We should not be updating at all lest we overwrite any non-null existing
     previous_segment. Therefore, when the first condition fails and we are forced to evaluate
     the second condition for update eligibility, we need to filter out all of the incremental
     updates that don't have any previous_segment.

 */

WITH rfm_segmentation_log AS (
    SELECT
        user_set_id,
        CASE WHEN bad_sets.set_uuid IS NOT NULL THEN 'bad_sets' ELSE rfm_segment END AS rfm_segment,
        MAX(date_utc) AS date_utc
    FROM {{ ref('user_segment_user_set_daily_summary') }} AS seg
    LEFT JOIN {{ ref('bad_sets') }} as bad_sets ON seg.user_set_id = bad_sets.set_uuid

    {% if is_incremental() %}
        WHERE (date_utc >= (SELECT MAX(updated_at)::DATE - INTERVAL '7 days' FROM {{ this }}))
    {% endif %}

    GROUP BY 1, 2
),

all_segments AS (
    SELECT
        user_set_id,
        LISTAGG(rfm_segment, '||') WITHIN GROUP (ORDER BY date_utc DESC) AS aggregate_segments,
        MAX(date_utc) AS updated_at
    FROM rfm_segmentation_log
    GROUP BY 1
),

new_data AS (
    SELECT
        user_set_id,
        NULLIF(SPLIT_PART(aggregate_segments, '||', 2), '') AS previous_segment,
        NULLIF(SPLIT_PART(aggregate_segments, '||', 1), '') AS current_segment,
        updated_at
    FROM all_segments
)

{% if is_incremental() %}

SELECT
    user_set_id,
    NVL(n.previous_segment, o.previous_segment) AS previous_segment,
    n.current_segment,
    n.updated_at
FROM new_data n
LEFT JOIN {{ this }} o USING (user_set_id)
WHERE
    (n.current_segment <> NVL(o.current_segment, ''))
    OR ((n.current_segment = NVL(o.current_segment, ''))
        AND (NVL(n.previous_segment, '') <> NVL(o.previous_segment, '')))

{% else %}

SELECT
    user_set_id,
    previous_segment,
    current_segment,
    updated_at
FROM new_data

{% endif %}
