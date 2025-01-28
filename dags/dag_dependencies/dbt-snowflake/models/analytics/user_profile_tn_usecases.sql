{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='USER_ID_HEX'
    )
}}
with final_tbl as
 (

        SELECT USER_ID_HEX,
               CREATED_AT AS UPDATED_AT,
               "client_details.client_data.user_data.username" AS USERNAME,
                "payload.tn_use_cases" as USE_CASE,
          
               LAST_VALUE(NULLIF(TRIM("payload.other_tn_use_case_text"), '') IGNORE NULLS) OVER
                   (PARTITION BY USER_ID_HEX ORDER BY CREATED_AT
                    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS OTHER_USE_CASES

            ,ROW_NUMBER() OVER (PARTITION BY USER_ID_HEX,"payload.tn_use_cases" ORDER BY CREATED_AT DESC) as rn
        FROM {{ source('party_planner_realtime', 'user_updates') }}
        WHERE CREATED_AT >= (SELECT MAX(UPDATED_AT) - INTERVAL '7 days' FROM {{ this }})
         and len("client_details.client_data.user_data.username")>0
    
    )
 


SELECT distinct final_tbl.USER_ID_HEX,
       final_tbl.UPDATED_AT,
       final_tbl.USERNAME,
{% if is_incremental() %}
       COALESCE(final_tbl.USE_CASE, cur_tbl.USE_CASE)                 AS USE_CASE,
       COALESCE(final_tbl.OTHER_USE_CASES, cur_tbl.OTHER_USE_CASES)   AS OTHER_USE_CASES
{% else %}
       final_tbl.USE_CASE,
       final_tbl.OTHER_USE_CASES
{% endif %}
FROM final_tbl
{% if is_incremental() %}

LEFT JOIN {{ this }} AS cur_tbl ON final_tbl.USER_ID_HEX = cur_tbl.USER_ID_HEX

where (final_tbl.UPDATED_AT >cur_tbl.UPDATED_AT or cur_tbl.UPDATED_AT is NULL)
and  final_tbl.rn=1

{% else %}
where final_tbl.rn=1
{% endif %}


