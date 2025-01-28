{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}
/*
Base user daily messages table is an incremental table that holds daily user level messages
*/

  select *
    from {{ref('base_user_messages')}}
    {% if is_incremental() and target.name != 'dev'%}
        where
        date_utc >= (select max(sub.date_utc) - interval '1 days' FROM {{ this }} sub)
    {% endif %}
    {% if target.name == 'dev' %}
        where
        date_utc >= CURRENT_DATE - INTERVAL '1 week'
    {% endif %}

