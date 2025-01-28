{% macro normalized_client_type(field_name='client_type') -%}
    CASE
       WHEN {{ field_name }} ILIKE '%TN_ANDROID%' THEN 'TN_ANDROID'
       WHEN {{ field_name }} ILIKE '%TN_IOS%' THEN 'TN_IOS_FREE'
       WHEN {{ field_name }} ILIKE '%TN_WEB%' OR {{ field_name }} ILIKE '%TN_ELECTRON%' THEN 'TN_WEB'
       WHEN {{ field_name }} ILIKE '%2L_ANDROID%' THEN '2L_ANDROID'
       ELSE 'OTHER'
    END
{%- endmacro %}

{% macro pp_normalized_client_type() -%}
    CASE
        WHEN ("client_details.client_data.client_platform" = 'WEB') THEN 'TN_WEB'
        WHEN ("client_details.client_data.client_platform" = 'IOS') THEN 'TN_IOS_FREE'
        WHEN ("client_details.client_data.client_platform" = 'CP_ANDROID')
            AND ("client_details.client_data.brand" = 'BRAND_2NDLINE') THEN '2L_ANDROID'
        WHEN ("client_details.client_data.client_platform" = 'CP_ANDROID')
            AND ("client_details.client_data.brand" = 'BRAND_TEXTNOW') THEN 'TN_ANDROID'
        ELSE 'OTHER'
    END
{%- endmacro %}
