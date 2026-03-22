{{ config(
    materialized='incremental',
    unique_key=['date'],
    on_schema_change='fail'
) }}

SELECT
    date,
    nouveaux_abonnes,
    CURRENT_TIMESTAMP() AS _at_load
FROM {{ ref('int_abonnes') }}

{% if is_incremental() %}
  WHERE date > (SELECT COALESCE(MAX(date), CAST('1900-01-01' AS DATE)) FROM {{ this }})
{% endif %}
