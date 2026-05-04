/*
  Modèle      : fct_abonnes
  Source      : prime-force-478609-s4.silver_linki.int_abonnes
  Cible       : prime-force-478609-s4.gold_linki.fct_abonnes

  Description :
    Table de faits des abonnés LinkedIn. Évolution quotidienne des nouveaux abonnés.
    Modèle incrémental : merge sur date, nouvelles dates uniquement.
*/

{{ config(
    materialized='incremental',
    unique_key=['date'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

SELECT
    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    date,
    nouveaux_abonnes,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    CURRENT_TIMESTAMP() AS _at_load
FROM {{ ref('int_abonnes') }}

{% if is_incremental() %}
  WHERE date > (SELECT COALESCE(MAX(date), CAST('1900-01-01' AS DATE)) FROM {{ this }})
{% endif %}
