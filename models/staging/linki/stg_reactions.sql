/*
  Modèle      : stg_reactions
  Source      : prime-force-478609-s4.linky_bucket_landing.reactions
  Cible       : prime-force-478609-s4.bronze_linki.stg_reactions

  Description :
    Typage des colonnes des réactions LinkedIn issues du bucket GCS via Fivetran.
    Extraction du post_id (8 premiers chiffres) depuis l'URL pour jointure avec fct_posts.
    Génération de la surrogate key sur la date, le type et le lien.
*/

WITH extracted AS (
    SELECT
        LEFT(REGEXP_EXTRACT(link, r'(\d{10,})'), 8) AS post_id_raw,
        date,
        type,
        link
    FROM {{ source('linky_bucket_landing', 'reactions') }}
)

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    {{ dbt_utils.generate_surrogate_key(['date', 'type', 'link']) }}  AS id_reaction,
    {{ dbt_utils.generate_surrogate_key(['post_id_raw']) }}           AS post_id,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    date,
    type,
    link,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    CURRENT_TIMESTAMP() AS _at_load
FROM extracted