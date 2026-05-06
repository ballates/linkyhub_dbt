/*
  Modèle      : stg_comments
  Source      : prime-force-478609-s4.linky_bucket_landing.comments
  Cible       : prime-force-478609-s4.bronze_linki.stg_comments

  Description :
    Typage des colonnes des commentaires LinkedIn issues du bucket GCS via Fivetran.
    Extraction du post_id (8 premiers chiffres) depuis l'URL pour jointure avec fct_posts.
    Génération de la surrogate key sur la date, le lien et le message.
*/

WITH extracted AS (
    SELECT
        LEFT(REGEXP_EXTRACT(link, r'(\d{10,})'), 8) AS post_id_raw,
        date,
        link,
        message
    FROM {{ source('linky_bucket_landing', 'comments') }}
)

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    {{ dbt_utils.generate_surrogate_key(['date', 'link', 'message']) }}  AS id_comment,
    {{ dbt_utils.generate_surrogate_key(['post_id_raw']) }}              AS post_id,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    date,
    link,
    message,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    CURRENT_TIMESTAMP() AS _at_load
FROM extracted