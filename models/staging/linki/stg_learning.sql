/*
  Modèle      : stg_learning
  Source      : prime-force-478609-s4.linky_bucket_landing.learning
  Cible       : prime-force-478609-s4.bronze_linki.stg_learning

  Description :
    Renommage et aliasing des colonnes des formations LinkedIn Learning.
    Données chargées via Fivetran depuis GCS (linki_bucket).
    Aucune logique métier. Aucun filtre.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    {{ dbt_utils.generate_surrogate_key(['content_title']) }}  AS id_learning,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    content_title                          AS title,
    content_description                    AS description,
    content_type                           AS type,
    content_last_watched_date_if_viewed_   AS last_watched_date,
    content_completed_at_if_completed_     AS completed_at,
    content_saved                          AS saved,
    notes_taken_on_videos_if_taken_        AS notes,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    CURRENT_TIMESTAMP() AS _at_load
FROM {{ source('linky_bucket_landing', 'learning') }}
