/*
  Modèle      : stg_learning
  Source      : prime-force-478609-s4.linki_bucket_set.learning
  Cible       : prime-force-478609-s4.bronze_linki.stg_learning

  Description :
    Renommage des colonnes techniques (string_field_*) des formations LinkedIn Learning.
    Aucune logique métier. Aucun filtre.
*/

WITH renamed AS (
    SELECT
        string_field_0  AS title,
        string_field_1  AS description,
        string_field_2  AS type,
        string_field_3  AS last_watched_date,
        string_field_4  AS completed_at,
        string_field_5  AS saved,
        string_field_6  AS notes
    FROM {{ source('linki_bucket_set', 'learning') }}
    WHERE string_field_0 != 'Content Title'
)

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    {{ dbt_utils.generate_surrogate_key(['title']) }}  AS id_learning,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    title,
    description,
    type,
    last_watched_date,
    completed_at,
    saved,
    notes,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    CURRENT_TIMESTAMP() AS _at_load
FROM renamed
