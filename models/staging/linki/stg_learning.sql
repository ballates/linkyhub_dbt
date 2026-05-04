/*
  Modèle      : stg_learning
  Source      : prime-force-478609-s4.linki_bucket_set.learning
  Cible       : prime-force-478609-s4.bronze_linki.stg_learning

  Description :
    Renommage des colonnes techniques (string_field_*) des formations LinkedIn Learning.
    Aucune logique métier. Aucun filtre.
*/

SELECT
    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    string_field_0      AS title,
    string_field_1      AS description,
    string_field_2      AS type,
    string_field_3      AS last_watched_date,
    string_field_4      AS completed_at,
    string_field_5      AS saved,
    string_field_6      AS notes,
    string_field_7      AS field_extra,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    CURRENT_TIMESTAMP() AS _at_load
FROM {{ source('linki_bucket_set', 'learning') }}
