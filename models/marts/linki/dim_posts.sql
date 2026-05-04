/*
  Modèle      : dim_posts
  Source      : prime-force-478609-s4.silver_linki.int_posts
  Cible       : prime-force-478609-s4.gold_linki.dim_posts

  Description :
    Dimension des posts LinkedIn. Génération du label de post pour l'affichage BI.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    id_post,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    url_post,
    date_publication_post,
    CONCAT('Post du ', FORMAT_DATE('%d-%m-%Y', date_publication_post)) AS label_post
FROM {{ ref('int_posts') }}
