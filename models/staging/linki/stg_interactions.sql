SELECT
    url_du_post,
    date_de_publication_du_post,
    interactions,
    CURRENT_TIMESTAMP() AS _at_load
FROM {{ source('google_drive', 'contenu_2025_01_21_2026_01_20_ben_mbairo_interactions') }}