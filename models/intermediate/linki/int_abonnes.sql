SELECT
    SAFE.PARSE_DATE('%d/%m/%Y', date_)  AS date,
    nouveaux_abonnes,
    CURRENT_TIMESTAMP()                 AS _at_load
FROM {{ ref('stg_abonnes') }}