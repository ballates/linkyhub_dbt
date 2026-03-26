SELECT
    COALESCE(
        SAFE_CAST(date_ AS DATE),
        SAFE.PARSE_DATE('%d/%m/%Y', CAST(date_ AS STRING))
    )                                   AS date,
    nouveaux_abonnes,
    _at_load
FROM {{ ref('stg_abonnes') }}