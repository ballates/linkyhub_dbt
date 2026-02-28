SELECT
    date,
    nouveaux_abonnes,
    CURRENT_TIMESTAMP() AS _at_load
FROM {{ ref('int_abonnes') }}
