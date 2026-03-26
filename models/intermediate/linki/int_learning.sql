SELECT
    FARM_FINGERPRINT(title)                                         AS id_learning,
    title,
    description,
    type,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M UTC', last_watched_date)   AS last_watched_date,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M UTC', completed_at)        AS completed_at,
    saved,
    notes,
    _at_load
FROM {{ ref('stg_learning') }}