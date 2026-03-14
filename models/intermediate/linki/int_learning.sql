SELECT
    Title                                                       AS title,
    Description                                                 AS description,
    Type                                                        AS type,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M UTC', `Last Watched Date`) AS last_watched_date,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M UTC', `Completed At`)      AS completed_at,
    Saved                                                       AS saved,
    Notes                                                       AS notes,
    CURRENT_TIMESTAMP()                                         AS _at_load
FROM {{ ref('stg_learning') }}