SELECT
    FARM_FINGERPRINT(CONCAT(title, COALESCE(type, ''))) AS learning_key,
    title,
    description,
    type,
    last_watched_date,
    completed_at,
    saved,
    notes
FROM {{ ref('int_learning') }}