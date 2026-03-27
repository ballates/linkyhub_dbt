SELECT
    id_learning,
    title,
    description,
    type,
    last_watched_date,
    completed_at,
    saved,
    notes
FROM {{ ref('int_learning') }}
WHERE title IS NOT NULL
