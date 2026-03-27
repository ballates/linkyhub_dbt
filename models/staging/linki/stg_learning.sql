SELECT
    string_field_0      AS title,
    string_field_1      AS description,
    string_field_2      AS type,
    string_field_3      AS last_watched_date,
    string_field_4      AS completed_at,
    string_field_5      AS saved,
    string_field_6      AS notes,
    string_field_7      AS field_extra,
    CURRENT_TIMESTAMP() AS _at_load
FROM {{ source('linki_bucket_set', 'learning') }}