SELECT
    *,
    CURRENT_TIMESTAMP() AS _at_load
FROM {{ source('linki_bucket_set', 'interactions') }}