SELECT
    string_field_0      AS Title,
    string_field_1      AS Description,
    string_field_2      AS Type,
    string_field_3      AS `Last Watched Date`,
    string_field_4      AS `Completed At`,
    string_field_5      AS Saved,
    string_field_6      AS Notes,
    string_field_7,
    CURRENT_TIMESTAMP() AS _at_load
FROM {{ source('linki_bucket_set', 'learning') }}