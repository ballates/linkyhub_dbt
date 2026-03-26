SELECT
    name,
    url,
    authority,
    started_on,
    finished_on,
    license_number,
    CURRENT_TIMESTAMP()   AS _at_load
FROM {{ ref('stg_certifications') }}