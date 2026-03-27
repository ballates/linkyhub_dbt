SELECT
    id_certification,
    name,
    url,
    authority,
    started_on,
    finished_on,
    license_number,
    _at_load
FROM {{ ref('stg_certifications') }}