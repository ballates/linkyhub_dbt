SELECT
    id_certification,
    name,
    url,
    authority,
    started_on,
    finished_on,
    license_number
FROM {{ ref('int_certifications') }}
