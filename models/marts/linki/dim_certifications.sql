SELECT
    FARM_FINGERPRINT(CONCAT(name, COALESCE(authority, ''))) AS certification_id,
    name,
    url,
    authority,
    started_on,
    finished_on,
    license_number
FROM {{ ref('int_certifications') }}