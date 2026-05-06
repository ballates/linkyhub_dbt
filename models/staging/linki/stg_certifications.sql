/*
  Modèle      : stg_certifications
  Source      : prime-force-478609-s4.linky_bucket_landing.certifications
  Cible       : prime-force-478609-s4.bronze_linki.stg_certifications

  Description :
    Renommage, typage et parsing des dates des certifications LinkedIn issues du bucket GCS.
    Données chargées via Fivetran depuis GCS (linki_bucket).
    Génération de la surrogate key sur le nom, l'organisme et le numéro de licence.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    {{ dbt_utils.generate_surrogate_key(['name', 'authority', 'license_number']) }} AS id_certification,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    name,
    url,
    authority,
    SAFE.PARSE_TIMESTAMP('%b %Y', started_on)     AS started_on,
    SAFE.PARSE_TIMESTAMP('%b %Y', finished_on)    AS finished_on,
    license_number,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    CURRENT_TIMESTAMP()                          AS _at_load
FROM {{ source('linky_bucket_landing', 'certifications') }}
