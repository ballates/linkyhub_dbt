/*
  Modèle      : stg_certifications
  Source      : prime-force-478609-s4.linki_bucket_set.certifications
  Cible       : prime-force-478609-s4.bronze_linki.stg_certifications

  Description :
    Renommage, typage et parsing des dates des certifications LinkedIn issues du bucket GCS.
    Génération de la surrogate key sur le nom, l'organisme et le numéro de licence.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    {{ dbt_utils.generate_surrogate_key(['Name', 'Authority', '`License Number`']) }} AS id_certification,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    Name                                         AS name,
    Url                                          AS url,
    Authority                                    AS authority,
    SAFE.PARSE_TIMESTAMP('%b %Y', `Started On`)  AS started_on,
    SAFE.PARSE_TIMESTAMP('%b %Y', `Finished On`) AS finished_on,
    `License Number`                             AS license_number,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    CURRENT_TIMESTAMP()                          AS _at_load
FROM {{ source('linki_bucket_set', 'certifications') }}
