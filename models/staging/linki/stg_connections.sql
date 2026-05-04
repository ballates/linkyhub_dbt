/*
  Modèle      : stg_connections
  Source      : prime-force-478609-s4.linki_bucket_set.connections
  Cible       : prime-force-478609-s4.bronze_linki.stg_connections

  Description :
    Renommage, typage et parsing multi-format des dates de connexion LinkedIn (formats FR et EN).
    Normalisation de l'adresse email. Génération de la surrogate key sur prénom, nom, URL et email.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    {{ dbt_utils.generate_surrogate_key(['`First Name`', '`Last Name`', 'URL', '`Email Address`']) }} AS id_connection,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    `First Name`                AS first_name,
    `Last Name`                 AS last_name,
    URL                         AS url,
    {{ normalize_email('`Email Address`') }} AS email_address,
    Company                     AS company,
    Position                    AS position,
    COALESCE(
        -- Format français : "11-janv-26", "27-nov-25"
        SAFE.PARSE_DATE('%d-%b-%y',
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            LOWER(`Connected On`),
            'janv','jan'),'févr','feb'),'mars','mar'),'avr','apr'),
            'mai','may'),'juin','jun'),'juil','jul'),'août','aug'),
            'sept','sep'),'oct','oct'),'nov','nov'),'déc','dec')
        ),
        -- Format anglais court : "27 Nov 25"
        SAFE.PARSE_DATE('%d %b %y', `Connected On`),
        -- Format anglais long : "31 Dec 2025"
        SAFE.PARSE_DATE('%d %b %Y', `Connected On`)
    )                           AS connected_on,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    CURRENT_TIMESTAMP()         AS _at_load
FROM {{ source('linki_bucket_set', 'connections') }}
