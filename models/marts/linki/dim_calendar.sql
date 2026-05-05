/*
  Modèle      : dim_calendar
  Source      : Générée (date_spine de 2021 à aujourd'hui + 5 ans)
  Cible       : prime-force-478609-s4.gold_linki.dim_calendar

  Description :
    Dimension calendaire générée dynamiquement. Contient les attributs temporels
    (année, trimestre, mois, semaine, jour, weekend) pour les jointures avec les facts.
*/

WITH date_spine AS (
    {{ generate_date_spine('2021-01-01', (modules.datetime.date.today().year + 5) ~ '-12-31') }}
)

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    ROW_NUMBER() OVER (ORDER BY date) AS id_calendar,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    date,
    EXTRACT(YEAR FROM date)                 AS annee,
    EXTRACT(QUARTER FROM date)              AS trimestre,
    EXTRACT(MONTH FROM date)                AS mois,
    FORMAT_DATE('%B', date)                 AS nom_mois,
    FORMAT_DATE('%b', date)                 AS nom_mois_court,
    EXTRACT(WEEK FROM date)                 AS semaine,
    EXTRACT(DAY FROM date)                  AS jour,
    FORMAT_DATE('%A', date)                 AS nom_jour,
    IF(EXTRACT(DAYOFWEEK FROM date) IN (1, 7), TRUE, FALSE) AS est_weekend
FROM date_spine
