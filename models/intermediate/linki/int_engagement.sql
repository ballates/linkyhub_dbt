/*
  Modèle      : int_engagement
  Source      : prime-force-478609-s4.bronze_linki.stg_comments
                prime-force-478609-s4.bronze_linki.stg_reactions
  Cible       : prime-force-478609-s4.silver_linki.int_engagement

  Description :
    Union des commentaires et réactions LinkedIn en une table d'engagement unifiée.
    type_event = 'comment' ou 'reaction'.
    content = message du commentaire OU type de réaction (Like, Celebrate, etc.).
*/

WITH comments AS (
    SELECT
        id_comment      AS id_engagement,
        post_id,
        'comment'       AS type_event,
        date,
        link,
        message         AS content,
        _at_load
    FROM {{ ref('stg_comments') }}
),

reactions AS (
    SELECT
        id_reaction     AS id_engagement,
        post_id,
        'reaction'      AS type_event,
        date,
        link,
        type            AS content,
        _at_load
    FROM {{ ref('stg_reactions') }}
)

SELECT * FROM comments
UNION ALL
SELECT * FROM reactions