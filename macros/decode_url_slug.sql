{% macro decode_url_slug(column) %}
    -- Décode les caractères URL-encodés et capitalise la première lettre.
    -- UNNEST([...]) est un pattern BigQuery pour éviter de dupliquer l'expression.
    (SELECT CONCAT(UPPER(LEFT(decoded, 1)), SUBSTR(decoded, 2))
     FROM UNNEST([
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            {{ column }},
        '%C3%A9', 'é'),   -- é
        '%C3%A8', 'è'),   -- è
        '%C3%AA', 'ê'),   -- ê
        '%C3%AB', 'ë'),   -- ë
        '%C3%A0', 'à'),   -- à
        '%C3%A2', 'â'),   -- â
        '%C3%B4', 'ô'),   -- ô
        '%C3%B9', 'ù'),   -- ù
        '%C3%BB', 'û'),   -- û
        '%C3%AE', 'î'),   -- î
        '%C3%AF', 'ï'),   -- ï
        '%C3%A7', 'ç'),   -- ç
        '%C3%89', 'É'),   -- É
        '%C3%88', 'È'),   -- È
        '%C3%8A', 'Ê'),   -- Ê
        '%C3%80', 'À'),   -- À
        '%C3%82', 'Â'),   -- Â
        '%C3%87', 'Ç'),   -- Ç
        '%C3%99', 'Ù'),   -- Ù
        '%20',    ' ')     -- espace
     ]) AS decoded)
{% endmacro %}
