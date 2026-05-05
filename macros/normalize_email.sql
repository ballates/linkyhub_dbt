{% macro normalize_email(email_column) %}
    /**
    Normalise une adresse email :
    - Convertit en minuscules
    - Supprime les espaces avant/après
    */
    LOWER(TRIM({{ email_column }}))
{% endmacro %}
