{% macro normalize_email(email_column) %}
    /**
    Normalise une adresse email :
    - Convertit en minuscules
    - Supprime les espaces avant/après
    - Valide le format de base
    */
    LOWER(TRIM({{ email_column }}))
{% endmacro %}
