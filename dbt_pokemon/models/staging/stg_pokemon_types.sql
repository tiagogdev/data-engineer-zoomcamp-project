{{ config(
    materialized = 'view'
) }}

SELECT
    DISTINCT id,
    name AS pokemon_name,
    types AS pokemon_types
FROM
    {{ source(
        'staging',
        'pokemon_info'
    ) }}
ORDER BY
    id

    {% if var(
        'is_test_run',
        default = true
    ) %}
LIMIT
    100
{% endif %}
