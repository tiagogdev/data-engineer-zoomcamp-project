{{ config(
    materialized = 'view'
) }}

SELECT
    DISTINCT pokemon_id,
    name AS pokemon_name,
    height AS pokemon_height,
    weight AS pokemon_weight
FROM
    {{ source(
        'staging',
        'pokemon_info'
    ) }}
ORDER BY
    pokemon_id

    {% if var(
        'is_test_run',
        default = true
    ) %}
LIMIT
    100
{% endif %}
