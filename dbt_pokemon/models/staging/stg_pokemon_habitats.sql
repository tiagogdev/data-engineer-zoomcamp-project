{{ config(
    materialized = 'view'
) }}

SELECT
    DISTINCT pi.id AS pokemon_id,
    ph.habitat AS pokemon_habitat,
    ph.pokemon_name AS pokemon_name
FROM
    {{ source(
        'staging',
        'pokemon_habitats'
    ) }}
    ph
    INNER JOIN {{ source(
        'staging',
        'pokemon_info'
    ) }} pi
    ON ph.pokemon_name = pi.name
ORDER BY
    pi.id

    {% if var(
        'is_test_run',
        default = true
    ) %}
LIMIT
    100
{% endif %}
