{{ config(
    materialized = 'view'
) }}

SELECT
    DISTINCT pi.pokemon_id AS pokemon_id,
    pg.generation_name AS pokemon_generation,
    pg.pokemon_name AS pokemon_name
FROM
    {{ source(
        'staging',
        'pokemon_base_stats'
    ) }}
    pg
    INNER JOIN {{ source(
        'staging',
        'pokemon_info'
    ) }} pi
    ON pg.pokemon_name = pi.name
ORDER BY
    pi.pokemon_id

    {% if var(
        'is_test_run',
        default = true
    ) %}
LIMIT
    100
{% endif %}
