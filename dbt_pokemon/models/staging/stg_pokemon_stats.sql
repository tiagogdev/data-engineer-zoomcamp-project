{{ config(
    materialized = 'view'
) }}

SELECT
    DISTINCT pi.id AS pokemon_id,
    pbs.pokemon_name AS pokemon_generation,
    pbs.base_stat_name AS base_stat_name,
    pbs.base_stat AS base_stat_value
FROM
    {{ source(
        'staging',
        'pokemon_base_stats'
    ) }}
    pbs
    INNER JOIN {{ source(
        'staging',
        'pokemon_info'
    ) }} pi
    ON pbs.pokemon_name = pi.name
ORDER BY
    pi.id

    {% if var(
        'is_test_run',
        default = true
    ) %}
LIMIT
    100
{% endif %}
