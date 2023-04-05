{{ config(
    materialized = 'view'
) }}

SELECT
    DISTINCT pi.pokemon_id AS pokemon_id,
    pm.learned_by_pokemon AS pokemon_name,
    pm.move_name AS pokemon_move,
    pm.move_accuracy AS move_accuracy
FROM
    {{ source(
        'staging',
        'pokemon_moves'
    ) }}
    pm
    INNER JOIN {{ source(
        'staging',
        'pokemon_info'
    ) }} pi
    ON pm.learned_by_pokemon = pi.name
WHERE
    pm.move_accuracy IS NOT NULL
ORDER BY
    pi.pokemon_id,
    pm.move_accuracy

    {% if var(
        'is_test_run',
        default = true
    ) %}
LIMIT
    100
{% endif %}
