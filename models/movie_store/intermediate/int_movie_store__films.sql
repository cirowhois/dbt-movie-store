-- level 1 - actors
with
    actors as (select * from {{ ref("stg_movie_store__actors") }}),

    -- level 1 - categories
    categories as (select * from {{ ref("stg_movie_store__categories") }}),

    -- level 1 - languages
    languages as (select * from {{ ref("stg_movie_store__languages") }}),

    -- level 2 - film actors
    film_actors as (select * from {{ ref("stg_movie_store__film_actors") }}),

    -- level 2 - film categories
    film_categories as (select * from {{ ref("stg_movie_store__film_categories") }}),

    -- level 3 - films
    films as (select * from {{ ref("stg_movie_store__films") }}),

    -- joins
    films_joins as (
        select
            -- identifiers
            {{
                dbt_utils.generate_surrogate_key(
                    ["f.film_id", "f.language_id", "fa.actor_id", "fc.category_id"]
                )
            }} as films_sk,
            f.film_id,
            f.language_id,
            fa.actor_id,
            fc.category_id,

            -- film attributes
            f.title,
            f.description,
            f.release_year,
            trim(
                coalesce(a.first_name, '') || ' ' || coalesce(a.last_name, '')
            ) as actors_name,
            c.name as category,
            l.name as language,
            f.length,
            f.rating,
            f.special_features,
            f.fulltext,

            -- rental attributes
            f.rental_duration,
            f.rental_rate,
            f.replacement_cost_in_dollars

        from films f
        left join film_actors fa on f.film_id = fa.film_id
        left join actors a on fa.actor_id = a.actor_id
        left join film_categories fc on f.film_id = fc.film_id
        left join categories c on fc.category_id = c.category_id
        left join languages l on f.language_id = l.language_id

    )

-- final query
select *
from films_joins
