-- import film actors data
with
    film_actors as (
        select
            {{ dbt_utils.generate_surrogate_key(["fa.actor_id", "fa.film_id"]) }}
            as film_actors_sk,
            cast(fa.actor_id as numeric) as actor_id,
            cast(fa.film_id as numeric) as film_id,
            cast(fa.last_update as timestamp) as last_updated_at
        from {{ source("movie_store", "film_actor") }} fa
    )

-- final query
select *
from film_actors