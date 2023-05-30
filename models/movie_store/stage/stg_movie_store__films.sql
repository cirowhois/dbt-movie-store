-- import films data
with
    films as (
        select
            cast(film_id as numeric) as film_id,
            cast(title as string) as title,
            cast(description as string) as description,
            cast(release_year as numeric) as release_year,
            cast(language_id as numeric) as language_id,
            cast(rental_duration as numeric) as rental_duration,
            cast(rental_rate as numeric) as rental_rate,
            cast(length as numeric) as length,
            cast(replacement_cost as numeric) as replacement_cost_in_dollars,
            cast(rating as string) as rating,
            cast(last_update as timestamp) as last_updated_at,
            cast(special_features as string) as special_features,
            cast(fulltext as string) as fulltext
        from {{ source("movie_store", "film") }}
    )

-- final query
select *
from films
