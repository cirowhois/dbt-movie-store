-- import film categories data
with
    film_categories as (
        select
              {{ dbt_utils.generate_surrogate_key(["fc.film_id", "fc.category_id"]) }}
              as film_categories_sk
            , cast(fc.film_id as numeric) as film_id
            , cast(fc.category_id as numeric) as category_id
            , cast(fc.last_update as timestamp) as last_updated_at
        from {{ source("movie_store", "film_category") }} fc
    )

-- final query
select *
from film_categories
