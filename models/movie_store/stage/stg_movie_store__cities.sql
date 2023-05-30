-- import cities data
with
    cities as (

        select
              cast(c.city_id as numeric) as city_id
            , cast(c.city as string) as city
            , cast(c.country_id as numeric) as country_id
            , cast(c.last_update as timestamp) as last_updated_at
        from {{ source("movie_store", "city") }} c
    )

-- final query
select *
from cities
