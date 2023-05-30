-- import countries data
with
    countries as (

        select
              cast(c.country_id as numeric) as country_id
            , cast(c.country as string) as country
            , cast(c.last_update as timestamp) as last_updated_at
        from {{ source("movie_store", "country") }} c
    )

-- final query
select *
from countries
