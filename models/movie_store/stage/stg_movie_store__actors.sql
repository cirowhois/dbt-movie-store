-- import actors data
with
    actors as (

        select
              cast(actor_id as numeric) as actor_id
            , cast(first_name as string) as first_name
            , cast(last_name as string) as last_name
            , cast(last_update as timestamp) as last_updated_at
        from {{ source("movie_store", "actor") }}
    )

-- final query
select *
from actors
