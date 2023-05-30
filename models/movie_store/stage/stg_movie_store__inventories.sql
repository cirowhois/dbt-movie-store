-- import inventories data
with
    inventories as (
        select
              cast(inventory_id as numeric) as inventory_id
            , cast(film_id as numeric) as film_id
            , cast(store_id as numeric) as store_id
            , cast(last_update as timestamp) as last_updated_at
        from {{ source("movie_store", "inventory") }}
    )

-- final query
select *
from inventories
