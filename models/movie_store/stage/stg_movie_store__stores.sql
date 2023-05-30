-- import stores data
with
    stores as (
        select
            cast(store_id as numeric) as store_id,
            cast(manager_staff_id as numeric) as manager_staff_id,
            cast(address_id as numeric) as address_id,
            cast(last_update as timestamp) as last_updated_at,
        from {{ source("movie_store", "store") }}
    )

-- final query
select *
from stores
