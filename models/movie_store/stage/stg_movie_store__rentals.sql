-- import rentals data
with
    rentals as (
        select
              cast(rental_id as numeric) as rental_id
            , cast(rental_date as datetime) as rental_date
            , cast(inventory_id as numeric) as inventory_id
            , cast(customer_id as numeric) as customer_id
            , cast(return_date as datetime) as return_date
            , cast(staff_id as numeric) as staff_id
            , cast(last_update as timestamp) as last_updated_at,
        from {{ source("movie_store", "rental") }}
    )

-- final query
select *
from rentals
