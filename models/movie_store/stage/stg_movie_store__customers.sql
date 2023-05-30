-- import customers data
with
    customers as (
        select
            cast(customer_id as numeric) as customer_id,
            cast(store_id as numeric) as store_id,
            cast(first_name as string) as first_name,
            cast(last_name as string) as last_name,
            cast(email as string) as email,
            cast(address_id as numeric) as address_id,
            cast(activebool as bool) as is_active,
            cast(create_date as date) as created_date,
            cast(last_update as timestamp) as last_updated_at
        from {{ source("movie_store", "customer") }}
    )

-- final query
select *
from customers
order by 1
