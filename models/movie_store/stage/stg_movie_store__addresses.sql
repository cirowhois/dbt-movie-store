-- import addresses data
with
    addresses as (

        select
              cast(a.address_id as numeric) as address_id
            , cast(a.address as string) as address
            , cast(a.address2 as string) as address2
            , cast(a.district as string) as district
            , cast(a.city_id as numeric) as city_id
            , cast(a.postal_code as numeric) as postal_code
            , cast(a.phone as numeric) as phone
            , cast(a.last_update as timestamp) as last_updated_at
        from {{ source("movie_store", "address") }} a
    )

-- final query
select *
from addresses
