-- level 1 - actors
with
    cities as (select * from {{ ref("stg_movie_store__cities") }}),

    -- level 1 - categories
    countries as (select * from {{ ref("stg_movie_store__countries") }}),

    -- level 1 - languages
    addresses as (select * from {{ ref("stg_movie_store__addresses") }}),

    -- joins
    addresses_joins as (
        select
            -- identifiers
            a.address_id,
            a.city_id,
            c.country_id,
            
            -- location
            trim(
                coalesce(a.address, '') || ' ' || coalesce(a.address2, '')
            ) as full_address,
            a.district,
            c.city,
            cc.country,
            a.postal_code,
            a.phone

        from addresses a
        left join cities c on a.city_id = c.city_id
        left join countries cc on c.country_id = cc.country_id
    )

-- final query
select *
from addresses_joins
