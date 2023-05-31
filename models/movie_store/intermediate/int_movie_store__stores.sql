-- stores
with
    stores as (select * from {{ ref("stg_movie_store__stores") }}),

    -- staffs
    staffs as (select * from {{ ref("stg_movie_store__staffs") }}),

    -- joins
    stores_joins as (
        select
            -- identifiers
            s.store_id,
            s.manager_staff_id,
            s.address_id,

            -- staff attributes
            trim(
                coalesce(ss.first_name, '') || ' ' || coalesce(ss.last_name, '')
            ) as staff_name,
            ss.email as staff_email
        from stores s
        left join staffs ss on s.manager_staff_id = ss.staff_id
    )

-- final query
select *
from stores_joins
