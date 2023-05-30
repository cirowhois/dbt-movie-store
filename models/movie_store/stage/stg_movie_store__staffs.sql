-- import staffs data
with
    staffs as (
        select
            cast(staff_id as numeric) as staff_id,
            cast(first_name as string) as first_name,
            cast(last_name as string) as last_name,
            cast(address_id as numeric) as address_id,
            cast(email as string) as email,
            cast(store_id as numeric) as store_id,
            cast(active as bool) as is_active,
            cast(username as string) as username,
            cast(password as string) as password,
            cast(last_update as timestamp) as last_updated_at,
            picture
        from {{ source("movie_store", "staff") }}
    )

-- final query
select *
from staffs
