-- import categories data
with
    categories as (
        select
              cast(category_id as numeric) as category_id
            , cast(name as string) as name
            , cast(last_update as timestamp) as last_updated_at
        from {{ source("movie_store", "category") }}
    )

-- final query
select *
from categories
