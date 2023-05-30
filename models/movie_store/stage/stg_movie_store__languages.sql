-- import languages data
with
    languages as (
        select
              cast(language_id as numeric) as language_id
            , cast(name as string) as name
            , cast(last_update as timestamp) as last_updated_at
        from {{ source("movie_store", "language") }}
    )

-- final query
select *
from languages
