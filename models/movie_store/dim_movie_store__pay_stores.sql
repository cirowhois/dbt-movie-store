{{ config(materialized="table", schema="movie_store") }}

select
      store_id
    , staff_name
    , staff_email
    , store_address
    , store_district
    , store_postal_code
    , store_phone
    , store_city
    , store_country
    , count(distinct(rental_id)) as rental_count
    , sum(amount_in_dollars) as total_amount
    , count(case when is_delayed = 'yes' then 1 else null end) as delayed_rentals
    , count(case when is_delayed = 'no' then 1 else null end) as ontime_rentals
from {{ ref("fct_movie_store__payments") }}
group by 1,2,3,4,5,6,7,8,9
