{{ config(materialized="table", schema="movie_store") }}

select
    customer_id,
    customers_name,
    customer_city,
    customer_country,
    count(distinct(rental_id)) as rental_count,
    sum(total_amount) as total_amount,
    count(case when is_delayed = 'yes' then 1 else null end) as delayed_rentals,
    count(case when is_delayed = 'no' then 1 else null end) as ontime_rentals
from {{ ref("fct_movie_store__payments") }}
group by 1,2,3,4