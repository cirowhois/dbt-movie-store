{{ config(materialized="table", schema="movie_store") }}

select
    film_id,
    film_name,
    film_release_year,
    film_category,
    film_language,
    film_length,
    film_rating,
    film_rep_cost,
    count(distinct(rental_id)) as rental_count,
    sum(amount_in_dollars) as total_amount,
    count(case when is_delayed = 'yes' then 1 else null end) as delayed_rentals,
    count(case when is_delayed = 'no' then 1 else null end) as ontime_rentals
from {{ ref("fct_movie_store__payments") }}
group by 1,2,3,4,5,6,7,8
