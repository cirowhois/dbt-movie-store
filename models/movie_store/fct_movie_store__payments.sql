{{ config(materialized="table", schema="movie_store") }}


with
    -- films
    films as (select * from {{ ref("int_movie_store__films") }}),

    -- location
    location as (select * from {{ ref("int_movie_store__location") }}),

    -- rentals
    rentals as (select * from {{ ref("int_movie_store__rentals") }}),

    -- stores
    stores as (select * from {{ ref("int_movie_store__stores") }}),

    -- fact joins
    fct_joins as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    ["r.customer_id", "r.rental_id", "r.film_id", "r.store_id"]
                )
            }} as payments_sk

            -- payment
            , sum(r.amount_in_dollars) as total_amount
            , count(distinct(r.payment_id)) as total_payments

            -- rental
            , r.rental_id
            , r.rental_date_at
            , r.return_date_at
            , r.rental_period
            , case 
                when r.rental_period > f.rental_duration then 'yes'
                else 'no'
              end as is_delayed  

            -- customers
            , r.customer_id
            , r.customers_name
            , l.city as customer_city
            , l.country as customer_country

            -- films
            , r.film_id
            , f.title as film_name
            , f.release_year as film_release_year
            , f.category as film_category
            , f.language as film_language
            , f.length as film_length
            , f.rating as film_rating
            , f.rental_duration as film_rental_duration
            , f.rental_rate as film_rental_rate
            , f.replacement_cost_in_dollars as film_rep_cost

            -- store
            , r.store_id
            , s.staff_name
            , s.staff_email
            , lc.full_address as store_address
            , lc.district as store_district
            , lc.city as store_city
            , lc.country as store_country
            , lc.postal_code as store_postal_code
            , lc.phone as store_phone

        from rentals r
        left join location l on r.customer_address_id = l.address_id
        left join films f on r.film_id = f.film_id
        left join stores s on r.store_id = s.store_id
        left join location lc on s.address_id = lc.address_id
        group by 1,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
    )

-- final query
select *
from fct_joins
