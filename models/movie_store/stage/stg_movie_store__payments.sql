-- import payments data
with
    payments as (
        select
              cast(payment_id as numeric) as payment_id
            , cast(customer_id as numeric) as customer_id
            , cast(staff_id as numeric) as staff_id
            , cast(rental_id as numeric) as rental_id
            , cast(amount as numeric) as amount_in_dollars
            , cast(payment_date as timestamp) as payment_date_at
        from {{ source("movie_store", "payment") }}
    )

-- final query
select *
from payments
