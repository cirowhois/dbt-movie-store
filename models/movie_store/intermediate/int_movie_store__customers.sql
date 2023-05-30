-- customers
with
    customers as (select * from {{ ref("stg_movie_store__customers") }}),

    -- inventories
    inventories as (select * from {{ ref("stg_movie_store__inventories") }}),

    -- rentals
    rentals as (select * from {{ ref("stg_movie_store__rentals") }}),

    -- payments
    payments as (select * from {{ ref("stg_movie_store__payments") }}),

    -- joins
    customers_joins as (
        select
            -- identifiers
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "c.customer_id",
                        "c.store_id",
                        "c.address_id",
                        "r.rental_id",
                        "r.inventory_id",
                        "r.staff_id",
                        "p.payment_id",
                        "i.film_id"
                    ]
                )
            }} as customers_sk,
            c.customer_id,
            c.store_id,
            c.address_id,
            r.rental_id,
            r.inventory_id,
            r.staff_id,
            p.payment_id,
            i.film_id,

            -- customers attributes
            trim(
                coalesce(c.first_name, '') || ' ' || coalesce(c.last_name, '')
            ) as customers_name,
            c.email as customers_email,
            c.is_active,
            c.created_date,
            -- rentals attributes
            r.rental_date,
            r.return_date,
            date_diff(r.return_date, r.rental_date, day) as rental_period,
            -- payments attributes
            p.amount_in_dollars,
            p.payment_date_at

        from customers c
        left join rentals r on c.customer_id = r.customer_id
        left join payments p on c.customer_id = p.customer_id
        left join inventories i on r.inventory_id = i.inventory_id

    )

-- final query
select *
from customers_joins
