with
    -- customers
    customers as (select * from {{ ref("stg_movie_store__customers") }}),

    -- inventories
    inventories as (select * from {{ ref("stg_movie_store__inventories") }}),

    -- rentals
    rentals as (select * from {{ ref("stg_movie_store__rentals") }}),

    -- payments
    payments as (select * from {{ ref("stg_movie_store__payments") }}),

    -- joins
    rentals_joins as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "p.payment_id",
                        "p.customer_id",
                        "p.rental_id",
                        "i.inventory_id",
                        "i.film_id",
                        "i.store_id"
                    ]
                )
            }} as rentals_sk,
            p.payment_id,
            p.customer_id,
            p.rental_id,
            i.inventory_id,
            i.film_id,
            i.store_id,
            p.amount_in_dollars,
            p.payment_date_at,
            r.rental_date_at,
            r.return_date_at,
            date_diff(r.return_date_at, r.rental_date_at, day) as rental_period,
            trim(
                coalesce(c.first_name, '') || ' ' || coalesce(c.last_name, '')
            ) as customers_name,
            c.address_id as customer_address_id,
            email as customers_email
        from payments p
        left join rentals r on p.rental_id = r.rental_id
        left join inventories i on r.inventory_id = i.inventory_id
        left join customers c on p.customer_id = c.customer_id
    )

-- final query
select *
from rentals_joins
