{{ config(
    materialized='table',
	schema='movie_store'
) }}

-- films
with
    films as (select * from {{ ref("int_movie_store__films") }}),

    -- location
    location as (select * from {{ ref("int_movie_store__location") }}),

    -- customers
    customers as (select * from {{ ref("int_movie_store__customers") }}),

    -- stores
    stores as (select * from {{ ref("int_movie_store__stores") }}),

    -- fact joins
    fct_joins as (
        select 
        * 
        from customers
    )

-- final query
select *
from fct_joins
