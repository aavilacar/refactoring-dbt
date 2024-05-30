with

    -- Import CTEs
    customers as (select * from {{ source("jaffle_shop", "customers") }}),

    orders as (select * from {{ source("jaffle_shop", "orders") }}),

    payments as (select * from {{ source("stripe", "payment") }}),

    -- Logical CTEs
    completed_payments as (
        select
            orderid as order_id,
            max(created) as payment_finalized_date,
            sum(amount) / 100.0 as total_amount_paid
        from payments
        where status <> 'fail'
        group by 1
    ),

    -- Final CTE
    paid_orders as (
        select
            orders.id as order_id,
            orders.user_id as customer_id,
            orders.order_date as order_placed_at,
            orders.status as order_status,
            completed_payments.total_amount_paid,
            completed_payments.payment_finalized_date,
            customers.first_name as customer_first_name,
            customers.last_name as customer_last_name
        from orders
        left join completed_payments on orders.id = completed_payments.order_id
        left join customers on orders.user_id = customers.id
    ),

    final as (
        select
            order_id,
            customer_id,
            order_placed_at,
            order_status,
            total_amount_paid,
            payment_finalized_date,
            customer_first_name,
            customer_last_name,

            -- Sales transaction sequence
            row_number() over (order by order_id) as transaction_seq,

            -- Customer sales sequence
            row_number() over (
                partition by customer_id order by order_id
            ) as customer_sales_seq,

            -- New vs returning customer
            case
                when
                    rank() over (
                        partition by customer_id order by order_placed_at, order_id
                    )
                    = 1
                then 'new'
                else 'return'
            end as nvsr,

            -- Customer lifetime value
            sum(total_amount_paid) over (
                partition by customer_id order by order_placed_at
            ) as customer_lifetime_value,

            -- First day of sale
            first_value(order_placed_at) over (
                partition by customer_id order by order_placed_at
            ) as fdos

        from paid_orders
        order by order_id
    )

-- Simple Select Statement
select *
from final
