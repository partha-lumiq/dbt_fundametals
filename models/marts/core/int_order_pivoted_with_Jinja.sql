{%- set paymets_mode =['bank_transfer','coupon','credit_card','gift_card'] -%}
with payments as(
select * from {{ ref('stg_payments') }}
),

pivoted as (
    select
        order_id,
        {% for payment_method in paymets_mode -%}

            sum(case when payment_method ='{{payment_method}}' then amount else 0 end) as {{payment_method}}_amount 

        {%- if not loop.last -%}
            ,
        {%- endif %}
        {% endfor -%}
    from payments
    where status = 'success'
    group by 1
)
select * from pivoted

