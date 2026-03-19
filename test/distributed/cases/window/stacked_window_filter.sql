drop table if exists stacked_window_orders;
create table stacked_window_orders (customer_id int, amount decimal(12, 2), order_date date);
insert into stacked_window_orders values (1, 10.00, '2024-01-01'), (1, 20.00, '2024-01-02'), (1, 30.00, '2024-01-03'), (2, 7.00, '2024-01-01'), (2, 9.00, '2024-01-02');

-- Regression coverage for issue #23882: stacked window expressions with an outer filter on the earlier window column.
select x.customer_id, x.second_cum_amount from (select customer_id, row_number() over (partition by customer_id order by order_date) as rn, sum(amount) over (partition by customer_id order by order_date rows unbounded preceding) as second_cum_amount from stacked_window_orders where year(order_date) = 2024) as x where x.rn = 2 order by x.customer_id;

drop table if exists stacked_window_orders;
