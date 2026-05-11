-- Regression test for issue #24219.
-- The catalog-side EXISTS input intentionally has more than one execution
-- batch, but the outer customer row must still be emitted only once.

drop table if exists issue_24219_customer;
drop table if exists issue_24219_web_sales;
drop table if exists issue_24219_catalog_sales;

create table issue_24219_customer (
  c_customer_sk int
);

create table issue_24219_web_sales (
  ws_bill_customer_sk int
);

create table issue_24219_catalog_sales (
  cs_ship_customer_sk int,
  cs_sold_item_sk int
);

insert into issue_24219_customer values (1);

insert into issue_24219_catalog_sales
select
  1,
  result
from generate_series(1, 9000, 1) g;

select c_customer_sk
from issue_24219_customer c
where exists (
        select 1
        from issue_24219_web_sales ws
        where c.c_customer_sk = ws.ws_bill_customer_sk
      )
   or exists (
        select 1
        from issue_24219_catalog_sales cs
        where c.c_customer_sk = cs.cs_ship_customer_sk
      )
order by c_customer_sk;

select count(*) as exists_or_count
from issue_24219_customer c
where exists (
        select 1
        from issue_24219_web_sales ws
        where c.c_customer_sk = ws.ws_bill_customer_sk
      )
   or exists (
        select 1
        from issue_24219_catalog_sales cs
        where c.c_customer_sk = cs.cs_ship_customer_sk
      );

drop table if exists issue_24219_customer;
drop table if exists issue_24219_web_sales;
drop table if exists issue_24219_catalog_sales;
