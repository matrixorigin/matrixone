-- @bvt:issue#24208
-- Reproduces the q34 root cause on the AP merge-group path.
-- The first partial batch contains only non-null customer keys, while a
-- later batch contains the NULL group that used to be dropped.

drop table if exists issue_24208_store_sales_ap;
drop table if exists issue_24208_sales_ext;

create table issue_24208_store_sales_ap(
  ss_row_id bigint,
  ss_ticket_number int,
  ss_customer_sk int
);
create table issue_24208_sales_ext(
  ss_row_id bigint,
  filler int
);

insert into issue_24208_store_sales_ap
select
  result,
  1,
  case when result <= 50000 then 10 else null end
from generate_series(1, 100000, 1) g;

insert into issue_24208_sales_ext
select
  ((result - 1) % 100000) + 1,
  1
from generate_series(1, 1000000, 1) g;

set @@max_dop = 12;

select count(*) as group_cnt
from (
  select
    ss_ticket_number,
    ss_customer_sk,
    count(*) cnt
  from issue_24208_store_sales_ap
  join issue_24208_sales_ext using (ss_row_id)
  group by ss_ticket_number, ss_customer_sk
) s;

select
  ss_ticket_number,
  ifnull(cast(ss_customer_sk as char), 'NULL') as customer_key,
  count(*) cnt
from issue_24208_store_sales_ap
join issue_24208_sales_ext using (ss_row_id)
group by ss_ticket_number, ss_customer_sk
order by customer_key;

select
  ss_ticket_number,
  ifnull(cast(ss_customer_sk as char), 'NULL') as customer_key,
  count(*) cnt
from issue_24208_store_sales_ap
join issue_24208_sales_ext using (ss_row_id)
group by ss_ticket_number, ss_customer_sk
order by customer_key;

set @@max_dop = 0;

drop table if exists issue_24208_store_sales_ap;
drop table if exists issue_24208_sales_ext;
