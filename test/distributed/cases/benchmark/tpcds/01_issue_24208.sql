-- @bvt:issue#24208
-- Repro case from q34.
-- This reproduces: `hd_dep_count / hd_vehicle_count > 1.2` on BIGINT with boundary count.
-- If integer division uses float64, borderline value may be rounded to 1.2 and filtered out unexpectedly.

drop table if exists issue_24208_store_sales;
drop table if exists issue_24208_date_dim;
drop table if exists issue_24208_store;
drop table if exists issue_24208_household_demographics;
drop table if exists issue_24208_customer;

create table if not exists issue_24208_store_sales(
  ss_sold_date_sk bigint,
  ss_store_sk bigint,
  ss_hdemo_sk bigint,
  ss_ticket_number bigint,
  ss_customer_sk bigint
);
create table if not exists issue_24208_date_dim(
  d_date_sk bigint,
  d_dom int,
  d_year int
);
create table if not exists issue_24208_store(
  s_store_sk bigint,
  s_county varchar(32)
);
create table if not exists issue_24208_household_demographics(
  hd_demo_sk bigint,
  hd_dep_count bigint,
  hd_vehicle_count bigint,
  hd_buy_potential varchar(20)
);
create table if not exists issue_24208_customer(
  c_customer_sk bigint,
  c_last_name varchar(32),
  c_first_name varchar(32),
  c_salutation varchar(16),
  c_preferred_cust_flag char(1)
);

insert into issue_24208_date_dim values
  (1, 1, 2000),
  (2, 2, 2001),
  (3, 25, 2002);
insert into issue_24208_store values
  (10, 'Williamson County');
insert into issue_24208_household_demographics values
  (101, 3, 2, '1001-5000'),
  (102, 1164675422829135152, 970562852357612545, '1001-5000');
insert into issue_24208_customer values
  (5001, 'Issue', 'Repro', 'Mr', 'Y');

-- 14 rows are safely >1.2, 1 boundary row relies on precise bigint division.
insert into issue_24208_store_sales values
  (1, 10, 101, 9001, 5001),
  (1, 10, 101, 9001, 5001),
  (1, 10, 101, 9001, 5001),
  (1, 10, 101, 9001, 5001),
  (1, 10, 101, 9001, 5001),
  (1, 10, 101, 9001, 5001),
  (1, 10, 101, 9001, 5001),
  (1, 10, 101, 9001, 5001),
  (1, 10, 101, 9001, 5001),
  (1, 10, 101, 9001, 5001),
  (1, 10, 101, 9001, 5001),
  (1, 10, 101, 9001, 5001),
  (1, 10, 101, 9001, 5001),
  (1, 10, 101, 9001, 5001),
  (1, 10, 102, 9001, 5001);

select
  c_last_name,
  c_first_name,
  c_salutation,
  c_preferred_cust_flag,
  ss_ticket_number,
  cnt
from (
  select
    ss_ticket_number,
    ss_customer_sk,
    count(*) cnt
  from
    issue_24208_store_sales,
    issue_24208_date_dim,
    issue_24208_store,
    issue_24208_household_demographics
  where issue_24208_store_sales.ss_sold_date_sk = issue_24208_date_dim.d_date_sk
    and issue_24208_store_sales.ss_store_sk = issue_24208_store.s_store_sk
    and issue_24208_store_sales.ss_hdemo_sk = issue_24208_household_demographics.hd_demo_sk
    and (issue_24208_date_dim.d_dom between 1 and 3 or issue_24208_date_dim.d_dom between 25 and 28)
    and (
      issue_24208_household_demographics.hd_buy_potential = '1001-5000'
      or issue_24208_household_demographics.hd_buy_potential = '0-500'
    )
    and issue_24208_household_demographics.hd_vehicle_count > 0
    and (case
      when issue_24208_household_demographics.hd_vehicle_count > 0
      then issue_24208_household_demographics.hd_dep_count / issue_24208_household_demographics.hd_vehicle_count
      else null
    end) > 1.2
    and issue_24208_date_dim.d_year in (2000, 2000 + 1, 2000 + 2)
    and issue_24208_store.s_county in (
      'Williamson County','Williamson County','Williamson County','Williamson County',
      'Williamson County','Williamson County','Williamson County','Williamson County'
    )
  group by ss_ticket_number, ss_customer_sk
) dn,
issue_24208_customer
where ss_customer_sk = issue_24208_customer.c_customer_sk
  and cnt between 15 and 20
order by
  c_last_name,
  c_first_name,
  c_salutation,
  c_preferred_cust_flag desc,
  ss_ticket_number;

drop table if exists issue_24208_store_sales;
drop table if exists issue_24208_date_dim;
drop table if exists issue_24208_store;
drop table if exists issue_24208_household_demographics;
drop table if exists issue_24208_customer;
