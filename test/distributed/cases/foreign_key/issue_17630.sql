drop database if exists issue_17630;
create database issue_17630;
use issue_17630;

create table region_17630 (
  r_regionkey int primary key,
  r_name varchar(32)
);

create table nation_17630 (
  n_nationkey int primary key,
  n_regionkey int,
  constraint fk_nation_region_17630 foreign key (n_regionkey) references region_17630(r_regionkey)
);

create table customer_17630 (
  c_custkey int primary key,
  c_nationkey int,
  constraint fk_customer_nation_17630 foreign key (c_nationkey) references nation_17630(n_nationkey)
);

show create table customer_17630;
show create table nation_17630;

drop database issue_17630;
