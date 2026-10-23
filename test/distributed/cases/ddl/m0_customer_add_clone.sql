-- @suit
-- @case
-- @desc: customer ADD, nullable inserts and snapshot-clone isolation in default COPY and experimental M0 builds
-- @label:bvt

drop snapshot if exists m0_customer_snapshot;
drop database if exists m0_customer_child;
drop database if exists m0_customer_parent;
create database m0_customer_parent;
use m0_customer_parent;
set autocommit = 1;
create table customer(c_id int not null, c_d_id int not null, c_w_id int not null, c_credit_lim decimal(12,2), c_ytd_payment decimal(12,2), c_balance decimal(12,2), primary key(c_w_id,c_d_id,c_id));
insert into customer values (1,1,1,50000,10000,10),(2,2,2,60000,6000,null),(3,3,3,70000,1000,30);
alter table customer add column loyalty_tier_t0_s0 varchar(8);
alter table customer add column credit_lim_t0_s0 decimal(12,2);
select count(*) as parent_public_ordinals from information_schema.columns where table_schema='m0_customer_parent' and table_name='customer' and ((column_name='loyalty_tier_t0_s0' and ordinal_position=7) or (column_name='credit_lim_t0_s0' and ordinal_position=8));
select ordinal_position as filtered_parent_ordinal from information_schema.columns where table_schema='m0_customer_parent' and table_name='customer' and column_name='loyalty_tier_t0_s0';
select count(*) as missing_suffix_rows from customer where loyalty_tier_t0_s0 is null and credit_lim_t0_s0 is null;
update customer set loyalty_tier_t0_s0 = case when c_ytd_payment > 9000 then 'Gold' when c_ytd_payment > 5000 then 'Silver' else 'Bronze' end, credit_lim_t0_s0 = c_credit_lim;
select count(*) as parent_values from customer where (c_id=1 and loyalty_tier_t0_s0='Gold' and credit_lim_t0_s0=50000) or (c_id=2 and loyalty_tier_t0_s0='Silver' and credit_lim_t0_s0=60000) or (c_id=3 and loyalty_tier_t0_s0='Bronze' and credit_lim_t0_s0=70000);
create snapshot m0_customer_snapshot for database m0_customer_parent;
create database m0_customer_child clone m0_customer_parent {snapshot='m0_customer_snapshot'};
use m0_customer_child;
alter table customer add column loyalty_tier_t1_s1 varchar(8);
alter table customer add column credit_lim_t1_s1 decimal(12,2);
select count(*) as child_public_ordinals from information_schema.columns where table_schema='m0_customer_child' and table_name='customer' and ((column_name='loyalty_tier_t0_s0' and ordinal_position=7) or (column_name='credit_lim_t0_s0' and ordinal_position=8) or (column_name='loyalty_tier_t1_s1' and ordinal_position=9) or (column_name='credit_lim_t1_s1' and ordinal_position=10));
select count(*) as child_missing_suffix from customer where loyalty_tier_t1_s1 is null and credit_lim_t1_s1 is null and credit_lim_t0_s0 is not null;
update customer set loyalty_tier_t0_s0='Child', loyalty_tier_t1_s1='Gold', credit_lim_t1_s1=c_credit_lim where c_id=1;
select count(*) as child_changed from customer where c_id=1 and loyalty_tier_t0_s0='Child' and loyalty_tier_t1_s1='Gold' and credit_lim_t1_s1=50000;
select count(*) as child_partial_nulls from customer where c_id in (2,3) and loyalty_tier_t1_s1 is null and credit_lim_t1_s1 is null;
insert into customer(c_id,c_d_id,c_w_id,c_credit_lim,c_ytd_payment,c_balance) values (4,4,4,80000,12000,40);
select count(*) as inserted_nullable_suffix from customer where c_id=4 and loyalty_tier_t0_s0 is null and credit_lim_t0_s0 is null and loyalty_tier_t1_s1 is null and credit_lim_t1_s1 is null;
select count(credit_lim_t0_s0) as inherited_not_null_rows from customer;
select count(credit_lim_t1_s1) as child_not_null_rows from customer;
insert into customer(c_id,c_d_id,c_w_id) values (null,5,5);
select count(*) as rows_after_failed_insert from customer;
select count(*) as parent_unchanged from m0_customer_parent.customer where (c_id=1 and loyalty_tier_t0_s0='Gold' and credit_lim_t0_s0=50000) or (c_id=2 and loyalty_tier_t0_s0='Silver' and credit_lim_t0_s0=60000) or (c_id=3 and loyalty_tier_t0_s0='Bronze' and credit_lim_t0_s0=70000);
select count(*) as parent_no_child_insert from m0_customer_parent.customer where c_id=4;
select count(*) as snapshot_rows from m0_customer_parent.customer {snapshot='m0_customer_snapshot'};
select count(*) as nullable_child_columns from information_schema.columns where table_schema='m0_customer_child' and table_name='customer' and column_name in ('loyalty_tier_t0_s0','credit_lim_t0_s0','loyalty_tier_t1_s1','credit_lim_t1_s1') and is_nullable='YES';
select count(c_id) as count_not_null from customer;
select count(null) as count_null from customer;
select count(distinct c_balance) as count_distinct_nullable from customer;
select count(c.c_id) as count_outer_nullable from m0_customer_parent.customer p left join customer c on p.c_id=c.c_id and c.c_id=1;
select c_id from customer where c_id<0;
create table comment_control(a int, b int, primary key(a,b));
insert into comment_control values (1,2);
alter table comment_control add column x int comment 'kept';
select count(*) as kept_comments from information_schema.columns where table_schema='m0_customer_child' and table_name='comment_control' and column_name='x' and column_comment='kept';
select count(*) as comment_control_rows from comment_control where a=1 and b=2 and x is null;
-- DEFAULT false layouts must support standalone CREATE INDEX via safe COPY exit.
create table birth_index_control(c_id int not null, c_d_id int not null, c_w_id int not null, c_credit_lim decimal(12,2), c_ytd_payment decimal(12,2), c_balance decimal(12,2), primary key(c_w_id,c_d_id,c_id));
insert into birth_index_control values (1,1,1,50000,100,10),(2,1,1,50000,100,20),(3,1,1,50000,100,30);
alter table birth_index_control add column flag boolean default false;
update birth_index_control set flag=null where c_id=3;
create index ix on birth_index_control(c_balance);
select count(*) as indexed_false_rows from birth_index_control where flag=false;
select count(*) as indexed_null_rows from birth_index_control where flag is null;
select count(*) as indexed_lookup from birth_index_control where c_balance=20 and flag=false;
insert into birth_index_control(c_id,c_d_id,c_w_id,c_balance) values (4,1,1,40);
select count(*) as indexed_new_default from birth_index_control where c_id=4 and flag=false;
drop table birth_index_control;
drop snapshot m0_customer_snapshot;
drop database m0_customer_child;
drop database m0_customer_parent;
