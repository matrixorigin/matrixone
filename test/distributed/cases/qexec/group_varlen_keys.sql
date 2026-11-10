drop database if exists group_varlen_keys;
create database group_varlen_keys;
use group_varlen_keys;

-- Declared character widths must not truncate UTF-8 grouping keys.
create table exceptions (
    exception_type varchar(3) default null,
    order_no varchar(255) default null,
    amount_due decimal(15,2) default null,
    amount_paid decimal(38,2) not null,
    diff decimal(38,2) default null,
    currency varchar(255) default null
);
insert into exceptions values
    ('多付', '500000006', 3840.00, 4000.00, -160.00, 'CNY'),
    ('未付款', '500000003', 2400.00, 0.00, 2400.00, 'CNY'),
    ('未付清', '500000009', 3600.00, 2000.00, 1600.00, 'CNY');

select order_no, exception_type, hex(exception_type) as key_hex, diff
from exceptions order by order_no;

select exception_type, count(*) as exception_count, sum(abs(diff)) as exception_amount
from exceptions group by exception_type order by exception_type;

-- An injective expression provides an independent grouping control.
select hex(exception_type) as key_hex, count(*) as exception_count, sum(abs(diff)) as exception_amount
from exceptions group by hex(exception_type) order by key_hex;

select order_no, exception_type
from exceptions group by order_no, exception_type order by order_no;

-- Non-null keys can also differ only beyond an eight-byte prefix.
create table nonnull_keys (k varchar(3) not null);
insert into nonnull_keys values ('未付一'), ('未付丁'), ('未付一');
select k, count(*) from nonnull_keys group by k order by k;

drop database group_varlen_keys;
