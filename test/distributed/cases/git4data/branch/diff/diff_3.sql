drop database if exists test_mixed_diff;
create database test_mixed_diff;
use test_mixed_diff;

-- case 1: validate diff on composite primary key mixing numeric/temporal types
create table orders_eu_source ( tenant_id int, order_code varchar(8), priority tinyint, amount decimal(12,2), order_date date, last_update timestamp, note varchar(32), primary key (tenant_id, order_code));

insert into orders_eu_source values(100, 'A100', 1, 120.50, '2024-03-01', '2024-03-01 08:15:00', 'first order');
insert into orders_eu_source values(100, 'A101', 2,  80.00, '2024-03-02', '2024-03-02 13:45:00', 'low priority');
insert into orders_eu_source values(101, 'B200', 1, 305.75, '2024-03-03', '2024-03-03 09:10:10', 'vip client');
insert into orders_eu_source values(101, 'B201', 3,  45.00, '2024-03-04', '2024-03-04 20:30:20', 'beta check');

create snapshot sp_orders_base for table test_mixed_diff orders_eu_source;

data branch create table orders_branch_x from orders_eu_source{snapshot='sp_orders_base'};
data branch create table orders_branch_y from orders_eu_source{snapshot='sp_orders_base'};

-- branch X adds a new tenant record and updates timestamps
update orders_branch_x
   set amount = amount + 10.00, last_update = '2024-03-05 10:00:00'
 where tenant_id = 100 and order_code = 'A100';
delete from orders_branch_x where tenant_id = 101 and order_code = 'B201';
insert into orders_branch_x values(102, 'C300', 1, 512.25, '2024-03-05', '2024-03-05 17:05:00', 'new market');

create snapshot sp_orders_x for table test_mixed_diff orders_branch_x;

-- branch Y tracks a different update + note change
update orders_branch_y
   set priority = 4, note = 'escalated audit'
 where tenant_id = 101 and order_code = 'B200';
insert into orders_branch_y values(100, 'A102', 2,  95.75, '2024-03-05', '2024-03-05 07:45:00', 'rush slot');

create snapshot sp_orders_y for table test_mixed_diff orders_branch_y;

data branch diff orders_branch_x{snapshot='sp_orders_x'} against orders_branch_y{snapshot='sp_orders_y'};

drop snapshot sp_orders_x;
drop snapshot sp_orders_y;
drop snapshot sp_orders_base;
drop table orders_branch_x;
drop table orders_branch_y;
drop table orders_eu_source;

-- case 2: detect diff on string primary key with decimal/flag/binary columns
create table sku_inventory (sku_code char(6),batch smallint,quantity int,cost_per_unit decimal(8,3),enabled tinyint,checksum varbinary(4),primary key (sku_code, batch));

insert into sku_inventory values('SKU001', 1, 4, 11.250, 1,  x'0A0B0C0D');
insert into sku_inventory values('SKU001', 2, 3, 10.500, 1,  x'FEEDBEEF');
insert into sku_inventory values('SKU002', 1, 2,  4.750, 0, x'01020304');
insert into sku_inventory values('SKU003', 1, 6,  7.125, 1,  x'0F0F0F0F');

create snapshot sp_sku_base for table test_mixed_diff sku_inventory;

data branch create table sku_branch_a from sku_inventory{snapshot='sp_sku_base'};
data branch create table sku_branch_b from sku_inventory{snapshot='sp_sku_base'};

-- branch A simulates a recall and binary hash rotation
update sku_branch_a
   set enabled = 0, checksum = x'00112233'
 where sku_code = 'SKU001' and batch = 1;
delete from sku_branch_a where sku_code = 'SKU003';
insert into sku_branch_a values('SKU004', 1, 5, 9.990, 1, x'A1B2C3D4');

create snapshot sp_sku_a for table test_mixed_diff sku_branch_a;

-- branch B converges cost to rounded decimal and toggles flags differently
update sku_branch_b
   set cost_per_unit = 11.000
 where sku_code = 'SKU001' and batch = 1;
update sku_branch_b
   set enabled = 1
 where sku_code = 'SKU002';
insert into sku_branch_b values('SKU001', 3, 1,  8.125, 0, x'11223344');

create snapshot sp_sku_b for table test_mixed_diff sku_branch_b;

-- @ignore:7
data branch diff sku_branch_a{snapshot='sp_sku_a'} against sku_branch_b{snapshot='sp_sku_b'};

drop snapshot sp_sku_a;
drop snapshot sp_sku_b;
drop snapshot sp_sku_base;
drop table sku_branch_a;
drop table sku_branch_b;
drop table sku_inventory;

-- case 3: compare diff when temporal primary key collides with float/text updates
create table sensor_events (event_time timestamp,location varchar(12),reading double,quality char(1),comments varchar(40),primary key (event_time, location));

insert into sensor_events values('2024-03-10 09:00:00', 'north', 19.5, 'A', 'baseline');
insert into sensor_events values('2024-03-10 09:05:00', 'north', 19.8, 'A', 'stable');
insert into sensor_events values('2024-03-10 09:00:00', 'south', 22.1, 'B', 'warmup');
insert into sensor_events values('2024-03-10 09:05:00', 'south', 21.7, 'B', 'oscillation');
insert into sensor_events values('2024-03-10 09:10:00', 'south', 21.5, 'A', 'steady');

create snapshot sp_sensor_base for table test_mixed_diff sensor_events;

data branch create table sensor_branch_live from sensor_events{snapshot='sp_sensor_base'};
data branch create table sensor_branch_model from sensor_events{snapshot='sp_sensor_base'};

-- live branch filters out noisy read and adjusts quality grades
update sensor_branch_live
   set quality = 'C', comments = 'manual override'
 where event_time = '2024-03-10 09:05:00' and location = 'north';
delete from sensor_branch_live
 where event_time = '2024-03-10 09:10:00' and location = 'south';

create snapshot sp_sensor_live for table test_mixed_diff sensor_branch_live;

-- model branch smooths readings and adds forward prediction
update sensor_branch_model
   set reading = 19.6, comments = 'smoothed'
 where event_time = '2024-03-10 09:05:00' and location = 'north';
insert into sensor_branch_model values('2024-03-10 09:10:00', 'north', 19.4, 'A', 'prediction');

create snapshot sp_sensor_model for table test_mixed_diff sensor_branch_model;

data branch diff sensor_branch_live{snapshot='sp_sensor_live'} against sensor_branch_model{snapshot='sp_sensor_model'};

drop snapshot sp_sensor_live;
drop snapshot sp_sensor_model;
drop snapshot sp_sensor_base;
drop table sensor_branch_live;
drop table sensor_branch_model;
drop table sensor_events;

drop database test_mixed_diff;
