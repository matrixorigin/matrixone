drop database if exists test_null_diff;
create database test_null_diff;
use test_null_diff;

-- case 1: diff branches that keep toggling NULL across wide column types
create table customer_lifecycle (
    customer_id int,
    region varchar(12),
    loyalty_level tinyint,
    credit_limit decimal(12,2),
    credit_score int,
    preferred_channel varchar(12),
    risk_note varchar(40),
    last_contact timestamp,
    is_active tinyint,
    birthdate date,
    primary key(customer_id)
);

insert into customer_lifecycle values
(1001, 'east', 3, 8000.00, 720, 'app', 'stable usage', '2024-03-01 09:20:00', 1, '1990-02-10'),
(1002, 'north', null, null, null, null, 'missing bureau', null, 1, '1984-05-05'),
(1003, 'west', 2, 4500.00, 655, 'web', null, '2024-03-02 15:11:00', 0, null),
(1004, null, 1, 3200.50, 610, 'branch', 'watchlist', null, 1, '1992-09-01');

create snapshot sp_customer_base for table test_null_diff customer_lifecycle;

data branch create table customer_branch_alpha from customer_lifecycle{snapshot='sp_customer_base'};
data branch create table customer_branch_beta from customer_lifecycle{snapshot='sp_customer_base'};

update customer_branch_alpha
   set credit_score = null,
       risk_note = null,
       last_contact = null
 where customer_id = 1001;

update customer_branch_alpha
   set loyalty_level = 4,
       credit_limit = null,
       preferred_channel = 'sms',
       is_active = 0
 where customer_id = 1003;

delete from customer_branch_alpha
 where customer_id = 1004;

insert into customer_branch_alpha values
(1005, 'south', null, null, 580, null, 'manual review', '2024-03-04 08:00:00', 1, null);

create snapshot sp_customer_alpha for table test_null_diff customer_branch_alpha;

update customer_branch_beta
   set credit_limit = credit_limit + 500,
       preferred_channel = null,
       last_contact = '2024-03-03 12:00:00'
 where customer_id = 1001;

update customer_branch_beta
   set risk_note = 'reactivated',
       credit_score = 670
 where customer_id = 1003;

insert into customer_branch_beta values
(1006, 'central', 1, 2500.00, null, 'ivr', null, null, 1, '1995-10-12');

drop snapshot if exists sp_customer_beta;
create snapshot sp_customer_beta for table test_null_diff customer_branch_beta;

data branch diff customer_branch_alpha{snapshot='sp_customer_alpha'} against customer_branch_beta{snapshot='sp_customer_beta'};

drop snapshot sp_customer_alpha;
drop snapshot sp_customer_beta;
drop snapshot sp_customer_base;
drop table customer_branch_alpha;
drop table customer_branch_beta;
drop table customer_lifecycle;

-- case 2: diff on composite key inventory where NULL flips collide with numeric/temporal types
create table instrument_positions (
    account_id bigint,
    instrument_code char(5),
    bucket varchar(6),
    exposure double,
    delta decimal(10,4),
    margin_rate float,
    notes varchar(40),
    settle_date date,
    settle_time timestamp,
    primary key (account_id, instrument_code, bucket)
);

insert into instrument_positions values
(5001, 'FX001', 'spot', 120000.25, 0.4321, 0.12, 'hedged', '2024-03-01', '2024-03-01 16:00:00'),
(5001, 'FX002', 'swap', null, null, 0.05, null, null, null),
(5002, 'EQ100', 'beta', 8500.00, -0.1250, null, 'partial', '2024-03-02', null),
(5003, 'CB200', 'spot', 62000.00, 0.0050, 0.01, 'new issue', '2024-03-03', '2024-03-03 09:45:00');

drop snapshot if exists sp_position_base;
create snapshot sp_position_base for table test_null_diff instrument_positions;

data branch create table position_branch_live from instrument_positions{snapshot='sp_position_base'};
data branch create table position_branch_model from instrument_positions{snapshot='sp_position_base'};

update position_branch_live
   set exposure = null,
       margin_rate = null,
       notes = 'awaiting quote'
 where account_id = 5001 and instrument_code = 'FX001' and bucket = 'spot';

update position_branch_live
   set delta = null,
       notes = null
 where account_id = 5002 and instrument_code = 'EQ100' and bucket = 'beta';

delete from position_branch_live
 where account_id = 5003 and instrument_code = 'CB200' and bucket = 'spot';

insert into position_branch_live values
(5004, 'IR300', 'swap', null, 0.2500, null, 'pilot', '2024-03-05', null);

drop snapshot if exists sp_position_live;
create snapshot sp_position_live for table test_null_diff position_branch_live;

update position_branch_model
   set exposure = 125000.75,
       margin_rate = 0.15,
       notes = null
 where account_id = 5001 and instrument_code = 'FX001' and bucket = 'spot';

update position_branch_model
   set settle_date = '2024-03-04',
       settle_time = '2024-03-04 10:00:00'
 where account_id = 5002 and instrument_code = 'EQ100' and bucket = 'beta';

insert into position_branch_model values
(5005, 'EQ200', 'spot', 41000.00, null, 0.08, 'hedge-ext', null, null);

drop snapshot if exists sp_position_model;
create snapshot sp_position_model for table test_null_diff position_branch_model;

data branch diff position_branch_live{snapshot='sp_position_live'} against position_branch_model{snapshot='sp_position_model'};

drop snapshot sp_position_live;
drop snapshot sp_position_model;
drop snapshot sp_position_base;
drop table position_branch_live;
drop table position_branch_model;
drop table instrument_positions;

drop database test_null_diff;
