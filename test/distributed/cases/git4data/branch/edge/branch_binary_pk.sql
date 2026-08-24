-- DATA BRANCH must preserve and sort fixed- and variable-length binary primary keys.
drop database if exists branch_binary_pk;
create database branch_binary_pk;
use branch_binary_pk;

create table binary_base(k binary(4) primary key, v int);
insert into binary_base values
  (x'00', 10),
  (x'0061', 20),
  (x'61', 30),
  (x'ff', 40);
data branch create table binary_branch from binary_base;
select hex(k) as k, v from binary_branch order by k;
update binary_branch set v = 31 where hex(k) = '61000000';
select v as binary_base_v from binary_base where hex(k) = '61000000';
select v as binary_branch_v from binary_branch where hex(k) = '61000000';

create table varbinary_base(k varbinary(8) primary key, v int);
insert into varbinary_base values
  (x'00', 10),
  (x'0001', 20),
  (x'61', 30),
  (x'ff', 40);
data branch create table varbinary_branch from varbinary_base;
select hex(k) as k, v from varbinary_branch order by k;
update varbinary_branch set v = 31 where k = x'61';
select v as varbinary_base_v from varbinary_base where k = x'61';
select v as varbinary_branch_v from varbinary_branch where k = x'61';

drop database branch_binary_pk;
