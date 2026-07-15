-- @suit

-- @case
-- @desc: Prepared user variables preserve binary literal semantics.
-- @label:bvt
drop database if exists prepare_binary_param;
create database prepare_binary_param;
use prepare_binary_param;

create table indexed_values (
  id int primary key,
  b binary(4),
  label varchar(20),
  key idx_b (b)
);
create table scanned_values (
  id int primary key,
  b binary(4),
  label varchar(20)
);
insert into indexed_values values (1, 0x41420000, 'indexed'), (2, 0x43440000, 'other');
insert into scanned_values values (1, 0x41420000, 'scanned'), (2, 0x43440000, 'other');

prepare indexed_lookup from 'select id, hex(b), label from indexed_values where b = ? and label = ?';
prepare scanned_lookup from 'select id, hex(b), label from scanned_values where b = ? and label = ?';

set @binary_source = (0x41420000);
set @binary_value = @binary_source;
set @indexed_label = 'indexed';
set @scanned_label = 'scanned';
execute indexed_lookup using @binary_value, @indexed_label;
execute scanned_lookup using @binary_value, @scanned_label;

set @text_source = 'AB';
set @binary_value = @text_source;
execute indexed_lookup using @binary_value, @indexed_label;
execute scanned_lookup using @binary_value, @scanned_label;

set @binary_value = 0x41420000;
execute indexed_lookup using @binary_value, @indexed_label;
execute scanned_lookup using @binary_value, @scanned_label;

deallocate prepare indexed_lookup;
deallocate prepare scanned_lookup;
drop database prepare_binary_param;
