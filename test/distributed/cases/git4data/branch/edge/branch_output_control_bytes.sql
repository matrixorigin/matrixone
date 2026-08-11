-- DATA BRANCH DIFF OUTPUT AS must preserve every control byte that is emitted raw.
drop database if exists br_output_control_bytes;
create database br_output_control_bytes;
use br_output_control_bytes;

create table base(a int primary key, v varchar(64), txt text);
insert into base values (1, 'base', 'base');

data branch create table control_left from base;
data branch create table control_right from base;
update control_left set
  v = concat(
    char(1), char(2), char(3), char(4), char(5), char(6), char(7),
    char(11), char(12),
    char(14), char(15), char(16), char(17), char(18), char(19), char(20),
    char(21), char(22), char(23), char(24), char(25),
    char(27), char(28), char(29), char(30), char(31), char(127)
  ),
  txt = concat(
    char(1), char(2), char(3), char(4), char(5), char(6), char(7),
    char(11), char(12),
    char(14), char(15), char(16), char(17), char(18), char(19), char(20),
    char(21), char(22), char(23), char(24), char(25),
    char(27), char(28), char(29), char(30), char(31), char(127)
  )
where a = 1;

data branch diff control_left against control_right output as control_diff_output;
select __mo_diff_source, __mo_diff_flag, hex(v) as v_hex, hex(txt) as txt_hex
from control_diff_output;

drop database br_output_control_bytes;
