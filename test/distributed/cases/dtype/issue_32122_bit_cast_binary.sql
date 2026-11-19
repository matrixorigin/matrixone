-- issue #32122: explicit CAST(BIT AS BINARY)/(BINARY(n)) encodes exactly
-- ceil(declaredWidth/8) big-endian bytes before target truncation/padding.
drop database if exists issue_32122;
create database issue_32122;
use issue_32122;
create table t(id int primary key, b bit(64));
insert into t values (1,258),(2,255),(3,0),(4,null),(5,18446744073709551615);
select id, length(b), hex(cast(b as binary)), hex(cast(b as binary(8))) from t order by id;
create table w(id int primary key, b1 bit(1), b8 bit(8), b9 bit(9), b16 bit(16), b64 bit(64));
insert into w values (1, 1, 255, 258, 258, 258);
select id, hex(cast(b1 as binary)), hex(cast(b8 as binary)), hex(cast(b9 as binary)), hex(cast(b16 as binary)), hex(cast(b64 as binary)) from w order by id;
select id, hex(cast(b64 as binary(4))), hex(cast(b64 as binary(2))), hex(cast(b64 as binary(16))), hex(cast(b64 as varbinary(4))) from w order by id;
prepare p1 from 'select hex(cast(b as binary)) from t where id = ?';
set @id = 1;
execute p1 using @id;
deallocate prepare p1;
select id, cast(b as unsigned) from t order by id;
create view vb as select id, cast(b as unsigned) u from t;
select id, u+0 from vb order by id;
drop database issue_32122;
