-- HEX/BIT literals are unsigned integers in numeric context; casting or storing
-- them into DECIMAL(p,s) must keep that value at every scale.
select cast(x'41' as decimal(5,1)), cast(x'41' as decimal(10,2)), cast(x'41' as decimal(10,0)), cast(x'41' as decimal);
select cast(0x0100 as decimal(10,3)), cast(b'1000001' as decimal(6,2)), convert(x'41', decimal(5,1));
select cast(x'41' as decimal(30,2)), cast(x'41' as decimal(50,2));
select cast(x'ffffffffffffffff' as decimal(25,2));
select cast(x'41' as decimal(3,2));
drop database if exists hex_dec_scale;
create database hex_dec_scale;
use hex_dec_scale;
create table t (id int primary key, d decimal(10,2), u decimal(10,2) unsigned, w decimal(40,3));
insert into t values (1, x'96', x'96', x'96');
insert into t select 2, x'41', x'41', b'1000001';
select * from t order by id;
drop database hex_dec_scale;
