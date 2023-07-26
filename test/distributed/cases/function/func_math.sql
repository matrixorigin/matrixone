-- @suite
-- @setup
drop table if exists t1;
create table t1 (id int,d int,fl float,de decimal);
insert into t1 values(1,1,123.0213,1.001);
insert into t1 values(2,2,1.0213,1.001);
insert into t1 values(3,null,null,null);
insert into t1 values(4,0,0,0);

-- @case
-- @desc:test for sinh,atan,acos,cot,tan,sin
-- @label:bvt
select sinh(d),sinh(fl)from t1;
select atan(d),atan(fl)from t1;
select acos(d),acos(fl)from t1;

select cot(d),cot(fl) from t1;
select cot(d),cot(fl) from t1 where d <> 0;

select tan(d),tan(fl) from t1;
select sin(d),sin(fl) from t1;


-- @suite
-- @setup
drop table if exists abs;
create table abs(id int,d int,dl double,fl float,de decimal);
insert into abs values(1,-10,-10,-10.0321,-10.312);
insert into abs values(2,-2,-2,-2.0321,-2.3765);
insert into abs values(3,-10,-18446744073709551614,-10.0321,-10.312);
insert into abs values(4,-31232,-9223372036854775808,-1312.0321,-973.3072);
insert into abs values(1,-6,-432432.43,-8756.4321,-356.421);
insert into abs values(1,null,null,null,null);

-- @case
-- @desc:test for abs
-- @label:bvt
select abs(d),abs(dl),abs(fl) from abs;
select abs(d)-2 from t1;
select abs(d)*2 from t1;
select abs(tan(d))*2 from t1;


-- @suite
-- @setup
-- @bvt:issue#10748
drop table if exists ceil;
create table ceil(id int,d int,dl double,fl float,de decimal);
insert into ceil values(1,5,5,-5.5,-5.5);
insert into ceil values(2,-2,18446744073709551614,-2.5,-5.2);
insert into ceil values(2,-1,18446744073709551614,1.23,-1.23);
insert into ceil values(2,-1,1844674407370955161,1.23,-1.23);
insert into ceil values(2,-1,-9223372036854775808,1.23,-1.23);
insert into ceil values(2,-1,-184467440737095516,1.23,-1.23);
insert into ceil values(2,-1,-922337203685477580,1.23,-1.23);
insert into ceil values(2,-1,-922337203685477580,1.23,-1.23);
insert into ceil values(2,-1,-99999999999999999.9,1.23,-1.23);
insert into ceil values(2,-1,-99999999999999999.9,1.23,-1.23);

-- @case
-- @desc:test for abs
-- @label:bvt
select ceil(d),ceil(dl),ceil(fl) from ceil;


drop table t1;
drop table ceil;
drop table abs;
-- @bvt:issue
