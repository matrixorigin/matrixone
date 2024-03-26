create table vtab1(id int primary key auto_increment,`vecf32` vecf32(0));
create table vtab1(id int primary key auto_increment,`vecf32` vecf32(-1));
create table vtab2(a int primary key,b vecf32(3) primary key,c vecf64(3));
create table vtab32(id int primary key auto_increment,`vecf32_3` vecf32(3),`vecf32_5` vecf32(5));
create table vtab64(id int primary key auto_increment,`vecf64_3` vecf64(3),`vecf64_5` vecf64(5));

insert into vtab32(vecf32_3,vecf32_5) values("[0.8166459,NULL,0.4886152]",NULL);
insert into vtab32(vecf32_3,vecf32_5) values(NULL,NULL);
insert into vtab32(vecf32_3,vecf32_5) values("[0.8166459,0.66616553,0.4886152]",NULL);
insert into vtab32(vecf32_3,vecf32_5) values("0.1726299,3.29088557,30.4330937","0.1726299,3.29088557,30.4330937");
insert into vtab32(vecf32_3,vecf32_5) values ("[0.1726299,3.29088557,30.4330937]","[0.45052445,2.19845265,9.579752,123.48039162,4635.89423394]");
insert into vtab32(vecf32_3,vecf32_5) values ("[8.5606893,6.7903588,821.977768]","[0.46323407,23.49801546,563.9229458,56.07673508,8732.9583881]");
insert into vtab64(vecf64_3,vecf64_5) values("[0.8166459,NULL,0.4886152]",NULL);
insert into vtab64(vecf64_3,vecf64_5) values(NULL,NULL);
insert into vtab64(vecf64_3,vecf64_5) values("[0.8166459,0.66616553,0.4886152]",NULL);
insert into vtab64(vecf64_3,vecf64_5) values ("[8.5606893,6.7903588,821.977768]","[0.46323407,23.49801546,563.9229458,56.07673508,8732.9583881]");
insert into vtab64(vecf64_3,vecf64_5) values ("[0.9260021,0.26637346,0.06567037]","[0.45756745,65.2996871,321.623636,3.60082066,87.58445764]");

select * from vtab32;
select * from vtab32 where vecf32_3 > "[1,1,1]";
select * from vtab32 where vecf32_3 >= "[0.8166459, 0.66616553, 0.4886152]";
select * from vtab32 where vecf32_5 <= "[0.45052445,2.19845265,9.579752,123.48039162,4635.89423394]";
select * from vtab32 where vecf32_5 <= "[0.4505244500,002.19845265,9.579752,123.48039162,4635.89423394]";
select * from vtab32 where vecf32_5 < "[0.45052445,02.19845265,9.579752,123.48039162,4635.89423394]";
select * from vtab32 order by vecf32_3;
select * from vtab64 order by vecf64_5 desc;
select vecf32_3,count(*) from vtab32 group by vecf32_3 order by vecf32_3;
select * from vtab64;
select * from vtab32 where id = 1;
select * from vtab32 where vecf32_3 = "[0.1726299,3.29088557,30.4330937]";
select * from vtab32 where vecf32_3 is null;
select * from vtab32 where vecf32_3 is not null;

select * from vtab64 where id = 2;
select * from vtab64 where vecf64_5 = "[0.46323407,23.49801546,563.9229458,56.07673508,8732.9583881]";
select * from vtab64 where vecf64_5 is null;
select * from vtab64 where vecf64_5 is not null;

select a.id,a.vecf32_3, a.vecf32_5, b.id, b.vecf64_3, b.vecf64_5 from vtab32 a, vtab64 b where a.id = b.id;

select * from vtab64 join vtab32 on vtab64.vecf32_3 = vtab32.vecf32_3;

select * from vtab32 left join vtab64 on vtab64.vecf32_5 = vtab32.vecf32_5;
select count(*), SUMMATION(vecf32_3) from vtab32 group by SUMMATION(vecf32_3) order by SUMMATION(vecf32_3);



select id,vecf32_3 + vecf32_3, vecf32_5 + vecf32_5 from vtab32;
select id,vecf32_3 + "[1,1,1]", vecf32_5 + vecf32_5 from vtab32;
select id,vecf32_3 - vecf32_3, vecf32_5 - vecf32_5 from vtab32;
select id,vecf32_3 * vecf32_3, vecf32_5 * vecf32_5 from vtab32;
select id,vecf32_3 / vecf32_3, vecf32_5 / vecf32_5 from vtab32;

select id,vecf64_3 + vecf64_3, vecf64_5 + vecf64_5 from vtab64;
select id,vecf64_3 + "[1,1,1]", vecf64_5 + vecf64_5 from vtab64;
select id,vecf64_3 - vecf64_3, vecf64_5 - vecf64_5 from vtab64;
select id,vecf64_3 * vecf64_3, vecf64_5 * vecf64_5 from vtab64;
select id,vecf64_3 / vecf64_3, vecf64_5 / vecf64_5 from vtab64;

select a.id,a.vecf32_3 + b.vecf64_3, a.vecf32_5 + b.vecf64_5 from vtab32 a, vtab64 b where a.id = b.id;

select a.id,a.vecf32_3 + b.vecf64_3, a.vecf32_5 + b.vecf64_5 from vtab32 a, vtab64 b where a.vecf32_3 is not null;

select a.id,a.vecf32_3 + b.vecf64_3, a.vecf32_5 + b.vecf64_5 from vtab32 a, vtab64 b where a.vecf32_3 is null;

update vtab32 set vecf32_3 = '0.752072,0.41402978,0.058849692' where id = 1;
select * from vtab32;

update vtab64 set vecf64_3 = NULL where id = 1;
select * from vtab32;

update vtab32 set vecf32_3 = '[5.33816308, 78.35970277, 982.7183621]' where id = 1;
select vecf32_3 from vtab32;

update vtab64 set vecf64_5 = '[43234.87576795,32.8673464,12312.5788535,0.30837893,22.21907276]';
select vecf64_5 from vtab64;


delete from vtab32 where vecf32_3 = '[5.33816308, 78.35970277, 982.7183621]';

create table vtab64_1(id int primary key auto_increment,`vecf64_3` vecf64(3),`vecf64_5` vecf64(5));
load data infile '$resources/vector/vector.csv' into table vtab64_1 fields terminated by ',';
select * from vtab64_1;
(select * from vtab64) except (select * from vtab64_1);

create table vector_exp_01(c1 int,c2 vecf32(5),c3 tinyint unsigned,c4 bigint,c5 decimal(4,2),c6 float,c7 double);
insert into vector_exp_01 values(10 ,"[1, 0, 1, 6, 6]",3,10,7.1,0.36,2.10),(60,"[6, 0, 8, 10,129]",2,5,3.26,4.89,1.26),(20,"[ 9, 18, 1, 4, 132]",6,1,9.36,6.9,5.6);
insert into vector_exp_01 values(34, "[995.3, 0.005, 0, 85.36, 5.12]",30,52,61.1,9.36,20.10),(102,"[50.01, 4.417, 930.21, 20.14, 15.02]",90,120,30.1,74.36,72.10);
select c2 from vector_exp_01;
select c2+2364 from vector_exp_01;
select c2-4 from vector_exp_01;
select c2*25 from vector_exp_01;
select c2/5 from vector_exp_01;
select c2/0 from vector_exp_01;

select c2+63.2548 from vector_exp_01;
select c2-0.1258 from vector_exp_01;
select c2*2.698 from vector_exp_01;
select c2/2.41 from vector_exp_01;
select c2/0.0 from vector_exp_01;
-- vector +/-* int
select c2+c1 from vector_exp_01;
select c2-c1 from vector_exp_01;
select c2*c1 from vector_exp_01;
select c2/c1 from vector_exp_01;
-- vector +/-* tinyint unsigned
-- @bvt:issue#15115
select c2+c3 from vector_exp_01;
select c2-c3 from vector_exp_01;
select c2*c3 from vector_exp_01;
select c2/c3 from vector_exp_01;
-- @bvt:issue
-- vector +/-* bigint
select c2+c4 from vector_exp_01;
select c2-c4 from vector_exp_01;
select c2*c4 from vector_exp_01;
select c2/c4 from vector_exp_01;
-- vector +/-* decimal
select c2+c5 from vector_exp_01;
select c2-c5 from vector_exp_01;
select c2*c5 from vector_exp_01;
select c2/c5 from vector_exp_01;
-- vector +/-* double and float
select c2+c6,c2+c7 from vector_exp_01;
select c2-c6,c2-c7 from vector_exp_01;
select c2*c6,c2*c7 from vector_exp_01;
select c2/c6,c2/c7 from vector_exp_01;

select cast("[76875768584509877574546435800000005,8955885757767774774774774456466]" as vecf32(2)) *623;

