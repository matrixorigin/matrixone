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

select SUMMATION(vecf32_3),SUMMATION(vecf32_5) from vtab32;
select SUMMATION(vecf64_3),SUMMATION(vecf64_5) from vtab64;

select SUMMATION("[0.45052445,2.19845265,9.579752]");
select SUMMATION(1);
select SUMMATION(NULL);
select id, SUMMATION(vecf32_3) from vtab32 group by id order by id;
select id, count(vecf32_3) from vtab32 group by id having id > 1 order by id ;
select count(*), SUMMATION(vecf32_3) from vtab32 group by SUMMATION(vecf32_3) Having SUMMATION(vecf32_3) > 33.89660960435867 order by SUMMATION(vecf32_3) desc ;
select * from vtab32 where SUMMATION(vecf32_3) is null;
select * from vtab32 where SUMMATION(vecf32_3) = 33.89660960435867;
select * from vtab32 where SUMMATION(vecf32_3) > 33.89660960435867;
select distinct(SUMMATION(vecf32_3)) from vtab32;
select sum(SUMMATION(vecf32_3)) from vtab32;
select min(SUMMATION(vecf32_3)) from vtab32;
select max(SUMMATION(vecf32_3)) from vtab32;
select avg(SUMMATION(vecf32_3)) from vtab32;
select count(SUMMATION(vecf32_3)) from vtab32;
select abs(SUMMATION(vecf64_3)) from vtab64;
select atan(SUMMATION(vecf64_3)) from vtab64;
select SUMMATION(vecf32_3) - SUMMATION(vecf32_5) from vtab32;
select SUMMATION(vecf32_3) * SUMMATION(vecf32_5) from vtab32;
select SUMMATION(vecf64_3) + SUMMATION(vecf64_5) from vtab64;
select SUMMATION(vecf64_3) / SUMMATION(vecf64_5) from vtab64;
select * from (select SUMMATION(vecf32_3),SUMMATION(vecf32_5) from vtab32);
select SUMMATION(vecf64_3),SUMMATION(vecf64_5) from (select * from vtab64);
WITH qn AS (select SUMMATION(vecf32_3),SUMMATION(vecf32_5) from vtab32) SELECT * FROM qn;



select inner_product(vecf32_3,"[1,1,1]") from vtab32;
select inner_product(vecf32_3,"[0,0,-1]") from vtab32;
select inner_product(vecf32_3,vecf32_3), inner_product(vecf32_5,vecf32_5) from vtab32;
select inner_product(vecf64_3,vecf64_3), inner_product(vecf64_5,vecf64_5) from vtab64;
select inner_product("[0.45052445,2.19845265,9.579752]","[1,1,1]");
select inner_product(1,1);
select inner_product(NULL,NULL);
select id, inner_product(vecf32_3,vecf32_3) from vtab32 group by id order by id;
select count(*), inner_product(vecf32_3,vecf32_3) from vtab32 group by inner_product(vecf32_3,vecf32_3) Having inner_product(vecf32_3,vecf32_3) > 937.0329415976587 order by inner_product(vecf32_3,vecf32_3) desc ;
select * from vtab32 where inner_product(vecf32_3,vecf32_3) is null;
select * from vtab32 where inner_product(vecf32_3,vecf32_3) = 675766.8704508307;
select * from vtab32 where inner_product(vecf32_3,vecf32_3) <= 1.3494319015309593;
select distinct(inner_product(vecf32_3,vecf32_3)) from vtab32;
select sum(inner_product(vecf32_3,vecf32_3)) from vtab32;
select min(inner_product(vecf32_5,vecf32_5)) from vtab32;
select max(inner_product(vecf32_3,vecf32_3)) from vtab32;
select avg(inner_product(vecf32_5,vecf32_5)) from vtab32;
select count(inner_product(vecf32_5,vecf32_5)) from vtab32;
select sin(inner_product(vecf64_3,vecf64_3)) from vtab64;
select cos(inner_product(vecf64_5,vecf64_5)) from vtab64;
select inner_product(vecf32_3,vecf32_3) - inner_product(vecf32_5,vecf32_5) from vtab32;
select inner_product(vecf32_3,vecf32_3) * inner_product(vecf32_5,vecf32_5) from vtab32;
select inner_product(vecf64_3,vecf64_3) + inner_product(vecf64_5,vecf64_5) from vtab64;
select inner_product(vecf64_3,vecf64_3) / inner_product(vecf64_5,vecf64_5) from vtab64;
select * from (select inner_product(vecf32_3,vecf32_3),inner_product(vecf32_5,vecf32_5) from vtab32);
select inner_product(vecf64_3,vecf64_3), inner_product(vecf64_5,vecf64_5) from (select * from vtab64);
WITH qn AS (select inner_product(vecf32_3,vecf32_3),inner_product(vecf32_5,vecf32_5) from vtab32) SELECT * FROM qn;



select l1_norm(vecf32_3), l1_norm(vecf32_5) from vtab32;
select l1_norm(vecf64_3), l1_norm(vecf64_5) from vtab64;
select l1_norm(vecf64_3 - vecf32_3),l1_norm(vecf64_5 - vecf32_5) from vtab32 a, vtab64 b where a.id = b.id;
select l1_norm(NULL);
select l1_norm(1);
select l1_norm("[1,2,3]");
select count(*), l1_norm(vecf32_3) from vtab32 group by l1_norm(vecf32_3) Having l1_norm(vecf32_3) > 1.9714266657829285 order by l1_norm(vecf32_3) desc ;
select * from vtab32 where l1_norm(vecf32_3) is null;
select * from vtab32 where l1_norm(vecf32_3) = 1.9714266657829285;
select * from vtab32 where l1_norm(vecf32_3) > 1.9714266657829285;
select distinct(l1_norm(vecf32_3)) from vtab32;
select sum(l1_norm(vecf32_3)) from vtab32;
select min(l1_norm(vecf32_3)) from vtab32;
select max(l1_norm(vecf32_3)) from vtab32;
select avg(l1_norm(vecf32_3)) from vtab32;
select count(l1_norm(vecf32_3)) from vtab32;
select abs(l1_norm(vecf64_3)) from vtab64;
select atan(l1_norm(vecf64_3)) from vtab64;
select l1_norm(vecf32_3) - l1_norm(vecf32_5) from vtab32;
select l1_norm(vecf32_3) * l1_norm(vecf32_5) from vtab32;
select l1_norm(vecf64_3) + l1_norm(vecf64_5) from vtab64;
select l1_norm(vecf64_3) / l1_norm(vecf64_5) from vtab64;
select * from (select l1_norm(vecf32_3),l1_norm(vecf32_5) from vtab32);
select l1_norm(vecf64_3),l1_norm(vecf64_5) from (select * from vtab64);
WITH qn AS (select l1_norm(vecf32_3),l1_norm(vecf32_5) from vtab32) SELECT * FROM qn;




select l2_norm(vecf32_3), l2_norm(vecf32_5) from vtab32;
select l2_norm(vecf64_3), l2_norm(vecf64_5) from vtab64;
select l2_norm(vecf64_3 - vecf32_3),l1_norm(vecf64_5 - vecf32_5) from vtab32 a, vtab64 b where a.id = b.id;
select l2_norm(NULL);
select l2_norm(1);
select l2_norm("[1,2,3]");
select count(*), l2_norm(vecf32_3) from vtab32 group by l2_norm(vecf32_3) Having l2_norm(vecf32_3) > 1.1616505074810406 order by l2_norm(vecf32_3) desc ;
select * from vtab32 where l2_norm(vecf32_3) is null;
select * from vtab32 where l2_norm(vecf32_3) = 1.1616505074810406;
select * from vtab32 where l2_norm(vecf32_3) > 1.1616505074810406;
select distinct(l2_norm(vecf32_3)) from vtab32;
select sum(l2_norm(vecf32_3)) from vtab32;
select min(l2_norm(vecf32_3)) from vtab32;
select max(l2_norm(vecf32_3)) from vtab32;
select avg(l2_norm(vecf32_3)) from vtab32;
select count(l2_norm(vecf32_3)) from vtab32;
select abs(l2_norm(vecf64_3)) from vtab64;
select atan(l2_norm(vecf64_3)) from vtab64;
select l2_norm(vecf32_3) - l2_norm(vecf32_5) from vtab32;
select l2_norm(vecf32_3) * l2_norm(vecf32_5) from vtab32;
select l2_norm(vecf64_3) + l2_norm(vecf64_5) from vtab64;
select l2_norm(vecf64_3) / l2_norm(vecf64_5) from vtab64;
select * from (select l2_norm(vecf32_3),l2_norm(vecf32_5) from vtab32);
select l2_norm(vecf64_3),l2_norm(vecf64_5) from (select * from vtab64);
WITH qn AS (select l2_norm(vecf32_3),l2_norm(vecf32_5) from vtab32) SELECT * FROM qn;

select vector_dims(vecf32_5),vector_dims(vecf32_3) from vtab32;
select vector_dims(vecf64_5),vector_dims(vecf64_3) from vtab64;
select vector_dims(vecf64_3 - vecf32_3),vector_dims(vecf64_5 - vecf32_5) from vtab32 a, vtab64 b where a.id = b.id;
select vector_dims(NULL);
select vector_dims(1);
select vector_dims("[1,2,3]");
select count(*), vector_dims(vecf32_3) from vtab32 group by vector_dims(vecf32_3) Having vector_dims(vecf32_3) > 1  order by vector_dims(vecf32_3) desc ;
select * from vtab32 where vector_dims(vecf32_3) is null;
select * from vtab32 where vector_dims(vecf32_3) = 3;
select * from vtab32 where vector_dims(vecf32_3) > 3;
select distinct(vector_dims(vecf32_3)) from vtab32;
select sum(vector_dims(vecf32_3)) from vtab32;
select min(vector_dims(vecf32_3)) from vtab32;
select max(vector_dims(vecf32_3)) from vtab32;
select avg(vector_dims(vecf32_3)) from vtab32;
select count(vector_dims(vecf32_3)) from vtab32;
select abs(vector_dims(vecf64_3)) from vtab64;
select atan(vector_dims(vecf64_3)) from vtab64;
select vector_dims(vecf32_3) - vector_dims(vecf32_5) from vtab32;
select vector_dims(vecf32_3) * vector_dims(vecf32_5) from vtab32;
select vector_dims(vecf64_3) + vector_dims(vecf64_5) from vtab64;
select vector_dims(vecf64_3) / vector_dims(vecf64_5) from vtab64;
select * from (select vector_dims(vecf32_3),vector_dims(vecf32_5) from vtab32);
select vector_dims(vecf64_3),vector_dims(vecf64_5) from (select * from vtab64);
WITH qn AS (select vector_dims(vecf32_3),vector_dims(vecf32_5) from vtab32) SELECT * FROM qn;



create table vtab32_1(id int primary key auto_increment,`vecf32_3` vecf32(3),`vecf32_3_1` vecf32(3),`vecf32_5` vecf32(5),`vecf32_5_1` vecf32(5));
create table vtab64_1(id int primary key auto_increment,`vecf64_3` vecf64(3),`vecf64_3_1` vecf64(3),`vecf64_5` vecf64(5),`vecf64_5_1` vecf64(5));

insert into vtab32_1(vecf32_3,vecf32_5) values("[0.8166459,NULL,0.4886152]",NULL);
insert into vtab32_1(vecf32_3,vecf32_5) values(NULL,NULL);
insert into vtab32_1(vecf32_3,vecf32_5) values("[0.8166459,0.66616553,0.4886152]",NULL);
insert into vtab32_1(vecf32_3,vecf32_5) values("0.1726299,3.29088557,30.4330937","0.1726299,3.29088557,30.4330937");
insert into vtab32_1(vecf32_3,vecf32_5) values ("[0.1726299,3.29088557,30.4330937]","[0.45052445,2.19845265,9.579752,123.48039162,4635.89423394]");
insert into vtab32_1(vecf32_3,vecf32_5) values ("[8.5606893,6.7903588,821.977768]","[0.46323407,23.49801546,563.9229458,56.07673508,8732.9583881]");
insert into vtab64_1(vecf64_3,vecf64_5) values("[0.8166459,NULL,0.4886152]",NULL);
insert into vtab64_1(vecf64_3,vecf64_5) values(NULL,NULL);
insert into vtab64_1(vecf64_3,vecf64_5) values("[0.8166459,0.66616553,0.4886152]",NULL);
insert into vtab64_1(vecf64_3,vecf64_5) values ("[8.5606893,6.7903588,821.977768]","[0.46323407,23.49801546,563.9229458,56.07673508,8732.9583881]");
insert into vtab64_1(vecf64_3,vecf64_5) values ("[0.9260021,0.26637346,0.06567037]","[0.45756745,65.2996871,321.623636,3.60082066,87.58445764]");
update vtab32_1 set `vecf32_3_1` = `vecf32_3` + "[7.326893,10.787382173,21.32132143]";
update vtab32_1 set `vecf32_5_1` = `vecf32_5` + "[7.326893,10.787382173,21.32132143,4.32132,98.321321]";
update vtab64_1 set `vecf64_3_1` = `vecf64_3` + "[32.89849324,1.98392832,192.095843]";
update vtab64_1 set `vecf64_5_1` = `vecf64_5` + "[32.89849324,1.98392832,192.095843,90.321321,23.12312321]";

select cosine_similarity(vecf32_3,"[1,1,1]") from vtab32;
select cosine_similarity(vecf32_3,"[0,0,-1]") from vtab32;
select cosine_similarity(a.vecf32_3,b.vecf64_3), cosine_similarity(a.vecf32_5,b.vecf64_5) from vtab32 a , vtab64 b where a.id = b.id;
select cosine_similarity(b.vecf64_3, a.vecf32_3), cosine_similarity(b.vecf64_5, a.vecf32_5) from vtab32 a , vtab64 b where a.id = b.id;
select cosine_similarity("[0.45052445,2.19845265,9.579752]","[1,1,1]");
select cosine_similarity(1,1);
select cosine_similarity(NULL,NULL);
select count(*), cosine_similarity(a.vecf32_3,b.vecf64_3) from vtab32 a , vtab64 b where a.id = b.id group by cosine_similarity(a.vecf32_3,b.vecf64_3) HAVING cosine_similarity(a.vecf32_3,b.vecf64_3) > 0.3 order by cosine_similarity(a.vecf32_3,b.vecf64_3) desc ;

select cosine_similarity(vecf32_3,vecf32_3_1) from vtab32_1;
select cosine_similarity(vecf32_5,vecf32_5_1) from vtab32_1;
select cosine_similarity(vecf64_3,vecf64_3_1) from vtab64_1;
select cosine_similarity(vecf64_5,vecf64_5_1) from vtab64_1;

select * from vtab32_1 where cosine_similarity(vecf32_3_1,vecf32_3) is null;
select * from vtab32_1 where cosine_similarity(vecf32_3,vecf32_3_1) = 0.9788139235276682;
select * from vtab32_1 where cosine_similarity(vecf32_3,vecf32_3) <= 0.9788139235276682;
select distinct(cosine_similarity(vecf32_3,vecf32_3_1)) from vtab32_1;
select sum(cosine_similarity(vecf32_3,vecf32_3_1)) from vtab32_1;
select min(cosine_similarity(vecf32_5,vecf32_5_1)) from vtab32_1;
select max(cosine_similarity(vecf32_3,vecf32_3_1)) from vtab32_1;
select avg(cosine_similarity(vecf32_5,vecf32_5_1)) from vtab32_1;
select count(cosine_similarity(vecf32_5,vecf32_5_1)) from vtab32_1;
select sin(cosine_similarity(vecf64_3,vecf64_3_1)) from vtab64_1;
select cos(cosine_similarity(vecf64_5,vecf64_5_1)) from vtab64_1;
select cosine_similarity(vecf32_3,vecf32_3_1) - cosine_similarity(vecf32_5,vecf32_5_1) from vtab32_1;
select cosine_similarity(vecf32_3,vecf32_3_1) * cosine_similarity(vecf32_5,vecf32_5_1) from vtab32_1;
select cosine_similarity(vecf64_3,vecf64_3_1) + cosine_similarity(vecf64_5,vecf64_5_1) from vtab64_1;
select cosine_similarity(vecf64_3,vecf64_3_1) / cosine_similarity(vecf64_5,vecf64_5_1) from vtab64_1;
select * from (select cosine_similarity(vecf32_3,vecf32_3_1),cosine_similarity(vecf32_5,vecf32_5_1) from vtab32_1);
select cosine_similarity(vecf64_3,vecf64_3_1), cosine_similarity(vecf64_5,vecf64_5_1) from (select * from vtab64_1);
WITH qn AS (select cosine_similarity(vecf32_3,vecf32_3_1),cosine_similarity(vecf32_5,vecf32_5_1) from vtab32_1) SELECT * FROM qn;
select cosine_similarity(vecf32_3,vecf32_3), cosine_similarity(vecf32_5,vecf32_5) from vtab32;
select cosine_similarity(vecf64_3,vecf64_3), cosine_similarity(vecf64_5,vecf64_5) from vtab64;

-- subvector function
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",-1) as sv ;
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",4) ;
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",9) ;
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",10) ;
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",0) ;
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",1) ;
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",NULL) ;
select subvector(NULL,4);
select subvector(NULL,NULL);
select subvector("abc",4);
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",3.4) ;
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",5,null) ;
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",5,0) ;
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",5,1) ;
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",5,3) ;
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",5,10) ;
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",6,5) ;
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",6,-2) ;
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",6,"3") ;
select subvector(NULL,3,4);
select subvector("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0]",NULL,4);

create table vector_01(c1 int,c2 vecf32(128));
insert into vector_01 values(10 ,"[1, 0, 1, 6, 6, 17, 47, 39, 2, 0, 1, 25, 27, 10, 56, 130, 18, 5, 2, 6, 15, 2, 19, 130, 42, 28, 1, 1, 2, 1, 0, 5, 0, 2, 4, 4, 31, 34, 44, 35, 9, 3, 8, 11, 33, 12, 61, 130, 130, 17, 0, 1, 6, 2, 9, 130, 111, 36, 0, 0, 11, 9, 1, 12, 2, 100, 130, 28, 7, 2, 6, 7, 9, 27, 130, 83, 5, 0, 1, 18, 130, 130, 84, 9, 0, 0, 2, 24, 111, 24, 0, 1, 37, 24, 2, 10, 12, 62, 33, 3, 0, 0, 0, 1, 3, 16, 106, 28, 0, 0, 0, 0, 17, 46, 85, 10, 0, 0, 1, 4, 11, 4, 2, 2, 9, 14, 8, 8]"),(60,"[0, 1, 1, 3, 0, 3, 46, 20, 1, 4, 17, 9, 1, 17, 108, 15, 0, 3, 37, 17, 6, 15, 116, 16, 6, 1, 4, 7, 7, 7, 9, 6, 0, 8, 10, 4, 26, 129, 27, 9, 0, 0, 5, 2, 11, 129, 129, 12, 103, 4, 0, 0, 2, 31, 129, 129, 94, 4, 0, 0, 0, 3, 13, 42, 0, 15, 38, 2, 70, 129, 1, 0, 5, 10, 40, 12, 74, 129, 6, 1, 129, 39, 6, 1, 2, 22, 9, 33, 122, 13, 0, 0, 0, 0, 5, 23, 4, 11, 9, 12, 45, 38, 1, 0, 0, 4, 36, 38, 57, 32, 0, 0, 82, 22, 9, 5, 13, 11, 3, 94, 35, 3, 0, 0, 0, 1, 16, 97]"),(9776,"[10, 3, 8, 5, 48, 26, 5, 16, 17, 0, 0, 2, 132, 53, 1, 16, 112, 6, 0, 0, 7, 2, 1, 48, 48, 15, 18, 31, 3, 0, 0, 9, 6, 10, 19, 27, 50, 46, 17, 9, 18, 1, 4, 48, 132, 23, 3, 5, 132, 9, 4, 3, 11, 0, 2, 46, 84, 12, 10, 10, 1, 0, 12, 76, 26, 22, 16, 26, 35, 15, 3, 16, 15, 1, 51, 132, 125, 8, 1, 2, 132, 51, 67, 91, 8, 0, 0, 30, 126, 39, 32, 38, 4, 0, 1, 12, 24, 2, 2, 2, 4, 7, 2, 19, 93, 19, 70, 92, 2, 3, 1, 21, 36, 58, 132, 94, 0, 0, 0, 0, 21, 25, 57, 48, 1, 0, 0, 1]");
insert into vector_01 values(34, " [16, 15, 0, 0, 5, 46, 5, 5, 4, 0, 0, 0, 28, 118, 12, 5, 75, 44, 5, 0, 6, 32, 6, 49, 41, 74, 9, 1, 0, 0, 0, 9, 1, 9, 16, 41, 71, 80, 3, 0, 0, 4, 3, 5, 51, 106, 11, 3, 112, 28, 13, 1, 4, 8, 3, 104, 118, 14, 1, 1, 0, 0, 0, 88, 3, 27, 46, 118, 108, 49, 2, 0, 1, 46, 118, 118, 27, 12, 0, 0, 33, 118, 118, 8, 0, 0, 0, 4, 118, 95, 40, 0, 0, 0, 1, 11, 27, 38, 12, 12, 18, 29, 3, 2, 13, 30, 94, 78, 30, 19, 9, 3, 31, 45, 70, 42, 15, 1, 3, 12, 14, 22, 16, 2, 3, 17, 24, 13]"),(102,"[41, 0, 0, 7, 1, 1, 20, 67, 9, 0, 0, 0, 0, 31, 120, 61, 25, 0, 0, 0, 0, 10, 120, 90, 32, 0, 0, 1, 13, 11, 22, 50, 4, 0, 2, 93, 40, 15, 37, 18, 12, 2, 2, 19, 8, 44, 120, 25, 120, 5, 0, 0, 0, 2, 48, 97, 102, 14, 3, 3, 11, 9, 34, 41, 0, 0, 4, 120, 56, 3, 4, 5, 6, 15, 37, 116, 28, 0, 0, 3, 120, 120, 24, 6, 2, 0, 1, 28, 53, 90, 51, 11, 11, 2, 12, 14, 8, 6, 4, 30, 9, 1, 4, 22, 25, 79, 120, 66, 5, 0, 0, 6, 42, 120, 91, 43, 15, 2, 4, 39, 12, 9, 9, 12, 15, 5, 24, 36]");
select subvector(c2,20,5) from vector_01;
select subvector(c2,120) as sv from vector_01;
select subvector(c2,-120) from vector_01;
select subvector(c2,-120,2) from vector_01;
select subvector(c2,-129,2) from vector_01;
select subvector(c2,4,-5) from vector_01;
select subvector(c2,NULL,5) from vector_01;
select subvector(c2,8,"a") from vector_01;
select subvector(c2,NULL,NULL) from vector_01;
select subvector(c2,c1,5) from vector_01;
select subvector(c2,50,c1) from vector_01;
select subvector(c2,50,cast("4" as int)) from vector_01 where c1>80;

select subvector(c2,50,cast("4" as int)),subvector(c2,50,cast("4" as int))*3.5 from vector_01 where c1>80;
select subvector(c2,50,5),subvector(c2,50,5) + cast("[4.0,0.82,0.09,3.8,2.98]" as vecf32(5)) from vector_01;

-- Vector & Scalar Arithemetic

