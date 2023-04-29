create table fake_pk(id int);
insert into fake_pk(id) values(1);
select * from fake_pk order by id;
insert into fake_pk(id) values(2);
select * from fake_pk order by id;
insert into fake_pk values(3);
select * from fake_pk order by id;
update fake_pk set id = 11 where id = 1;
select * from fake_pk order by id;
delete from fake_pk where id = 1;
select * from fake_pk order by id;


create table fake_pk_with_cluster_by(id int, v int) cluster by (id);
insert into fake_pk_with_cluster_by values(1,1);
select * from fake_pk_with_cluster_by order by id;
update fake_pk_with_cluster_by set v = 10 where id = 1;
select * from fake_pk_with_cluster_by order by id;

create table fake_pk_with_multi_cluster_by(id int, v int) cluster by (id, v);
insert into fake_pk_with_multi_cluster_by values(1,1);
select * from fake_pk_with_multi_cluster_by order by id;
update fake_pk_with_multi_cluster_by set v = 10 where id = 1;
select * from fake_pk_with_multi_cluster_by order by id;
