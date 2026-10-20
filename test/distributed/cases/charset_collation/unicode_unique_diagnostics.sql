-- Native Unicode comparison keys must never appear in public diagnostics.
drop database if exists unicode_unique_diagnostics;
create database unicode_unique_diagnostics;
use unicode_unique_diagnostics;

create table u_single(id int primary key, s varchar(30) collate utf8mb4_unicode_ci, unique key uk_named(s));
insert into u_single values(1,'Alpha');
insert into u_single values(2,'alpha');
insert ignore into u_single values(2,'alpha');
show warnings;
insert into u_single values(1,'Other');
select count(*) from u_single;

create table u_prefix(id int primary key, s varchar(30) collate utf8mb4_unicode_ci, unique key uk_prefix(s(3)));
insert into u_prefix values(1,'Alpha');
insert into u_prefix values(2,'alphaZ');
insert ignore into u_prefix values(2,'alphaZ');
show warnings;

create table u_composite(id int primary key, s varchar(30) collate utf8mb4_unicode_ci, n int, unique key uk_composite(s,n));
insert into u_composite values(1,'Alpha',7),(3,'Beta',8);
insert into u_composite values(2,'alpha',7);
update ignore u_composite set s='alpha',n=7 where id=3;
show warnings;
select id,s,n from u_composite order by id;

drop database unicode_unique_diagnostics;
