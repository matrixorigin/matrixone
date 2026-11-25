-- Necessary key domains must preserve exact composite joins, NULLs and duplicates.
drop database if exists local_domain_bvt;
create database local_domain_bvt;
use local_domain_bvt;
set @local_domain_saved_hints=@@session.optimizer_hints;
create table dimension(id int primary key, label varchar(20));
create table pairs(part int, supplier int, primary key(part,supplier));
create table facts(part int, supplier int, payload int);
insert into dimension values (1,'keep'),(2,'drop'),(3,'keep');
insert into pairs values (1,10),(1,20),(2,30),(3,40);
insert into facts values (1,10,11),(1,10,12),(1,20,13),(1,30,14),
                        (2,30,15),(null,10,16),(1,null,17),(99,10,18),(3,40,19);

select f.payload from facts f
join pairs p on f.part=p.part and f.supplier=p.supplier
join dimension d on p.part=d.id
where d.label like '%keep%' order by f.payload;

-- The optimization-disabled control has the same duplicate and NULL semantics.
set session optimizer_hints='runtimeFilter=1';
select f.payload from facts f
join pairs p on f.part=p.part and f.supplier=p.supplier
join dimension d on p.part=d.id
where d.label like '%keep%' order by f.payload;
set session optimizer_hints=@local_domain_saved_hints;

-- No joinable source keys may produce no result, never a partial-source DROP.
select f.payload from facts f
join pairs p on f.part=p.part and f.supplier=p.supplier
join dimension d on p.part=d.id
where d.label like '%absent%' order by f.payload;

-- A source limit cannot be crossed by necessary-domain propagation.
select f.payload from facts f
join pairs p on f.part=p.part and f.supplier=p.supplier
join (select id from dimension where label like '%keep%' order by id limit 1) d
on p.part=d.id order by f.payload;

-- A preserved-side row must survive even when its key is outside the domain.
select f.payload,d.id from facts f left join dimension d
on f.part=d.id and d.label like '%keep%'
where f.payload in (14,15,16,19) order by f.payload;
set session optimizer_hints=@local_domain_saved_hints;
drop database local_domain_bvt;
