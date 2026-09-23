-- repeated execution of a prepared update join must not hang and applies each round
drop database if exists prep_update_join;
create database prep_update_join;
use prep_update_join;

create table parent(pid int primary key, code varchar(16) unique);
create table acct(
tenant int not null,
acct_id int not null,
status varchar(16),
amount decimal(12,2),
parent_id int,
primary key(tenant, acct_id),
key idx_status_amount(status, amount),
key idx_parent_status(parent_id, status),
constraint fk_acct_parent foreign key(parent_id) references parent(pid)
);
insert into parent values (1,'p1'),(2,'p2');
insert into acct values (1,101,'open',10.50,1),(2,201,'open',30.75,2);

prepare s1 from 'update acct a join parent p on a.parent_id=p.pid set a.amount=a.amount+?, a.status=? where p.code=? and a.status=?';
set @delta1='3.33';
set @delta2='4.44';
set @closed='closed';
set @open='open';
set @code1='p1';
set @code2='p2';
execute s1 using @delta1, @closed, @code1, @open;
execute s1 using @delta2, @closed, @code2, @open;
select tenant, acct_id, status, amount, parent_id from acct order by tenant;

update acct set status='open';
execute s1 using @delta1, @closed, @code1, @open;
execute s1 using @delta2, @closed, @code2, @open;
select tenant, acct_id, status, amount, parent_id from acct order by tenant;

deallocate prepare s1;

-- TIME assignment casts must stay at the write boundary rather than entering the join result mapping
create table temporal_dst(id int primary key, tm time(6));
create table temporal_src(k int primary key);
insert into temporal_dst values(1, '00:00:01');
insert into temporal_src values(10);
set @temporal_old_sql_mode=@@session.sql_mode;
set session sql_mode='STRICT_TRANS_TABLES';
prepare temporal_update from 'update temporal_dst d join temporal_src s on s.k=? set d.tm=? where d.id=?';
set @temporal_key=10;
set @temporal_value='02:03:04.000005';
set @temporal_id=1;
execute temporal_update using @temporal_key, @temporal_value, @temporal_id;
select id, cast(tm as char) as tm from temporal_dst;
set @temporal_value='838:59:59.000001';
execute temporal_update using @temporal_key, @temporal_value, @temporal_id;
select id, cast(tm as char) as tm from temporal_dst;
set @temporal_value='03:04:05.000006';
execute temporal_update using @temporal_key, @temporal_value, @temporal_id;
select id, cast(tm as char) as tm from temporal_dst;
deallocate prepare temporal_update;
set session sql_mode=@temporal_old_sql_mode;

drop database prep_update_join;
