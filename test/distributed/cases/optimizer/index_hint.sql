drop database if exists mysql_compat_index_hint;
create database mysql_compat_index_hint;
use mysql_compat_index_hint;

create table t (
  id int primary key,
  a varchar(8) not null,
  b int not null,
  c int not null,
  status varchar(12),
  score int,
  dt datetime,
  key idx_a_b_c_id(a,b,c,id),
  key idx_b_a_id(b,a,id),
  key idx_status_dt(status,dt,id)
);

insert into t values
(2,'g02',2,14,'archived',22,'2024-03-03 02:00:00'),
(42,'g02',5,29,'archived',58,'2024-07-15 18:00:00'),
(81,'g01',7,37,'pending',83,'2024-10-26 09:00:00'),
(82,'g02',8,44,'archived',94,'2024-11-27 10:00:00'),
(112,'g02',1,42,'active',20,'2024-05-01 16:00:00'),
(152,'g02',4,4,'active',56,'2024-09-13 08:00:00'),
(155,'g05',7,25,'hold',89,'2024-12-16 11:00:00'),
(192,'g02',7,19,'active',92,'2024-01-25 00:00:00'),
(222,'g02',0,17,'archived',14,'2024-07-27 06:00:00'),
(232,'g02',10,34,'active',23,'2024-05-09 16:00:00');

select mo_ctl('dn', 'flush', 'mysql_compat_index_hint.t');
select Sleep(1);

-- @separator:table
-- @regex("Index Table Scan.*idx_b_a_id",true)
explain select id,a,b,c
from t use index(idx_b_a)
where b = 7 and a in ('g01','g03','g05')
order by a,id;

select id,a,b,c
from t use index(idx_b_a_id)
where b = 7 and a in ('g01','g03','g05')
order by a,id;

-- @separator:table
-- @regex("Index Table Scan",false)
explain select id,a,b,c
from t use index()
where b = 7 and a in ('g01','g03','g05')
order by a,id;

-- @separator:table
-- @regex("Index Table Scan.*idx_a_b_c_id",true)
explain select id,a,b,c
from t force index for order by(idx_a_b_c_id)
order by a,b,c,id
limit 8;

select id,a,b,c
from t force index for order by(idx_a_b_c_id)
order by a,b,c,id
limit 8;

-- @separator:table
-- @regex("Index Table Scan.*idx_a_b_c_id",true)
explain select a,count(*)
from t force index for group by(idx_a_b_c_id)
group by a;

-- @separator:table
-- @regex("Index Table Scan.*idx_a_b_c_id",false)
explain select a,count(*)
from t ignore index for group by(idx_a_b_c_id)
group by a;

-- @separator:table
-- @regex("Index Table Scan.*idx_a_b_c_id",false)
explain select id,a,b,c
from t ignore index(idx_a_b_c_id)
where a in ('g02','g04')
order by a,b,c,id
limit 8;

select count(*)
from t force index(idx_missing)
where a = 'g01';

create table owner_tasks (
  user_id int not null,
  session_id int not null,
  task_id int not null,
  status varchar(12),
  due int,
  policy varchar(12),
  score int,
  primary key(user_id, session_id, task_id),
  key idx_status_due_owner(status, due, user_id, session_id, task_id),
  key idx_policy_owner(policy, user_id, session_id, task_id),
  key idx_score_owner(score, user_id, session_id, task_id)
);

insert into owner_tasks values
(1,1,1,'active',10,'keep',10),
(1,1,2,'active',20,'keep',20),
(1,2,1,'active',30,'drop',30),
(2,1,1,'active',40,'keep',40),
(2,1,2,'done',50,'keep',20),
(2,2,1,'done',60,'drop',60);

select mo_ctl('dn', 'flush', 'mysql_compat_index_hint.owner_tasks');
select Sleep(1);

select count(*) from owner_tasks where status = 'active';
select count(*) from owner_tasks force index(idx_status_due_owner) where status = 'active';
select count(*) from owner_tasks ignore index(idx_status_due_owner) where status = 'active';
select count(*) from owner_tasks force index(idx_policy_owner) where policy = 'keep';
select count(*) from owner_tasks force index(idx_score_owner) where score = 20;

drop database mysql_compat_index_hint;
