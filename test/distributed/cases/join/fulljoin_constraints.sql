-- =============================================================
-- FULL OUTER JOIN coverage of column / table constraints
--
-- Verifies that FULL OUTER JOIN still produces correct results
-- when participating tables carry:
--   - PRIMARY KEY  (single / composite)
--   - UNIQUE KEY   (single / composite)
--   - NOT NULL + DEFAULT
--   - AUTO_INCREMENT
--   - CHECK (parsed; MO does not enforce, but should not break join)
--   - ON UPDATE CURRENT_TIMESTAMP
--   - FOREIGN KEY (incl. ON DELETE / ON UPDATE actions)
--   - Generated / computed columns
--   - Secondary (non-unique) indexes
-- =============================================================

drop database if exists fulljoin_cons_db;
create database fulljoin_cons_db;
use fulljoin_cons_db;

-- -------------------------------------------------------------
-- 1. PRIMARY KEY (single + composite) on both sides
-- -------------------------------------------------------------
drop table if exists pk_a;
drop table if exists pk_b;
create table pk_a(id int primary key, v varchar(10));
create table pk_b(id int primary key, v varchar(10));
insert into pk_a values (1,'a1'),(2,'a2'),(3,'a3');
insert into pk_b values (2,'b2'),(3,'b3'),(4,'b4');
select * from pk_a full outer join pk_b on pk_a.id = pk_b.id order by pk_a.id, pk_b.id;

drop table if exists ck_a;
drop table if exists ck_b;
create table ck_a(k1 int, k2 int, v varchar(10), primary key(k1,k2));
create table ck_b(k1 int, k2 int, v varchar(10), primary key(k1,k2));
insert into ck_a values (1,1,'a11'),(1,2,'a12'),(2,1,'a21');
insert into ck_b values (1,2,'b12'),(2,1,'b21'),(2,2,'b22');
select * from ck_a full outer join ck_b on ck_a.k1 = ck_b.k1 and ck_a.k2 = ck_b.k2
order by ck_a.k1, ck_a.k2, ck_b.k1, ck_b.k2;

-- -------------------------------------------------------------
-- 2. UNIQUE KEY (single + composite)
-- -------------------------------------------------------------
drop table if exists uq_a;
drop table if exists uq_b;
create table uq_a(id int, u int unique, v varchar(5));
create table uq_b(id int, u int unique, v varchar(5));
insert into uq_a values (1,10,'a'),(2,20,'b'),(3,30,'c');
insert into uq_b values (4,20,'B'),(5,30,'C'),(6,40,'D');
select * from uq_a full outer join uq_b on uq_a.u = uq_b.u order by uq_a.u, uq_b.u;

drop table if exists cu_a;
drop table if exists cu_b;
create table cu_a(id int, a int, b int, v varchar(5), unique key uk_ab(a,b));
create table cu_b(id int, a int, b int, v varchar(5), unique key uk_ab(a,b));
insert into cu_a values (1,1,1,'a'),(2,1,2,'b'),(3,2,1,'c');
insert into cu_b values (1,1,2,'B'),(2,2,1,'C'),(3,2,2,'D');
select * from cu_a full outer join cu_b on cu_a.a=cu_b.a and cu_a.b=cu_b.b
order by cu_a.a, cu_a.b, cu_b.a, cu_b.b;

-- -------------------------------------------------------------
-- 3. NOT NULL + DEFAULT
-- -------------------------------------------------------------
drop table if exists nn_a;
drop table if exists nn_b;
create table nn_a(id int primary key, status varchar(10) not null default 'new', cnt int not null default 0);
create table nn_b(id int primary key, note varchar(10) not null default 'n/a');
insert into nn_a(id) values (1),(2),(3);
insert into nn_a(id, status, cnt) values (4,'done',10);
insert into nn_b(id) values (2),(3),(5);
insert into nn_b(id, note) values (6,'hi');
-- NOT NULL columns on one side become NULL after FULL JOIN padding — verify
select * from nn_a full outer join nn_b on nn_a.id = nn_b.id order by nn_a.id, nn_b.id;

-- -------------------------------------------------------------
-- 4. AUTO_INCREMENT
-- -------------------------------------------------------------
drop table if exists ai_a;
drop table if exists ai_b;
create table ai_a(id int primary key auto_increment, v varchar(10));
create table ai_b(id int primary key auto_increment, v varchar(10));
insert into ai_a(v) values ('a1'),('a2'),('a3');
insert into ai_b(v) values ('b1'),('b2'),('b3'),('b4');
select * from ai_a full outer join ai_b on ai_a.id = ai_b.id order by ai_a.id, ai_b.id;

-- -------------------------------------------------------------
-- 5. CHECK constraint (parsed, not enforced — must not break join)
-- -------------------------------------------------------------
drop table if exists ck_c1;
drop table if exists ck_c2;
create table ck_c1(id int primary key, age int check(age >= 0), v varchar(5));
create table ck_c2(id int primary key, age int check(age < 200), v varchar(5));
insert into ck_c1 values (1,20,'a'),(2,30,'b'),(3,40,'c');
insert into ck_c2 values (2,30,'B'),(4,50,'D'),(5,60,'E');
select * from ck_c1 full outer join ck_c2 on ck_c1.id = ck_c2.id order by ck_c1.id, ck_c2.id;
select * from ck_c1 full outer join ck_c2 on ck_c1.age = ck_c2.age order by ck_c1.age, ck_c2.age;

-- -------------------------------------------------------------
-- 6. ON UPDATE CURRENT_TIMESTAMP + default NOW()
-- -------------------------------------------------------------
drop table if exists ts_a;
drop table if exists ts_b;
create table ts_a(
    id int primary key,
    created datetime default now(),
    updated timestamp default current_timestamp on update current_timestamp,
    v varchar(10)
);
create table ts_b(
    id int primary key,
    updated timestamp default current_timestamp on update current_timestamp,
    v varchar(10)
);
insert into ts_a(id, v) values (1,'a1'),(2,'a2'),(3,'a3');
insert into ts_b(id, v) values (2,'b2'),(3,'b3'),(4,'b4');
-- Don't select timestamp columns directly (they vary); validate join + NULL padding
select ts_a.id, ts_a.v, ts_b.id, ts_b.v,
       ts_a.updated is null as l_upd_null,
       ts_b.updated is null as r_upd_null
from ts_a full outer join ts_b on ts_a.id = ts_b.id
order by ts_a.id, ts_b.id;

-- -------------------------------------------------------------
-- 7. FOREIGN KEY (with ON DELETE / ON UPDATE actions)
-- -------------------------------------------------------------
drop table if exists fk_child;
drop table if exists fk_parent;
create table fk_parent(id int primary key, name varchar(10));
create table fk_child(
    cid  int primary key auto_increment,
    pid  int,
    info varchar(10),
    foreign key (pid) references fk_parent(id) on delete cascade on update cascade
);
insert into fk_parent values (1,'p1'),(2,'p2'),(3,'p3');
insert into fk_child(pid, info) values (1,'c1a'),(1,'c1b'),(2,'c2'),(NULL,'orphan');

-- parent-anchored FULL JOIN: parents with no children and children pointing to NULL
select fk_parent.id p_id, fk_parent.name,
       fk_child.cid, fk_child.pid, fk_child.info
from fk_parent full outer join fk_child on fk_parent.id = fk_child.pid
order by fk_parent.id, fk_child.cid;

-- -------------------------------------------------------------
-- 8. Generated / computed columns as join key
-- -------------------------------------------------------------
drop table if exists g1;
drop table if exists g2;
create table g1(a int, b int, c int as (a+b));
create table g2(a int, b int, c int as (a+b));
insert into g1(a,b) values (1,2),(3,4),(5,6);
insert into g2(a,b) values (3,4),(10,10),(5,6);
-- Join on the generated column
select g1.a l_a, g1.b l_b, g1.c l_c, g2.a r_a, g2.b r_b, g2.c r_c
from g1 full outer join g2 on g1.c = g2.c order by g1.c, g2.c;

-- -------------------------------------------------------------
-- 9. Secondary (non-unique) index on the join key
-- -------------------------------------------------------------
drop table if exists sx_a;
drop table if exists sx_b;
create table sx_a(id int primary key, k int, v varchar(5), key idx_k(k));
create table sx_b(id int primary key, k int, v varchar(5), key idx_k(k));
insert into sx_a values (1,10,'a1'),(2,10,'a2'),(3,20,'a3'),(4,30,'a4');
insert into sx_b values (1,10,'b1'),(2,20,'b2'),(3,20,'b3'),(4,40,'b4');
select * from sx_a full outer join sx_b on sx_a.k = sx_b.k
order by sx_a.k, sx_a.id, sx_b.k, sx_b.id;

-- -------------------------------------------------------------
-- 10. Combined: PK + UK + FK + NOT NULL + DEFAULT + CHECK + INDEX
-- -------------------------------------------------------------
drop table if exists combo_orders;
drop table if exists combo_items;
drop table if exists combo_users;
create table combo_users(
    uid int primary key,
    email varchar(50) not null unique,
    status enum('active','disabled') not null default 'active'
);
create table combo_orders(
    oid int primary key auto_increment,
    uid int not null,
    amt decimal(10,2) not null check(amt >= 0),
    created datetime default now(),
    key idx_uid(uid),
    foreign key(uid) references combo_users(uid)
);
create table combo_items(
    iid int primary key auto_increment,
    oid int not null,
    sku varchar(20) not null,
    qty int not null default 1 check(qty > 0),
    unique key uk_oid_sku(oid, sku),
    foreign key(oid) references combo_orders(oid)
);
insert into combo_users values (1,'u1@x','active'),(2,'u2@x','active'),(3,'u3@x','disabled');
insert into combo_orders(uid, amt) values (1,10.0),(1,20.0),(2,5.5);
insert into combo_items(oid, sku, qty) values (1,'s1',1),(1,'s2',2),(3,'s3',1);

-- user <-FULL-> orders (some users have no orders, every order has a user)
select combo_users.uid, combo_users.email, combo_orders.oid, combo_orders.amt
from combo_users full outer join combo_orders on combo_users.uid = combo_orders.uid
order by combo_users.uid, combo_orders.oid;

-- orders <-FULL-> items with WHERE on a NOT NULL column of right side
select combo_orders.oid, combo_orders.amt, combo_items.iid, combo_items.sku, combo_items.qty
from combo_orders full outer join combo_items on combo_orders.oid = combo_items.oid
order by combo_orders.oid, combo_items.iid;

-- Triple FULL JOIN across all three constrained tables
select combo_users.uid, combo_orders.oid, combo_items.iid, combo_items.sku
from combo_users
     full outer join combo_orders on combo_users.uid  = combo_orders.uid
     full outer join combo_items  on combo_orders.oid = combo_items.oid
order by combo_users.uid, combo_orders.oid, combo_items.iid;

-- cleanup
drop table if exists combo_items;
drop table if exists combo_orders;
drop table if exists combo_users;
drop table if exists fk_child;
drop table if exists fk_parent;
drop database if exists fulljoin_cons_db;
