drop database if exists diff_test;
create database diff_test;
use diff_test;

-- ============================================================
-- case 1: branch table vs original - update twice (revert value)
--   Tests: after surgical repair (value changed back), does DIFF
--   still report the row as UPDATED?
-- ============================================================
create table orders (id int primary key, price decimal(10,2), status varchar(20));
insert into orders values (1, 100.00, 'pending'), (2, 200.00, 'pending'), (3, 300.00, 'pending'), (4, 400.00, 'pending'), (5, 500.00, 'pending');
create snapshot sp_case1 for table diff_test orders;

-- clone a branch from snapshot
data branch create table orders_cloned from orders{snapshot="sp_case1"};

-- accident: update 3 rows on origin
update orders set price = 999.99 where id in (1, 2, 3);

-- diff: cloned (untouched) vs origin (modified)
data branch diff orders against orders_cloned;
data branch diff orders_cloned against orders;

-- repair: revert to original values
update orders set price = 100.00 where id = 1;
update orders set price = 200.00 where id = 2;
update orders set price = 300.00 where id = 3;

-- diff again: origin values are now same as cloned, should show no diffs
data branch diff orders against orders_cloned;
data branch diff orders_cloned against orders;

-- verify: current data vs snapshot data are identical
select * from orders order by id;
select * from orders{snapshot="sp_case1"} order by id;

drop snapshot sp_case1;
drop table orders;
drop table orders_cloned;

-- ============================================================
-- case 4: branch table vs original - merge cloned changes back into original
--   Same setup as case 1, but cloned also has a non-overlapping update.
--   Merge should apply the cloned-side update while preserving origin-side changes.
-- ============================================================
create table orders (id int primary key, price decimal(10,2), status varchar(20));
insert into orders values (1, 100.00, 'pending'), (2, 200.00, 'pending'), (3, 300.00, 'pending'), (4, 400.00, 'pending'), (5, 500.00, 'pending');
create snapshot sp_case4 for table diff_test orders;

data branch create table orders_cloned from orders{snapshot="sp_case4"};

update orders set price = 999.99 where id in (1, 2, 3);
update orders_cloned set status = 'branch_only' where id = 4;

data branch diff orders_cloned against orders;

data branch merge orders_cloned into orders;

data branch diff orders_cloned against orders;

select * from orders order by id;
select * from orders_cloned order by id;

drop snapshot sp_case4;
drop table orders;
drop table orders_cloned;

-- ============================================================
-- case 2: origin vs origin{snapshot} - update twice (revert value)
--   Tests: same-table DIFF. After repair, DIFF should show no
--   changes because values match the snapshot.
-- ============================================================
create table products (id int primary key, name varchar(50), stock int);
insert into products values (1, 'widget', 100), (2, 'gadget', 200), (3, 'gizmo', 300), (4, 'tool', 400);
create snapshot sp_case2 for table diff_test products;

-- accident: update 2 rows
update products set stock = 0 where id in (1, 2);

-- diff: origin vs snapshot
data branch diff products against products{snapshot="sp_case2"};

-- repair: revert values
update products set stock = 100 where id = 1;
update products set stock = 200 where id = 2;

-- diff again after repair: values restored, should show no diffs
data branch diff products against products{snapshot="sp_case2"};

-- verify: values match snapshot
select * from products order by id;
select * from products{snapshot="sp_case2"} order by id;

drop snapshot sp_case2;
drop table products;

-- ============================================================
-- case 3: snapshot source merge into current table
--   Snapshot has no source-side changes after the LCA. Merging it into
--   the current table is a no-op and should not rewind current changes.
-- ============================================================
create table inventory (id int primary key, sku varchar(20), qty int, location varchar(20));
insert into inventory values (1, 'A001', 50, 'east'), (2, 'A002', 60, 'west'), (3, 'A003', 70, 'north'), (4, 'A004', 80, 'south');
create snapshot sp_case3 for table diff_test inventory;

-- current table diverges from snapshot
update inventory set qty = 999 where id in (1, 2);

-- diff before merge: current-side updates are visible
data branch diff inventory{snapshot="sp_case3"} against inventory;

-- merging an unchanged snapshot source into current is a no-op
data branch merge inventory{snapshot="sp_case3"} into inventory;

-- after merge, current-side updates should remain visible
data branch diff inventory{snapshot="sp_case3"} against inventory;

select * from inventory order by id;
select * from inventory{snapshot="sp_case3"} order by id;

drop snapshot sp_case3;
drop table inventory;

drop database diff_test;
