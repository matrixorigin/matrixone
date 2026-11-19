-- Issue #30615/#30604: a mo-dump style restore interleaves CREATE and INSERT
-- with foreign_key_checks disabled. After re-enabling checks, every foreign
-- key in the cyclic pair must be enforced on both the child and the parent
-- side; SHOW CREATE output alone is not enforcement evidence.
drop database if exists i30615_dump;
create database i30615_dump;
use i30615_dump;
SET foreign_key_checks=0;
CREATE TABLE b (id INT PRIMARY KEY, a_id INT, CONSTRAINT fk_b FOREIGN KEY (a_id) REFERENCES a(id) ON DELETE RESTRICT ON UPDATE RESTRICT);
INSERT INTO b VALUES (2,1);
CREATE TABLE a (id INT PRIMARY KEY, b_id INT, CONSTRAINT fk_a FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE RESTRICT ON UPDATE RESTRICT);
INSERT INTO a VALUES (1,2);
SET foreign_key_checks=1;
-- ON UPDATE RESTRICT: mutating either parent key must be rejected.
update i30615_dump.b set id=3 where id=2;
update i30615_dump.a set id=7 where id=1;
-- ON DELETE RESTRICT: removing either parent row must be rejected.
delete from i30615_dump.b where id=2;
delete from i30615_dump.a where id=1;
-- child-side inserts that would orphan a row must be rejected as well.
insert into i30615_dump.a values (9,999);
insert into i30615_dump.b values (8,888);
select * from i30615_dump.a order by id;
select * from i30615_dump.b order by id;
select count(*) from i30615_dump.a a left join i30615_dump.b b on a.b_id=b.id where a.b_id is not null and b.id is null;
select count(*) from i30615_dump.b b left join i30615_dump.a a on b.a_id=a.id where b.a_id is not null and a.id is null;

-- The schema-first ordering must behave identically.
drop database if exists i30615_sf;
create database i30615_sf;
use i30615_sf;
SET foreign_key_checks=0;
CREATE TABLE b (id INT PRIMARY KEY, a_id INT, CONSTRAINT fk_b FOREIGN KEY (a_id) REFERENCES a(id) ON DELETE RESTRICT ON UPDATE RESTRICT);
CREATE TABLE a (id INT PRIMARY KEY, b_id INT, CONSTRAINT fk_a FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE RESTRICT ON UPDATE RESTRICT);
INSERT INTO b VALUES (2,null);
INSERT INTO a VALUES (1,2);
UPDATE b SET a_id=1 WHERE id=2;
SET foreign_key_checks=1;
update i30615_sf.b set id=3 where id=2;
update i30615_sf.a set id=7 where id=1;
delete from i30615_sf.b where id=2;
select * from i30615_sf.a order by id;
select * from i30615_sf.b order by id;

-- The parent created with checks enabled while a child forward-references it
-- must still install the parent-side enforcement for its own foreign key.
drop database if exists i30615_bind;
create database i30615_bind;
use i30615_bind;
SET foreign_key_checks=0;
CREATE TABLE b (id INT PRIMARY KEY, a_id INT, CONSTRAINT fk_b FOREIGN KEY (a_id) REFERENCES a(id) ON DELETE RESTRICT ON UPDATE RESTRICT);
SET foreign_key_checks=1;
CREATE TABLE a (id INT PRIMARY KEY, b_id INT, CONSTRAINT fk_a FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE RESTRICT ON UPDATE RESTRICT);
INSERT INTO b VALUES (2,null);
INSERT INTO a VALUES (1,2);
UPDATE b SET a_id=1 WHERE id=2;
update i30615_bind.b set id=3 where id=2;
update i30615_bind.a set id=7 where id=1;
delete from i30615_bind.b where id=2;
delete from i30615_bind.a where id=1;
insert into i30615_bind.a values (9,999);
select * from i30615_bind.a order by id;
select * from i30615_bind.b order by id;

-- A plain (non cyclic) table pair created under disabled checks keeps full
-- parent-side enforcement as well.
drop database if exists i30615_pair;
create database i30615_pair;
use i30615_pair;
SET foreign_key_checks=0;
CREATE TABLE p (id INT PRIMARY KEY);
CREATE TABLE c (id INT PRIMARY KEY, pid INT, CONSTRAINT fk_pc FOREIGN KEY (pid) REFERENCES p(id) ON DELETE RESTRICT ON UPDATE RESTRICT);
INSERT INTO p VALUES (1);
INSERT INTO c VALUES (1,1);
SET foreign_key_checks=1;
update i30615_pair.p set id=5 where id=1;
delete from i30615_pair.p where id=1;
insert into i30615_pair.c values (2,9);
select * from i30615_pair.p order by id;
select * from i30615_pair.c order by id;

drop database i30615_dump;
drop database i30615_sf;
drop database i30615_bind;
drop database i30615_pair;
