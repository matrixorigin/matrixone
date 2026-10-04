drop database if exists issue27581;
drop database if exists issue27581_caller;
create database issue27581;
create database issue27581_caller;
use issue27581;

create sequence seq1 start with 10;
create table t1(n int);
insert into t1 values (11);
create view seq_view as select nextval('seq1') as n;
create view curr_view as select currval('seq1') as n;
create view set_view as select setval('seq1', '50') as n;
create view set_false_view as select setval('seq1', '70', false) as n;
create view table_view as select n from t1;

use issue27581_caller;
create sequence seq1 start with 100;
select * from issue27581.set_view;
select * from issue27581.seq_view;
select * from issue27581.curr_view;
select nextval('seq1') as caller_n;
select * from issue27581.set_false_view;
select * from issue27581.seq_view;
select nextval('seq1') as caller_n;
select * from issue27581.table_view;

drop database issue27581;
drop database issue27581_caller;
