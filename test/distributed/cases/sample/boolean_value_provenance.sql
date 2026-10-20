-- VALUES cells and SAMPLE grouping slots must keep their value producers.
drop database if exists boolean_value_provenance;
create database boolean_value_provenance;
use boolean_value_provenance;

values row(1=1),row(1=0) order by column_0;
select distinct p from (values row(1=1),row(1=0)) d(p) order by p;

create table bools(id int primary key, flag boolean, n int);
insert into bools values (1,true,7),(2,false,null),(3,null,0);

-- The omitted first grouping key must not supply the selected predicate.
select sample(id,1 rows) as id,n>0 as p from bools group by flag,n>0 order by id;
-- Reverse the stored/computed keys and check exact values, not just metadata.
select sample(id,1 rows) as id,cast(flag as signed) as b from bools group by n>0,flag order by id;
select sample(id,1 rows) as id,n>0 as p from bools group by flag,n+1,n>0 order by id;

prepare values_predicate from 'select p from (values row(1=1),row(1=0)) d(p) order by p';
execute values_predicate;
deallocate prepare values_predicate;
prepare sample_predicate from 'select sample(id,1 rows) as id,n>0 as p from bools group by flag,n>0 order by id';
execute sample_predicate;
deallocate prepare sample_predicate;

use mo_catalog;
drop database boolean_value_provenance;
