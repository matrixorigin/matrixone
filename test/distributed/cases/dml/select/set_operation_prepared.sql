-- @suite
-- @setup
drop database if exists set_operation_prepared;
create database set_operation_prepared;
use set_operation_prepared;
create table inputs (side int, k int, payload char(2));
insert into inputs values (0,1,'a'),(0,1,'a'),(0,2,'b'),(0,null,null),(1,1,'a'),(1,null,null);
set @left_side=0;
set @right_side=1;

-- 同一 PREPARE 模板跨越非空、空、再次非空的执行账户。
prepare numeric_minus from 'select k from (select k from inputs where side=? except select k from inputs where side=?) s order by k';
execute numeric_minus using @left_side,@right_side;
set @left_side=2;
execute numeric_minus using @left_side,@right_side;
set @left_side=0;
execute numeric_minus using @left_side,@right_side;
deallocate prepare numeric_minus;

prepare numeric_minus_all from 'select k from (select k from inputs where side=? except all select k from inputs where side=?) s order by k';
execute numeric_minus_all using @left_side,@right_side;
set @left_side=2;
execute numeric_minus_all using @left_side,@right_side;
set @left_side=0;
execute numeric_minus_all using @left_side,@right_side;
deallocate prepare numeric_minus_all;

prepare numeric_intersect from 'select k from (select k from inputs where side=? intersect select k from inputs where side=?) s order by k';
execute numeric_intersect using @left_side,@right_side;
set @left_side=2;
execute numeric_intersect using @left_side,@right_side;
set @left_side=0;
execute numeric_intersect using @left_side,@right_side;
deallocate prepare numeric_intersect;

prepare numeric_intersect_all from 'select k from (select k from inputs where side=? intersect all select k from inputs where side=?) s order by k';
execute numeric_intersect_all using @left_side,@right_side;
set @left_side=2;
execute numeric_intersect_all using @left_side,@right_side;
set @left_side=0;
execute numeric_intersect_all using @left_side,@right_side;
deallocate prepare numeric_intersect_all;

-- CHAR 的物理等值键执行器同样不能保留前一账户的结果内存。
prepare keyed_minus from 'select payload from (select payload from inputs where side=? except select payload from inputs where side=?) s order by payload';
execute keyed_minus using @left_side,@right_side;
set @left_side=2;
execute keyed_minus using @left_side,@right_side;
set @left_side=0;
execute keyed_minus using @left_side,@right_side;
deallocate prepare keyed_minus;

prepare keyed_minus_all from 'select payload from (select payload from inputs where side=? except all select payload from inputs where side=?) s order by payload';
execute keyed_minus_all using @left_side,@right_side;
set @left_side=2;
execute keyed_minus_all using @left_side,@right_side;
set @left_side=0;
execute keyed_minus_all using @left_side,@right_side;
deallocate prepare keyed_minus_all;

prepare keyed_intersect from 'select payload from (select payload from inputs where side=? intersect select payload from inputs where side=?) s order by payload';
execute keyed_intersect using @left_side,@right_side;
set @left_side=2;
execute keyed_intersect using @left_side,@right_side;
set @left_side=0;
execute keyed_intersect using @left_side,@right_side;
deallocate prepare keyed_intersect;

prepare keyed_intersect_all from 'select payload from (select payload from inputs where side=? intersect all select payload from inputs where side=?) s order by payload';
execute keyed_intersect_all using @left_side,@right_side;
set @left_side=2;
execute keyed_intersect_all using @left_side,@right_side;
set @left_side=0;
execute keyed_intersect_all using @left_side,@right_side;
deallocate prepare keyed_intersect_all;
drop database set_operation_prepared;
