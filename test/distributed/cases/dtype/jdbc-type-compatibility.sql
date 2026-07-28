
create table t1(a BOOL, b bool, c boolean);
insert into t1 values(true, true, true);
insert into t1 values(false, false, false);

-- @meta_cmp(true)
select * from t1;