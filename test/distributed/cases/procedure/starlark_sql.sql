drop database if exists procedure_test;
create database procedure_test;
use procedure_test;

create table t(i int primary key, j int);

create or replace procedure sp_selvar(in varname varchar) language 'starlark'
$$
rs1, err = mo.sql("select 1")
rs2, err2 = mo.sql("select 2")
mo.setvar(varname, rs1[0][0] + rs2[0][0])
$$;

call sp_selvar('foo');
select @foo;

create or replace procedure sp_ins(in x int) language 'starlark'
$$
s = "insert into t values ({}, {})".format(x, x)
mo.sql(s)
$$;

create or replace procedure sp_ins2(in x int) language 'starlark'
$$
mo.sql("call sp_ins({})".format(x))
mo.sql("call sp_ins({})".format(x+1))
$$;

-- @bvt:issue#22165
create or replace procedure sp_ins2_sum(inout x int) language 'starlark'
$$
def tx(y):
    insA = "insert into t values ({}, {})".format(y, y)
    insB = "insert into t values ({}, {})".format(y+1, y+1)
    mo.sql(insA)
    mo.sql(insB)

tx(x)
res, err = mo.sql("select sum(i) from t")
out_x = int(res[0][0]) if err is None else -1
$$
;
-- @bvt:issue


call sp_ins(1); -- ok
call sp_ins(2); -- ok
select * from t;
call sp_ins(1); -- fail
select * from t;

call sp_ins2(10); -- ok;
select * from t;
call sp_ins2(9); -- fail;
-- @bvt:issue#22208
select * from t;
-- @bvt:issue

-- @bvt:issue#22165
set @v = 100;    
call sp_ins2_sum(@v); -- ok
select @v;
set @v = 100;    
call sp_ins2_sum(@v); 
select @v;
-- @bvt:issue

drop database if exists procedure_test;
