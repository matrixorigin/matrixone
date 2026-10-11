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

set @v = 100;
call sp_ins2_sum(@v); -- ok
select @v;
set @v = 100;    
call sp_ins2_sum(@v); 
select @v;

-- Background procedure sessions must preserve vector Top-K results (#29147).
set @saved_hnsw = @@experimental_hnsw_index;
set @saved_ivf = @@experimental_ivf_index;
set experimental_hnsw_index = 1;
set experimental_ivf_index = 1;
create table vec_hnsw(id bigint primary key, v vecf32(4));
insert into vec_hnsw values (1,'[0,0,0,0]'),(2,'[1,1,1,1]'),(3,'[5,5,5,5]'),(4,'[2,2,2,2]');
create index idx_hnsw using hnsw on vec_hnsw(v) op_type 'vector_l2_ops' m=4 ef_construction=16 ef_search=16;
-- @separator:table
-- @regex("Vector Index Scan on idx_hnsw", true)
explain select id from vec_hnsw order by l2_distance(v,'[0.9,0.9,0.9,0.9]') limit 2;
select id from vec_hnsw order by l2_distance(v,'[0.9,0.9,0.9,0.9]') limit 2;
create procedure p_hnsw() 'begin select id from vec_hnsw order by l2_distance(v,''[0.9,0.9,0.9,0.9]'') limit 2; end';
call p_hnsw();
create procedure sl_hnsw(out ids varchar, out error_text varchar) language 'starlark'
$$
rows, err = mo.sql("select id from vec_hnsw order by l2_distance(v,'[0.9,0.9,0.9,0.9]') limit 2")
out_ids = "{},{}".format(rows[0][0], rows[1][0]) if err == None else ""
out_error_text = "" if err == None else str(err)
$$;
call sl_hnsw(@ids, @error_text);
select @ids, @error_text;
create table vec_ivfflat(id bigint primary key, v vecf32(4));
insert into vec_ivfflat values (1,'[0,0,0,0]'),(2,'[1,1,1,1]'),(3,'[5,5,5,5]'),(4,'[2,2,2,2]');
create index idx_ivfflat using ivfflat on vec_ivfflat(v) lists=1 op_type 'vector_l2_ops';
-- @separator:table
-- @regex("Vector Index Scan", true)
explain select id from vec_ivfflat order by l2_distance(v,'[0.9,0.9,0.9,0.9]') limit 2;
select id from vec_ivfflat order by l2_distance(v,'[0.9,0.9,0.9,0.9]') limit 2;
create procedure p_ivfflat() 'begin select id from vec_ivfflat order by l2_distance(v,''[0.9,0.9,0.9,0.9]'') limit 2; end';
call p_ivfflat();
create procedure sl_ivfflat(out ids varchar, out error_text varchar) language 'starlark'
$$
rows, err = mo.sql("select id from vec_ivfflat order by l2_distance(v,'[0.9,0.9,0.9,0.9]') limit 2")
out_ids = "{},{}".format(rows[0][0], rows[1][0]) if err == None else ""
out_error_text = "" if err == None else str(err)
$$;
call sl_ivfflat(@ids, @error_text);
select @ids, @error_text;
set experimental_hnsw_index = @saved_hnsw;
set experimental_ivf_index = @saved_ivf;

drop database if exists procedure_test;
