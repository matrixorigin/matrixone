drop database if exists starlark_null_test;
create database starlark_null_test;
use starlark_null_test;

-- NULL arguments are None; zero and the empty string retain their types.
create procedure p_in(in i int, in s varchar, out value varchar) language 'starlark'
$$
out_value = "{}:{}:{}:{}".format(type(i), str(i), type(s), str(s))
$$;
call p_in(null, null, @slk_value);
select @slk_value;
call p_in(0, '', @slk_value);
select @slk_value;
call p_in(7, 'text', @slk_value);
select @slk_value;
set @slk_i = null;
set @slk_s = null;
call p_in(@slk_i, @slk_s, @slk_value);
select @slk_value;

-- INOUT accepts NULL and preserves None on the outbound binding.
create procedure p_inout(inout i int) language 'starlark'
$$
out_i = i
$$;
call p_inout(@slk_i);
select @slk_i is null;
set @slk_i = 7;
call p_inout(@slk_i);
select @slk_i;

-- getvar distinguishes NULL from the empty string without an error.
create procedure p_get(out value varchar) language 'starlark'
$$
v, err = mo.getvar("slk_s")
out_value = "{}:{}:{}".format(type(v), str(v), err == None)
$$;
call p_get(@slk_value);
select @slk_value;
set @slk_s = '';
call p_get(@slk_value);
select @slk_value;

-- NULL positions survive across rows; every non-NULL SQL cell stays a string.
create procedure p_sql(out value varchar) language 'starlark'
$$
rows, err = mo.sql("select a, b, c, d from (select 1 as ord, null as a, '' as b, 0 as c, 'text' as d union all select 2, 'text', 0, '', null) t order by ord")
out_value = str(rows) if err == None else "ERR:" + err
$$;
call p_sql(@slk_value);
select @slk_value;

-- Existing None -> SQL NULL conversion works for OUT and setvar.
create procedure p_out(out n varchar, out s varchar) language 'starlark'
$$
out_n = None
out_s = ""
mo.setvar("slk_set_null", None)
mo.setvar("slk_set_empty", "")
$$;
call p_out(@slk_n, @slk_s);
select @slk_n is null, @slk_s is null, @slk_s = '', @slk_set_null is null, @slk_set_empty is null, @slk_set_empty = '';

drop database starlark_null_test;
