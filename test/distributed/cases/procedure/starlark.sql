drop database if exists procedure_test;
create database procedure_test;
use procedure_test;

--
-- most basic starlark stored procedure tests.
--

drop procedure if exists sp_empty;
create or replace procedure sp_empty () language 'starlark'
'';

call sp_empty();
call sp_empty('foo');

-- @bvt:issue#22165
create or replace procedure sp_empty () language 'starlark'
'
def add(x, y):
    return x + y

foo = add(1, 2)
';

call sp_empty();
call sp_empty('foo');
-- @bvt:issue


create procedure sp_badparam(in mo int) language 'starlark'
'';

create procedure sp_badparam(in mo.foo int) language 'starlark'
'';

create procedure sp_badparam(inout out_foo int) language 'starlark'
'';

create or replace procedure sp_x2(in i int, out o int) language 'starlark'
'
out_o = i + i
';

call sp_x2(1, @result);
select @result;

call sp_x2(5, @result);
select @result;

create or replace procedure sp_iox2(inout io int) language 'starlark'
'
out_io = io + io
'
;

set @ioparam = 1;
call sp_iox2(@ioparam);
select @ioparam;
call sp_iox2(@ioparam);
select @ioparam;

create or replace procedure sp_quote(in s varchar, out qs varchar) language 'starlark'
'
out_qs = mo.quote(s)
';

call sp_quote('''', @qs);
select @qs;
call sp_quote($$foo'bar''zoo$$, @qs2);
select @qs2;

create or replace procedure sp_jq(in jq varchar, in data varchar, out jqresult varchar) language 'starlark'
'   
out_jqresult = mo.jq(jq, data)
';

call sp_jq('.0 + .1', '[1, 2]', @res);
select @res;
call sp_jq('.[0] + .[1]', '[1, 2]', @res);
select @res;

drop database if exists procedure_test;
