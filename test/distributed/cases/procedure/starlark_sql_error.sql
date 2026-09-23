-- mo.sql reports a failure as a structured error: it still prints and
-- concatenates as the message, and additionally carries the MySQL error number
-- and SQLSTATE, so a procedure can branch on the error CLASS instead of
-- matching on message text.
--
-- NOTE: the bodies below use conditional EXPRESSIONS rather than if/else
-- blocks on purpose. mo-tester strips leading whitespace from each line, and
-- Starlark is indentation-sensitive, so an indented body reaches the server
-- dedented and fails to parse ("got identifier, want indent") -- see the
-- sp_ins2_sum golden in starlark_sql.result, which records exactly that.
drop database if exists slk_err;
create database slk_err;
use slk_err;

create table t (a int primary key, b varchar(10));
insert into t values (1, 'one');
create table plog (id int, note varchar(300));

create or replace procedure sp_err() language 'starlark'
$$
# The codes come from mo.errno()/mo.sqlstate(); the `ok` value itself is the
# plain message string it has always been, so rows 3-7 exercise the ordinary
# string operations a procedure written before the codes existed would use.
rs, err = mo.sql("insert into t values (1, 'dup')")
code = -1 if err == None else mo.errno()
state = "none" if err == None else mo.sqlstate()
isdup = False if err == None else mo.errno() == 1062
truthy = False if err == None else bool(err)
contains = False if err == None else "Duplicate entry" in err
equals = False if err == None else err == ("" + err)
sized = -1 if err == None else len(err)
sliced = "" if err == None else err[0:9]
strmeth = False if err == None else err.startswith("Duplicate entry")
concat = "" if err == None else "concat: " + err
same = "" if err == None else "%s" % err
q, qe = mo.quote(concat)
q2, qe2 = mo.quote(same)
# a call that SUCCEEDS clears the record, so errno never reports a stale class
rs2, err2 = mo.sql("select 1")
after_ok = mo.errno()
mo.sql("insert into plog values (1, 'code={} sqlstate={}')".format(code, state))
mo.sql("insert into plog values (2, 'is_dup={}')".format(isdup))
mo.sql("insert into plog values (3, 'truthy={} contains={} equals={}')".format(truthy, contains, equals))
mo.sql("insert into plog values (4, 'len={} slice={}')".format(sized, sliced))
mo.sql("insert into plog values (7, 'startswith={} errno_after_success={}')".format(strmeth, after_ok))
mo.sql("insert into plog values (5, '{}')".format(q))
mo.sql("insert into plog values (6, '{}')".format(q2))
$$;

call sp_err();
select id, note from plog order by id;

-- the procedure carried on after the failure, and the statement that failed
-- changed nothing
select a, b from t order by a;

-- success yields None, so every derived value takes the None branch
create or replace procedure sp_ok(out e varchar) language 'starlark'
$$
rs, err = mo.sql("select 1")
out_e = "isNone" if err == None else "notNone"
$$;
set @e = 'unset';
call sp_ok(@e);
select @e as on_success;

-- an error value assigned to an OUT parameter still arrives as the message,
-- which is how procedures used it before it carried its codes
create or replace procedure sp_out(out e varchar) language 'starlark'
$$
rs, err = mo.sql("insert into t values (1, 'dup')")
out_e = err
$$;
set @e = 'unset';
call sp_out(@e);
select @e as out_param;

drop database if exists slk_err;
