-- Binary-string expressions use byte semantics and keep their runtime metadata.
set @binary_string = X'e4bda0';

select hex(left(X'e4bda061', 1)), hex(reverse(X'e4bda061')),
       hex(lpad(X'e4bda061', 5, X'78')), hex(rpad(X'e4bda061', 5, X'78')),
       ord(X'e4bda061'), instr(X'e4bda061', X'bd'), locate(X'bd', X'e4bda061'),
       X'e4bda061' like '____', hex(regexp_substr(X'e4bda061', '.')),
       hex(left(X'ff61', 1));

select char_length(replace(@binary_string, X'bd', X'78')),
       char_length(trim(@binary_string)), char_length(ltrim(@binary_string)),
       char_length(rtrim(@binary_string)), char_length(elt(1, @binary_string)),
       char_length(char(228, 189, 160));

select char_length(min(v)), char_length(max(v)), char_length(any_value(v))
from (select @binary_string v) s;

select char_length(group_concat(v separator ''))
from (select @binary_string v union all select @binary_string) s;

select char_length(first_value(v) over ()), char_length(last_value(v) over ()),
       char_length(nth_value(v, 1) over ()), char_length(lag(v, 0) over ()),
       char_length(lead(v, 0) over ())
from (select @binary_string v) s;

select char_length(cast(@binary_string as char)),
       char_length(cast(@binary_string as char(10))),
       char_length(convert(@binary_string, char)),
       char_length(convert(@binary_string using utf8mb4));

drop table if exists binary_string_ctas_var;
drop table if exists binary_string_ctas_expr;
drop table if exists binary_string_ctas_empty;
create table binary_string_ctas_var as select @binary_string c;
create table binary_string_ctas_expr as select replace(X'e4bda0', X'bd', X'78') c;
create table binary_string_ctas_empty as select X'' c;

select table_name, data_type, character_maximum_length
from information_schema.columns
where table_schema = database()
  and table_name in ('binary_string_ctas_var', 'binary_string_ctas_expr', 'binary_string_ctas_empty')
  and column_name = 'c'
order by table_name;

select char_length(c), hex(c) from binary_string_ctas_var;
select char_length(c), hex(c) from binary_string_ctas_expr;
select char_length(c), hex(c) from binary_string_ctas_empty;

drop table binary_string_ctas_var;
drop table binary_string_ctas_expr;
drop table binary_string_ctas_empty;
