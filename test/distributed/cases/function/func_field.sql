-- @suit

-- @case
-- @desc:test for FIELD() function
-- @label:bvt

-- Binary subjects compare bytes, including invalid UTF-8 and embedded NULs.
select field(_binary 'a', _binary 'A', _binary 'a') as binary_case,
       field(_binary X'FF', _binary X'FE', _binary X'FF') as invalid_bytes,
       field(cast('a' as binary), cast('A' as binary), cast('a' as binary)) as cast_binary;
select field(_binary 'a', 'A', 'a') as binary_subject,
       field('a', _binary 'A', _binary 'a') as text_subject,
       field('a', 'A', 'a') as text_control;
select field(_binary X'610062', X'610063', X'610062', X'610062') as embedded_nul,
       field(_binary '', null, _binary '') as empty_value,
       field(cast(null as binary), X'00', null) as null_subject,
       field(X'FF', X'FE', null) as no_match;

drop table if exists field_binary_subjects;
create table field_binary_subjects (id int primary key, b blob, vb varbinary(8));
insert into field_binary_subjects values (1, X'61', X'61'), (2, X'FF', X'FF'), (3, null, null);
select id, field(b, X'41', X'FE', X'61', X'FF') as blob_field,
       field(vb, X'41', X'FE', X'61', X'FF') as varbinary_field
from field_binary_subjects order by id;
drop table field_binary_subjects;

-- A bare SQL marker keeps its text comparison context; an explicit cast does not.
set @field_subject = X'41';
set @field_candidate = X'61';
prepare field_context_stmt from 'select field(?, ?) as bare_field, field(cast(? as binary), ?) as binary_field';
execute field_context_stmt using @field_subject, @field_candidate, @field_subject, @field_candidate;
set @field_subject = _binary 'a';
set @field_candidate = _binary 'A';
execute field_context_stmt using @field_subject, @field_candidate, @field_subject, @field_candidate;
set @field_subject = 'a';
set @field_candidate = 'a';
execute field_context_stmt using @field_subject, @field_candidate, @field_subject, @field_candidate;
set @field_subject = null;
execute field_context_stmt using @field_subject, @field_candidate, @field_subject, @field_candidate;
set @field_subject = X'41';
set @field_candidate = X'61';
execute field_context_stmt using @field_subject, @field_candidate, @field_subject, @field_candidate;
deallocate prepare field_context_stmt;
set @field_candidate = null;

-- Marker text context also survives domain-preserving wrappers, but not a fixed binary value.
set @field_subject = X'41';
set @field_candidate = X'61';
prepare field_nested_stmt from 'select field(coalesce(?, ?), ?) as coalesce_field, field(substring(?, 1), ?) as substring_field, field(coalesce(?, cast(''A'' as binary)), ?) as fixed_binary_field';
execute field_nested_stmt using @field_subject, @field_subject, @field_candidate, @field_subject, @field_candidate, @field_subject, @field_candidate;
set @field_subject = null;
execute field_nested_stmt using @field_subject, @field_subject, @field_candidate, @field_subject, @field_candidate, @field_subject, @field_candidate;
set @field_subject = X'41';
execute field_nested_stmt using @field_subject, @field_subject, @field_candidate, @field_subject, @field_candidate, @field_subject, @field_candidate;
-- Comparison context must not truncate a payload beyond the prepared envelope.
set @field_subject = repeat(_binary 'A', 70000);
set @field_candidate = @field_subject;
execute field_nested_stmt using @field_subject, @field_subject, @field_candidate, @field_subject, @field_candidate, @field_subject, @field_candidate;
deallocate prepare field_nested_stmt;
set @field_candidate = null;

-- Numeric control arguments do not own the returned string comparison domain.
set @field_subject = X'41';
set @field_candidate = X'61';
set @field_control = 1;
prepare field_control_stmt from 'select field(if(?, substring(?, ?), ?), ?) as if_control';
execute field_control_stmt using @field_control, @field_subject, @field_control, @field_subject, @field_candidate;
deallocate prepare field_control_stmt;

-- NULLIF keeps the marker domain across its CASE rewrite and cached reuse.
prepare field_nullif_stmt from 'select field(nullif(?, ''''), ?) as nullif_field';
execute field_nullif_stmt using @field_subject, @field_candidate;
set @field_subject = '';
execute field_nullif_stmt using @field_subject, @field_candidate;
set @field_subject = null;
execute field_nullif_stmt using @field_subject, @field_candidate;
set @field_subject = X'41';
execute field_nullif_stmt using @field_subject, @field_candidate;
deallocate prepare field_nullif_stmt;

-- Projection lineage preserves SQL marker context, not explicit binary casts.
prepare field_derived_stmt from 'select field(x, ?) as derived_field, field(cast(x as binary), ?) as binary_control from (select ? as x limit 1) d';
execute field_derived_stmt using @field_candidate, @field_candidate, @field_subject;
deallocate prepare field_derived_stmt;
prepare field_scalar_stmt from 'select field((select ? from (select 1 as x) d limit 1), ?) as scalar_field';
execute field_scalar_stmt using @field_subject, @field_candidate;
deallocate prepare field_scalar_stmt;
prepare field_max_stmt from 'select field(max(?), ?) as max_field';
execute field_max_stmt using @field_subject, @field_candidate;
deallocate prepare field_max_stmt;
set @field_control = null;

-- Nonempty text must survive its comparison cast; a NULL-only selector keeps byte comparison.
set @field_subject = 'A';
set @field_candidate = 'a';
prepare field_text_values_stmt from 'select field(nullif(?, ''''), ?) as nullif_text, field(greatest(?, ''@''), ?) as greatest_text, field(coalesce(?, ''fallback''), ?) as coalesce_text, field(if(true, ?, ''B''), ?) as if_text';
execute field_text_values_stmt using @field_subject, @field_candidate, @field_subject, @field_candidate, @field_subject, @field_candidate, @field_subject, @field_candidate;
set @field_candidate = 'z';
execute field_text_values_stmt using @field_subject, @field_candidate, @field_subject, @field_candidate, @field_subject, @field_candidate, @field_subject, @field_candidate;
deallocate prepare field_text_values_stmt;
set @field_candidate = 'a';
prepare field_null_values_stmt from 'select field(coalesce(?, null), ?) as coalesce_null, field(case when true then ? else null end, ?) as case_null, field(if(true, ?, null), ?) as if_null';
execute field_null_values_stmt using @field_subject, @field_candidate, @field_subject, @field_candidate, @field_subject, @field_candidate;
set @field_subject = X'41';
set @field_candidate = X'61';
execute field_null_values_stmt using @field_subject, @field_candidate, @field_subject, @field_candidate, @field_subject, @field_candidate;
deallocate prepare field_null_values_stmt;
prepare field_window_stmt from 'select field(x, ?) as window_field from (select max(?) over() as x, min(?) over() as y) d';
execute field_window_stmt using @field_candidate, @field_subject, @field_subject;
deallocate prepare field_window_stmt;

-- NULLIF retains original operands, not a guessed CASE shape.
set @field_subject = X'41';
set @field_candidate = X'61';
set @field_condition = 0;
prepare field_wrapped_nullif from 'select field(nullif(coalesce(?,?), ''''), ?) as wrapped_nullif';
execute field_wrapped_nullif using @field_subject, @field_subject, @field_candidate;
deallocate prepare field_wrapped_nullif;
prepare field_binary_nullif from 'select field(nullif(?, _binary ''''), ?) as binary_nullif';
execute field_binary_nullif using @field_subject, @field_candidate;
deallocate prepare field_binary_nullif;
prepare field_case_condition from 'select field(case when ? then null else ? end, ?) as parameter_case';
execute field_case_condition using @field_condition, @field_subject, @field_candidate;
deallocate prepare field_case_condition;

-- NULL-selector semantics survive every relational materialization boundary.
prepare field_null_derived from 'select field(x, ?) as null_derived from (select coalesce(?, null) as x limit 1) d';
execute field_null_derived using @field_candidate, @field_subject;
deallocate prepare field_null_derived;
prepare field_null_scalar from 'select field((select coalesce(?, null) from (select 1 as x) d limit 1), ?) as null_scalar';
execute field_null_scalar using @field_subject, @field_candidate;
deallocate prepare field_null_scalar;
prepare field_null_window from 'select field(x, ?) as null_window from (select max(coalesce(?,null)) over() as x) d';
execute field_null_window using @field_candidate, @field_subject;
deallocate prepare field_null_window;
set @field_condition = null;

-- A prepared CASE predicate preserves the charset of NULL-only value markers.
set @field_subject = 'A';
set @field_candidate = 'a';
set @field_condition = 0;
prepare field_text_case from 'select field(case when ? then null else ? end, ?) as text_case';
execute field_text_case using @field_condition, @field_subject, @field_candidate;
set @field_condition = 1;
execute field_text_case using @field_condition, @field_subject, @field_candidate;
set @field_condition = 0;
execute field_text_case using @field_condition, @field_subject, @field_candidate;
deallocate prepare field_text_case;
prepare field_text_scalar from 'select field((select coalesce(?,null) from (select 1 as id) d limit 1), ?) as text_scalar';
execute field_text_scalar using @field_subject, @field_candidate;
deallocate prepare field_text_scalar;
prepare field_text_window from 'select field(x, ?) as text_window from (select max(coalesce(?,null)) over() as x) d';
execute field_text_window using @field_candidate, @field_subject;
deallocate prepare field_text_window;
prepare field_scalar_case from 'select field((select case when ? then null else ? end limit 1), ?) as scalar_case';
execute field_scalar_case using @field_condition, @field_subject, @field_candidate;
deallocate prepare field_scalar_case;
prepare field_window_case from 'select field(x, ?) as window_case from (select max(case when ? then null else ? end) over() as x) d';
execute field_window_case using @field_candidate, @field_condition, @field_subject;
deallocate prepare field_window_case;

-- A scalar marker peer supplies context; a fixed binary returned value owns its domain.
set @field_subject = X'41';
set @field_candidate = X'61';
set @field_peer = X'42';
prepare field_nullif_scalar_peer from 'select field(nullif(?, (select ? limit 1)), ?) as scalar_peer';
execute field_nullif_scalar_peer using @field_subject, @field_peer, @field_candidate;
deallocate prepare field_nullif_scalar_peer;
prepare field_nullif_binary_value from 'select field(nullif(coalesce(?, cast(''A'' as binary)), ''''), ?) as fixed_binary_value';
execute field_nullif_binary_value using @field_subject, @field_candidate;
deallocate prepare field_nullif_binary_value;
prepare field_nullif_fixed_value from 'select field(nullif(_binary ''A'', ?), ?) as fixed_binary_literal';
execute field_nullif_fixed_value using @field_peer, @field_candidate;
deallocate prepare field_nullif_fixed_value;
set @field_condition = null;
set @field_peer = null;

-- Nested selectors compose CASE's prepared value domain without using its condition as a value.
set @field_subject = 'A';
set @field_candidate = 'a';
set @field_condition = 0;
prepare field_nested_case from 'select field(coalesce(case when ? then null else ? end,null),?) as nested_case';
execute field_nested_case using @field_condition, @field_subject, @field_candidate;
set @field_condition = 1;
execute field_nested_case using @field_condition, @field_subject, @field_candidate;
set @field_condition = 0;
execute field_nested_case using @field_condition, @field_subject, @field_candidate;
deallocate prepare field_nested_case;

-- Numeric and typed NULL peers are type boundaries, not untyped NULL-selector values.
prepare field_numeric_peer from 'select field(nullif(?,1),?) as numeric_peer';
execute field_numeric_peer using @field_subject, @field_candidate;
deallocate prepare field_numeric_peer;
prepare field_numeric_null_peer from 'select field(nullif(?,cast(null as signed)),?) as numeric_null_peer';
execute field_numeric_null_peer using @field_subject, @field_candidate;
deallocate prepare field_numeric_null_peer;
prepare field_text_null_peer from 'select field(nullif(?,cast(null as char)),?) as text_null_peer';
execute field_text_null_peer using @field_subject, @field_candidate;
deallocate prepare field_text_null_peer;
prepare field_explicit_binary_peer from 'select field(nullif(?,cast(''B'' as binary)),?) as binary_peer';
execute field_explicit_binary_peer using @field_subject, @field_candidate;
deallocate prepare field_explicit_binary_peer;
set @field_subject = X'41';
set @field_candidate = X'61';
prepare field_nested_case from 'select field(coalesce(case when ? then null else ? end,null),?) as binary_nested_case';
execute field_nested_case using @field_condition, @field_subject, @field_candidate;
deallocate prepare field_nested_case;
prepare field_numeric_peer from 'select field(nullif(?,1),?) as binary_numeric_peer';
execute field_numeric_peer using @field_subject, @field_candidate;
deallocate prepare field_numeric_peer;
prepare field_numeric_null_peer from 'select field(nullif(?,cast(null as signed)),?) as binary_numeric_null_peer';
execute field_numeric_null_peer using @field_subject, @field_candidate;
deallocate prepare field_numeric_null_peer;
prepare field_text_null_peer from 'select field(nullif(?,cast(null as char)),?) as binary_text_null_peer';
execute field_text_null_peer using @field_subject, @field_candidate;
deallocate prepare field_text_null_peer;
prepare field_explicit_binary_peer from 'select field(nullif(?,cast(''B'' as binary)),?) as binary_binary_peer';
execute field_explicit_binary_peer using @field_subject, @field_candidate;
deallocate prepare field_explicit_binary_peer;
set @field_condition = null;

-- Explicit binary subjects retain byte equality across prepared executions.
set @field_subject = _binary 'a';
prepare field_binary_stmt from 'select field(cast(? as binary), ''A'', ''a'', X''FE'', X''FF'') as prepared_field';
execute field_binary_stmt using @field_subject;
set @field_subject = X'FF';
execute field_binary_stmt using @field_subject;
set @field_subject = null;
execute field_binary_stmt using @field_subject;
set @field_subject = 'a';
execute field_binary_stmt using @field_subject;
deallocate prepare field_binary_stmt;
set @field_subject = null;

select field('Bb', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff');
select field('Gg', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff');
select field('aa', 'AA', 'BB','Aa', 'aA');
select field(' ', 'a', ' ', '\t', '\n');
select field('', ' ', NULL, '\r', '\n');
select field('', '', '\r', '\n');


select field(1, '1', 1);
select field(1, 'true');


select field(1, 1, 2, 3-2);
select field(1, 3-2, 2, 1);
select field(1, 1.0, 2, 1);
select field(1+1, 1, 2, 3, 1+1);

drop table if exists t;
create table t(
    i int,
    f float,
    d double
);
insert into t() values (1, 1.1, 2.2), (2, 3.3, 4.4), (0, 0, 0), (0, null, 0);
select * from t;
select field(1, i, f, d) from t;
select field(i, 0, 1, 2) from t;
select field(i, f, d, 0, 1, 2) from t;
select field(null, f, d, 0, 1, 2) from t;
select field('1', f, d, 0, 1, 2) from t;
select field(3.3, f, d, 0, 1, 2) from t;
select field(3, f, d, 0, 1, 2) from t;

drop table if exists t;
create table t(
    str1 char(20),
    str2 char(20)
);
insert into t values ('hello','world'), ('jaja','haha'), ('didi','dodo'), ('papa','gaga');
select field(str1, str2) from t;
select field(str2, str1) from t;
select field(str2, str1, NULL) from t;

drop table if exists t;
create table t(
    str1 varchar(50),
    str2 varchar(50),
    str3 varchar(50),
    str4 varchar(50)
);
insert into t values ('&*()&DJHKSY&F', 'JHKHJD21k..fdai', 'kl;ji*(', '86168907()*&*fd');
insert into t values ('&*()&DJHKSY&F', 'JHKHJD21k..fdaiJHKHJD21k..fdai', 'kl;ji*(', '86168907()*&*fd');
select field(str1, str2, str3, str4) from t;
select field('1', str1, str2) from t;
select field('&*()&DJHKSY&F', str1, str2) from t;
select field('&*()&DJHKSY&F', str1, str2, str3, str4) from t;
select field('', str1, str2, str3, str4) from t;

drop table if exists t1;
drop table if exists t2;
create table t1(
    str1 varchar(50),
    str2 varchar(50)
);
create table t2(
    str1 varchar(50),
    str2 varchar(50)
);
insert into t1 values ('',' '), ('aa', 'Aa'), ('null',null);
insert into t2 values ('','\r'), ('aa', 'AA'), (null, 'null');
select field(t1.str1, t2.str1) from t1 join t2 on t1.str1 = t2.str1;
select field(t1.str2, t2.str2) from t1 join t2 on t1.str1 = t2.str1;
select field(t1.str1, t2.str1) from t1 left join t2 on t1.str1 = t2.str1;
select field(t1.str1, t2.str1) from t1 right join t2 on t1.str1 = t2.str1;

drop table if exists t1;
drop table if exists t2;
create table t1(
    str1 char(50),
    str2 char(50),
    primary key (str1)
);
create table t2(
    str1 char(50),
    str2 char(50),
    primary key (str1)
);
insert into t1 values ('',' '), ('aa', 'Aa'), ('null',NULL);
insert into t2 values ('','\r'), ('aa', 'AA'), ('null', '');
select field(t1.str1, t2.str1) from t1 inner join t2 on t1.str1 = t2.str1;
select field(null, '');


select field(t1.str2, t2.str2) from t1 join t2 on t1.str1 = t2.str1;


drop table if exists t1;
drop table if exists t2;
create table t1(
    i int,
    f float,
    d double,
    primary key (i)
);
create table t2(
    i int,
    f float,
    d double,
    primary key (i)
);
insert into t1 values (9999999, 999.999, 888.888), (0, 0.0, 0.00);
insert into t2 values (9999999, 999.999, 888.888), (0, 0, 0);
select field(t1.i, t2.i) from t1 inner join t2 on t1.i = t2.i;
select field(t1.d, t2.d) from t1 left join t2 on t1.d = t2.d;
select field(t1.f, t2.f) from t1 right join t2 on t1.f = t2.f;
select field(t1.f, t2.d) from t1 right join t2 on t1.f = t2.f;
select field(t1.i, t2.f) from t1 right join t2 on t1.f = t2.f;

drop table if exists t1;
drop table if exists t2;
create table t1(
    i double,
    f decimal(6,3),
    primary key (i)
);
create table t2(
    i double,
    f decimal(6,3),
    primary key (i)
);
insert into t1 values (0.01, 0.001), (0.0, -1), (-0.000000001, 1);
insert into t2 values (0.01, 0.01), (-1.0, -1), (0.000000001, -1);
select field(t1.i, t2.i) from t1 inner join t2 on t1.i = t2.i;
select t2.f, t1.f, field(t2.f, t1.f) from t1 right join t2 on t1.i = t2.i;


select t1.i, t2.f, field(t1.i, t2.f) from t1 left join t2 on t1.i = t2.i;

