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

