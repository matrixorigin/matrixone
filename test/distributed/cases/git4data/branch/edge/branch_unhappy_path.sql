-- Data branch unhappy path coverage for validation and rejection behavior.

drop database if exists br_unhappy;
create database br_unhappy;
use br_unhappy;

create table base(a int primary key, b int);
insert into base values (1, 10), (2, 20);
create view v_base as select * from base;
-- @regex("is not BASE TABLE",true)
data branch create table v_branch from v_base;
-- @regex("does not exist",true)
data branch create table missing_branch from missing_base;

data branch create table br from base;
-- @regex("column \"missing\" not found",true)
data branch diff br against base columns (missing);
-- @bvt:issue#24924
-- @regex("table \"missing_branch\" does not exist",true)
data branch diff missing_branch against base;
-- @bvt:issue
-- @bvt:issue#24924
-- @regex("table \"missing_base\" does not exist",true)
data branch diff br against missing_base;
-- @bvt:issue

alter table br add column c int default 0;
-- @regex("schema is not equivalent",true)
data branch diff br against base;
-- @regex("schema is not equivalent",true)
data branch merge br into base;

create table no_pk(a int, b int);
insert into no_pk values (1, 10), (2, 20);
data branch create table no_pk_branch from no_pk;
-- @regex("requires a table with a primary key",true)
data branch pick no_pk_branch into no_pk keys(1);

create table single_pk(a int primary key, b int);
insert into single_pk values (1, 10), (2, 20);
data branch create table single_left from single_pk;
data branch create table single_right from single_pk;
insert into single_right values (3, 30);
-- @bvt:issue#24924
-- @regex("requires a KEYS or BETWEEN SNAPSHOT clause",true)
data branch pick single_right into single_left;
-- @bvt:issue
-- @regex("single-column primary key; use scalar values",true)
data branch pick single_right into single_left keys((3, 3));
-- @regex("KEYS subquery returned NULL",true)
data branch pick single_right into single_left keys(select cast(null as int));
-- @bvt:issue#24924
-- @regex("KEYS subquery returns 2 columns but table has a single-column primary key",true)
data branch pick single_right into single_left keys(select a, b from single_right);
-- @bvt:issue
create snapshot br_unhappy_single_sp for table br_unhappy single_left;
-- @regex("destination snapshot option is not supported",true)
data branch pick single_right into single_left{snapshot='br_unhappy_single_sp'} keys(3);

create table comp_pk(a int, b int, c varchar(20), primary key(a, b));
insert into comp_pk values (1, 1, 'base'), (2, 2, 'base');
data branch create table comp_left from comp_pk;
data branch create table comp_right from comp_pk;
insert into comp_right values (3, 3, 'new');
-- @regex("KEYS expression must be a tuple for composite primary key",true)
data branch pick comp_right into comp_left keys(3);
-- @regex("KEYS tuple has 3 elements but composite primary key has 2 columns",true)
data branch pick comp_right into comp_left keys((3, 3, 3));

-- @bvt:issue#24924
-- @regex("snapshot 'missing_from' not found",true)
data branch pick single_right into single_left between snapshot missing_from and missing_to keys(3);
-- @bvt:issue

create table exists_base(a int primary key);
create table exists_target(a int primary key);
-- @regex("already exists",true)
data branch create table exists_target from exists_base;
-- @regex("database .* already exists",true)
data branch create database br_unhappy from br_unhappy;

create table uniq_base(a int primary key, u int unique);
insert into uniq_base values (1, 10), (2, 20);
data branch create table uniq_left from uniq_base;
data branch create table uniq_right from uniq_base;
insert into uniq_left values (3, 30);
insert into uniq_right values (4, 30);
-- @regex("Duplicate entry '30'",true)
data branch merge uniq_right into uniq_left when conflict accept;

select count(*) as single_left_rows from single_left;
select count(*) as comp_left_rows from comp_left;

drop snapshot br_unhappy_single_sp;
drop database br_unhappy;

-- Known bug repro: database-branch view SQL rewrite must not alter string literals.
drop database if exists br_view_literal_copy;
drop database if exists br_view_literal_src;
create database br_view_literal_src;
use br_view_literal_src;
create table t(a int primary key);
insert into t values (1);
create view v_literal as select 'br_view_literal_src' as marker, a from t;
data branch create database br_view_literal_copy from br_view_literal_src;
-- @bvt:issue#24924
select marker from br_view_literal_copy.v_literal;
-- @bvt:issue
drop database br_view_literal_copy;
drop database br_view_literal_src;
