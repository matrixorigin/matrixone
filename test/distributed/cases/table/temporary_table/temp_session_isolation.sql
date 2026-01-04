drop database if exists tmp_session_iso;
create database tmp_session_iso;
use tmp_session_iso;

-- @session:id=1
use tmp_session_iso;
create temporary table t(a int);
insert into t values (1);
select * from t;

-- @session:id=2
use tmp_session_iso;
select * from t;

-- @session:id=1
drop database if exists tmp_session_iso;

