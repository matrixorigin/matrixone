-- A diagnostic hint in one connection must not disable index rewrites for another.
drop database if exists fulltext_optimizer_hints_scope;
create database fulltext_optimizer_hints_scope;
use fulltext_optimizer_hints_scope;
set experimental_fulltext_index = 1;

create table docs (id int primary key, body text);
insert into docs values (1, 'hello world'), (2, 'other words'), (3, 'hello again');
create fulltext index ft_body on docs (body);
select count(*) from docs where match(body) against('hello');

-- @session:id=1
use fulltext_optimizer_hints_scope;
set session optimizer_hints = 'applyIndices=1';
select @@session.optimizer_hints;
-- @session

select @@session.optimizer_hints;
select count(*) from docs where match(body) against('hello');

-- @session:id=1
set session optimizer_hints = '';
-- @session
select count(*) from docs where match(body) against('hello');

-- A stored-procedure local with the same name must not shadow the session sysvar.
create procedure ft_local_hint() 'begin declare optimizer_hints varchar(64) default ''applyIndices=1''; select count(*) from docs where match(body) against(''hello''); end';
call ft_local_hint();
drop procedure ft_local_hint;
drop database fulltext_optimizer_hints_scope;
