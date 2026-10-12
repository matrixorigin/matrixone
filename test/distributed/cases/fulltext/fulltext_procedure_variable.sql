-- #29149: a stored-procedure variable is a query-wide FULLTEXT search term.
drop database if exists fulltext_procedure_variable;
create database fulltext_procedure_variable;
use fulltext_procedure_variable;

create table docs(id bigint primary key, body text);
insert into docs values (1, 'matrix database engine'), (2, 'vector search engine'), (3, 'ordinary text');
create fulltext index ft on docs(body);

create procedure p_ft_fixed() 'begin select id from docs where match(body) against(''engine'') order by id; end';
create procedure p_ft_param(in q varchar(100)) 'begin select id from docs where match(body) against(q) order by id; end';
create procedure p_ft_local(in q varchar(100)) 'begin declare term varchar(100) default ''ordinary''; set term = q; select id from docs where match(body) against(term) order by id; end';
create procedure p_ft_boolean(in q varchar(100)) 'begin select id from docs where match(body) against(q in boolean mode) order by id; end';
create procedure p_ft_shadow(in q varchar(100)) 'begin begin declare q varchar(100) default ''ordinary''; select id from docs where match(body) against(q) order by id; end; end';
create procedure p_ft_inout(inout q varchar(100)) 'begin set q = ''ordinary''; select id from docs where match(body) against(q) order by id; end';
call p_ft_fixed();
call p_ft_param('engine');
call p_ft_param('ordinary');
call p_ft_param('engine');
call p_ft_param(NULL);
call p_ft_param('');
call p_ft_param('engine');
call p_ft_local('engine');
select id from docs where match(body) against('+engine' in boolean mode) order by id;
call p_ft_boolean('+engine');
call p_ft_shadow('engine');
set @term = 'engine';
call p_ft_inout(@term);
select @term = 'ordinary' as updated;
select id from docs where match(body) against(body) order by id;
call p_ft_param('engine');

set experimental_fulltext2_index = 1;
create table docs2(id bigint primary key, body text);
insert into docs2 values (1, 'matrix database engine'), (2, 'vector search engine'), (3, 'ordinary text');
create fulltext2 index ft2 on docs2(body);
create procedure p_ft2_param(in q varchar(100)) 'begin select id from docs2 where match(body) against(q) order by id; end';
create procedure p_ft2_count(in q varchar(100)) 'begin select count(*) as hit_count from docs2 where match(body) against(q); end';
create procedure p_ft2_local(in q varchar(100)) 'begin declare term varchar(100) default ''ordinary''; set term = q; select id from docs2 where match(body) against(term) order by id; end';
create procedure p_ft2_boolean(in q varchar(100)) 'begin select id from docs2 where match(body) against(q in boolean mode) order by id; end';
select id from docs2 where match(body) against('engine') order by id;
call p_ft2_param('engine');
call p_ft2_param('ordinary');
call p_ft2_param('engine');
call p_ft2_count('engine');
call p_ft2_count(NULL);
call p_ft2_count('');
call p_ft2_count('engine');
call p_ft2_param('engine');
call p_ft2_local('engine');
select id from docs2 where match(body) against('+engine' in boolean mode) order by id;
call p_ft2_boolean('+engine');
call p_ft2_param('engine');

drop database fulltext_procedure_variable;
