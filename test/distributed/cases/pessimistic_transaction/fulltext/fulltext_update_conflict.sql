set experimental_fulltext_index = 1;

drop database if exists fulltext_update_conflict_pess;
create database fulltext_update_conflict_pess;
use fulltext_update_conflict_pess;

create table ft_conflict(id int primary key, body text, fulltext ft_body(body));
insert into ft_conflict values (1, 'original token');

begin;
update ft_conflict set body = 'first token' where id = 1;
-- @session:id=1{
use fulltext_update_conflict_pess;
begin;
-- @wait:0:commit
update ft_conflict set body = 'second token' where id = 1;
commit;
-- @session}
commit;

select id, body from ft_conflict order by id;
select id from ft_conflict where match(body) against('original') order by id;
select id from ft_conflict where match(body) against('first') order by id;
select id from ft_conflict where match(body) against('second') order by id;

drop database fulltext_update_conflict_pess;
