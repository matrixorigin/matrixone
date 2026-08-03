set experimental_fulltext_index = 1;

drop database if exists fulltext_update_consistency;
create database fulltext_update_consistency;
use fulltext_update_consistency;

create table ft_sync(id int primary key, k int unique, body text, fulltext ft_body(body));
insert into ft_sync values (1, 1, 'sync old token'), (2, 2, 'sync peer token');
select id from ft_sync where match(body) against('old') order by id;
update ft_sync set body = 'sync new token' where id = 1;
select id from ft_sync where match(body) against('old') order by id;
select id from ft_sync where match(body) against('new') order by id;

-- Rolling back the statement cancels both the base mutation and all hidden
-- table maintenance steps.
begin;
update ft_sync set body = 'sync rollback token' where id = 1;
rollback;
select id from ft_sync where match(body) against('rollback') order by id;
select id from ft_sync where match(body) against('new') order by id;

-- The synchronous-index PK rejection happens before locks or mutations and
-- must leave both the base identity and hidden payload unchanged.
update ft_sync set id = 3, body = 'sync failed token' where id = 1;
select id, k, body from ft_sync order by id;
select id from ft_sync where match(body) against('failed') order by id;
select id from ft_sync where match(body) against('new') order by id;

-- Async maintenance is CDC-only. A committed PK update must replace the CDC
-- identity rather than retain an entry keyed by the old PK.
create table ft_async_pk(
    id bigint primary key,
    body text,
    fulltext ft_body(body) async
);
insert into ft_async_pk values (10, 'async identity token');
-- @wait_expect(2, 30)
select id from ft_async_pk where match(body) against('identity') order by id;
update ft_async_pk set id = 20 where id = 10;
-- @wait_expect(2, 30)
select id from ft_async_pk where match(body) against('identity') order by id;

-- Rolled-back PK changes never become visible to the CDC consumer.
begin;
update ft_async_pk set id = 30 where id = 20;
rollback;
-- @wait_expect(2, 30)
select id from ft_async_pk where match(body) against('identity') order by id;

-- Composite source identities exercise CDC tombstone and insert encoding.
create table ft_async_composite(
    tenant varchar(20),
    id bigint,
    body text,
    primary key(tenant, id),
    fulltext ft_body(body) async
);
insert into ft_async_composite values ('a', 1, 'composite identity token');
-- @wait_expect(2, 30)
select tenant, id from ft_async_composite where match(body) against('composite') order by tenant, id;
update ft_async_composite set tenant = 'b', id = 2 where tenant = 'a' and id = 1;
-- @wait_expect(2, 30)
select tenant, id from ft_async_composite where match(body) against('composite') order by tenant, id;

drop database fulltext_update_consistency;
