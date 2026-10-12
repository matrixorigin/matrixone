-- #29896: a single double quote in BOOLEAN MODE used to panic in
-- ParsePatternInBooleanMode (slice bounds out of range [1:0]) because a lone `"`
-- satisfies both the opening- and closing-quote check. A lone quote is not a
-- phrase; it is an ordinary token that matches nothing. It must never panic.

drop table if exists bookmark;
create table bookmark (
  id int not null auto_increment,
  url text not null,
  title text not null,
  excerpt text not null default (''),
  content text not null default (''),
  primary key (id),
  fulltext (title, excerpt, content)
);
insert into bookmark (url, title, excerpt, content) values ('https://owned.invalid/example', 'test title', 'test excerpt', 'test content');

-- direct: a lone quote matches nothing, no panic
select id from bookmark where match(title, excerpt, content) against('"' in boolean mode) order by id;

-- original issue shape: projection with a LIKE and an IN (MATCH ... BOOLEAN) subquery
select id, title from bookmark
where 1 and (
    url like '%"%' or
    id in (select id from bookmark where match(title, excerpt, content) against('"' in boolean mode))
)
order by id;

-- a well-formed phrase still matches
select id from bookmark where match(title, excerpt, content) against('"test title"' in boolean mode) order by id;

-- an ordinary token search still matches
select id from bookmark where match(title, excerpt, content) against('title' in boolean mode) order by id;

drop table if exists bookmark;
