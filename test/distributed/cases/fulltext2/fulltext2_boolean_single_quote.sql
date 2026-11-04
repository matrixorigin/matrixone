-- #29896 (fulltext2): a single double quote in BOOLEAN MODE must not panic.
-- fulltext2 uses its own boolean parser, but this confirms the query path end to
-- end: a lone quote matches nothing and never panics.

set experimental_fulltext2_index = 1;

drop table if exists bookmark2;
create table bookmark2 (
  id int not null auto_increment,
  title text not null,
  excerpt text not null default (''),
  content text not null default (''),
  primary key (id)
);
-- insert before creating the index so the base is built synchronously
insert into bookmark2 (title, excerpt, content) values ('test title', 'test excerpt', 'test content');
create fulltext2 index ft_idx on bookmark2 (title, excerpt, content);

-- a lone quote matches nothing, no panic
select id from bookmark2 where match(title, excerpt, content) against('"' in boolean mode) order by id;

-- a well-formed phrase still matches
select id from bookmark2 where match(title, excerpt, content) against('"test title"' in boolean mode) order by id;

-- an ordinary token search still matches
select id from bookmark2 where match(title, excerpt, content) against('title' in boolean mode) order by id;

drop table if exists bookmark2;
