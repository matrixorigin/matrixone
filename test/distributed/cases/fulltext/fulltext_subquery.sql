-- issue #25891
-- MATCH in an IN subquery must be rewritten through the flattened SEMI JOIN.

set experimental_fulltext_index = 1;

drop database if exists ft_match_subquery;
create database ft_match_subquery;
use ft_match_subquery;

create table articles (
    id int primary key,
    title varchar(255),
    content text,
    author_id int,
    fulltext index ft_articles(title, content) with parser gojieba
);

insert into articles values
    (1, 'Database Guide', 'database search engine', 1),
    (2, 'Vector Guide', 'vector search engine', 2),
    (3, 'Transaction Guide', 'database transaction engine', 1),
    (4, 'Mixed Guide', 'database vector integration', 2);

-- @regex("Table Function on fulltext_index_scan",true)
explain select id from articles
where id in (
    select id from articles
    where match(title, content) against('+database' in boolean mode)
)
order by id;

select id from articles
where id in (
    select id from articles
    where match(title, content) against('+database' in boolean mode)
)
order by id;

drop database ft_match_subquery;
