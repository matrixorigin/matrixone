-- Fulltext SQL composability and optimizer rewrite coverage.

set experimental_fulltext_index = 1;

drop database if exists ft_query_shapes;
create database ft_query_shapes;
use ft_query_shapes;

create table articles (
    id int primary key,
    title varchar(255),
    content text,
    author_id int,
    fulltext index ft_articles(title, content)
);

insert into articles values
    (1, 'Database Guide', 'database search engine', 1),
    (2, 'Vector Guide', 'vector search engine', 2),
    (3, 'Transaction Guide', 'database transaction engine', 1),
    (4, 'Mixed Guide', 'database vector integration', 2);

-- DISTINCT is supported when MATCH is rewritten at the source scan.
-- @sortkey:0
select distinct author_id
from articles
where match(title, content) against('+database' in boolean mode);

-- GROUP BY and HAVING preserve the rewritten fulltext input.
-- @sortkey:0
select author_id, count(*) as matched_count
from articles
where match(title, content) against('+database' in boolean mode)
group by author_id;

select author_id, count(*) as matched_count
from articles
where match(title, content) against('+database' in boolean mode)
group by author_id
having count(*) > 1;

-- Nested CTE and derived-table consumers are supported.
with matched as (
    select id from articles
    where match(title, content) against('+database' in boolean mode)
)
select count(*) from matched;

select count(*)
from (
    select id from articles
    where match(title, content) against('+database' in boolean mode)
) as matched;

-- Ordinary predicates combine with MATCH.
select id from articles
where id > 1
  and match(title, content) against('+database' in boolean mode)
order by id;

-- #25890: remove the tags and add positive output after optimizer support lands.
-- @bvt:issue#25890
select distinct author_id
from articles
where match(title, content) against('+database' in boolean mode)
order by author_id;
-- @bvt:issue

-- #25891: remove the tags and add positive output after optimizer support lands.
-- @bvt:issue#25891
select id from articles
where id in (
    select id from articles
    where match(title, content) against('+database' in boolean mode)
)
order by id;
-- @bvt:issue

drop database ft_query_shapes;
