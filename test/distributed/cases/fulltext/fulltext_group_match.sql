-- fulltext MATCH rewrite through grouped aggregate and final sort
drop database if exists fulltext_group_match;
create database fulltext_group_match;
use fulltext_group_match;
set experimental_fulltext_index = 1;

create table ft_gap (
    id bigint not null,
    category varchar(8) not null,
    body text null,
    title varchar(64) not null,
    primary key (id),
    fulltext index ft_gap_body_title (body, title)
);

insert into ft_gap values
    (1, 'north', 'aurora launch plan', 'aurora-one'),
    (2, 'north', 'aurora execution guide', 'aurora-two'),
    (3, 'south', 'nebula archive', 'nebula-one'),
    (4, 'south', 'aurora nebula mixed', 'mixed-one'),
    (5, 'south', null, 'empty-one'),
    (6, 'north', 'aurora inactive row', 'aurora-off');

select count(*) from ft_gap
where match(body, title) against('aurora' in natural language mode);

select category, count(*) from ft_gap
where match(body, title) against('aurora' in natural language mode)
group by category
order by category;

drop database fulltext_group_match;
