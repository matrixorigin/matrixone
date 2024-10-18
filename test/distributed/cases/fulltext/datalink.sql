set experimental_fulltext_index=1;

create table datasrc (id bigint primary key, fpath datalink, fulltext(fpath));

insert into datasrc values (0, 'file:///$resources/fulltext/mo.pdf'), (1, 'file:///$resources/fulltext/chinese.pdf');

select * from datasrc where match(fpath) against('matrixone');

select * from datasrc where match(fpath) against('慢慢地' in natural language mode);

drop table datasrc;

-- @bvt:issue#19437
create table datasrc (id bigint primary key, fpath datalink, fpath2 datalink, fulltext(fpath, fpath2));

insert into datasrc values (0, 'file:///$resources/fulltext/mo.pdf', 'file:///$resources/fulltext/chinese.pdf');

select * from datasrc where match(fpath, fpath2) against('+matrixone +慢慢地' in boolean mode);

drop table datasrc;
-- @bvt:issue
