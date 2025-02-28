set experimental_fulltext_index=1;
set ft_relevancy_algorithm="TF-IDF";

create stage ftstage URL='file:///$resources/fulltext/';

create table datasrc (id bigint primary key, fpath datalink, fulltext(fpath));

insert into datasrc values (0, 'stage://ftstage/mo.pdf'), (1, 'file:///$resources/fulltext/chinese.pdf'), 
(2, 'file:///$resources/fulltext/file-sample_100kB.docx');

select id from datasrc where match(fpath) against('matrixone');

select id from datasrc where match(fpath) against('慢慢地' in natural language mode);

select id from datasrc where match(fpath) against('Nulla facilisi' in natural language mode);

drop table datasrc;

create table datasrc (id bigint primary key, fpath datalink, fpath2 datalink, fulltext(fpath, fpath2));

insert into datasrc values (0, 'stage://ftstage/mo.pdf', 'file:///$resources/fulltext/chinese.pdf');

select id from datasrc where match(fpath, fpath2) against('+matrixone +慢慢地' in boolean mode);

update datasrc set fpath='stage://ftstage/notexist.pdf' where id=0;

drop table datasrc;

drop stage ftstage;
