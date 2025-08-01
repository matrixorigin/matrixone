-- session
set experimental_fulltext_index=1;
set ft_relevancy_algorithm="TF-IDF";
show variables like "experimental_fulltext_index";

drop database if exists test01;
create database test01;
use test01;
create table src (id bigint primary key, body varchar, title text);

insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
                       (4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
                       (5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
                       (6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說'),
                       (7, '59個簡單的英文和中文短篇小說', '適合初學者'),
                       (8, NULL, 'NOT INCLUDED'),
                       (9, 'NOT INCLUDED BODY', NULL),
                       (10, NULL, NULL);

create fulltext index ftidx on src (body, title);
drop snapshot if exists snap01;
create snapshot snap01 for account sys;
drop database test01;
restore account sys from snapshot snap01;
show variables like "experimental_fulltext_index";
set experimental_fulltext_index=0;
drop database test01;
drop snapshot snap01;




-- global
set global experimental_fulltext_index=1;
-- @session:id=1&user=sys:dump&password=111
show variables like "experimental_fulltext_index";
drop database if exists test01;
create database test01;
use test01;
create table src (id bigint primary key, body varchar, title text);

insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
                       (4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
                       (5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
                       (6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說'),
                       (7, '59個簡單的英文和中文短篇小說', '適合初學者'),
                       (8, NULL, 'NOT INCLUDED'),
                       (9, 'NOT INCLUDED BODY', NULL),
                       (10, NULL, NULL);

create fulltext index ftidx on src (body, title);
drop snapshot if exists snap01;
create snapshot snap01 for account sys;
drop database test01;
restore account sys from snapshot snap01;
show variables like "experimental_fulltext_index";
set global experimental_fulltext_index=0;
drop database test01;
drop snapshot snap01;
-- @session


