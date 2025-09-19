-- TODO: run all tests with both experimental_fulltext_index = 0 and 1
-- TODO: GENERATE the test case to cover all combinations of types (varchar, char and text)
set experimental_fulltext_index=1;
set ft_relevancy_algorithm="TF-IDF";

create table src (id bigint primary key, body varchar, title text, FULLTEXT ftidx (body, title) ASYNC);

insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
(4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
(5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
(6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說'),
(7, '59個簡單的英文和中文短篇小說', '適合初學者'),
(8, NULL, 'NOT INCLUDED'),
(9, 'NOT INCLUDED BODY', NULL),
(10, NULL, NULL);

select sleep(30);

select * from src where match(body, title) against('red');

show create table src;

alter table src rename to src1;

drop table src1;
