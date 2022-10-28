--env statement prepare
drop table if exists json_table_1;
drop table if exists json_table_2;
drop table if exists json_table_3;
drop table if exists json_table_3a;
drop table if exists json_table_3b;
drop table if exists json_table_4;
drop table if exists json_table_4a;
drop table if exists json_table_5;
drop table if exists json_table_5a;
drop table if exists json_view_1;

--覆盖json串 key value为字符，数字，中文，特殊字符， ' '，常量，日期格式字符串,true/false
create table json_table_1(j1 json);
insert into json_table_1 values('{"key10": "value1", "key2": "value2"}'),('{"key1": "@#$_%^&*()!@", "key123456": 223}'),('{"芝士面包": "12abc", "key_56": 78.90}'),('{"": "", "12_key": "中文mo"}'),('{"a 1": "b 1", "13key4": "中文mo"}'),('{"d1": "2020-10-09", "d2": "2019-08-20 12:30:00"}'),('{"d1": [true,false]}'),('{}');
insert into json_table_1 values('{"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee":"1234567890000000000000000000000000000000000000000000000","uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu":["aaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111111111"]}');
select j1 from json_table_1;

create table json_table_2(j1 json not null,j2 json null);
insert into json_table_2 values('{"key1": "value1", "key2": "value2"}','{}');
select * from json_table_2;
insert into json_table_2 values('{}','{"key1": "value1", "key2": "value2"}');
select '{"key1": "value1", "key2": "value2"}','{}','{"芝士面包": "12abc", "123456": "中文mo"}';

create table json_table_3(id int,j1 json);
Insert into json_table_3 values (1,'{
    "pages": [
        "pages/news/news",
        "pages/index/index",
        "pages/movie/movie",
        "pages/logs/logs"
    ],
    "window": {
        "backgroundTextStyle": "light",
        "navigationBarBackgroundColor": "white",
        "navigationBarTitleText": "哈哈",
        "navigationBarTextStyle": "black",
        "navigationStyle": "custom",
        "backgroundColor": "#ffffff"
    },
    "tabBar": {
        "color": "#000",
        "borderStyle": "black",
        "selectedColor": "#ff6600",
        "position": "bottom",
        "custom": "false",
        "list": [
            {
                "pagePath": "pages/news/news",
                "text": "新闻",
                "iconPath": "pages/images/yuedu.png",
                "selectedIconPath": "pages/images/193.jpg"
            },
            {
                "pagePath": "pages/movie/movie",
                "text": "电影",
                "iconPath": "pages/images/diany.png",
                "selectedIconPath": "pages/images/506.jpg"
            }
        ]
    },

    "networkTimeout": {
        "request": 6000,
        "downloadFile": 60000,
        "connectSocket": 60000 ,
        "uploadFile": 60000
    },
    "debug": true,
    "requiredBackgroundModes": [
        "audio",
        "location"
    ],
    "permission": {
        "scope.userLocation": {
            "desc": "你的位置信息将用于小程序位置接口的效果展示"
        }
    },
    "style": "v2"
}
');
select * from json_table_3;

--json重复值
create table json_table_3a(j1 json);
insert into json_table_3a values('{"x": 17, "x": "red"}'),('{"x": 17, "x": "red", "x": [3, 5, 7]}');
select * from json_table_3a;
--异常测试：非法json串,列约束pk，default,partiton by
create table json_table_4(j1 json);
insert into json_table_4 values('[1, 2,');
insert into json_table_4 values('{"key1": NULL, "": "value2"}');
insert into json_table_4 values('');
create table json_table_5(j1 json primary key,j2 json default '{"x": 17, "x": "red"}',j3 json not null );
create table json_table_5(j1 json) partition by hash(j1);
select j1 from json_table_1 where j1>'{"": "", "123456": "中文mo"}';

--update 全部数据
create table json_table_61(j1 json,a varchar(25),b int);
insert into json_table_61 values('{"010": "beijing", "021": "shanghai"}','apple',345),('{"phonenum":"17290839029","age":"45"}','pear',0);
select * from json_table_61;
update json_table_61 set j1='{"010": [56,"beijing","2002-09-09"]}' where a='apple';
select * from json_table_61;
update json_table_61 set j1='{"010": "beijing"}';

--delete全部数据
delete from json_table_61 where b=0;
select * from json_table_61;
delete from json_table_61;
select * from json_table_61;
-- agg function
select count(j1) from json_table_1 ;
select max(j1) from json_table_1 ;
select min(j1) from json_table_1 ;

--group by order by
create table json_table_3b(d1 int,j1 json);
insert into json_table_3b values(34,'{"key10": "value1", "key2": "value2"}'),(50,'{"key1": "@#$_%^&*()!@", "123456": "中文mo"}'),(1,'{"芝士面包": "12abc", "123456": "中文mo"}'),(45,'{"": "", "123456": "中文mo"}'),(22,'{"a 1": "b 1", "123456": "中文mo"}'),(88,'{"d1": "2020-10-09", "d2": "2019-08-20 12:30:00"}'),(4,'{"key10": "value1", "key2": "value2"}'),(501,'{"key1": "@#$_%^&*()!@", "123456": "中文mo"}'),(1111,'{"芝士面包": "12abc", "123456": "中文mo"}'),(415,'{"": "", "123456": "中文mo"}');
select max(d1),j1 from json_table_3b group by j1 order by j1;
select j1 from json_table_3b order by j1;

--filter
select * from json_table_3 where j1 is not null;
select * from json_table_3 where j1 is  null;

-- view
create view json_view_1 as select * from json_table_1;
select * from json_view_1;

-- load data
create table json_table_81(d1 int,j1 json);
load data infile'/Users/heni/test_data/json_table_3.txt' into table json_table_81 fields terminated by '|' ignore 1 lines;
create table json_table_82(d2 int,j2 json);
insert into json_table_82 select * from json_table_81;

--temporary/external table
create temporary table json_table_4a(j1 json);
insert into json_table_4a values('{"key1": "value1", "key2": "value2"}');
select * from json_table_4a;
create external table json_table_5a(d1 int,j1 json)infile{"filepath"='/Users/heni/test_data/json_table_3.txt'} fields terminated by '|' lines terminated by '\n' ignore 1 lines;
select * from json_table_5a;

-- union etc
select j1 from  json_table_1 union select j1 from  json_table_3;
select j1 from  json_table_1 intersect select j1 from  json_table_3;
select j1 from  json_table_1 minus select j1 from  json_table_3;

--
create table json_table_71(j1 json);
insert into  json_table_71 values('{
    "orderType": "BUY",
    "orderId": 20768330,
    "syncAction": "market_order",
    "itemCode": "DT_GOODS",
    "maxOfPeople": 214748,
    "itemName": "试用规格",
    "payFee": 0,
    "serviceStopTime": 1608825600000,
    "serviceStartTime": 1607481719000,
    "minOfPeople": 0,
    "paidtime": 1607481718000,
    "syncSeq": "0CD53B341284A223363FD8E4ACIHWBU98283"
}');
select json_extract(j1,'$.orderType') from json_table_71;
select json_extract(j1,'$.orderType.*') from json_table_71;
select json_extract(j1,'$.*') from json_table_71;
select  json_extract(j1,'$**.itemName') from json_table_71;
select  json_extract(j1,'$[*]') from json_table_71;
select  json_extract(j1,'$[0]') from json_table_71;
select  json_extract(j1,'$') from json_table_71;
select json_extract(j1,'$.itemName[2]') from json_table_71;
select json_extract(j1,'$**.minOfPeople') from json_table_71;
select json_extract(' {"a": [1, "2", {"aa": "bb"}]} ','$.a[2].aa');
select json_extract(' {"a": [1, "2", {"aa": ["yyy",56,89,{"aa2": ["aa3",{"aa4": [1,2,{"aa5": ["aa6", {"aa7": "bb"}]}]}]}]}]} ','$.a[2].aa[3].aa2[1].aa4[2].aa5[1].aa7');
select json_extract('{"a":1,"b":2,"c":3,"d":{"a":"x"}}', '$**.a');
select json_extract(' {"a.f": [1, "2", {"aa.f": "bb"}],"e.a.b":"888"} ','$**.f');
--异常测试：
select  json_extract('{"a":"a1","b":"b1"}','$.**');
select json_extract('bar','$.*');
select  json_extract(j1,'') from json_table_71;
