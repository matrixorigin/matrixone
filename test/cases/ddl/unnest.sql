--数组，jsontype,包含空
select * from json_table_1,unnest(json_table_1.j1) as u;
select * from unnest(json_table_1.j1,"$") as u;
select * from unnest(json_table_1.j1,"$.*") as u;
select * from unnest(' {"a": [1, "2", {"aa": ["yyy",56,89,{"aa2": ["aa3",{"aa4": [1,2,{"aa5": ["aa6", {"aa7": "bb"}]}]}]}]}]} ',"$.a[2].aa") as u;
select * from unnest(' {"a": [1, "2", {"aa": "b1"}]} ',"$.*") as u;
select * from unnest(' {} ',"$.*") as u;
select * from unnest(' [23,"gooooogle",874] ',"$") as u;
select * from unnest(' [23,"gooooogle",{"k1":89000}] ',"$") as u;
select * from unnest(' [23,"gooooogle",{"k1":89000}] ',"$[2]") as u;

--非数组，jsontype类型
select * from unnest(json_table_1.j1,"$.key10",true) as u;
select * from unnest(json_table_1.j1,"$.key_56",true) as u;
select * from unnest(json_table_1.j1,"$.key_56") as u;

--select xxx from unnest where xxx
select * from unnest(json_table_1.j1,"$") as u where u.`key`="key10";
select seq,value from unnest(json_table_1.j1,"$") as u where u.`path` like "%56";

--insert into table select xxx from unnest
create table unnest_table_1(col1 varchar(255),col2 int,col3 varchar(255),col4 varchar(255),col5 int,col6 varchar(255),col7 varchar(255));
insert into unnest_table_1 select * from unnest(json_table_1.j1,"$.*") as u;

-参数不是json类型,语法错误
select * from unnest('abc',"$.*") as u;
select unnest('abc',"$.*") ;