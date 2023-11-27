select sha2("abc", 0);
select sha2("123", 256);
select sha2("好好学习 天天向上", 224);
select sha2("hello world", 384);
select sha2("sha512", 512);
select sha2("hello world", 66666);
select sha2("+++++-------,./;[p][]", 512);
select sha2(null, 512);
select sha2(null, null);

create table shatwo01 (a text);
insert into shatwo01 values("网络安全"),("database"),(null);
select a,sha2(a,0) from shatwo01;
