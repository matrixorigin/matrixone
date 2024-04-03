-- insert ignore autoincrement primary key
create table insert_ignore_01(c1 int not null primary key,c2 varchar(10));
insert into insert_ignore_01(c2) values("a"),("b"),("c"),("d");
insert ignore into insert_ignore_01 values(3,"e"),(6,"f"),(1,"g");
insert ignore into insert_ignore_01(c2) values("h"),("g"),("k");
insert ignore into insert_ignore_01 values(NULL,NULL);

-- insert ignore unique key
create table insert_ignore_02(c1 int,c2 decimal(4,2),unique key(c1);
insert into insert_ignore_01 values(100,1234.56),(200,2345.67),(300,3456.78),(400,4567.89);




