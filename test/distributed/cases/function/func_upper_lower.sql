-- test function to_upper
select to_upper('abc'), upper('abc');
select to_upper('abc 123'), upper('abc 123');
select to_upper(null);

create table up_t(a varchar(10), b char(10));
insert into up_t values('abc', 'abc');
insert into up_t values(null, null);
insert into up_t values('abc 123', 'abc 123');
select to_upper(a), upper(a) from up_t;
select to_upper(b), upper(b) from up_t;


-- test function to_lower
select to_lower('ABC'), lower('ABC');
select to_lower('AbC 123'), lower('AbC 123');
select to_lower(null);

create table low_t(a varchar(10), b char(10));
insert into low_t values('ABC', 'ABC');
insert into low_t values(null, null);
insert into low_t values('AbC 123', 'AbC 123');
select to_lower(a), lower(a) from low_t;
select to_lower(b), lower(b) from low_t;