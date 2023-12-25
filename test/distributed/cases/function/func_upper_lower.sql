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


-- to_upper nested with string function
select to_upper(ltrim('   ueenjfwabc123'));
select upper(rtrim('  3782dfw23123123123   '));
select to_upper(trim('  32431 %^ 3829  3huicn2432g23   '));
select upper(substring('21214avewwe12',3,20));
select upper(reverse('sjkdakjevenjwvev')) as result;


-- to_lower nested with string function
select lower(ltrim('   uEENjfwabc123'));
select to_lower(rtrim('  3782dfWWWW23123123123   '));
select lower(trim('  32431 %^ 3829  3huICN2432g23   '));
select to_lower(substring('21214AewWE12',3,20));
select lower(reverse('sjkDAKjeveBJwvev')) as result;

