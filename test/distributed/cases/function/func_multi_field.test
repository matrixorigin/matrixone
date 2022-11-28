#str
SELECT FIELD('Bb', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff');
SELECT FIELD('Bb', 'Bb', 'Aa', 'Cc', 'Dd', 'Ff');
SELECT FIELD('Bb', 'Aa', 'Cc', 'Bb', 'Dd', 'Ff');
SELECT FIELD('Bb', 'Aa', 'Cc', 'Dd', 'Bb', 'Ff');
SELECT FIELD('Bb', 'Aa', 'Cc', 'Dd', 'Ff', 'Bb');
SELECT FIELD('Gg', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff');

#null
SELECT FIELD(null, 'Aa', 'Bb', 'Cc', 'Dd', 'Ff');
SELECT FIELD('Bb', null, 'Bb', 'Cc', 'Dd', 'Ff');
SELECT FIELD('Bb', 'Aa', null, 'Cc', 'Dd', 'Ff');
SELECT FIELD('Bb', null, null, null, 'Bb', null, 'Dd', 'Ff');

create table test(a varchar(20));
insert into test values('Aa');
insert into test values('Bb');
insert into test values('Cc');
insert into test values('Dd');
insert into test values('Ee');
insert into test values('Ff');
SELECT FIELD(null, a, 'Aa', 'Bb', 'Cc', 'Dd', 'Ff') from test;
SELECT FIELD(a, 'Aa', 'Bb', 'Cc', 'Dd', 'Ff') from test;
SELECT FIELD('Aa', a, 'Bb', 'Cc', 'Dd', 'Ff') from test;
SELECT FIELD('Aa', 'Bb', a, 'Cc', 'Dd', 'Ff') from test;
SELECT FIELD('Aa', 'Bb', 'Cc', a, 'Dd', 'Ff') from test;
SELECT FIELD('Aa', 'Aa', 'Cc', a, 'Dd', 'Ff') from test;
SELECT FIELD(a, null, null, null, null) from test;
drop table test;



#int
SELECT FIELD(1, 1, 2, 3, 4, 5);
SELECT FIELD(2, 1, 2, 3, 4, 5);
SELECT FIELD(3, 1, 2, 3, 4, 5);
SELECT FIELD(4, 1, 2, 3, 4, 5);
SELECT FIELD(5, 1, 2, 3, 4, 5);
SELECT FIELD(6, 1, 2, 3, 4, 5);

#null
SELECT FIELD(null, 1, 1, 2, 3, 4, 5);
SELECT FIELD(1, null, 2, 3, 4, 5);
SELECT FIELD(1, 1, null, 3, 4, 5);
SELECT FIELD(1, null, null, null, 1, null);

create table test(a int);
insert into test values(1);
insert into test values(2);
insert into test values(3);
insert into test values(4);
insert into test values(5);
insert into test values(6);
SELECT FIELD(null, a, 1, 2, 3, 4, 5) from test;
SELECT FIELD(a, 1, 2, 3, 4, 5) from test;
SELECT FIELD(1, a, 2, 3, 4, 5) from test;
SELECT FIELD(1, 2, a, 3, 4, 5) from test;
SELECT FIELD(1, 2, 3, a, 4, 5) from test;
SELECT FIELD(1, 2, 3, 4, a, 5) from test;
SELECT FIELD(a, null, null, null) from test;
drop table test;

#mixed
SELECT FIELD(1, '1', '2', 3, 4, 5);
SELECT FIELD(2, 1, '2', 3, 4, 5);
SELECT FIELD(3, 1, '2', 3, 4, 5);
SELECT FIELD(4, '1', '2', 3, 4, 5);
SELECT FIELD('5', 1, 2, 3, 4, 5);
SELECT FIELD('6', 1, 2, 3, 4, 5);

#null
SELECT FIELD(null, 1, '1', 2, '3', 4, 5);
SELECT FIELD(1, null, 2, '3', 4, 5);
SELECT FIELD(1, 1, null, 3, '4', 5);
SELECT FIELD(1, null, null, null, '1', null);

create table test(a int);
insert into test values(1);
insert into test values(2);
insert into test values(3);
insert into test values(4);
insert into test values(5);
insert into test values(6);
SELECT FIELD(null, a, '1', 2, 3, 4, 5) from test;
SELECT FIELD(a, '1', 2, 3, 4, 5) from test;
SELECT FIELD('1', a, 2, 3, 4, 5) from test;
SELECT FIELD(1, '2', a, 3, 4, 5) from test;
SELECT FIELD(1, 2, '3', a, 4, 5) from test;
SELECT FIELD('1', 2, 3, 4, a, 5) from test;
SELECT FIELD(a, null, null, null) from test;
drop table test;

#float
SELECT FIELD(1.39479374937490, 1.39479374937490, 1.39479374937491, 1.39479374937492, 1.39479374937493, 1.39479374937494);
SELECT FIELD(1.39479374937491, 1.39479374937490, 1.39479374937491, 1.39479374937492, 1.39479374937493, 1.39479374937494);
SELECT FIELD(1.39479374937492, 1.39479374937490, 1.39479374937491, 1.39479374937492, 1.39479374937493, 1.39479374937494);
SELECT FIELD(1.39479374937493, 1.39479374937490, 1.39479374937491, 1.39479374937492, 1.39479374937493, 1.39479374937494);
SELECT FIELD(1.39479374937494, 1.39479374937490, 1.39479374937491, 1.39479374937492, 1.39479374937493, 1.39479374937494);
SELECT FIELD(1.39479374937495, 1.39479374937490, 1.39479374937491, 1.39479374937492, 1.39479374937493, 1.39479374937494);
SELECT FIELD(1.39479374937496, 1.39479374937490, 1.39479374937491, 1.39479374937492, 1.39479374937493, 1.39479374937494);