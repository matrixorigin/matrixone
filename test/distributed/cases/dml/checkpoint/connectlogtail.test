
select enable_fault_injection();
select add_fault_point('fj/cn/recv/err', ':::', 'echo', 20, '');
select add_fault_point('fj/cn/recv/subsyserr', ':::', 'echo', 300, '');

select sleep(1);

select remove_fault_point('fj/cn/recv/subsyserr');

select add_fault_point('fj/cn/recv/rcacheerr', ':::', 'echo', 700, '');

select sleep(1);

select remove_fault_point('fj/cn/recv/rcacheerr');
select disable_fault_injection();

select sleep(1);

create database logtail_reconnect;
show databases;
drop database logtail_reconnect;


