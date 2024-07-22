select mo_cpu("total") >= mo_cpu("available");
select mo_memory("total") >= mo_memory("available");
select * from information_schema.files limit 1;
