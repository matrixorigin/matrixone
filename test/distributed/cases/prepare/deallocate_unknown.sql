-- unknown and already deallocated statements must report an error
deallocate prepare missing_stmt;
select 1;

prepare deallocated_stmt from 'select 1';
deallocate prepare deallocated_stmt;
deallocate prepare deallocated_stmt;
select 2;
