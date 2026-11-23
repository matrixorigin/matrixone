--- @metacmp(true)
-- SHOW/DESC column definitions have a surface-specific, bounded wire contract.
drop database if exists view_metadata_wire;
create database view_metadata_wire;
create table view_metadata_wire.src (x varchar(8), `odd column` varchar(8), `tick``column` int);
create view view_metadata_wire.v as select x, `tick``column` from view_metadata_wire.src;
show columns from view_metadata_wire.src;
show full columns from view_metadata_wire.src;
show columns from view_metadata_wire.v;
show full columns from view_metadata_wire.v;
desc view_metadata_wire.v;
desc view_metadata_wire.src `tick``column`;
desc view_metadata_wire.v x;
show full columns from view_metadata_wire.v where Field = 'absent';
prepare view_metadata_wire_stmt from 'show full columns from view_metadata_wire.v';
execute view_metadata_wire_stmt;
deallocate prepare view_metadata_wire_stmt;
-- Table-description aliases preserve metadata in both PREPARE grammar forms.
describe view_metadata_wire.src;
describe view_metadata_wire.v;
prepare view_metadata_wire_stmt from desc view_metadata_wire.src;
execute view_metadata_wire_stmt;
execute view_metadata_wire_stmt;
deallocate prepare view_metadata_wire_stmt;
prepare view_metadata_wire_stmt from describe view_metadata_wire.v;
execute view_metadata_wire_stmt;
deallocate prepare view_metadata_wire_stmt;
prepare view_metadata_wire_stmt from 'describe view_metadata_wire.src';
execute view_metadata_wire_stmt;
deallocate prepare view_metadata_wire_stmt;
prepare view_metadata_wire_stmt from 'desc view_metadata_wire.v';
execute view_metadata_wire_stmt;
deallocate prepare view_metadata_wire_stmt;

-- WHERE markers retain prepare context, parameter order, and empty-result metadata.
prepare show_where from 'show columns from view_metadata_wire.src where Field=?';
set @show_field='x';
execute show_where using @show_field;
set @show_field='absent';
execute show_where using @show_field;
set @show_field=NULL;
execute show_where using @show_field;
set @show_field='odd column';
execute show_where using @show_field;
deallocate prepare show_where;

prepare show_where from 'show full fields from view_metadata_wire.v where Field=? and Type=?';
set @show_field='tick`column', @show_type='int';
execute show_where using @show_field, @show_type;
set @show_type='varchar(8)';
execute show_where using @show_field, @show_type;
set @show_field='x';
execute show_where using @show_field, @show_type;
-- A schema refresh must preserve both WHERE bindings on the saved statement.
alter table view_metadata_wire.src modify column x varchar(12);
set @show_type='varchar(12)';
execute show_where using @show_field, @show_type;
deallocate prepare show_where;
set @show_field=NULL, @show_type=NULL;
drop database view_metadata_wire;
