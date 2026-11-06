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
drop database view_metadata_wire;
