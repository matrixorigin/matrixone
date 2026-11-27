-- Snapshot restore recreates physical tables. A restored DATA BRANCH database
-- must retain active table lineage, reject locally-added ordinary tables, and
-- remain deletable after those unrelated tables are removed.

drop snapshot if exists issue32820_branch_snapshot;
drop snapshot if exists issue32820_ordinary_snapshot;
drop database if exists issue32820_branch_source;
drop database if exists issue32820_branch_restored;
drop database if exists issue32820_ordinary;

create database issue32820_branch_source;
create table issue32820_branch_source.t(id int primary key);
insert into issue32820_branch_source.t values(1);
data branch create database issue32820_branch_restored from issue32820_branch_source;
create snapshot issue32820_branch_snapshot for database issue32820_branch_restored;
data branch delete database issue32820_branch_restored;
restore database issue32820_branch_restored{snapshot="issue32820_branch_snapshot"};
select dat_type from mo_catalog.mo_database where datname = 'issue32820_branch_restored';
select * from issue32820_branch_restored.t;
select count(*) as active_restored_lineage
from mo_catalog.mo_branch_metadata
where table_id = (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = 'issue32820_branch_restored' and relname = 't'
)
and p_table_id = (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = 'issue32820_branch_source' and relname = 't'
)
and level = 'database' and table_deleted = false;
select count(*) as restored_branch_protection
from mo_catalog.mo_snapshots
where kind = 'branch'
and sname = concat('__mo_branch_', (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = 'issue32820_branch_restored' and relname = 't'
))
and obj_id = (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = 'issue32820_branch_source' and relname = 't'
);

create table issue32820_branch_restored.local_t(id int primary key);
data branch delete database issue32820_branch_restored;
select count(*) as protected_local_table
from mo_catalog.mo_tables
where reldatabase = 'issue32820_branch_restored' and relname = 'local_t';
drop table issue32820_branch_restored.local_t;
data branch delete database issue32820_branch_restored;
select count(*) as restored_branch_exists
from mo_catalog.mo_database where datname = 'issue32820_branch_restored';

-- Ordinary snapshot restore keeps its existing behavior.
create database issue32820_ordinary;
create table issue32820_ordinary.t(id int primary key);
insert into issue32820_ordinary.t values(2);
create snapshot issue32820_ordinary_snapshot for database issue32820_ordinary;
drop database issue32820_ordinary;
restore database issue32820_ordinary{snapshot="issue32820_ordinary_snapshot"};
select coalesce(nullif(dat_type, ''), 'ordinary') as database_type
from mo_catalog.mo_database where datname = 'issue32820_ordinary';
select * from issue32820_ordinary.t;
drop database issue32820_ordinary;

drop snapshot if exists issue32820_branch_snapshot;
drop snapshot if exists issue32820_ordinary_snapshot;
drop database if exists issue32820_branch_source;
drop database if exists issue32820_branch_restored;
drop database if exists issue32820_ordinary;
