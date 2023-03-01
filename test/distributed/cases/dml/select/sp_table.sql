-- @bvt:issue#8185
select reldatabase,relname,relkind from mo_catalog.mo_tables where relname = '%!%mo_increment_columns' and account_id = 0 order by reldatabase;
-- @bvt:issue
select relname,relkind from mo_catalog.mo_tables where reldatabase = 'mo_catalog' and account_id = 0 order by relname;
