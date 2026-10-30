-- Invalid PARALLEL must identify PARALLEL.
load data infile 'load_option_diagnostics_unused.csv' into table mo_catalog.load_option_diagnostics_missing parallel 'invalid';
-- Invalid STRICT must identify STRICT.
load data infile 'load_option_diagnostics_unused.csv' into table mo_catalog.load_option_diagnostics_missing strict 'invalid';
select 1 as connection_usable;
