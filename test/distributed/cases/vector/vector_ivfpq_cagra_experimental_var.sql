-- experimental_ivfpq_index / experimental_cagra_index: system-variable surface
-- only — default value, SET (session), SET GLOBAL, SHOW VARIABLES. No index is
-- created, so this is build-independent and runs identically on CPU and GPU
-- builds (the GPU-backed CREATE INDEX path is intentionally not exercised here).

-- defaults: both off (0)
select @@experimental_ivfpq_index, @@experimental_cagra_index;

-- registered and visible via SHOW VARIABLES
show variables like 'experimental_ivfpq_index';
show variables like 'experimental_cagra_index';

-- SET (session): enable both, read back
set experimental_ivfpq_index = 1;
set experimental_cagra_index = 1;
select @@experimental_ivfpq_index, @@experimental_cagra_index;

-- toggle back off (session)
set experimental_ivfpq_index = 0;
set experimental_cagra_index = 0;
select @@experimental_ivfpq_index, @@experimental_cagra_index;

-- SET GLOBAL: enable both, read the global scope
set global experimental_ivfpq_index = 1;
set global experimental_cagra_index = 1;
select @@global.experimental_ivfpq_index, @@global.experimental_cagra_index;

-- reset global so the flags don't leak into other cases
set global experimental_ivfpq_index = 0;
set global experimental_cagra_index = 0;
select @@global.experimental_ivfpq_index, @@global.experimental_cagra_index;
