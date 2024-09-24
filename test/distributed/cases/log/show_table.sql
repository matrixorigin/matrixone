-- check system / system_metrics in sys account
-- @bvt:issue#14293
SHOW CREATE TABLE system_metrics.metric;
SHOW CREATE TABLE system.statement_info;
-- @bvt:issue
SHOW CREATE TABLE system.rawlog;
