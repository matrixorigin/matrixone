-- check system / system_metrics in sys account
-- @bvt:issue#19912
SHOW CREATE TABLE system_metrics.metric;
SHOW CREATE TABLE system.statement_info;
SHOW CREATE TABLE system.rawlog;
-- @bvt:issue