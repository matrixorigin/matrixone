-- others like select version();
set @ts=now();
select @@version_comment limit 1;

-- fir pr #7971
use system;
use mysql;
/* cloud_user */select * from user limit 0;

-- select count(1) as cnt, statement_id, statement, status from system.statement_info group by statement_id, statement, status having count(1) > 1;
-- WITH duplicated_ids AS (
--     SELECT statement_id
--     FROM system.statement_info
--     GROUP BY statement_id
--     HAVING COUNT(1) > 1
-- ),
--      same_status AS (
--          SELECT s.statement_id, s.status
--          FROM system.statement_info s
--                   JOIN duplicated_ids d ON s.statement_id = d.statement_id
--          GROUP BY s.statement_id, s.status
--          HAVING COUNT(1) > 1
--      ),
--      same_statement AS (
--          SELECT s.statement_id, s.status, s.statement
--          FROM system.statement_info s
--                   JOIN same_status ss ON s.statement_id = ss.statement_id AND s.status = ss.status
--          GROUP BY s.statement_id, s.status, s.statement
--          HAVING COUNT(1) > 1
--      )
-- SELECT COUNT(1) AS cnt, s.statement_id, s.status, s.statement
-- FROM system.statement_info s
--          JOIN same_statement ss
--               ON s.statement_id = ss.statement_id
--                   AND s.status = ss.status
--                   AND s.statement = ss.statement
-- GROUP BY s.statement_id, s.status, s.statement
-- ORDER BY cnt DESC;

WITH duplicated_ids AS (     SELECT statement_id     FROM system.statement_info     GROUP BY statement_id     HAVING COUNT(1) > 1 ),      same_status AS (          SELECT s.statement_id, s.status          FROM system.statement_info s                   JOIN duplicated_ids d ON s.statement_id = d.statement_id          GROUP BY s.statement_id, s.status          HAVING COUNT(1) > 1      ),      same_statement AS (          SELECT s.statement_id, s.status, s.statement          FROM system.statement_info s                   JOIN same_status ss ON s.statement_id = ss.statement_id AND s.status = ss.status          GROUP BY s.statement_id, s.status, s.statement          HAVING COUNT(1) > 1      ) SELECT COUNT(1) AS cnt, s.statement_id, s.status, s.statement FROM system.statement_info s          JOIN same_statement ss               ON s.statement_id = ss.statement_id                   AND s.status = ss.status                   AND s.statement = ss.statement GROUP BY s.statement_id, s.status, s.statement ORDER BY cnt DESC;

-- fix issue 14,836: move check-sql into ../zz_statement_query_type/query_version.*
-- select `database`, `aggr_count` from system.statement_info where statement LIKE '%select * from user limit 0%' order by request_at desc limit 1;
