-- @ignore:0,1
SELECT NOW() AS `current_time`, NOW() - INTERVAL 1 DAY AS `one_day_ago`;
-- @ignore:0,1
SELECT NOW() AS `current_time`, NOW() + INTERVAL 1 DAY AS `one_day_later`;
-- @ignore:0,1
SELECT NOW() AS `current_time`, NOW() - INTERVAL 1 WEEK AS one_week_ago;
-- @ignore:0,1
SELECT NOW() AS `current_time`, NOW() + INTERVAL 1 WEEK AS one_week_later;
-- @ignore:0,1
SELECT NOW() AS `current_time`, DATE_SUB(NOW(), INTERVAL 1 MONTH) AS one_month_ago;
-- @ignore:0,1
SELECT NOW() AS `current_time`, DATE_ADD(NOW(), INTERVAL 1 MONTH) AS one_month_later;
-- @ignore:0,1
SELECT NOW() AS `current_time`, DATE_SUB(NOW(), INTERVAL 1 YEAR) AS one_year_ago;
-- @ignore:0,1
SELECT NOW() AS `current_time`, DATE_ADD(NOW(), INTERVAL 1 YEAR) AS one_year_later;
-- @ignore:0,1
SELECT NOW() AS `current_time`, NOW() - INTERVAL 30 MINUTE AS thirty_minutes_ago;
-- @ignore:0,1
SELECT NOW() AS `current_time`, NOW() + INTERVAL 30 MINUTE AS thirty_minutes_later;
