-- @suite
-- @setup
drop table if exists ms_t_stk_sis;
create table ms_t_stk_sis (
  SITXDT varchar(24) DEFAULT NULL,
  SISTKC varchar(10) DEFAULT NULL,
  SISTKN varchar(100) DEFAULT NULL,
  SIHIGH decimal(16,4) DEFAULT NULL,
  SILOW decimal(16,4) DEFAULT NULL,
  SICLSE decimal(16,4) DEFAULT NULL,
  SIVOL bigint DEFAULT NULL
);
INSERT INTO ms_t_stk_sis (SITXDT, SISTKC, SISTKN, SIHIGH, SILOW, SICLSE, SIVOL) VALUES
('02JAN2025:00:00:00', '01870', 'ACME INTL HLDGS', 1.8400, 1.7600, 1.8400, 1395000),
('02JAN2025:00:00:00', '01871', 'CHINA ORIENTED', NULL, NULL, 0.1990, 0),
('02JAN2025:00:00:00', '01872', 'GUAN CHAO HLDGS', 1.2400, 1.0100, 1.0600, 230500);

-- @case
-- @desc:test scalar subquery with aggregation and non-equality correlated predicates
SELECT s.SISTKC,
  (SELECT AVG(s2.SIVOL)
   FROM ms_t_stk_sis s2
   WHERE s2.SISTKC = s.SISTKC
     AND STR_TO_DATE(s2.SITXDT, '%d%b%Y:%H:%i:%s') < STR_TO_DATE(s.SITXDT, '%d%b%Y:%H:%i:%s')
     AND STR_TO_DATE(s2.SITXDT, '%d%b%Y:%H:%i:%s') >= STR_TO_DATE(s.SITXDT, '%d%b%Y:%H:%i:%s') - INTERVAL 30 DAY
  ) AS avg_30d_vol
FROM ms_t_stk_sis s;

-- @case
-- @desc:test scalar subquery with aggregation and less-than correlated predicate
drop table if exists t1;
create table t1 (a int, b int);
insert into t1 values (1, 10), (2, 20), (3, 30), (1, 40), (2, 50);
select a, (select avg(t2.b) from t1 t2 where t2.a = t1.a and t2.b < t1.b) as avg_less from t1 order by a, b;

-- @teardown
drop table if exists ms_t_stk_sis;
drop table if exists t1;
