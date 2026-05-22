-- @suit

-- @case
-- @desc:test for PostgreSQL-style UPDATE ... SET ... FROM ... WHERE ...
-- @label:bvt

DROP TABLE IF EXISTS company;
DROP TABLE IF EXISTS vec_join_case;

CREATE TABLE company (
    id INT PRIMARY KEY,
    province VARCHAR(50)
);
INSERT INTO company VALUES (101, 'BJ'), (102, 'SH'), (103, 'GZ');

CREATE TABLE vec_join_case (
    id INT PRIMARY KEY,
    embedding VECF32(4),
    company_id INT,
    remark VARCHAR(100)
);
INSERT INTO vec_join_case VALUES
(10, '[0.1,0.2,0.3,0.4]', 101, 'init'),
(20, '[0.3,0.1,0.4,0.2]', 102, 'init'),
(30, '[0.5,0.6,0.4,0.3]', 103, 'init');

-- Basic PostgreSQL-style UPDATE ... FROM with vector distance predicate.
UPDATE vec_join_case t
SET remark = CONCAT('hot-', c.province)
FROM company c
WHERE c.id = t.company_id
  AND l2_distance(embedding, '[0.2,0.2,0.3,0.3]') < 0.35;
SELECT id, company_id, remark FROM vec_join_case ORDER BY id;

-- Reset and try without aliases.
UPDATE vec_join_case SET remark = 'init';
UPDATE vec_join_case
SET remark = company.province
FROM company
WHERE company.id = vec_join_case.company_id;
SELECT id, company_id, remark FROM vec_join_case ORDER BY id;

-- Multiple tables in the FROM clause.
DROP TABLE IF EXISTS region;
CREATE TABLE region (id INT PRIMARY KEY, name VARCHAR(20));
INSERT INTO region VALUES (1, 'east'), (2, 'south');
ALTER TABLE company ADD COLUMN region_id INT;
UPDATE company SET region_id = 1 WHERE id = 101;
UPDATE company SET region_id = 1 WHERE id = 102;
UPDATE company SET region_id = 2 WHERE id = 103;

UPDATE vec_join_case t
SET remark = r.name
FROM company c, region r
WHERE c.id = t.company_id AND c.region_id = r.id;
SELECT id, company_id, remark FROM vec_join_case ORDER BY id;

-- WITH ... UPDATE ... FROM (CTE referenced in FROM).
WITH cc AS (SELECT id, province FROM company)
UPDATE vec_join_case t
SET remark = c.province
FROM cc c
WHERE c.id = t.company_id;
SELECT id, company_id, remark FROM vec_join_case ORDER BY id;

DROP TABLE IF EXISTS company;
DROP TABLE IF EXISTS vec_join_case;
DROP TABLE IF EXISTS region;
