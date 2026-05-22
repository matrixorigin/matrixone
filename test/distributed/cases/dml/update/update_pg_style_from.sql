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

-- FROM-clause join tree must keep its associativity (b LEFT JOIN c on ...).
UPDATE vec_join_case SET remark = 'init';
UPDATE vec_join_case t
SET remark = COALESCE(r.name, 'no-region')
FROM company c LEFT JOIN region r ON c.region_id = r.id
WHERE c.id = t.company_id;
SELECT id, company_id, remark FROM vec_join_case ORDER BY id;

-- Source is a view: read-only references in FROM must not be rejected as
-- "cannot update from view".
DROP VIEW IF EXISTS v_company;
CREATE VIEW v_company AS SELECT id, province FROM company;
UPDATE vec_join_case SET remark = 'init';
UPDATE vec_join_case t
SET remark = v.province
FROM v_company v
WHERE v.id = t.company_id;
SELECT id, company_id, remark FROM vec_join_case ORDER BY id;
DROP VIEW v_company;

-- Unqualified SET LHS must bind to the target only. Both vec_join_case
-- and src_overlap expose a `remark` column; this used to be flagged as
-- ambiguous before the fix.
DROP TABLE IF EXISTS src_overlap;
CREATE TABLE src_overlap (company_id INT, remark VARCHAR(100));
INSERT INTO src_overlap VALUES (101, 'src-BJ'), (102, 'src-SH'), (103, 'src-GZ');
UPDATE vec_join_case SET remark = 'init';
UPDATE vec_join_case
SET remark = src_overlap.remark
FROM src_overlap
WHERE src_overlap.company_id = vec_join_case.company_id;
SELECT id, company_id, remark FROM vec_join_case ORDER BY id;
DROP TABLE src_overlap;

-- Generated-column protection must still apply: SET on a stored generated
-- column should be rejected. Base-column update through UPDATE ... FROM
-- should recompute the generated column.
DROP TABLE IF EXISTS gen_t;
CREATE TABLE gen_t (
    id INT PRIMARY KEY,
    base INT,
    gen_col INT AS (base * 2) STORED
);
INSERT INTO gen_t (id, base) VALUES (1, 10), (2, 20), (3, 30);
DROP TABLE IF EXISTS gen_src;
CREATE TABLE gen_src (id INT PRIMARY KEY, new_base INT);
INSERT INTO gen_src VALUES (1, 100), (2, 200);

-- Direct write to a generated column must error (captured in result file).
UPDATE gen_t SET gen_col = 999 FROM gen_src WHERE gen_src.id = gen_t.id;

-- Update of the base column must recompute the stored generated column.
UPDATE gen_t SET base = gen_src.new_base FROM gen_src WHERE gen_src.id = gen_t.id;
SELECT id, base, gen_col FROM gen_t ORDER BY id;

DROP TABLE gen_t;
DROP TABLE gen_src;
DROP TABLE IF EXISTS company;
DROP TABLE IF EXISTS vec_join_case;
DROP TABLE IF EXISTS region;
