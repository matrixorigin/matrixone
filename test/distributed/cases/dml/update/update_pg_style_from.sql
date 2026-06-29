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

-- Duplicate source rows must not make generated columns come from a different
-- source row than their referenced base columns.
DROP TABLE IF EXISTS gen_dup_t;
DROP TABLE IF EXISTS gen_dup_s;
CREATE TABLE gen_dup_t (
    id INT PRIMARY KEY,
    base INT,
    gen_col INT AS (ifnull(base, 0)) STORED
);
INSERT INTO gen_dup_t (id, base) VALUES (1, 5);
CREATE TABLE gen_dup_s (t_id INT, new_base INT);
INSERT INTO gen_dup_s VALUES (1, NULL), (1, 7);
UPDATE gen_dup_t SET base = s.new_base FROM gen_dup_s s WHERE s.t_id = gen_dup_t.id;
SELECT COUNT(*) AS valid_generated_row FROM gen_dup_t
WHERE (base IS NULL AND gen_col = 0) OR (base = 7 AND gen_col = 7);
DROP TABLE gen_dup_t;
DROP TABLE gen_dup_s;

-- Duplicate source rows must be deduped as whole rows. Per-column any_value()
-- can synthesize (new_a = 7, new_b = 'from-null-a'), which is not present in
-- the source.
DROP TABLE IF EXISTS whole_row_t;
DROP TABLE IF EXISTS whole_row_s;
CREATE TABLE whole_row_t (
    id INT PRIMARY KEY,
    a INT,
    b VARCHAR(20)
);
CREATE TABLE whole_row_s (
    t_id INT,
    new_a INT,
    new_b VARCHAR(20)
);
INSERT INTO whole_row_t VALUES (1, 0, 'orig');
INSERT INTO whole_row_s VALUES (1, NULL, 'from-null-a'), (1, 7, NULL);
UPDATE whole_row_t SET a = s.new_a, b = s.new_b FROM whole_row_s s WHERE s.t_id = whole_row_t.id;
SELECT COUNT(*) AS valid_whole_row FROM whole_row_t
WHERE (a IS NULL AND b = 'from-null-a') OR (a = 7 AND b IS NULL);
SELECT COUNT(*) AS synthesized_row FROM whole_row_t WHERE a = 7 AND b = 'from-null-a';
DROP TABLE whole_row_t;
DROP TABLE whole_row_s;

-- FK target with a generated column: the FK forces the fallback planner
-- (buildTableUpdate). Generated column protection must still apply there.
DROP TABLE IF EXISTS fk_parent;
DROP TABLE IF EXISTS fk_child;
DROP TABLE IF EXISTS fk_src;
CREATE TABLE fk_parent (id INT PRIMARY KEY);
INSERT INTO fk_parent VALUES (1), (2), (3);
CREATE TABLE fk_child (
    id INT PRIMARY KEY,
    parent_id INT,
    base INT,
    gen_col INT AS (base * 10) STORED,
    FOREIGN KEY (parent_id) REFERENCES fk_parent(id)
);
INSERT INTO fk_child (id, parent_id, base) VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
CREATE TABLE fk_src (id INT PRIMARY KEY, new_base INT);
INSERT INTO fk_src VALUES (1, 11), (2, 22), (3, 33);

-- Direct write to a generated column must error even on the fallback path.
UPDATE fk_child SET gen_col = 999 FROM fk_src WHERE fk_src.id = fk_child.id;

-- Base-column update must recompute the stored generated column on the
-- fallback path too.
UPDATE fk_child SET base = fk_src.new_base FROM fk_src WHERE fk_src.id = fk_child.id;
SELECT id, parent_id, base, gen_col FROM fk_child ORDER BY id;
DROP TABLE fk_child;
DROP TABLE fk_parent;
DROP TABLE fk_src;

-- Self-join: target and source share the same table.
DROP TABLE IF EXISTS sj;
CREATE TABLE sj (id INT PRIMARY KEY, parent_id INT, v VARCHAR(20));
INSERT INTO sj VALUES (1, NULL, 'root'), (2, 1, 'child2'), (3, 1, 'child3'), (4, 2, 'leaf');
UPDATE sj t SET v = p.v FROM sj p WHERE t.parent_id = p.id;
SELECT id, parent_id, v FROM sj ORDER BY id;
DROP TABLE sj;

-- ORDER BY / LIMIT are not part of the PG-style UPDATE ... FROM grammar; both
-- should be rejected at parse time.
DROP TABLE IF EXISTS ob_t;
DROP TABLE IF EXISTS ob_s;
CREATE TABLE ob_t (id INT PRIMARY KEY, v INT);
CREATE TABLE ob_s (id INT PRIMARY KEY, v INT);
INSERT INTO ob_t VALUES (1, 0), (2, 0);
INSERT INTO ob_s VALUES (1, 9), (2, 9);
UPDATE ob_t SET v = ob_s.v FROM ob_s WHERE ob_t.id = ob_s.id ORDER BY ob_t.id;
UPDATE ob_t SET v = ob_s.v FROM ob_s WHERE ob_t.id = ob_s.id LIMIT 1;
DROP TABLE ob_t;
DROP TABLE ob_s;

-- Duplicate-match on the fallback path: target row 1 is matched by both
-- (10,1,...) and (11,1,...) source rows. Because dup_t has a FK the fallback
-- planner (buildTableUpdate) handles this. It must still dedup duplicate
-- matches instead of silently double-writing target row 1.
DROP TABLE IF EXISTS dup_t;
DROP TABLE IF EXISTS dup_p;
DROP TABLE IF EXISTS dup_s;
CREATE TABLE dup_p (id INT PRIMARY KEY);
INSERT INTO dup_p VALUES (1), (2);
CREATE TABLE dup_t (
    id INT PRIMARY KEY,
    p_id INT,
    v VARCHAR(20),
    FOREIGN KEY (p_id) REFERENCES dup_p(id)
);
INSERT INTO dup_t VALUES (1, 1, 'a'), (2, 2, 'b');
CREATE TABLE dup_s (id INT PRIMARY KEY, t_id INT, v VARCHAR(20));
INSERT INTO dup_s VALUES (10, 1, 's1-first'), (11, 1, 's1-second'), (20, 2, 's2');
UPDATE dup_t SET v = s.v FROM dup_s s WHERE s.t_id = dup_t.id;
SELECT id, p_id, v FROM dup_t ORDER BY id;
DROP TABLE dup_t;
DROP TABLE dup_p;
DROP TABLE dup_s;

-- Fallback UPDATE ... FROM dedup must also pick a whole source row. FK on the
-- target forces buildTableUpdate instead of the new bindUpdate path.
DROP TABLE IF EXISTS fk_whole_row_t;
DROP TABLE IF EXISTS fk_whole_row_p;
DROP TABLE IF EXISTS fk_whole_row_s;
CREATE TABLE fk_whole_row_p (id INT PRIMARY KEY);
INSERT INTO fk_whole_row_p VALUES (1);
CREATE TABLE fk_whole_row_t (
    id INT PRIMARY KEY,
    p_id INT,
    a INT,
    b VARCHAR(20),
    FOREIGN KEY (p_id) REFERENCES fk_whole_row_p(id)
);
CREATE TABLE fk_whole_row_s (
    t_id INT,
    new_a INT,
    new_b VARCHAR(20)
);
INSERT INTO fk_whole_row_t VALUES (1, 1, 0, 'orig');
INSERT INTO fk_whole_row_s VALUES (1, NULL, 'from-null-a'), (1, 7, NULL);
UPDATE fk_whole_row_t SET a = s.new_a, b = s.new_b FROM fk_whole_row_s s WHERE s.t_id = fk_whole_row_t.id;
SELECT COUNT(*) AS valid_whole_row FROM fk_whole_row_t
WHERE (a IS NULL AND b = 'from-null-a') OR (a = 7 AND b IS NULL);
SELECT COUNT(*) AS synthesized_row FROM fk_whole_row_t WHERE a = 7 AND b = 'from-null-a';
DROP TABLE fk_whole_row_t;
DROP TABLE fk_whole_row_p;
DROP TABLE fk_whole_row_s;

-- Duplicate-match on the new UPDATE path without FK constraints must still
-- update each target row once instead of producing duplicate primary keys.
DROP TABLE IF EXISTS dup_no_fk_t;
DROP TABLE IF EXISTS dup_no_fk_s;
CREATE TABLE dup_no_fk_t (
    id INT PRIMARY KEY,
    v VARCHAR(20)
);
CREATE TABLE dup_no_fk_s (
    t_id INT,
    v VARCHAR(20)
);
INSERT INTO dup_no_fk_t VALUES (1, 'orig'), (2, 'orig');
INSERT INTO dup_no_fk_s VALUES (1, 'first'), (1, 'second'), (2, 'only');
UPDATE dup_no_fk_t SET v = s.v FROM dup_no_fk_s s WHERE s.t_id = dup_no_fk_t.id;
SELECT COUNT(*) AS row_cnt, COUNT(DISTINCT id) AS id_cnt FROM dup_no_fk_t;
SELECT id, COUNT(*) AS per_id_cnt FROM dup_no_fk_t GROUP BY id ORDER BY id;
SELECT v FROM dup_no_fk_t WHERE id = 2;
DROP TABLE dup_no_fk_t;
DROP TABLE dup_no_fk_s;

-- A GEOMETRY32 target column has no comparator, so a dedup key built from the
-- whole old target row would crash Node_PARTITION. Dedup now partitions on the
-- target row_id, so UPDATE ... FROM with duplicate source matches succeeds on
-- the new bindUpdate path (no FK) and updates each target row exactly once.
DROP TABLE IF EXISTS geo_dedup_t;
DROP TABLE IF EXISTS geo_dedup_s;
CREATE TABLE geo_dedup_t (
    id INT PRIMARY KEY,
    g GEOMETRY32,
    v VARCHAR(20)
);
CREATE TABLE geo_dedup_s (
    t_id INT,
    v VARCHAR(20)
);
INSERT INTO geo_dedup_t VALUES
    (1, ST_GeomFromText('POINT(1 2)'), 'orig'),
    (2, ST_GeomFromText('POINT(3 4)'), 'orig');
INSERT INTO geo_dedup_s VALUES (1, 'first'), (1, 'second'), (2, 'only');
UPDATE geo_dedup_t SET v = s.v FROM geo_dedup_s s WHERE s.t_id = geo_dedup_t.id;
SELECT COUNT(*) AS row_cnt, COUNT(DISTINCT id) AS id_cnt FROM geo_dedup_t;
SELECT id, COUNT(*) AS per_id_cnt FROM geo_dedup_t GROUP BY id ORDER BY id;
SELECT v FROM geo_dedup_t WHERE id = 2;
DROP TABLE geo_dedup_t;
DROP TABLE geo_dedup_s;

-- Same GEOMETRY32 guard on the fallback path: a foreign key on the target
-- forces buildTableUpdate, which must also dedup on row_id instead of the
-- whole old row.
DROP TABLE IF EXISTS geo_fk_dedup_t;
DROP TABLE IF EXISTS geo_fk_dedup_p;
DROP TABLE IF EXISTS geo_fk_dedup_s;
CREATE TABLE geo_fk_dedup_p (id INT PRIMARY KEY);
INSERT INTO geo_fk_dedup_p VALUES (1), (2);
CREATE TABLE geo_fk_dedup_t (
    id INT PRIMARY KEY,
    p_id INT,
    g GEOMETRY32,
    v VARCHAR(20),
    FOREIGN KEY (p_id) REFERENCES geo_fk_dedup_p(id)
);
CREATE TABLE geo_fk_dedup_s (
    t_id INT,
    v VARCHAR(20)
);
INSERT INTO geo_fk_dedup_t VALUES
    (1, 1, ST_GeomFromText('POINT(1 2)'), 'orig'),
    (2, 2, ST_GeomFromText('POINT(3 4)'), 'orig');
INSERT INTO geo_fk_dedup_s VALUES (1, 'first'), (1, 'second'), (2, 'only');
UPDATE geo_fk_dedup_t SET v = s.v FROM geo_fk_dedup_s s WHERE s.t_id = geo_fk_dedup_t.id;
SELECT COUNT(*) AS row_cnt, COUNT(DISTINCT id) AS id_cnt FROM geo_fk_dedup_t;
SELECT id, COUNT(*) AS per_id_cnt FROM geo_fk_dedup_t GROUP BY id ORDER BY id;
SELECT v FROM geo_fk_dedup_t WHERE id = 2;
DROP TABLE geo_fk_dedup_t;
DROP TABLE geo_fk_dedup_p;
DROP TABLE geo_fk_dedup_s;

-- Fallback multi-target UPDATE ... FROM with LEFT JOIN target must still drop
-- NULL rows of unmatched left-join targets. jt1.id=2 has no jt2 match, so
-- jt2's row_id is NULL for that row; it must be filtered out of jt2's update
-- pipeline while jt1.id=2 is still updated. Replacing the any_value dedup with
-- a row_number() window must not lose this NULL-row safeguard.
-- (Multi-target routes through the fallback buildTableUpdate path.)
DROP TABLE IF EXISTS jt1;
DROP TABLE IF EXISTS jt2;
DROP TABLE IF EXISTS js;
CREATE TABLE jt1 (id INT PRIMARY KEY, v VARCHAR(20));
CREATE TABLE jt2 (id INT PRIMARY KEY, k INT, v VARCHAR(20));
CREATE TABLE js (t_id INT, v VARCHAR(20));
INSERT INTO jt1 VALUES (1, 'a'), (2, 'b');
INSERT INTO jt2 VALUES (10, 1, 'x');
INSERT INTO js VALUES (1, 'new1'), (2, 'new2');
UPDATE jt1 LEFT JOIN jt2 ON jt2.k = jt1.id SET jt1.v = s.v, jt2.v = s.v FROM js s WHERE s.t_id = jt1.id;
SELECT id, v FROM jt1 ORDER BY id;
SELECT id, k, v FROM jt2 ORDER BY id;
SELECT COUNT(*) AS jt2_cnt FROM jt2;
DROP TABLE jt1;
DROP TABLE jt2;
DROP TABLE js;

-- Single-target UPDATE ... FROM with a LEFT JOIN on the source side routes
-- through the new bindUpdate path. The target is always INNER-joined so its
-- row_id is never NULL, and dedup on row_id must succeed for duplicate source
-- matches even when the source uses LEFT JOIN.
DROP TABLE IF EXISTS left_src_t;
DROP TABLE IF EXISTS left_src_s1;
DROP TABLE IF EXISTS left_src_s2;
CREATE TABLE left_src_t (id INT PRIMARY KEY, v VARCHAR(20));
CREATE TABLE left_src_s1 (t_id INT, v VARCHAR(20));
CREATE TABLE left_src_s2 (t_id INT, tag VARCHAR(10));
INSERT INTO left_src_t VALUES (1, 'orig'), (2, 'orig');
INSERT INTO left_src_s1 VALUES (1, 'first'), (1, 'second'), (2, 'only');
INSERT INTO left_src_s2 VALUES (1, 'tag1');
UPDATE left_src_t SET v = s1.v FROM left_src_s1 s1 LEFT JOIN left_src_s2 s2 ON s1.t_id = s2.t_id WHERE s1.t_id = left_src_t.id;
SELECT COUNT(*) AS row_cnt, COUNT(DISTINCT id) AS id_cnt FROM left_src_t;
SELECT id, COUNT(*) AS per_id_cnt FROM left_src_t GROUP BY id ORDER BY id;
SELECT v FROM left_src_t WHERE id = 2;
DROP TABLE left_src_t;
DROP TABLE left_src_s1;
DROP TABLE left_src_s2;

-- UPDATE ... FROM dedup via row_id must also work on tables without an
-- explicit PRIMARY KEY (fake-PK / hidden row_id only).
DROP TABLE IF EXISTS no_pk_t;
DROP TABLE IF EXISTS no_pk_s;
CREATE TABLE no_pk_t (v VARCHAR(20));
CREATE TABLE no_pk_s (t_id INT, v VARCHAR(20));
INSERT INTO no_pk_t VALUES ('orig');
INSERT INTO no_pk_s VALUES (1, 'first'), (1, 'second');
UPDATE no_pk_t SET v = s.v FROM no_pk_s s;
SELECT COUNT(*) AS row_cnt FROM no_pk_t;
SELECT v FROM no_pk_t;
DROP TABLE no_pk_t;
DROP TABLE no_pk_s;

DROP TABLE IF EXISTS company;
DROP TABLE IF EXISTS vec_join_case;
DROP TABLE IF EXISTS region;
