-- @suit

-- @case
-- @desc: multi-target UPDATE uses independent physical-row selection
-- @label: bvt

DROP TABLE IF EXISTS multi_update_target_a;
DROP TABLE IF EXISTS multi_update_target_b;

CREATE TABLE multi_update_target_a (
    id INT PRIMARY KEY,
    grp INT,
    x INT,
    y INT,
    UNIQUE KEY ux_a (id, x),
    KEY iy_a (y)
);

CREATE TABLE multi_update_target_b (
    id INT PRIMARY KEY,
    grp INT,
    x INT,
    y INT,
    UNIQUE KEY ux_b (id, x),
    KEY iy_b (y)
);

INSERT INTO multi_update_target_a VALUES
    (1, 1, 0, 0),
    (2, 1, 0, 0),
    (3, 2, 0, 0);

INSERT INTO multi_update_target_b VALUES
    (10, 1, 0, 0),
    (20, 1, 0, 0),
    (30, 3, 0, 0);

UPDATE multi_update_target_a a
JOIN multi_update_target_b b ON a.grp = b.grp
SET
    a.x = b.id,
    a.y = b.id,
    b.x = a.id,
    b.y = a.id;

SELECT COUNT(*) FROM multi_update_target_a WHERE x <> y;
SELECT COUNT(*) FROM multi_update_target_b WHERE x <> y;
SELECT COUNT(*) FROM multi_update_target_a WHERE grp = 1 AND x = 0;
SELECT COUNT(*) FROM multi_update_target_b WHERE grp = 1 AND x = 0;

UPDATE multi_update_target_a a
LEFT JOIN multi_update_target_b b ON a.grp = b.grp
SET
    a.x = 7,
    b.x = 9;

UPDATE multi_update_target_a a
RIGHT JOIN multi_update_target_b b ON a.grp = b.grp
SET
    a.y = 11,
    b.y = 13;

SELECT id, x, y FROM multi_update_target_a ORDER BY id;
SELECT id, x, y FROM multi_update_target_b ORDER BY id;

DROP TABLE multi_update_target_a;
DROP TABLE multi_update_target_b;

DROP TABLE IF EXISTS multi_update_ignore_a;
DROP TABLE IF EXISTS multi_update_ignore_b;
CREATE TABLE multi_update_ignore_a (id INT PRIMARY KEY, u INT UNIQUE);
CREATE TABLE multi_update_ignore_b (id INT PRIMARY KEY, v INT);
INSERT INTO multi_update_ignore_a VALUES (1, 1), (2, 2);
INSERT INTO multi_update_ignore_b VALUES (1, 0), (2, 0);
UPDATE IGNORE multi_update_ignore_a a
JOIN multi_update_ignore_b b ON a.id = b.id
SET a.u = 2, b.v = 9
WHERE a.id = 1;
SELECT ROW_COUNT();
SELECT * FROM multi_update_ignore_a ORDER BY id;
SELECT * FROM multi_update_ignore_b ORDER BY id;
DROP TABLE multi_update_ignore_a;
DROP TABLE multi_update_ignore_b;

DROP TABLE IF EXISTS multi_update_partition_target;
DROP TABLE IF EXISTS multi_update_plain_target;
CREATE TABLE multi_update_partition_target (
    id INT PRIMARY KEY,
    x INT,
    y INT
) PARTITION BY RANGE (id) (
    PARTITION p0 VALUES LESS THAN (2),
    PARTITION p1 VALUES LESS THAN (MAXVALUE)
);
CREATE TABLE multi_update_plain_target (
    id INT PRIMARY KEY,
    x INT
);
INSERT INTO multi_update_partition_target VALUES (1, 0, 0), (2, 0, 0);
INSERT INTO multi_update_plain_target VALUES (1, 0), (2, 0);

UPDATE multi_update_partition_target p
JOIN multi_update_plain_target n ON p.id = n.id
SET
    p.x = p.x + 1,
    n.x = n.x + 2;

SELECT id, x FROM multi_update_partition_target ORDER BY id;
SELECT id, x FROM multi_update_plain_target ORDER BY id;

UPDATE multi_update_plain_target n
JOIN multi_update_partition_target p ON n.id = p.id
SET
    n.x = n.x + 2,
    p.x = p.x + 1;

SELECT id, x FROM multi_update_partition_target ORDER BY id;
SELECT id, x FROM multi_update_plain_target ORDER BY id;

UPDATE multi_update_partition_target p
JOIN multi_update_plain_target n ON p.id = n.id
SET
    p.id = p.id + 10,
    n.x = 9
WHERE p.id = 1;

SELECT ROW_COUNT();
SELECT id, x FROM multi_update_partition_target ORDER BY id;
SELECT id, x FROM multi_update_plain_target ORDER BY id;

DROP TABLE multi_update_partition_target;
DROP TABLE multi_update_plain_target;

DROP TABLE IF EXISTS multi_update_partition_outer;
DROP TABLE IF EXISTS multi_update_partition_outer_sibling;
CREATE TABLE multi_update_partition_outer (
    id INT PRIMARY KEY,
    v INT
) PARTITION BY RANGE (id) (
    PARTITION p0 VALUES LESS THAN (2),
    PARTITION p1 VALUES LESS THAN (MAXVALUE)
);
CREATE TABLE multi_update_partition_outer_sibling (id INT PRIMARY KEY, v INT);
INSERT INTO multi_update_partition_outer VALUES (1, 0);
INSERT INTO multi_update_partition_outer_sibling VALUES (1, 0), (2, 0);
UPDATE multi_update_partition_outer p
RIGHT JOIN multi_update_partition_outer_sibling n ON p.id = n.id
SET p.v = p.v + 1, n.v = n.v + 1;
SELECT ROW_COUNT();
SELECT * FROM multi_update_partition_outer ORDER BY id;
SELECT * FROM multi_update_partition_outer_sibling ORDER BY id;
TRUNCATE TABLE multi_update_partition_outer;
UPDATE multi_update_partition_outer p
RIGHT JOIN multi_update_partition_outer_sibling n ON p.id = n.id
SET p.v = 9, n.v = n.v + 1;
SELECT ROW_COUNT();
SELECT * FROM multi_update_partition_outer ORDER BY id;
SELECT * FROM multi_update_partition_outer_sibling ORDER BY id;
DROP TABLE multi_update_partition_outer;
DROP TABLE multi_update_partition_outer_sibling;

DROP TABLE IF EXISTS multi_update_master_target;
DROP TABLE IF EXISTS multi_update_master_plain;
CREATE TABLE multi_update_master_target (
    id VARCHAR(30) PRIMARY KEY,
    a VARCHAR(30),
    b VARCHAR(30)
);
CREATE INDEX idx_multi_update_master USING MASTER ON multi_update_master_target(a, b);
CREATE TABLE multi_update_master_plain (
    id VARCHAR(30) PRIMARY KEY,
    v VARCHAR(30)
);
INSERT INTO multi_update_master_target VALUES ('1', 'old', 'value');
INSERT INTO multi_update_master_plain VALUES ('1', 'old');
UPDATE multi_update_master_target m
JOIN multi_update_master_plain p ON m.id = p.id
SET
    m.a = 'changed',
    p.v = 'z';
SELECT * FROM multi_update_master_target WHERE a = 'changed' AND b = 'value';
SELECT COUNT(*) FROM multi_update_master_target WHERE a = 'old' AND b = 'value';
SELECT * FROM multi_update_master_plain ORDER BY id;
UPDATE multi_update_master_target m
JOIN multi_update_master_plain p ON m.id = p.id
SET
    m.id = '2',
    p.v = 'pk-updated';
SELECT * FROM multi_update_master_target WHERE a = 'changed' AND b = 'value';
SELECT * FROM multi_update_master_plain ORDER BY id;
DROP TABLE multi_update_master_target;
DROP TABLE multi_update_master_plain;

DROP TABLE IF EXISTS multi_update_auto_child;
DROP TABLE IF EXISTS multi_update_auto_plain;
DROP TABLE IF EXISTS multi_update_auto_parent;
CREATE TABLE multi_update_auto_parent (pid INT PRIMARY KEY);
CREATE TABLE multi_update_auto_child (
    id INT PRIMARY KEY,
    pid INT AUTO_INCREMENT,
    FOREIGN KEY (pid) REFERENCES multi_update_auto_parent(pid)
);
CREATE TABLE multi_update_auto_plain (id INT PRIMARY KEY, v INT);
INSERT INTO multi_update_auto_parent VALUES (1), (2);
INSERT INTO multi_update_auto_child (id, pid) VALUES (1, 1);
INSERT INTO multi_update_auto_plain VALUES (1, 0);
UPDATE multi_update_auto_child c
JOIN multi_update_auto_plain p ON c.id = p.id
SET
    c.pid = DEFAULT,
    p.v = 9;
SELECT id, pid FROM multi_update_auto_child;
SELECT id, v FROM multi_update_auto_plain;
DROP TABLE multi_update_auto_child;
DROP TABLE multi_update_auto_plain;
DROP TABLE multi_update_auto_parent;

DROP TABLE IF EXISTS multi_update_outer_auto_child;
DROP TABLE IF EXISTS multi_update_outer_auto_plain;
CREATE TABLE multi_update_outer_auto_child (
    id INT PRIMARY KEY,
    seq INT AUTO_INCREMENT,
    v INT NOT NULL
);
CREATE TABLE multi_update_outer_auto_plain (id INT PRIMARY KEY, v INT);
INSERT INTO multi_update_outer_auto_child (id, v) VALUES (1, 1);
INSERT INTO multi_update_outer_auto_plain VALUES (2, 0);
UPDATE multi_update_outer_auto_child c
RIGHT JOIN multi_update_outer_auto_plain p ON c.id = p.id
SET
    c.seq = DEFAULT,
    p.v = 9;
SELECT ROW_COUNT();
SELECT * FROM multi_update_outer_auto_plain;
INSERT INTO multi_update_outer_auto_child (id, v) VALUES (2, 2);
SELECT id, seq, v FROM multi_update_outer_auto_child ORDER BY id;
DROP TABLE multi_update_outer_auto_child;
DROP TABLE multi_update_outer_auto_plain;

DROP TABLE IF EXISTS multi_update_cascade_child;
DROP TABLE IF EXISTS multi_update_cascade_parent;
DROP TABLE IF EXISTS multi_update_cascade_plain;
DROP TABLE IF EXISTS multi_update_cascade_source;
CREATE TABLE multi_update_cascade_parent (id INT AUTO_INCREMENT PRIMARY KEY);
CREATE TABLE multi_update_cascade_child (
    id INT PRIMARY KEY,
    parent_id INT,
    FOREIGN KEY (parent_id) REFERENCES multi_update_cascade_parent(id) ON UPDATE CASCADE
);
CREATE TABLE multi_update_cascade_plain (id INT PRIMARY KEY, v INT);
CREATE TABLE multi_update_cascade_source (id INT);
INSERT INTO multi_update_cascade_parent VALUES (1);
INSERT INTO multi_update_cascade_child VALUES (1, 1);
INSERT INTO multi_update_cascade_plain VALUES (1, 0);
INSERT INTO multi_update_cascade_source VALUES (1), (1);
UPDATE multi_update_cascade_parent p
JOIN multi_update_cascade_source s ON p.id = s.id
JOIN multi_update_cascade_plain o ON o.id = s.id
SET
    p.id = 2,
    o.v = 8;
SELECT ROW_COUNT();
SELECT * FROM multi_update_cascade_parent;
SELECT * FROM multi_update_cascade_child;
SELECT * FROM multi_update_cascade_plain;
DROP TABLE multi_update_cascade_child;
DROP TABLE multi_update_cascade_parent;
DROP TABLE multi_update_cascade_plain;
DROP TABLE multi_update_cascade_source;

DROP TABLE IF EXISTS multi_update_fk_overlap_child;
DROP TABLE IF EXISTS multi_update_fk_overlap_parent_a;
DROP TABLE IF EXISTS multi_update_fk_overlap_parent_b;
CREATE TABLE multi_update_fk_overlap_parent_a (id INT PRIMARY KEY);
CREATE TABLE multi_update_fk_overlap_parent_b (id INT PRIMARY KEY);
CREATE TABLE multi_update_fk_overlap_child (
    id INT PRIMARY KEY,
    parent_a_id INT,
    parent_b_id INT,
    v INT,
    FOREIGN KEY (parent_a_id) REFERENCES multi_update_fk_overlap_parent_a(id) ON UPDATE CASCADE,
    FOREIGN KEY (parent_b_id) REFERENCES multi_update_fk_overlap_parent_b(id) ON UPDATE SET NULL
);
INSERT INTO multi_update_fk_overlap_parent_a VALUES (1);
INSERT INTO multi_update_fk_overlap_parent_b VALUES (1);
INSERT INTO multi_update_fk_overlap_child VALUES (10, 1, 1, 0);

--error
UPDATE multi_update_fk_overlap_parent_a p
JOIN multi_update_fk_overlap_child c ON c.parent_a_id = p.id
SET p.id = 2, c.v = 5;
SELECT * FROM multi_update_fk_overlap_parent_a;
SELECT * FROM multi_update_fk_overlap_child;

--error
UPDATE multi_update_fk_overlap_parent_b p
JOIN multi_update_fk_overlap_child c ON c.parent_b_id = p.id
SET p.id = 2, c.v = 5;
SELECT * FROM multi_update_fk_overlap_parent_b;
SELECT * FROM multi_update_fk_overlap_child;

--error
UPDATE multi_update_fk_overlap_parent_a a
JOIN multi_update_fk_overlap_parent_b b ON a.id = b.id
SET a.id = 2, b.id = 2;
SELECT * FROM multi_update_fk_overlap_parent_a;
SELECT * FROM multi_update_fk_overlap_parent_b;
SELECT * FROM multi_update_fk_overlap_child;

DROP TABLE multi_update_fk_overlap_child;
DROP TABLE multi_update_fk_overlap_parent_a;
DROP TABLE multi_update_fk_overlap_parent_b;

DROP TABLE IF EXISTS multi_update_fulltext_target;
DROP TABLE IF EXISTS multi_update_fulltext_source;
DROP TABLE IF EXISTS multi_update_fulltext_plain;
CREATE TABLE multi_update_fulltext_target (id INT PRIMARY KEY, body TEXT);
CREATE FULLTEXT INDEX idx_multi_update_fulltext ON multi_update_fulltext_target(body);
CREATE TABLE multi_update_fulltext_source (id INT);
CREATE TABLE multi_update_fulltext_plain (id INT PRIMARY KEY, v INT);
INSERT INTO multi_update_fulltext_target VALUES (1, 'old token');
INSERT INTO multi_update_fulltext_source VALUES (1), (1);
INSERT INTO multi_update_fulltext_plain VALUES (1, 0);
UPDATE multi_update_fulltext_target f
JOIN multi_update_fulltext_source s ON f.id = s.id
JOIN multi_update_fulltext_plain p ON p.id = s.id
SET
    f.body = 'new token',
    p.v = 7;
SELECT COUNT(*) FROM multi_update_fulltext_target
WHERE MATCH(body) AGAINST('new' IN NATURAL LANGUAGE MODE);
SELECT COUNT(*) FROM multi_update_fulltext_target
WHERE MATCH(body) AGAINST('old' IN NATURAL LANGUAGE MODE);
DROP TABLE multi_update_fulltext_target;
DROP TABLE multi_update_fulltext_source;
DROP TABLE multi_update_fulltext_plain;

-- A non-leading FULLTEXT target must consume its own target-local final row
-- image, rather than interpreting the leading target's columns as its input.
DROP TABLE IF EXISTS multi_update_irregular_plain;
DROP TABLE IF EXISTS multi_update_irregular_fulltext;
CREATE TABLE multi_update_irregular_plain (id INT PRIMARY KEY, v INT);
CREATE TABLE multi_update_irregular_fulltext (id INT PRIMARY KEY, body TEXT);
CREATE FULLTEXT INDEX idx_multi_update_irregular_fulltext
    ON multi_update_irregular_fulltext(body);
INSERT INTO multi_update_irregular_plain VALUES (1, 0);
INSERT INTO multi_update_irregular_fulltext VALUES (1, 'oldf');
UPDATE multi_update_irregular_plain p
JOIN multi_update_irregular_fulltext f ON p.id = f.id
SET p.v = 7, f.body = 'newf';
SELECT * FROM multi_update_irregular_plain;
SELECT * FROM multi_update_irregular_fulltext;
SELECT COUNT(*) FROM multi_update_irregular_fulltext
WHERE MATCH(body) AGAINST('newf' IN NATURAL LANGUAGE MODE);
SELECT COUNT(*) FROM multi_update_irregular_fulltext
WHERE MATCH(body) AGAINST('oldf' IN NATURAL LANGUAGE MODE);
DROP TABLE multi_update_irregular_plain;
DROP TABLE multi_update_irregular_fulltext;

-- Index rewrites remain enabled for read-only sources in a multi-target UPDATE.
DROP TABLE IF EXISTS multi_update_match_a;
DROP TABLE IF EXISTS multi_update_match_b;
DROP TABLE IF EXISTS multi_update_match_docs;
CREATE TABLE multi_update_match_a (id INT PRIMARY KEY, v INT);
CREATE TABLE multi_update_match_b (id INT PRIMARY KEY, v INT);
CREATE TABLE multi_update_match_docs (id INT PRIMARY KEY, body TEXT);
CREATE FULLTEXT INDEX idx_multi_update_match_docs ON multi_update_match_docs(body);
INSERT INTO multi_update_match_a VALUES (1, 0), (2, 0);
INSERT INTO multi_update_match_b VALUES (1, 0), (2, 0);
INSERT INTO multi_update_match_docs VALUES (1, 'needle'), (2, 'haystack');
UPDATE multi_update_match_a a
JOIN multi_update_match_b b ON a.id = b.id
JOIN multi_update_match_docs d ON d.id = a.id
SET a.v = 1, b.v = 2
WHERE MATCH(d.body) AGAINST('needle' IN NATURAL LANGUAGE MODE);
SELECT * FROM multi_update_match_a ORDER BY id;
SELECT * FROM multi_update_match_b ORDER BY id;
DROP TABLE multi_update_match_a;
DROP TABLE multi_update_match_b;
DROP TABLE multi_update_match_docs;

-- MASTER maintenance deletes by the immutable old PK when the PK changes.
DROP TABLE IF EXISTS multi_update_master_pk;
CREATE TABLE multi_update_master_pk (
    id VARCHAR(30) PRIMARY KEY,
    a VARCHAR(30),
    b VARCHAR(30)
);
CREATE INDEX idx_multi_update_master_pk USING MASTER ON multi_update_master_pk(a, b);
INSERT INTO multi_update_master_pk VALUES ('1', 'alpha', 'one');
UPDATE multi_update_master_pk SET id = '2' WHERE id = '1';
UPDATE multi_update_master_pk SET id = '3' WHERE id = '2';
SET @multi_update_master_table = (
    SELECT DISTINCT index_table_name
    FROM mo_catalog.mo_indexes
    WHERE name = 'idx_multi_update_master_pk'
);
SET @multi_update_master_sql = CONCAT(
    'SELECT __mo_index_pri_col, COUNT(*) FROM `',
    @multi_update_master_table,
    '` GROUP BY __mo_index_pri_col ORDER BY __mo_index_pri_col'
);
PREPARE multi_update_master_stmt FROM @multi_update_master_sql;
EXECUTE multi_update_master_stmt;
DEALLOCATE PREPARE multi_update_master_stmt;
DROP TABLE multi_update_master_pk;

DROP TABLE IF EXISTS multi_update_auto_a;
DROP TABLE IF EXISTS multi_update_auto_b;
CREATE TABLE multi_update_auto_a (id INT AUTO_INCREMENT PRIMARY KEY, v INT);
CREATE TABLE multi_update_auto_b (id INT AUTO_INCREMENT PRIMARY KEY, v INT);
INSERT INTO multi_update_auto_a(v) VALUES (0);
INSERT INTO multi_update_auto_b(v) VALUES (0);
UPDATE multi_update_auto_a a
JOIN multi_update_auto_b b ON a.id = b.id
SET a.id = DEFAULT, b.id = DEFAULT;
SELECT ROW_COUNT();
SELECT * FROM multi_update_auto_a;
SELECT * FROM multi_update_auto_b;
INSERT INTO multi_update_auto_a(v) VALUES (1);
INSERT INTO multi_update_auto_b(v) VALUES (1);
SELECT * FROM multi_update_auto_a ORDER BY id;
SELECT * FROM multi_update_auto_b ORDER BY id;
DROP TABLE multi_update_auto_a;
DROP TABLE multi_update_auto_b;

DROP TABLE IF EXISTS multi_update_eval_empty;
DROP TABLE IF EXISTS multi_update_eval_sibling;
CREATE TABLE multi_update_eval_empty (id INT PRIMARY KEY, v INT);
CREATE TABLE multi_update_eval_sibling (id INT PRIMARY KEY, v INT);
INSERT INTO multi_update_eval_sibling VALUES (1, 0), (2, 0);
UPDATE multi_update_eval_empty a
RIGHT JOIN multi_update_eval_sibling b ON a.id = b.id
SET a.v = 'not-an-int', b.v = 9;
SELECT ROW_COUNT();
SELECT * FROM multi_update_eval_sibling ORDER BY id;
UPDATE multi_update_eval_sibling SET v = 0;
PREPARE multi_update_eval_stmt FROM
    'UPDATE multi_update_eval_empty a RIGHT JOIN multi_update_eval_sibling b ON a.id = b.id SET a.v = ?, b.v = ?';
SET @multi_update_bad = 'still-not-an-int';
SET @multi_update_good = 10;
EXECUTE multi_update_eval_stmt USING @multi_update_bad, @multi_update_good;
SET @multi_update_good = 11;
EXECUTE multi_update_eval_stmt USING @multi_update_bad, @multi_update_good;
DEALLOCATE PREPARE multi_update_eval_stmt;
SELECT * FROM multi_update_eval_sibling ORDER BY id;
DROP TABLE multi_update_eval_empty;
DROP TABLE multi_update_eval_sibling;

DROP TABLE IF EXISTS multi_update_eval_target;
DROP TABLE IF EXISTS multi_update_eval_check_target;
DROP TABLE IF EXISTS multi_update_eval_source;
DROP TABLE IF EXISTS multi_update_eval_check_source;
DROP TABLE IF EXISTS multi_update_eval_sibling;
CREATE TABLE multi_update_eval_target (id INT PRIMARY KEY, v INT NOT NULL);
CREATE TABLE multi_update_eval_check_target (
    id INT PRIMARY KEY,
    v INT,
    CONSTRAINT positive_v CHECK (v > 0)
);
CREATE TABLE multi_update_eval_source (id INT PRIMARY KEY, nv VARCHAR(32));
CREATE TABLE multi_update_eval_check_source (id INT PRIMARY KEY, nv VARCHAR(32));
CREATE TABLE multi_update_eval_sibling (id INT PRIMARY KEY, v INT);
INSERT INTO multi_update_eval_target VALUES (1, 0);
INSERT INTO multi_update_eval_check_target VALUES (1, 1);
INSERT INTO multi_update_eval_source VALUES (1, '5'), (2, 'not-an-int');
INSERT INTO multi_update_eval_check_source VALUES (1, '5'), (2, '-5');
INSERT INTO multi_update_eval_sibling VALUES (1, 0), (2, 0);
UPDATE multi_update_eval_target a
JOIN multi_update_eval_source s ON a.id = 1
JOIN multi_update_eval_sibling b ON b.id = s.id
SET a.v = s.nv, b.v = b.v + 1;
SELECT ROW_COUNT();
SELECT * FROM multi_update_eval_target;
SELECT * FROM multi_update_eval_sibling ORDER BY id;
--error
UPDATE multi_update_eval_target a
JOIN multi_update_eval_source s ON a.id = 1 AND s.id = 1
JOIN multi_update_eval_sibling b ON b.id = s.id
SET a.v = NULL, b.v = b.v + 1;
SELECT * FROM multi_update_eval_target;
SELECT * FROM multi_update_eval_sibling ORDER BY id;
UPDATE multi_update_eval_check_target a
JOIN multi_update_eval_check_source s ON a.id = 1
JOIN multi_update_eval_sibling b ON b.id = s.id
SET a.v = s.nv, b.v = b.v + 1;
SELECT ROW_COUNT();
SELECT * FROM multi_update_eval_check_target;
SELECT * FROM multi_update_eval_sibling ORDER BY id;
DROP TABLE multi_update_eval_target;
DROP TABLE multi_update_eval_check_target;
DROP TABLE multi_update_eval_source;
DROP TABLE multi_update_eval_check_source;
DROP TABLE multi_update_eval_sibling;

DROP TABLE IF EXISTS multi_update_repeated_alias;
CREATE TABLE multi_update_repeated_alias (id INT PRIMARY KEY, x INT, y INT);
INSERT INTO multi_update_repeated_alias VALUES (1, 0, 0), (2, 0, 0);
--error
UPDATE multi_update_repeated_alias a
JOIN multi_update_repeated_alias b ON a.id <> b.id
SET a.x = 1, b.y = 2;
SELECT * FROM multi_update_repeated_alias ORDER BY id;
DROP TABLE multi_update_repeated_alias;

DROP DATABASE IF EXISTS multi_update_db_a;
DROP DATABASE IF EXISTS multi_update_db_b;
CREATE DATABASE multi_update_db_a;
CREATE DATABASE multi_update_db_b;
CREATE TABLE multi_update_db_a.t (id INT PRIMARY KEY, v INT);
CREATE TABLE multi_update_db_b.t (id INT PRIMARY KEY, v INT);
INSERT INTO multi_update_db_a.t VALUES (1, 0);
INSERT INTO multi_update_db_b.t VALUES (1, 0);
UPDATE multi_update_db_a.t a
JOIN multi_update_db_b.t b ON a.id = b.id
SET a.v = 11, b.v = 21;
SELECT * FROM multi_update_db_a.t;
SELECT * FROM multi_update_db_b.t;
DROP DATABASE multi_update_db_a;
DROP DATABASE multi_update_db_b;
