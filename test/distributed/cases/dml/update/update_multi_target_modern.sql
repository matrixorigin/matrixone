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

DROP TABLE IF EXISTS multi_update_alias_target;
CREATE TABLE multi_update_alias_target (
    id INT PRIMARY KEY,
    x INT,
    y INT
);
INSERT INTO multi_update_alias_target VALUES (1, 0, 0), (2, 0, 0);

UPDATE multi_update_alias_target a
JOIN multi_update_alias_target b ON a.id = b.id
SET
    a.x = 1,
    b.y = 2;

SELECT ROW_COUNT();
SELECT id, x, y FROM multi_update_alias_target ORDER BY id;

UPDATE multi_update_alias_target a
JOIN multi_update_alias_target b ON a.id = 1 AND b.id = 1
SET
    a.x = 3,
    b.y = 4;

SELECT id, x, y FROM multi_update_alias_target ORDER BY id;

UPDATE multi_update_alias_target SET x = 0, y = 0;
UPDATE multi_update_alias_target a
JOIN multi_update_alias_target b ON a.id <> b.id
SET
    a.x = 1,
    b.y = 2;

SELECT ROW_COUNT();
SELECT id, x, y FROM multi_update_alias_target ORDER BY id;

CREATE TABLE multi_update_alias_source (
    target_id INT,
    source_x INT,
    source_y INT
);
INSERT INTO multi_update_alias_source VALUES
    (1, NULL, 1),
    (1, 2, 2),
    (2, NULL, 1),
    (2, 2, 2);

UPDATE multi_update_alias_target SET x = 0, y = 0;
UPDATE multi_update_alias_target a
JOIN multi_update_alias_target b ON a.id = b.id
JOIN multi_update_alias_source s ON s.target_id = a.id
SET
    a.x = s.source_x,
    a.y = s.source_y,
    b.id = b.id;

SELECT COUNT(*) AS mixed_source_rows
FROM multi_update_alias_target
WHERE NOT ((x IS NULL AND y = 1) OR (x = 2 AND y = 2));

CREATE TABLE multi_update_third_target (
    id INT PRIMARY KEY,
    z INT
);
INSERT INTO multi_update_third_target VALUES (1, 0), (2, 0);

UPDATE multi_update_alias_target SET x = 0, y = 0;
UPDATE multi_update_alias_target a
JOIN multi_update_alias_target b ON a.id <> b.id
JOIN multi_update_third_target u ON u.id = a.id
SET
    a.x = 1,
    b.y = 2,
    u.z = 3;

SELECT ROW_COUNT();
SELECT id, x, y FROM multi_update_alias_target ORDER BY id;
SELECT id, z FROM multi_update_third_target ORDER BY id;

DROP TABLE multi_update_third_target;
DROP TABLE multi_update_alias_source;
DROP TABLE multi_update_alias_target;

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

UPDATE multi_update_partition_target a
JOIN multi_update_partition_target b ON a.id = b.id
SET
    a.x = 7,
    b.y = 8;

SELECT ROW_COUNT();
SELECT id, x, y FROM multi_update_partition_target ORDER BY id;

DROP TABLE multi_update_partition_target;
DROP TABLE multi_update_plain_target;

DROP TABLE IF EXISTS multi_update_on_update;
CREATE TABLE multi_update_on_update (
    id INT PRIMARY KEY,
    x INT,
    y INT,
    updated_at TIMESTAMP DEFAULT '2000-01-01 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_updated_at (updated_at)
);
INSERT INTO multi_update_on_update (id, x, y) VALUES (1, 0, 0), (2, 0, 0);
UPDATE multi_update_on_update a
JOIN multi_update_on_update b ON a.id = b.id
SET
    a.x = 1,
    b.y = 2;
SELECT id, x, y FROM multi_update_on_update ORDER BY id;
SELECT COUNT(*) FROM multi_update_on_update WHERE updated_at = '2000-01-01 00:00:00';
SELECT COUNT(*) FROM multi_update_on_update WHERE updated_at IS NULL;
SELECT COUNT(*) FROM multi_update_on_update FORCE INDEX (idx_updated_at) WHERE updated_at IS NULL;
DROP TABLE multi_update_on_update;

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
