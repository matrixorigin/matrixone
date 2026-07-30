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

SELECT id, x, y FROM multi_update_alias_target ORDER BY id;

UPDATE multi_update_alias_target a
JOIN multi_update_alias_target b ON a.id = 1 AND b.id = 1
SET
    a.x = 3,
    b.y = 4;

SELECT id, x, y FROM multi_update_alias_target ORDER BY id;
DROP TABLE multi_update_alias_target;

DROP TABLE IF EXISTS multi_update_partition_target;
DROP TABLE IF EXISTS multi_update_plain_target;
CREATE TABLE multi_update_partition_target (
    id INT PRIMARY KEY,
    x INT
) PARTITION BY RANGE (id) (
    PARTITION p0 VALUES LESS THAN (2),
    PARTITION p1 VALUES LESS THAN (MAXVALUE)
);
CREATE TABLE multi_update_plain_target (
    id INT PRIMARY KEY,
    x INT
);
INSERT INTO multi_update_partition_target VALUES (1, 0), (2, 0);
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

DROP TABLE multi_update_partition_target;
DROP TABLE multi_update_plain_target;
