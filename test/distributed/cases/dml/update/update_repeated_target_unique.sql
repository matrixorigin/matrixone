-- @suit

-- @case
-- @desc: repeated UPDATE targets arbitrate unique keys together
-- @label: bvt

DROP DATABASE IF EXISTS issue31824_bvt;
CREATE DATABASE issue31824_bvt;
USE issue31824_bvt;

CREATE TABLE t (
    id INT PRIMARY KEY,
    u INT,
    v INT,
    payload INT,
    UNIQUE KEY uk_u (u),
    UNIQUE KEY uk_v (v)
);
INSERT INTO t VALUES (1, 10, 100, 0), (2, 20, 200, 0), (3, 30, 300, 0);

-- Row 1 conflicts on v; it must not reserve u=500 against row 2.
UPDATE IGNORE t x JOIN t y ON x.id IN (1, 2) AND y.id = x.id
SET x.u = 500, x.v = IF(x.id = 1, 300, 400), y.payload = y.payload + 1;
SELECT ROW_COUNT();
SELECT id, u, v, payload FROM t ORDER BY id;
SELECT id FROM t FORCE INDEX (uk_u) WHERE u = 500;
SELECT id FROM t FORCE INDEX (uk_v) WHERE v = 400;

-- Ordinary UPDATE must reject a duplicate without publishing the sibling write.
UPDATE t x JOIN t y ON x.id = 1 AND y.id = 2
SET x.u = 500, y.payload = 9;
SELECT id, u, v, payload FROM t ORDER BY id;
SELECT id FROM t FORCE INDEX (uk_u) WHERE u = 500;

DROP DATABASE issue31824_bvt;
