-- @suit
-- @setup
DROP DATABASE IF EXISTS issue_28164_collation_gate;

-- @case
-- @desc: Canonical metadata and legacy behavior; expression COLLATE is characterized, not a native activation gate.
-- @label:bvt
CREATE DATABASE issue_28164_collation_gate;
USE issue_28164_collation_gate;
CREATE TABLE legacy (id INT PRIMARY KEY, name VARCHAR(20) COLLATE utf8mb4_general_ci, token VARCHAR(20) COLLATE utf8mb4_bin) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
INSERT INTO legacy VALUES (1, 'Alpha', 'Alpha'), (2, 'alpha', 'alpha');
SELECT id FROM legacy WHERE name COLLATE utf8mb4_general_ci = 'alpha' ORDER BY id;
SELECT 1 COLLATE utf8mb4_general_ci AS legacy_numeric;
SELECT NULL COLLATE utf8mb4_general_ci AS legacy_null;
CREATE TABLE native_column (name VARCHAR(20) COLLATE utf8mb4_0900_bin);
CREATE TABLE native_default (id INT) COLLATE utf8mb4_0900_bin;
CREATE TABLE folded_legacy_expression (v INT DEFAULT (LENGTH('a' COLLATE utf8mb4_0900_ai_ci)));
ALTER TABLE legacy DEFAULT COLLATE utf8mb4_0900_bin;
ALTER TABLE legacy CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
SELECT COLUMN_NAME, COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='legacy' ORDER BY ORDINAL_POSITION;
SHOW TABLES;
DROP DATABASE issue_28164_collation_gate;
