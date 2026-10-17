-- @suit
-- @setup
DROP DATABASE IF EXISTS issue_28164_binary_convert;

-- @case
-- @desc: Strict COPY rejects rows whose UTF-8 bytes exceed the binary width; the old table remains usable.
-- @label:bvt
CREATE DATABASE issue_28164_binary_convert;
USE issue_28164_binary_convert;
SET SESSION sql_mode = 'STRICT_TRANS_TABLES';
CREATE TABLE rejected (id INT PRIMARY KEY, v VARCHAR(2));
INSERT INTO rejected VALUES (1, '中文');
SELECT id, HEX(v), LENGTH(v), CHAR_LENGTH(v) FROM rejected ORDER BY id;
ALTER TABLE rejected CONVERT TO CHARACTER SET binary;
SHOW CREATE TABLE rejected;
SHOW TABLES;
SELECT id, HEX(v), LENGTH(v), CHAR_LENGTH(v) FROM rejected ORDER BY id;
INSERT INTO rejected VALUES (2, 'ab');
SELECT id, HEX(v), LENGTH(v) FROM rejected ORDER BY id;

CREATE TABLE converted (id INT PRIMARY KEY, v VARCHAR(2));
INSERT INTO converted VALUES (1, 'ab');
ALTER TABLE converted CONVERT TO CHARACTER SET binary;
SHOW CREATE TABLE converted;
SELECT id, HEX(v), LENGTH(v) FROM converted ORDER BY id;
INSERT INTO converted VALUES (2, '中');
INSERT INTO converted VALUES (2, 'xy');
INSERT INTO converted VALUES (3, NULL);
CREATE TABLE source (v VARBINARY(3));
INSERT INTO source VALUES ('中');
INSERT INTO converted SELECT 4, v FROM source;
INSERT INTO converted VALUES (4, 'zz');
SELECT id, HEX(v), LENGTH(v) FROM converted ORDER BY id;

CREATE TABLE existing (id INT PRIMARY KEY, b VARBINARY(2), v VARCHAR(2) CHARACTER SET utf8mb4);
INSERT INTO existing VALUES (1, X'6162', 'ab');
ALTER TABLE existing CONVERT TO CHARACTER SET binary;
SHOW CREATE TABLE existing;
SELECT id, HEX(b), LENGTH(b), HEX(v), LENGTH(v) FROM existing ORDER BY id;

CREATE TABLE fixed (id INT PRIMARY KEY, v CHAR(2));
INSERT INTO fixed VALUES (1, 'ab');
ALTER TABLE fixed CONVERT TO CHARACTER SET binary;
SHOW CREATE TABLE fixed;
INSERT INTO fixed VALUES (2, 'a');
INSERT INTO fixed VALUES (3, NULL);
INSERT INTO fixed VALUES (4, '中');
SELECT id, HEX(v), LENGTH(v) FROM fixed ORDER BY id;
SET SESSION sql_mode = '';
CREATE TABLE lenient (id INT PRIMARY KEY, v VARBINARY(2));
INSERT INTO lenient VALUES (1, '中');
INSERT INTO lenient VALUES (2, X'616263');
INSERT INTO lenient SELECT 3, '中';
UPDATE lenient SET v = '中' WHERE id = 2;
SET SESSION sql_mode = 'STRICT_TRANS_TABLES';
INSERT IGNORE INTO lenient VALUES (4, '中');
SELECT id, HEX(v), LENGTH(v) FROM lenient ORDER BY id;
CREATE TABLE default_rejected (id INT PRIMARY KEY, v VARCHAR(2) DEFAULT '中文');
ALTER TABLE default_rejected CONVERT TO CHARACTER SET binary;
INSERT INTO default_rejected(id) VALUES (1);
SELECT id, HEX(v), LENGTH(v) FROM default_rejected;
CREATE TABLE generated_rejected (id INT PRIMARY KEY, v VARCHAR(2) GENERATED ALWAYS AS ('中') STORED);
INSERT INTO generated_rejected(id) VALUES (1);
ALTER TABLE generated_rejected CONVERT TO CHARACTER SET binary;
SELECT id, HEX(v), LENGTH(v) FROM generated_rejected;
CREATE TABLE default_fits (id INT PRIMARY KEY, v CHAR(2) DEFAULT 'a');
ALTER TABLE default_fits CONVERT TO CHARACTER SET binary;
INSERT INTO default_fits(id) VALUES (1);
SELECT id, HEX(v), LENGTH(v) FROM default_fits;
CREATE TABLE combined_add_first (id INT PRIMARY KEY);
ALTER TABLE combined_add_first ADD COLUMN v VARCHAR(2) CHARACTER SET utf8mb4, CONVERT TO CHARACTER SET binary;
INSERT INTO combined_add_first VALUES (1, '中');
INSERT INTO combined_add_first VALUES (1, 'ab');
SELECT id, HEX(v), LENGTH(v) FROM combined_add_first;
CREATE TABLE combined_convert_first (id INT PRIMARY KEY);
ALTER TABLE combined_convert_first CONVERT TO CHARACTER SET binary, ADD COLUMN v VARCHAR(2) CHARACTER SET utf8mb4;
INSERT INTO combined_convert_first VALUES (1, '中');
INSERT INTO combined_convert_first VALUES (1, 'ab');
SELECT id, HEX(v), LENGTH(v) FROM combined_convert_first;
CREATE TABLE combined_modify_first (id INT PRIMARY KEY, v VARCHAR(2));
ALTER TABLE combined_modify_first MODIFY COLUMN v VARCHAR(2) CHARACTER SET utf8mb4, CONVERT TO CHARACTER SET binary;
INSERT INTO combined_modify_first VALUES (1, '中');
INSERT INTO combined_modify_first VALUES (1, 'ab');
SELECT id, HEX(v), LENGTH(v) FROM combined_modify_first;
CREATE TABLE combined_modify_last (id INT PRIMARY KEY, v VARCHAR(2));
ALTER TABLE combined_modify_last CONVERT TO CHARACTER SET binary, MODIFY COLUMN v VARCHAR(2) CHARACTER SET utf8mb4;
INSERT INTO combined_modify_last VALUES (1, '中');
INSERT INTO combined_modify_last VALUES (1, 'ab');
SELECT id, HEX(v), LENGTH(v) FROM combined_modify_last;
DROP DATABASE issue_28164_binary_convert;
