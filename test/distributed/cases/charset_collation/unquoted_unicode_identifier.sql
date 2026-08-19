-- @suit
-- @setup
DROP DATABASE IF EXISTS unicode_identifier_repro;
CREATE DATABASE unicode_identifier_repro;
USE unicode_identifier_repro;

-- @case
-- @desc: Unquoted BMP identifiers are accepted through the MySQL protocol
-- @label:bvt
CREATE TABLE t_ãg (a INT);
INSERT INTO t_ãg VALUES (7);
SELECT a FROM t_ãg;
DROP TABLE t_ãg;

-- @case
-- @desc: Supplementary Unicode code points are rejected as MySQL identifiers
-- @label:bvt
CREATE TABLE 😀 (a INT);

-- @cleanup
DROP DATABASE unicode_identifier_repro;
