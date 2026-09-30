-- @suite
-- @case
DROP DATABASE IF EXISTS view_metadata_on_demand;
CREATE DATABASE view_metadata_on_demand;
USE view_metadata_on_demand;

CREATE TABLE src (
  id INT,
  code VARCHAR(5),
  qty INT NOT NULL DEFAULT 7,
  price DECIMAL(10,2)
);
CREATE VIEW v AS SELECT id, code, qty, price, qty * price AS total FROM src;

ALTER TABLE src MODIFY COLUMN code VARCHAR(60);
ALTER TABLE src MODIFY COLUMN qty BIGINT NOT NULL DEFAULT 9;
ALTER TABLE src MODIFY COLUMN price DECIMAL(20,5);

DESC v;
SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale,
       is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'view_metadata_on_demand' AND table_name = 'v'
ORDER BY ordinal_position;

CREATE TABLE copied AS SELECT id, code, qty, price FROM v;
DESC copied;

DROP TABLE src;
-- @pattern
DESC v;
CREATE TABLE src (
  id BIGINT,
  code VARCHAR(90),
  qty BIGINT NOT NULL DEFAULT 11,
  price DECIMAL(24,6)
);
DESC v;
SELECT column_name, column_type, column_default
FROM information_schema.columns
WHERE table_schema = 'view_metadata_on_demand' AND table_name = 'v'
ORDER BY ordinal_position;

-- Prepared SHOW must track the target even when it starts as an ordinary table.
CREATE TABLE metadata_target (code VARCHAR(5));
PREPARE show_target FROM 'SHOW COLUMNS FROM view_metadata_on_demand.metadata_target';
EXECUTE show_target;
-- @session:id=1{
DROP TABLE view_metadata_on_demand.metadata_target;
CREATE VIEW view_metadata_on_demand.metadata_target AS
  SELECT code FROM view_metadata_on_demand.src;
ALTER TABLE view_metadata_on_demand.src MODIFY COLUMN code VARCHAR(120);
-- @session}
EXECUTE show_target;
SELECT column_name, column_type FROM information_schema.columns
WHERE table_schema = 'view_metadata_on_demand' AND table_name = 'metadata_target';
-- @session:id=1{
DROP VIEW view_metadata_on_demand.metadata_target;
CREATE TABLE view_metadata_on_demand.metadata_target (code BIGINT);
-- @session}
EXECUTE show_target;
DEALLOCATE PREPARE show_target;

-- Restore replaces the source, not the dependent View. A new statement in
-- another session must describe the restored schema without a recovery worker.
DROP SNAPSHOT IF EXISTS view_metadata_restore;
CREATE SNAPSHOT view_metadata_restore FOR ACCOUNT;
ALTER TABLE src MODIFY COLUMN code VARCHAR(180);
-- @session:id=1{
PREPARE restored_view FROM 'SHOW COLUMNS FROM view_metadata_on_demand.v';
EXECUTE restored_view;
-- @session}
RESTORE TABLE view_metadata_on_demand.src {snapshot='view_metadata_restore'};
-- @session:id=1{
EXECUTE restored_view;
SELECT column_name, column_type FROM information_schema.columns
WHERE table_schema = 'view_metadata_on_demand' AND table_name = 'v'
ORDER BY ordinal_position;
DEALLOCATE PREPARE restored_view;
-- @session}
CREATE TABLE restored_copy AS SELECT code FROM v;
DESC restored_copy;
DROP SNAPSHOT view_metadata_restore;

DROP DATABASE view_metadata_on_demand;
-- @suite
