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

DROP DATABASE view_metadata_on_demand;
-- @suite
