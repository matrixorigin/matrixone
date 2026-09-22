-- MatrixOne-native AST hash: values are retained, not anonymized as a MySQL digest.
SET @hash_saved_sql_mode=@@session.sql_mode;
SET @hash_saved_optimizer_hints=@@session.optimizer_hints;
SET SESSION sql_mode='';
SET SESSION optimizer_hints='';
DROP DATABASE IF EXISTS mo_statement_hash_bvt;
CREATE DATABASE mo_statement_hash_bvt;
USE mo_statement_hash_bvt;

-- Independent golden value is SHA-256 of the exact formatter bytes "select 1".
-- @metacmp(true)
SELECT MO_STATEMENT_HASH('SELECT 1') AS hash_value;
SELECT MO_STATEMENT_HASH(' select /* ordinary comment */ 1; ') = MO_STATEMENT_HASH('SELECT 1') AS same_format,
MO_STATEMENT_HASH('SELECT 1') <> MO_STATEMENT_HASH('SELECT 2') AS distinct_literal,
MO_STATEMENT_HASH('SELECT a') <> MO_STATEMENT_HASH('SELECT ''a''') AS identifier_not_literal;
SELECT MO_STATEMENT_HASH('SELECT @MiXeD') = MO_STATEMENT_HASH('SELECT @`mixed`') AS same_variable,
MO_STATEMENT_HASH('SELECT @mixed') <> MO_STATEMENT_HASH('SELECT @other') AS distinct_variable,
MO_STATEMENT_HASH(NULL) IS NULL AS null_input;

-- Column-fed, constant and NULL arguments share the public masked-row contract.
CREATE TABLE inputs(id INT PRIMARY KEY, sql_text TEXT);
INSERT INTO inputs VALUES (1,'SELECT 1'),(2,'SELECT FROM'),(3,NULL);
SELECT id, CASE WHEN id=2 THEN 'masked' ELSE MO_STATEMENT_HASH(sql_text) END AS hash_value FROM inputs ORDER BY id;
SELECT id, CASE WHEN id=1 THEN MO_STATEMENT_HASH('SELECT 1') ELSE 'masked' END AS hash_value FROM inputs ORDER BY id;
SELECT id, CASE WHEN id<0 THEN MO_STATEMENT_HASH('SELECT FROM') ELSE 'masked' END AS hash_value FROM inputs ORDER BY id;
SELECT MO_STATEMENT_HASH(sql_text) AS hash_value FROM inputs WHERE id<0;

-- Parsing is authoritative: failure produces an error, and the next call recovers.
SELECT MO_STATEMENT_HASH('SELECT FROM');
SELECT MO_STATEMENT_HASH('SELECT 1; SELECT 2');
SELECT MO_STATEMENT_HASH('/* comment only */');
SELECT MO_STATEMENT_HASH('SELECT 1') = '822ae07d4783158bc1912bb623e5107cc9002d519e1143a9c200ed6ee18b6d0f' AS recovered;

-- One prepared plan must use the execution-time mode even for its constant input.
PREPARE hash_probe FROM 'SELECT MO_STATEMENT_HASH(?) AS parameter_hash, MO_STATEMENT_HASH(''SELECT "name"'') AS constant_hash';
SET @hash_input='SELECT "name"';
EXECUTE hash_probe USING @hash_input;
SET SESSION sql_mode='ANSI_QUOTES';
EXECUTE hash_probe USING @hash_input;
SET SESSION sql_mode='';
EXECUTE hash_probe USING @hash_input;
SET @hash_input='SELECT FROM';
EXECUTE hash_probe USING @hash_input;
SET @hash_input=NULL;
EXECUTE hash_probe USING @hash_input;
SET @hash_input='SELECT 1';
EXECUTE hash_probe USING @hash_input;
DEALLOCATE PREPARE hash_probe;

-- The existing scheduler hint exercises AP placement without a large dataset.
UPDATE inputs SET sql_text='SELECT "name"' WHERE id=1;
SET SESSION optimizer_hints='execType=2';
SET SESSION sql_mode='ANSI_QUOTES';
SELECT GROUP_CONCAT(CASE WHEN id=2 THEN 'masked' ELSE MO_STATEMENT_HASH(sql_text) END ORDER BY id) AS ap_hashes FROM inputs;
SET SESSION sql_mode='';
SELECT GROUP_CONCAT(CASE WHEN id=2 THEN 'masked' ELSE MO_STATEMENT_HASH(sql_text) END ORDER BY id) AS ap_hashes FROM inputs;
SELECT GROUP_CONCAT(MO_STATEMENT_HASH(sql_text)) AS empty_hashes FROM inputs WHERE id<0;

SET SESSION optimizer_hints=@hash_saved_optimizer_hints;
SET SESSION sql_mode=@hash_saved_sql_mode;
SET @hash_input=NULL,@hash_saved_sql_mode=NULL,@hash_saved_optimizer_hints=NULL;
DROP DATABASE mo_statement_hash_bvt;
SELECT COUNT(*) AS leftover_databases FROM mo_catalog.mo_database WHERE datname='mo_statement_hash_bvt';
