-- @suit
-- @case
-- @desc: EXPORT_SET numeric context stops at independent producers
-- @label:bvt

PREPARE export_set_boundary FROM "SELECT EXPORT_SET(ABS(?)+0,'Y','N','',4)";
SET @export_set_value=CAST(2.5 AS DECIMAL(3,1));
EXECUTE export_set_boundary USING @export_set_value;
SET @export_set_value='2.5';
EXECUTE export_set_boundary USING @export_set_value;
DEALLOCATE PREPARE export_set_boundary;
PREPARE export_set_boundary FROM "SELECT EXPORT_SET(CAST(ABS(?) AS DOUBLE)+0,'Y','N','',4)";
SET @export_set_value=CAST(2.5 AS DECIMAL(3,1));
EXECUTE export_set_boundary USING @export_set_value;
DEALLOCATE PREPARE export_set_boundary;

-- An independent ABS producer retains its REAL domain, including DECIMAL bindings.
PREPARE export_set_boundary FROM "SELECT EXPORT_SET((SELECT ABS(?)),'Y','N','',4)";
SET @export_set_value='2.5';
EXECUTE export_set_boundary USING @export_set_value;
SET @export_set_value=CAST(2.5 AS DECIMAL(3,1));
EXECUTE export_set_boundary USING @export_set_value;
SET @export_set_value=NULL;
EXECUTE export_set_boundary USING @export_set_value;
DEALLOCATE PREPARE export_set_boundary;
PREPARE export_set_boundary FROM "SELECT EXPORT_SET(ABS(?),'Y','N','',4)";
SET @export_set_value='2.5';
EXECUTE export_set_boundary USING @export_set_value;
DEALLOCATE PREPARE export_set_boundary;

-- A semantic REAL peer differs from an implicit FLOAT envelope around integer zero.
PREPARE export_set_boundary FROM "SELECT EXPORT_SET(IF(TRUE,ABS(?),0e0),'Y','N','',4)";
EXECUTE export_set_boundary USING @export_set_value;
SET @export_set_value=CAST(2.5 AS DECIMAL(3,1));
EXECUTE export_set_boundary USING @export_set_value;
DEALLOCATE PREPARE export_set_boundary;
PREPARE export_set_boundary FROM "SELECT EXPORT_SET(IF(TRUE,ABS(?),0),'Y','N','',4)";
EXECUTE export_set_boundary USING @export_set_value;
DEALLOCATE PREPARE export_set_boundary;

-- Native conditional branches delegate scalar-output conversion; computed constants remain checked.
SELECT EXPORT_SET(IF(TRUE,(SELECT 1e100),0e0),'Y','N','',4);
SELECT EXPORT_SET(CASE WHEN TRUE THEN (SELECT 1e100) ELSE 0e0 END,'Y','N','',4);
SELECT EXPORT_SET((SELECT ABS(1e100)),'Y','N','',4);
SELECT EXPORT_SET((SELECT ABS(1e100) FROM (SELECT 1) d),'Y','N','',4);

DROP TABLE IF EXISTS export_set_producer_real;
CREATE TABLE export_set_producer_real(x DOUBLE);
INSERT INTO export_set_producer_real VALUES(1e100),(1e100);
SELECT EXPORT_SET((SELECT ABS(x) FROM export_set_producer_real LIMIT 1),'Y','N','',4);
SELECT EXPORT_SET(IF(TRUE,(SELECT ABS(x) FROM export_set_producer_real LIMIT 1),0e0),'Y','N','',4);
SELECT EXPORT_SET(IF(FALSE,(SELECT ABS(x) FROM export_set_producer_real LIMIT 1),0e0),'Y','N','',4);
SELECT EXPORT_SET((SELECT x+0 FROM export_set_producer_real LIMIT 1),'Y','N','',4);
SELECT EXPORT_SET((SELECT SUM(x) FROM export_set_producer_real),'Y','N','',4);
SELECT EXPORT_SET((SELECT FIRST_VALUE(x) OVER() FROM export_set_producer_real LIMIT 1),'Y','N','',4);
SELECT EXPORT_SET((SELECT ABS(x) FROM export_set_producer_real WHERE FALSE),'Y','N','',4);
SELECT EXPORT_SET((SELECT ABS(x) FROM export_set_producer_real),'Y','N','',4);
DROP TABLE export_set_producer_real;
