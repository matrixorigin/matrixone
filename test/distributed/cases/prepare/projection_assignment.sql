-- Final write casts remain authoritative when a prepared producer changes type.
DROP TABLE IF EXISTS prepared_projection_assignment;
CREATE TABLE prepared_projection_assignment(i BIGINT,d DECIMAL(12,2),b BIT(8));
PREPARE projection_assignment FROM
  'INSERT INTO prepared_projection_assignment SELECT MIN(?),MIN(?),MIN(?)';
SET @projection_value = CAST(5 AS DOUBLE);
EXECUTE projection_assignment USING @projection_value,@projection_value,@projection_value;
SELECT i,d,CAST(b AS UNSIGNED) FROM prepared_projection_assignment;
DEALLOCATE PREPARE projection_assignment;
DROP TABLE prepared_projection_assignment;
