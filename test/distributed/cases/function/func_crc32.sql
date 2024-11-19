SELECT crc32('123');
SELECT crc32(null);
SELECT crc32('123456');
CREATE TABLE crc32test(col1 varchar(100));
INSERT INTO crc32test VALUES ('123'),(NULL),('123456');
SELECT crc32(col1) FROM crc32test;