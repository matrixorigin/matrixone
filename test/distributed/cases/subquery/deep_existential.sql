-- Independent nonunique inputs: duplicates are occurrences, NULL is never an equality witness.
DROP DATABASE IF EXISTS deep_existential_bvt;
CREATE DATABASE deep_existential_bvt;
USE deep_existential_bvt;
CREATE TABLE ot(id INT,grp INT,flag INT);
CREATE TABLE it(id INT,grp INT);
CREATE TABLE jt(id INT,grp INT);
INSERT INTO ot VALUES (1,NULL,1),(2,0,0),(3,1,1),(3,1,1),(4,2,NULL),(5,3,0);
INSERT INTO it VALUES (10,1),(10,1),(20,2),(NULL,NULL);
INSERT INTO jt VALUES (10,1),(10,1),(20,2),(30,3),(NULL,NULL);
SELECT o.id FROM ot o WHERE EXISTS(SELECT 1 FROM it i WHERE EXISTS(SELECT 1 FROM jt j WHERE j.id=i.id AND j.grp=o.grp)) ORDER BY o.id;
SELECT o.id FROM ot o WHERE NOT EXISTS(SELECT 1 FROM it i WHERE EXISTS(SELECT 1 FROM jt j WHERE j.id=i.id AND j.grp=o.grp)) ORDER BY o.id;
SELECT o.id FROM ot o WHERE EXISTS(SELECT 1 FROM it i WHERE EXISTS(SELECT 1 FROM jt j WHERE j.id=i.id OR j.grp=o.grp)) ORDER BY o.id;
SELECT o.id FROM ot o WHERE NOT EXISTS(SELECT 1 FROM it i WHERE EXISTS(SELECT 1 FROM jt j WHERE j.id=i.id OR j.grp=o.grp)) ORDER BY o.id;
SELECT o.id FROM ot o WHERE EXISTS(SELECT 1 FROM it i WHERE EXISTS(SELECT 1 FROM jt j WHERE j.grp=o.grp)) ORDER BY o.id;
SELECT o.id FROM ot o WHERE o.grp IN(SELECT i.grp FROM it i WHERE i.id IN(SELECT j.id FROM jt j WHERE j.id=i.id AND j.grp=o.grp)) ORDER BY o.id;
SELECT o.id FROM ot o WHERE NOT EXISTS(SELECT 1 FROM it i WHERE EXISTS(SELECT 1 FROM jt j WHERE j.id=i.id AND j.grp=o.grp AND o.flag=1)) ORDER BY o.id;
SELECT o.id FROM ot o WHERE EXISTS(SELECT 1 FROM it i WHERE EXISTS(SELECT 1 FROM jt j WHERE o.id=i.id AND i.id=j.id AND o.grp=j.grp AND i.grp=j.grp)) ORDER BY o.id;
PREPARE deep_stmt FROM 'SELECT o.id FROM ot o WHERE o.id > ? AND EXISTS(SELECT 1 FROM it i WHERE EXISTS(SELECT 1 FROM jt j WHERE j.id=i.id AND j.grp=o.grp)) ORDER BY o.id';
SET @deep_id=2;
EXECUTE deep_stmt USING @deep_id;
DEALLOCATE PREPARE deep_stmt;
-- Empty I still guards every OR branch, including a branch that references only J/O.
DELETE FROM it;
SELECT o.id FROM ot o WHERE EXISTS(SELECT 1 FROM it i WHERE EXISTS(SELECT 1 FROM jt j WHERE j.grp=o.grp)) ORDER BY o.id;
SELECT o.id FROM ot o WHERE EXISTS(SELECT 1 FROM it i WHERE EXISTS(SELECT 1 FROM jt j WHERE j.id=i.id OR j.grp=o.grp)) ORDER BY o.id;
SELECT o.id FROM ot o WHERE NOT EXISTS(SELECT 1 FROM it i WHERE EXISTS(SELECT 1 FROM jt j WHERE j.id=i.id OR j.grp=o.grp)) ORDER BY o.id;
-- Empty J cannot provide an AND or OR witness; ANTI keeps duplicate O occurrences.
INSERT INTO it VALUES (10,1);
DELETE FROM jt;
SELECT o.id FROM ot o WHERE EXISTS(SELECT 1 FROM it i WHERE EXISTS(SELECT 1 FROM jt j WHERE j.id=i.id AND j.grp=o.grp)) ORDER BY o.id;
SELECT o.id FROM ot o WHERE NOT EXISTS(SELECT 1 FROM it i WHERE EXISTS(SELECT 1 FROM jt j WHERE j.id=i.id AND j.grp=o.grp)) ORDER BY o.id;
SELECT o.id FROM ot o WHERE EXISTS(SELECT 1 FROM it i WHERE EXISTS(SELECT 1 FROM jt j WHERE j.id=i.id OR j.grp=o.grp)) ORDER BY o.id;
-- NULL-only membership under a truth-only IN, including inside NOT EXISTS.
INSERT INTO jt VALUES (NULL,NULL);
SELECT o.id FROM ot o WHERE o.grp IN(SELECT i.grp FROM it i WHERE i.id IN(SELECT j.id FROM jt j WHERE j.id=i.id AND j.grp=o.grp)) ORDER BY o.id;
SELECT o.id FROM ot o WHERE NOT EXISTS(SELECT i.grp FROM it i WHERE i.id IN(SELECT j.id FROM jt j WHERE j.id=i.id AND j.grp=o.grp)) ORDER BY o.id;
DROP DATABASE deep_existential_bvt;
