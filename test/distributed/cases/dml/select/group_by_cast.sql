-- issue #25131: GROUP BY / ORDER BY on the same CAST(col AS UNSIGNED/SIGNED)
-- expression must bind successfully (the column name used to leak into the
-- cast target type, so ORDER BY failed to match the grouped expression).

drop table if exists zsd04;
create table zsd04(vgbel varchar(20), vgpos varchar(10), kwmeng int, vbeln varchar(20), lfimg int);
insert into zsd04 values
  ('2023102053','10',5,'x',1),
  ('2023102053','20',7,'2023102053',2),
  ('2023102053','10',3,'y',4),
  ('2023102053','20',9,'z',6);

-- the two reproductions from the issue
SELECT CAST(vgpos AS UNSIGNED) AS line_item_no, MAX(COALESCE(kwmeng,0)) AS ordered_qty
FROM zsd04 WHERE vgbel='2023102053'
GROUP BY CAST(vgpos AS UNSIGNED)
ORDER BY CAST(vgpos AS UNSIGNED);

SELECT CAST(vgpos AS UNSIGNED) AS line_item_no,
       SUM(CASE WHEN vbeln = vgbel THEN 0 ELSE COALESCE(lfimg,0) END) AS delivered_qty
FROM zsd04 WHERE vgbel='2023102053'
GROUP BY CAST(vgpos AS UNSIGNED)
ORDER BY CAST(vgpos AS UNSIGNED);

-- CAST AS SIGNED, same shape
SELECT CAST(vgpos AS SIGNED) AS n, COUNT(*) AS c
FROM zsd04
GROUP BY CAST(vgpos AS SIGNED)
ORDER BY CAST(vgpos AS SIGNED);

-- CAST AS UNSIGNED INTEGER keyword variant
SELECT CAST(vgpos AS UNSIGNED INTEGER) AS n
FROM zsd04
GROUP BY CAST(vgpos AS UNSIGNED INTEGER)
ORDER BY CAST(vgpos AS UNSIGNED INTEGER);

-- descending order on the cast
SELECT CAST(vgpos AS UNSIGNED) AS n, MAX(kwmeng)
FROM zsd04
GROUP BY CAST(vgpos AS UNSIGNED)
ORDER BY CAST(vgpos AS UNSIGNED) DESC;

-- cast on an integer column too
SELECT CAST(kwmeng AS UNSIGNED) AS k, COUNT(*)
FROM zsd04
GROUP BY CAST(kwmeng AS UNSIGNED)
ORDER BY CAST(kwmeng AS UNSIGNED);

drop table if exists zsd04;
