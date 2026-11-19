-- issue #31443: explicit CAST(text AS SIGNED/UNSIGNED) reports INTEGER in the truncation warning
select cast('42tail' as signed);
show warnings;
select cast('42tail' as unsigned);
show warnings;
select cast('42tail' as signed integer);
show warnings;
select cast('abc' as signed);
show warnings;
select cast('42' as signed);
show warnings;
