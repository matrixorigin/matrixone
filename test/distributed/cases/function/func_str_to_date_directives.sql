-- Date directives that construct a calendar day from an ordinal or a week.
SET sql_mode='';
SELECT STR_TO_DATE('29th February 2024','%D %M %Y') AS ordinal_day,
       STR_TO_DATE('2024 060','%Y %j') AS day_of_year,
       STR_TO_DATE('2024-02-29 Thursday','%Y-%m-%d %W') AS full_weekday,
       STR_TO_DATE('2024-02-29 Thu','%Y-%m-%d %a') AS short_weekday;
SELECT STR_TO_DATE('2024-09-1','%x-%v-%w') AS iso_week_year,
       STR_TO_DATE('2024-09-1','%X-%V-%w') AS sunday_week_year,
       STR_TO_DATE('2024-09-1','%Y-%u-%w') AS monday_week,
       STR_TO_DATE('2024-09-1','%Y-%U-%w') AS sunday_week;
SELECT STR_TO_DATE('2023 366','%Y %j') AS ordinal_rollover,
       STR_TO_DATE('2024-00-0','%Y-%U-%w') AS week_zero,
       STR_TO_DATE('2024-53-0','%X-%V-%w') AS week_53;
SELECT STR_TO_DATE('2024-54-0','%Y-%U-%w') IS NULL AS invalid_week,
       STR_TO_DATE('2024-09-1','%Y-%V-%w') IS NULL AS missing_week_year,
       STR_TO_DATE('2024-09-1','%X-%v-%w') IS NULL AS mismatched_week_year;
SELECT STR_TO_DATE('2024-12-31-060','%Y-%m-%d-%j') AS ordinal_overrides_date,
       STR_TO_DATE('2024-060-09-1','%Y-%j-%u-%w') AS week_overrides_ordinal,
       STR_TO_DATE('2024 060 12 AM','%Y %j %h %p') AS midnight_ordinal;
-- MySQL counts a leading plus within numeric directive width and preserves
-- parsed fields when the input ends before a repeated directive.
SELECT STR_TO_DATE('+1st February 2024','%D %M %Y') AS plus_day,
       STR_TO_DATE('2024 +60','%Y %j') AS plus_ordinal,
       STR_TO_DATE('2024-+9-1','%Y-%u-%w') AS plus_week,
       STR_TO_DATE('2024 060','%Y %j%j') AS repeated_ordinal;
SELECT STR_TO_DATE('2024-09-Mondayé','%Y-%u-%W') IS NULL AS non_ascii_weekday_suffix,
       STR_TO_DATE('2024-02-29 -01','%Y-%m-%d %j') IS NULL AS negative_ordinal;
DROP DATABASE IF EXISTS issue_29331_str_to_date;
CREATE DATABASE issue_29331_str_to_date;
USE issue_29331_str_to_date;
CREATE TABLE inputs(raw VARCHAR(40));
INSERT INTO inputs VALUES ('2024 060'), ('2024 abc'), (NULL);
SELECT raw, STR_TO_DATE(raw,'%Y %j') AS parsed FROM inputs ORDER BY raw;
DROP DATABASE issue_29331_str_to_date;
SET sql_mode=DEFAULT;
