-- FLOAT and DOUBLE reject non-finite conversion results while preserving
-- NULL, DEFAULT, and ERROR ON ERROR response modes.
select json_value('"NaN"', '$' returning float null on error);
select json_value('"NaN"', '$' returning float default 7 on error);
select json_value('"NaN"', '$' returning float error on error);
select json_value('"NaN"', '$' returning double null on error);
select json_value('"NaN"', '$' returning double default 7 on error);
select json_value('"1.25"', '$' returning float);
select json_value('"1.25"', '$' returning double);
select json_value('"NaN"', '$' returning double error on error);
