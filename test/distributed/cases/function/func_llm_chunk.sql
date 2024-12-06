select llm_chunk(cast('file://$resources/llm_test/chunk/1.txt' as datalink), "fixed_width; 2");

-- correct chunk with large fixed width
select llm_chunk(cast('file://$resources/llm_test/chunk/1.txt' as datalink), "fixed_width; 30");

-- chinese character
select llm_chunk(cast('file://$resources/llm_test/chunk/2.txt' as datalink), "fixed_width; 2");

-- correct chunk with paragraph
select llm_chunk(cast('file://$resources/llm_test/chunk/3.txt' as datalink), "paragraph");

-- correct chunk with sentence
select llm_chunk(cast('file://$resources/llm_test/chunk/4.txt' as datalink), "sentence");
-- result verification
-- correct chunk with fixed width
WITH chunked AS (SELECT llm_chunk(cast('file://$resources/llm_test/chunk/1.txt' as datalink), "fixed_width; 2") AS chunks) SELECT CASE WHEN CONCAT(JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[0][2]')), JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[1][2]')), JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[2][2]')), JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[3][2]')), JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[4][2]')), JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[5][2]')), JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[6][2]')), JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[7][2]')), JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[8][2]')), JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[9][2]'))) = '12345678901234567890' THEN 'Pass' ELSE 'Fail' END AS verification_result FROM chunked;

WITH chunked AS (SELECT llm_chunk(cast('file://$resources/llm_test/chunk/2.txt' as datalink), "fixed_width; 30") AS chunks) SELECT CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[0][2]')) = 'MO数据库' THEN 'Pass' ELSE 'Fail' END AS verification_result FROM chunked;

WITH chunked AS (SELECT llm_chunk(cast('file://$resources/llm_test/chunk/2.txt' as datalink), "document") AS chunks) SELECT CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[0][2]')) = 'MO数据库' THEN 'Pass' ELSE 'Fail' END AS verification_result FROM chunked;

WITH chunked AS (SELECT llm_chunk(cast('file://$resources/llm_test/chunk/3.txt' as datalink), "paragraph") AS chunks) SELECT CASE WHEN CONCAT(JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[0][2]')), JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[1][2]')), JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[2][2]'))) = '123\n456\n789\n' THEN 'Pass' ELSE 'Fail' END AS verification_result FROM chunked;

WITH chunked AS (SELECT llm_chunk(cast('file://$resources/llm_test/chunk/4.txt' as datalink), "sentence") AS chunks) SELECT CASE WHEN CONCAT(JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[0][2]')), JSON_UNQUOTE(JSON_EXTRACT(chunks, '$[1][2]'))) = 'Welcome to the MatrixOne documentation center!\n\nThis center holds related concepts and technical architecture introductions, product features, user guides, and reference manuals to help you work with MatrixOne.' THEN 'Pass' ELSE 'Fail' END AS verification_result FROM chunked;