-- correct chunk with fixed width
select llm_chunk(cast('file://$resources/llm_test/chunk/1.txt' as datalink), "fixed_width; 2");

-- correct chunk with large fixed width
select llm_chunk(cast('file://$resources/llm_test/chunk/1.txt' as datalink), "fixed_width; 30");

-- chinese character
select llm_chunk(cast('file://$resources/llm_test/chunk/2.txt' as datalink), "fixed_width; 2");

-- correct chunk with paragraph
select llm_chunk(cast('file://$resources/llm_test/chunk/3.txt' as datalink), "paragraph");

-- correct chunk with sentence
select llm_chunk(cast('file://$resources/llm_test/chunk/4.txt' as datalink), "sentence");
