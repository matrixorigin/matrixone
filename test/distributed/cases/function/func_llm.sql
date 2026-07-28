-- should echo
select llm_chat('', '', 'echo', '', '[{"role": "user", "content": "hello world"}]');
select llm_chat('', '', 'echo', null, '[{"role": "user", "content": "hello world"}]');
select llm_chat('', '', 'echo', null, null);

-- errors
select llm_chat('', '', 'echo', '', 'hello world');
select llm_chat('', '', 'echo', null, '{"role": "user", "content": "hello world"}');
select llm_chat('', '', '', '');
select llm_chat(null, '', '', '', 'hello world');
select llm_chat(123, '', '', '', 'hello world');
select llm_chat('', null, 'echo', '', 'hello world');
select llm_chat('', '', null, '', 'hello world');

select llm_chat('', '', 'echo', '', col1) from parse_jsonl_data($$["echo", [{"role":"user", "content":"foo"}]]
["echo", [{"role":"user", "content":"bar"}]]
$$, 'ss'
) t;

select llm_chat('', '', col0, '', col1) from parse_jsonl_data($$["echo", "foo"]
["echo", "bar"]
$$, 'ss'
) t;

-- should ok
select llm_embedding('', '', 'echo', '', 'hello world');
select llm_embedding('', '', 'echo', null, 'hello world');
select llm_embedding('', '', 'echo', null, null);

-- errors
select llm_embedding('', '', '', '');
select llm_embedding(null, '', '', '', 'hello world');
select llm_embedding(123, '', '', '', 'hello world');
select llm_embedding('', null, 'echo', '', 'hello world');
select llm_embedding('', '', null, '', 'hello world');

select llm_embedding('', '', 'echo', '', col1) from parse_jsonl_data($$["echo", "foo"]
["echo", "bar"]
$$, 'ss'
) t;

select llm_embedding('', '', col0, '', col1) from parse_jsonl_data($$["echo", "foo"]
["echo", "bar"]
$$, 'ss'
) t;

