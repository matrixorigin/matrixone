drop database if exists procedure_test;
create database procedure_test;
use procedure_test;

--
-- most basic starlark stored procedure tests.
--

drop procedure if exists llm_chat;
create or replace procedure llm_chat (in prompt varchar, out reply varchar) language 'starlark'
$$
mo.llm_connect('', '', 'echo', '')
reply, err = mo.llm_chat(prompt)
out_reply = reply if not err else err
$$;

call llm_chat('[{"role":"user", "content":"2+2=?"}]', @result);
select @result;

create or replace procedure llm_ask (in q varchar, out reply varchar) language 'starlark'
$$
prompt = '[{{"role":"system", "content":"You are a helpful assistant."}}, {{"role":"user", "content":"{}"}}]'.format(q)
reply, err = mo.llm_chat(prompt)
out_reply = reply if not err else err
$$;

set @llm_server = '';
set @llm_model = 'echo';
call llm_ask('2+2=?', @result);
select @result;

--
-- the following need installing ollama and pulling a model.
--
-- set @llm_server = 'ollama';
-- set @llm_model = 'qwen2.5:14b';
-- call llm_chat('[{"role":"user", "content":"2+2=?"}]', @result);
-- select @result;

drop database if exists procedure_test;
