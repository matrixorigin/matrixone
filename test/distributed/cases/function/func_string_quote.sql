select quote('Let''s go!');
select quote('Let\\''s go!');
select quote(null);

select
    quote(''),
    quote('abc'),
    quote('Don''t!'),
    quote('back\\slash'),
    quote(concat('a', unhex('00'), 'b')),
    quote(concat('a', unhex('1A'), 'b'));

select
    quote('''') as q_single_quote,
    quote('\\') as q_backslash,
    quote('''\\') as q_quote_backslash,
    quote('\\''') as q_backslash_quote,
    quote('a''b\\c') as q_mixed_escape,
    quote('  leading and trailing  ') as q_spaces,
    quote('你好''mo\\') as q_utf8_escape;

select
    quote(concat('a', unhex('00'), 'b', unhex('1A'), 'c\\d''e')) as q_all_special,
    quote(cast('cast varchar' as varchar(20))) as q_cast_varchar,
    quote(cast('cast text' as text)) as q_cast_text,
    quote(cast('cast blob' as blob)) as q_cast_blob;

select
    quote('NULL') as q_word_null_upper,
    quote('null') as q_word_null_lower,
    quote('NULL ') as q_word_null_space;

drop table if exists t_quote;
create table t_quote(
    id int,
    c char(20),
    v varchar(20),
    t text,
    b blob
);

insert into t_quote values
    (1, 'char', 'varchar', 'text', 'blob'),
    (2, 'a''b', 'a\\b', concat('a', unhex('00'), 'b'), concat('a', unhex('1A'), 'b')),
    (3, 'mix''\\', 'space  ', '你好''mo\\', concat('a', unhex('00'), 'b', unhex('1A'), 'c')),
    (4, '', '', '', ''),
    (5, null, null, null, null);

select id, quote(c), quote(v), quote(t), quote(b) from t_quote order by id;

drop table t_quote;
