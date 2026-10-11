-- 每个 FIELD 的 CASE 返回域属于消费者，不属于共享投影参数。
set @c=0,@t='a',@s=null,@x='A';
prepare field_shared_columns from 'select field(case when ? then null else s end,?) as first_case,field(case when ? then null else coalesce(s,x) end,?) as second_case from (select ? s,? x limit 1)d';
execute field_shared_columns using @c,@t,@c,@t,@s,@x;
set @s='A',@x='B';
execute field_shared_columns using @c,@t,@c,@t,@s,@x;
deallocate prepare field_shared_columns;

-- 独立消费者与交换列顺序都不改变返回域。
set @s=null,@x='A';
prepare field_single_column from 'select field(case when ? then null else coalesce(s,x) end,?) as second_only from (select ? s,? x limit 1)d';
execute field_single_column using @c,@t,@s,@x;
set @s='A',@x='B';
execute field_single_column using @c,@t,@s,@x;
deallocate prepare field_single_column;
set @s=null,@x='A';
prepare field_reversed_columns from 'select field(case when ? then null else coalesce(s,x) end,?) as second_case,field(case when ? then null else s end,?) as first_case from (select ? s,? x limit 1)d';
execute field_reversed_columns using @c,@t,@c,@t,@s,@x;
set @s='A',@x='B';
execute field_reversed_columns using @c,@t,@c,@t,@s,@x;
deallocate prepare field_reversed_columns;

-- 固定 BINARY 返回声明不覆盖已经解析的 FIELD 数值比较合同。
set @s=42,@t=42;
prepare field_numeric_first from 'select field(coalesce(?,cast(''A'' as binary)),?) as numeric_first';
execute field_numeric_first using @s,@t;
set @s='A',@t='a';
execute field_numeric_first using @s,@t;
set @s=X'41',@t=X'61';
execute field_numeric_first using @s,@t;
deallocate prepare field_numeric_first;

-- 新句柄文本首次绑定仍然采用固定 BINARY；数值绑定可以重新解析。
set @s='A',@t='a';
prepare field_text_first from 'select field(coalesce(?,cast(''A'' as binary)),?) as text_first';
execute field_text_first using @s,@t;
set @s=42,@t=42;
execute field_text_first using @s,@t;
set @s='A',@t='a';
execute field_text_first using @s,@t;
deallocate prepare field_text_first;
set @c=null,@t=null,@s=null,@x=null;
