-- timestamp still can not read
select statement_id, span_id, node_uuid, node_type, logger_name, level, caller, message, extra from system.log_info limit 0;
