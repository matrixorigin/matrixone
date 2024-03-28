select table_name from information_schema.tables where table_schema collate utf8_general_ci = 'information_schema' and table_name collate utf8_general_ci = 'parameters';
