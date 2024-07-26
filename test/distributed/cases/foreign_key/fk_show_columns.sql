drop account if exists fk_show_column;
create account fk_show_column admin_name 'root' identified by '111';

desc	mo_catalog.mo_database                ;
desc	mo_catalog.mo_tables                  ;
desc	mo_catalog.mo_columns                 ;
desc	mo_catalog.mo_account                 ;
desc	mo_catalog.mo_user                    ;
desc	mo_catalog.mo_role                    ;
desc	mo_catalog.mo_user_grant              ;
desc	mo_catalog.mo_role_grant              ;
desc	mo_catalog.mo_role_privs              ;
-- @ignore:0
desc	mo_catalog.mo_user_defined_function   ;
desc	mo_catalog.mo_stored_procedure        ;
desc	mo_catalog.mo_mysql_compatibility_mode;
desc	mo_catalog.mo_increment_columns       ;
desc	mo_catalog.mo_indexes                 ;
desc	mo_catalog.mo_table_partitions        ;
desc	mo_catalog.mo_pubs                    ;
desc	mo_catalog.mo_stages                  ;
desc	mo_catalog.mo_sessions                ;
desc	mo_catalog.mo_configurations          ;
desc	mo_catalog.mo_locks                   ;
desc	mo_catalog.mo_variables               ;
desc	mo_catalog.mo_transactions            ;
desc	mo_catalog.mo_cache                   ;
desc	mo_catalog.mo_foreign_keys            ;
desc	mo_catalog.mo_snapshots               ;


show columns from	mo_catalog.mo_database                ;
show columns from	mo_catalog.mo_tables                  ;
show columns from	mo_catalog.mo_columns                 ;
show columns from	mo_catalog.mo_account                 ;
show columns from	mo_catalog.mo_user                    ;
show columns from	mo_catalog.mo_role                    ;
show columns from	mo_catalog.mo_user_grant              ;
show columns from	mo_catalog.mo_role_grant              ;
show columns from	mo_catalog.mo_role_privs              ;
-- @ignore:0
show columns from	mo_catalog.mo_user_defined_function   ;
show columns from	mo_catalog.mo_stored_procedure        ;
show columns from	mo_catalog.mo_mysql_compatibility_mode;
show columns from	mo_catalog.mo_increment_columns       ;
show columns from	mo_catalog.mo_indexes                 ;
show columns from	mo_catalog.mo_table_partitions        ;
show columns from	mo_catalog.mo_pubs                    ;
show columns from	mo_catalog.mo_stages                  ;
show columns from	mo_catalog.mo_sessions                ;
show columns from	mo_catalog.mo_configurations          ;
show columns from	mo_catalog.mo_locks                   ;
show columns from	mo_catalog.mo_variables               ;
show columns from	mo_catalog.mo_transactions            ;
show columns from	mo_catalog.mo_cache                   ;
show columns from	mo_catalog.mo_foreign_keys            ;
show columns from	mo_catalog.mo_snapshots           ;

-- @session:id=1&user=fk_show_column:root:accountadmin&password=111
desc	mo_catalog.mo_database                ;
desc	mo_catalog.mo_tables                  ;
desc	mo_catalog.mo_columns                 ;
desc	mo_catalog.mo_account                 ;
desc	mo_catalog.mo_user                    ;
desc	mo_catalog.mo_role                    ;
desc	mo_catalog.mo_user_grant              ;
desc	mo_catalog.mo_role_grant              ;
desc	mo_catalog.mo_role_privs              ;
desc	mo_catalog.mo_user_defined_function   ;
desc	mo_catalog.mo_stored_procedure        ;
desc	mo_catalog.mo_mysql_compatibility_mode;
desc	mo_catalog.mo_increment_columns       ;
desc	mo_catalog.mo_indexes                 ;
desc	mo_catalog.mo_table_partitions        ;
desc	mo_catalog.mo_pubs                    ;
desc	mo_catalog.mo_stages                  ;
desc	mo_catalog.mo_sessions                ;
desc	mo_catalog.mo_configurations          ;
desc	mo_catalog.mo_locks                   ;
desc	mo_catalog.mo_variables               ;
desc	mo_catalog.mo_transactions            ;
desc	mo_catalog.mo_cache                   ;
desc	mo_catalog.mo_foreign_keys            ;
desc	mo_catalog.mo_snapshots               ;


show columns from	mo_catalog.mo_database                ;
show columns from	mo_catalog.mo_tables                  ;
show columns from	mo_catalog.mo_columns                 ;
show columns from	mo_catalog.mo_account                 ;
show columns from	mo_catalog.mo_user                    ;
show columns from	mo_catalog.mo_role                    ;
show columns from	mo_catalog.mo_user_grant              ;
show columns from	mo_catalog.mo_role_grant              ;
show columns from	mo_catalog.mo_role_privs              ;
show columns from	mo_catalog.mo_user_defined_function   ;
show columns from	mo_catalog.mo_stored_procedure        ;
show columns from	mo_catalog.mo_mysql_compatibility_mode;
show columns from	mo_catalog.mo_increment_columns       ;
show columns from	mo_catalog.mo_indexes                 ;
show columns from	mo_catalog.mo_table_partitions        ;
show columns from	mo_catalog.mo_pubs                    ;
show columns from	mo_catalog.mo_stages                  ;
show columns from	mo_catalog.mo_sessions                ;
show columns from	mo_catalog.mo_configurations          ;
show columns from	mo_catalog.mo_locks                   ;
show columns from	mo_catalog.mo_variables               ;
show columns from	mo_catalog.mo_transactions            ;
show columns from	mo_catalog.mo_cache                   ;
show columns from	mo_catalog.mo_foreign_keys            ;
show columns from	mo_catalog.mo_snapshots           ;

-- @session
drop account if exists fk_show_column;