drop account if exists fk_show_column;
create account fk_show_column admin_name 'root' identified by '111';

-- @regex("Field", true)
desc	mo_catalog.mo_database                ;
-- @regex("Field", true)
desc	mo_catalog.mo_tables                  ;
-- @regex("Field", true)
desc	mo_catalog.mo_columns                 ;
-- @regex("Field", true)
desc	mo_catalog.mo_account                 ;
-- @regex("Field", true)
desc	mo_catalog.mo_user                    ;
-- @regex("Field", true)
desc	mo_catalog.mo_role                    ;
-- @regex("Field", true)
desc	mo_catalog.mo_user_grant              ;
-- @regex("Field", true)
desc	mo_catalog.mo_role_grant              ;
-- @regex("Field", true)
desc	mo_catalog.mo_role_privs              ;
-- @ignore:0
-- @regex("Field", true)
desc	mo_catalog.mo_user_defined_function   ;
-- @regex("Field", true)
desc	mo_catalog.mo_stored_procedure        ;
-- @regex("Field", true)
desc	mo_catalog.mo_mysql_compatibility_mode;
-- @regex("Field", true)
desc	mo_catalog.mo_increment_columns       ;
-- @regex("Field", true)
desc	mo_catalog.mo_indexes                 ;
-- @bvt:issue#16438
-- @regex("Field", true)
desc	mo_catalog.mo_table_partitions        ;
-- @bvt:issue
-- @regex("Field", true)
desc	mo_catalog.mo_pubs                    ;
-- @regex("Field", true)
desc	mo_catalog.mo_stages                  ;
-- @regex("Field", true)
desc	mo_catalog.mo_sessions                ;
-- @regex("Field", true)
desc	mo_catalog.mo_configurations          ;
-- @regex("Field", true)
desc	mo_catalog.mo_locks                   ;
-- @regex("Field", true)
desc	mo_catalog.mo_variables               ;
-- @regex("Field", true)
desc	mo_catalog.mo_transactions            ;
-- @regex("Field", true)
desc	mo_catalog.mo_cache                   ;
-- @regex("Field", true)
desc	mo_catalog.mo_foreign_keys            ;
-- @regex("Field", true)
desc	mo_catalog.mo_snapshots               ;


-- @regex("Field", true)
show columns from	mo_catalog.mo_database                ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_tables                  ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_columns                 ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_account                 ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_user                    ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_role                    ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_user_grant              ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_role_grant              ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_role_privs              ;
-- @ignore:0
-- @regex("Field", true)
show columns from	mo_catalog.mo_user_defined_function   ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_stored_procedure        ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_mysql_compatibility_mode;
-- @regex("Field", true)
show columns from	mo_catalog.mo_increment_columns       ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_indexes                 ;
-- @bvt:issue#16438
-- @regex("Field", true)
show columns from	mo_catalog.mo_table_partitions        ;
-- @bvt:issue
-- @regex("Field", true)
show columns from	mo_catalog.mo_pubs                    ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_stages                  ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_sessions                ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_configurations          ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_locks                   ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_variables               ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_transactions            ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_cache                   ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_foreign_keys            ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_snapshots           ;

-- @session:id=1&user=fk_show_column:root:accountadmin&password=111
-- @regex("Field", true)
desc	mo_catalog.mo_database                ;
-- @regex("Field", true)
desc	mo_catalog.mo_tables                  ;
-- @regex("Field", true)
desc	mo_catalog.mo_columns                 ;
desc	mo_catalog.mo_account                 ;
-- @regex("Field", true)
desc	mo_catalog.mo_user                    ;
-- @regex("Field", true)
desc	mo_catalog.mo_role                    ;
-- @regex("Field", true)
desc	mo_catalog.mo_user_grant              ;
-- @regex("Field", true)
desc	mo_catalog.mo_role_grant              ;
-- @regex("Field", true)
desc	mo_catalog.mo_role_privs              ;
-- @regex("Field", true)
desc	mo_catalog.mo_user_defined_function   ;
-- @regex("Field", true)
desc	mo_catalog.mo_stored_procedure        ;
-- @regex("Field", true)
desc	mo_catalog.mo_mysql_compatibility_mode;
-- @regex("Field", true)
desc	mo_catalog.mo_increment_columns       ;
-- @regex("Field", true)
desc	mo_catalog.mo_indexes                 ;
-- @bvt:issue#16438
-- @regex("Field", true)
desc	mo_catalog.mo_table_partitions        ;
-- @bvt:issue
desc	mo_catalog.mo_pubs                    ;
-- @regex("Field", true)
desc	mo_catalog.mo_stages                  ;
-- @regex("Field", true)
desc	mo_catalog.mo_sessions                ;
-- @regex("Field", true)
desc	mo_catalog.mo_configurations          ;
-- @regex("Field", true)
desc	mo_catalog.mo_locks                   ;
-- @regex("Field", true)
desc	mo_catalog.mo_variables               ;
-- @regex("Field", true)
desc	mo_catalog.mo_transactions            ;
-- @regex("Field", true)
desc	mo_catalog.mo_cache                   ;
-- @regex("Field", true)
desc	mo_catalog.mo_foreign_keys            ;
-- @regex("Field", true)
desc	mo_catalog.mo_snapshots               ;


-- @regex("Field", true)
show columns from	mo_catalog.mo_database                ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_tables                  ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_columns                 ;
show columns from	mo_catalog.mo_account                 ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_user                    ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_role                    ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_user_grant              ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_role_grant              ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_role_privs              ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_user_defined_function   ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_stored_procedure        ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_mysql_compatibility_mode;
-- @regex("Field", true)
show columns from	mo_catalog.mo_increment_columns       ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_indexes                 ;
-- @bvt:issue#16438
-- @regex("Field", true)
show columns from	mo_catalog.mo_table_partitions        ;
-- @bvt:issue
show columns from	mo_catalog.mo_pubs                    ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_stages                  ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_sessions                ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_configurations          ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_locks                   ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_variables               ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_transactions            ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_cache                   ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_foreign_keys            ;
-- @regex("Field", true)
show columns from	mo_catalog.mo_snapshots           ;

-- @session
drop account if exists fk_show_column;
