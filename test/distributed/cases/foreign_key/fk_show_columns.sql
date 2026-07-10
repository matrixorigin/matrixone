drop account if exists fk_show_column;
create account fk_show_column admin_name 'root' identified by '111';

-- @ignore:6
desc	mo_catalog.mo_database                ;
-- @ignore:6
desc	mo_catalog.mo_tables                  ;
-- @ignore:6
desc	mo_catalog.mo_columns                 ;
-- @ignore:6
desc	mo_catalog.mo_account                 ;
-- @ignore:6
desc	mo_catalog.mo_user                    ;
-- @ignore:6
desc	mo_catalog.mo_role                    ;
-- @ignore:6
desc	mo_catalog.mo_user_grant              ;
-- @ignore:6
desc	mo_catalog.mo_role_grant              ;
-- @ignore:6
desc	mo_catalog.mo_role_privs              ;
-- @ignore:0
-- @ignore:6
desc	mo_catalog.mo_user_defined_function   ;
-- @ignore:6
desc	mo_catalog.mo_stored_procedure        ;
-- @ignore:6
desc	mo_catalog.mo_mysql_compatibility_mode;
-- @ignore:6
desc	mo_catalog.mo_increment_columns       ;
-- @ignore:6
desc	mo_catalog.mo_indexes                 ;
-- @bvt:issue#16438
-- @ignore:6
desc	mo_catalog.mo_table_partitions        ;
-- @bvt:issue
-- @ignore:6
desc	mo_catalog.mo_pubs                    ;
-- @ignore:6
desc	mo_catalog.mo_stages                  ;
-- @ignore:6
desc	mo_catalog.mo_sessions                ;
-- @ignore:6
desc	mo_catalog.mo_configurations          ;
-- @ignore:6
desc	mo_catalog.mo_locks                   ;
-- @ignore:6
desc	mo_catalog.mo_variables               ;
-- @ignore:6
desc	mo_catalog.mo_transactions            ;
-- @ignore:6
desc	mo_catalog.mo_cache                   ;
-- @ignore:6
desc	mo_catalog.mo_foreign_keys            ;
-- @ignore:6
desc	mo_catalog.mo_snapshots               ;


-- @ignore:6
show columns from	mo_catalog.mo_database                ;
-- @ignore:6
show columns from	mo_catalog.mo_tables                  ;
-- @ignore:6
show columns from	mo_catalog.mo_columns                 ;
-- @ignore:6
show columns from	mo_catalog.mo_account                 ;
-- @ignore:6
show columns from	mo_catalog.mo_user                    ;
-- @ignore:6
show columns from	mo_catalog.mo_role                    ;
-- @ignore:6
show columns from	mo_catalog.mo_user_grant              ;
-- @ignore:6
show columns from	mo_catalog.mo_role_grant              ;
-- @ignore:6
show columns from	mo_catalog.mo_role_privs              ;
-- @ignore:0
-- @ignore:6
show columns from	mo_catalog.mo_user_defined_function   ;
-- @ignore:6
show columns from	mo_catalog.mo_stored_procedure        ;
-- @ignore:6
show columns from	mo_catalog.mo_mysql_compatibility_mode;
-- @ignore:6
show columns from	mo_catalog.mo_increment_columns       ;
-- @ignore:6
show columns from	mo_catalog.mo_indexes                 ;
-- @bvt:issue#16438
-- @ignore:6
show columns from	mo_catalog.mo_table_partitions        ;
-- @bvt:issue
-- @ignore:6
show columns from	mo_catalog.mo_pubs                    ;
-- @ignore:6
show columns from	mo_catalog.mo_stages                  ;
-- @ignore:6
show columns from	mo_catalog.mo_sessions                ;
-- @ignore:6
show columns from	mo_catalog.mo_configurations          ;
-- @ignore:6
show columns from	mo_catalog.mo_locks                   ;
-- @ignore:6
show columns from	mo_catalog.mo_variables               ;
-- @ignore:6
show columns from	mo_catalog.mo_transactions            ;
-- @ignore:6
show columns from	mo_catalog.mo_cache                   ;
-- @ignore:6
show columns from	mo_catalog.mo_foreign_keys            ;
-- @ignore:6
show columns from	mo_catalog.mo_snapshots           ;

-- @session:id=1&user=fk_show_column:root:accountadmin&password=111
-- @ignore:6
desc	mo_catalog.mo_database                ;
-- @ignore:6
desc	mo_catalog.mo_tables                  ;
-- @ignore:6
desc	mo_catalog.mo_columns                 ;
-- @ignore:6
desc	mo_catalog.mo_account                 ;
-- @ignore:6
desc	mo_catalog.mo_user                    ;
-- @ignore:6
desc	mo_catalog.mo_role                    ;
-- @ignore:6
desc	mo_catalog.mo_user_grant              ;
-- @ignore:6
desc	mo_catalog.mo_role_grant              ;
-- @ignore:6
desc	mo_catalog.mo_role_privs              ;
-- @ignore:6
desc	mo_catalog.mo_user_defined_function   ;
-- @ignore:6
desc	mo_catalog.mo_stored_procedure        ;
-- @ignore:6
desc	mo_catalog.mo_mysql_compatibility_mode;
-- @ignore:6
desc	mo_catalog.mo_increment_columns       ;
-- @ignore:6
desc	mo_catalog.mo_indexes                 ;
-- @bvt:issue#16438
-- @ignore:6
desc	mo_catalog.mo_table_partitions        ;
-- @bvt:issue
-- @ignore:6
desc	mo_catalog.mo_pubs                    ;
-- @ignore:6
desc	mo_catalog.mo_stages                  ;
-- @ignore:6
desc	mo_catalog.mo_sessions                ;
-- @ignore:6
desc	mo_catalog.mo_configurations          ;
-- @ignore:6
desc	mo_catalog.mo_locks                   ;
-- @ignore:6
desc	mo_catalog.mo_variables               ;
-- @ignore:6
desc	mo_catalog.mo_transactions            ;
-- @ignore:6
desc	mo_catalog.mo_cache                   ;
-- @ignore:6
desc	mo_catalog.mo_foreign_keys            ;
-- @ignore:6
desc	mo_catalog.mo_snapshots               ;


-- @ignore:6
show columns from	mo_catalog.mo_database                ;
-- @ignore:6
show columns from	mo_catalog.mo_tables                  ;
-- @ignore:6
show columns from	mo_catalog.mo_columns                 ;
-- @ignore:6
show columns from	mo_catalog.mo_account                 ;
-- @ignore:6
show columns from	mo_catalog.mo_user                    ;
-- @ignore:6
show columns from	mo_catalog.mo_role                    ;
-- @ignore:6
show columns from	mo_catalog.mo_user_grant              ;
-- @ignore:6
show columns from	mo_catalog.mo_role_grant              ;
-- @ignore:6
show columns from	mo_catalog.mo_role_privs              ;
-- @ignore:6
show columns from	mo_catalog.mo_user_defined_function   ;
-- @ignore:6
show columns from	mo_catalog.mo_stored_procedure        ;
-- @ignore:6
show columns from	mo_catalog.mo_mysql_compatibility_mode;
-- @ignore:6
show columns from	mo_catalog.mo_increment_columns       ;
-- @ignore:6
show columns from	mo_catalog.mo_indexes                 ;
-- @bvt:issue#16438
-- @ignore:6
show columns from	mo_catalog.mo_table_partitions        ;
-- @bvt:issue
-- @ignore:6
show columns from	mo_catalog.mo_pubs                    ;
-- @ignore:6
show columns from	mo_catalog.mo_stages                  ;
-- @ignore:6
show columns from	mo_catalog.mo_sessions                ;
-- @ignore:6
show columns from	mo_catalog.mo_configurations          ;
-- @ignore:6
show columns from	mo_catalog.mo_locks                   ;
-- @ignore:6
show columns from	mo_catalog.mo_variables               ;
-- @ignore:6
show columns from	mo_catalog.mo_transactions            ;
-- @ignore:6
show columns from	mo_catalog.mo_cache                   ;
-- @ignore:6
show columns from	mo_catalog.mo_foreign_keys            ;
-- @ignore:6
show columns from	mo_catalog.mo_snapshots           ;

-- @session
drop account if exists fk_show_column;