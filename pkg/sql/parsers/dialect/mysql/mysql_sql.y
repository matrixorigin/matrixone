// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

%{
package mysql

import (
    "fmt"
    "strings"
    "go/constant"

    "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
    "github.com/matrixorigin/matrixone/pkg/sql/parsers/util"
    "github.com/matrixorigin/matrixone/pkg/defines"
)
%}

%struct {
    id  int
    str string
    item interface{}
}

%union {
    statement tree.Statement
    statements []tree.Statement

    alterTable tree.AlterTable
    alterTableOptions tree.AlterTableOptions
    alterTableOption tree.AlterTableOption
    alterPartitionOption  tree.AlterPartitionOption
    alterColPosition *tree.ColumnPosition
    alterColumnOrderBy []*tree.AlterColumnOrder
    alterColumnOrder *tree.AlterColumnOrder

    PartitionNames tree.IdentifierList

    atTimeStamp *tree.AtTimeStamp
    tableDef tree.TableDef
    tableDefs tree.TableDefs
    tableName *tree.TableName
    tableNames tree.TableNames
    columnTableDef *tree.ColumnTableDef
    tableOption tree.TableOption
    tableOptions []tree.TableOption
    tableExprs tree.TableExprs
    tableExpr tree.TableExpr
    rowFormatType tree.RowFormatType
    matchType tree.MatchType
    attributeReference *tree.AttributeReference
    loadParam *tree.ExternParam
    tailParam *tree.TailParameter
    connectorOption *tree.ConnectorOption
    connectorOptions []*tree.ConnectorOption

    functionName *tree.FunctionName
    funcArg tree.FunctionArg
    funcArgs tree.FunctionArgs
    funcArgDecl *tree.FunctionArgDecl
    funcReturn *tree.ReturnType

    procName *tree.ProcedureName
    procArg tree.ProcedureArg
    procArgs tree.ProcedureArgs
    procArgDecl *tree.ProcedureArgDecl
    procArgType tree.InOutArgType

    from *tree.From
    where *tree.Where
    groupBy tree.GroupBy
    aliasedTableExpr *tree.AliasedTableExpr
    direction tree.Direction
    nullsPosition tree.NullsPosition
    orderBy tree.OrderBy
    order *tree.Order
    limit *tree.Limit
    unionTypeRecord *tree.UnionTypeRecord
    parenTableExpr *tree.ParenTableExpr
    identifierList tree.IdentifierList
    joinCond tree.JoinCond
    selectLockInfo *tree.SelectLockInfo

    columnType *tree.T
    unresolvedName *tree.UnresolvedName
    lengthScaleOpt tree.LengthScaleOpt
    tuple *tree.Tuple
    funcType tree.FuncType

    columnAttribute tree.ColumnAttribute
    columnAttributes []tree.ColumnAttribute
    attributeNull tree.AttributeNull
    expr tree.Expr
    exprs tree.Exprs
    rowsExprs []tree.Exprs
    comparisonOp tree.ComparisonOp
    referenceOptionType tree.ReferenceOptionType
    referenceOnRecord *tree.ReferenceOnRecord

    select *tree.Select
    selectStatement tree.SelectStatement
    selectExprs tree.SelectExprs
    selectExpr tree.SelectExpr

    insert *tree.Insert
    replace *tree.Replace
    createOption tree.CreateOption
    createOptions []tree.CreateOption
    indexType tree.IndexType
    indexCategory tree.IndexCategory
    keyParts []*tree.KeyPart
    keyPart *tree.KeyPart
    indexOption *tree.IndexOption
    comparisionExpr *tree.ComparisonExpr

    userMiscOption tree.UserMiscOption
    userMiscOptions []tree.UserMiscOption
    updateExpr *tree.UpdateExpr
    updateExprs tree.UpdateExprs
    completionType tree.CompletionType
    varAssignmentExpr *tree.VarAssignmentExpr
    varAssignmentExprs []*tree.VarAssignmentExpr
    setRole *tree.SetRole
    setDefaultRole *tree.SetDefaultRole
    privilege *tree.Privilege
    privileges []*tree.Privilege
    objectType tree.ObjectType
    privilegeType tree.PrivilegeType
    privilegeLevel *tree.PrivilegeLevel
    unresolveNames []*tree.UnresolvedName

    partitionOption *tree.PartitionOption
    clusterByOption *tree.ClusterByOption
    partitionBy *tree.PartitionBy
    windowSpec *tree.WindowSpec
    frameClause *tree.FrameClause
    frameBound *tree.FrameBound
    frameType tree.FrameType
    partition *tree.Partition
    partitions []*tree.Partition
    values tree.Values
    numVal *tree.NumVal
    subPartition *tree.SubPartition
    subPartitions []*tree.SubPartition

    upgrade_target *tree.Target

    subquery *tree.Subquery
    funcExpr *tree.FuncExpr

    roles []*tree.Role
    role *tree.Role
    usernameRecord *tree.UsernameRecord
    authRecord *tree.AuthRecord
    user *tree.User
    users []*tree.User
    tlsOptions []tree.TlsOption
    tlsOption tree.TlsOption
    resourceOption tree.ResourceOption
    resourceOptions []tree.ResourceOption
    unresolvedObjectName *tree.UnresolvedObjectName
    lengthOpt int32
    unsignedOpt bool
    zeroFillOpt bool
    ifNotExists bool
    defaultOptional bool
    sourceOptional bool
    connectorOptional bool
    fullOpt bool
    boolVal bool
    int64Val int64
    strs []string

    duplicateKey tree.DuplicateKey
    fields *tree.Fields
    fieldsList []*tree.Fields
    lines *tree.Lines
    varExpr *tree.VarExpr
    varExprs []*tree.VarExpr
    loadColumn tree.LoadColumn
    loadColumns []tree.LoadColumn
    assignments []*tree.Assignment
    assignment *tree.Assignment
    properties []tree.Property
    property tree.Property
    exportParm *tree.ExportParam

    epxlainOptions []tree.OptionElem
    epxlainOption tree.OptionElem
    whenClause *tree.When
    whenClauseList []*tree.When
    withClause *tree.With
    cte *tree.CTE
    cteList []*tree.CTE

    accountAuthOption tree.AccountAuthOption
    alterAccountAuthOption tree.AlterAccountAuthOption
    accountIdentified tree.AccountIdentified
    accountStatus tree.AccountStatus
    accountComment tree.AccountComment
    stageComment tree.StageComment
    stageStatus tree.StageStatus
    stageUrl tree.StageUrl
    stageCredentials tree.StageCredentials
    accountCommentOrAttribute tree.AccountCommentOrAttribute
    userIdentified *tree.AccountIdentified
    accountRole *tree.Role
    showType tree.ShowType
    joinTableExpr *tree.JoinTableExpr

    indexHintType tree.IndexHintType
    indexHintScope tree.IndexHintScope
    indexHint *tree.IndexHint
    indexHintList []*tree.IndexHint
    indexVisibility tree.VisibleType

    killOption tree.KillOption
    statementOption tree.StatementOption

    tableLock tree.TableLock
    tableLocks []tree.TableLock
    tableLockType tree.TableLockType
    cstr *tree.CStr
    incrementByOption *tree.IncrementByOption
    minValueOption  *tree.MinValueOption
    maxValueOption  *tree.MaxValueOption
    startWithOption *tree.StartWithOption
    cycleOption     *tree.CycleOption
    alterTypeOption *tree.TypeOption

    whenClause2 *tree.WhenStmt
    whenClauseList2 []*tree.WhenStmt

    elseIfClause *tree.ElseIfStmt
    elseIfClauseList []*tree.ElseIfStmt
    subscriptionOption *tree.SubscriptionOption
    accountsSetOption *tree.AccountsSetOption

    transactionCharacteristic *tree.TransactionCharacteristic
    transactionCharacteristicList []*tree.TransactionCharacteristic
    isolationLevel tree.IsolationLevelType
    accessMode tree.AccessModeType

    timeWindow  *tree.TimeWindow
    timeInterval *tree.Interval
    timeSliding *tree.Sliding
    timeFill *tree.Fill
    fillMode tree.FillMode

    snapshotObject tree.ObjectInfo
    allCDCOption   *tree.AllOrNotCDC

}

%token LEX_ERROR
%nonassoc EMPTY
%left <str> UNION EXCEPT INTERSECT MINUS
%nonassoc LOWER_THAN_ORDER
%nonassoc ORDER
%nonassoc LOWER_THAN_COMMA
%token <str> SELECT INSERT UPDATE DELETE FROM WHERE GROUP HAVING BY LIMIT OFFSET FOR CONNECT MANAGE GRANTS OWNERSHIP REFERENCE
%nonassoc LOWER_THAN_SET
%nonassoc <str> SET
%token <str> ALL DISTINCT DISTINCTROW AS EXISTS ASC DESC INTO DUPLICATE DEFAULT LOCK KEYS NULLS FIRST LAST AFTER
%token <str> INSTANT INPLACE COPY DISABLE ENABLE UNDEFINED MERGE TEMPTABLE DEFINER INVOKER SQL SECURITY CASCADED
%token <str> VALUES
%token <str> NEXT VALUE SHARE MODE
%token <str> SQL_NO_CACHE SQL_CACHE
%left <str> JOIN STRAIGHT_JOIN LEFT RIGHT INNER OUTER CROSS NATURAL USE FORCE CROSS_L2
%nonassoc LOWER_THAN_ON
%nonassoc <str> ON USING
%left <str> SUBQUERY_AS_EXPR
%right <str> '('
%left <str> ')'
%nonassoc LOWER_THAN_STRING
%nonassoc <str> ID AT_ID AT_AT_ID STRING VALUE_ARG LIST_ARG COMMENT COMMENT_KEYWORD QUOTE_ID STAGE CREDENTIALS STAGES SNAPSHOTS
%token <item> INTEGRAL HEX FLOAT
%token <str>  HEXNUM BIT_LITERAL
%token <str> NULL TRUE FALSE
%nonassoc LOWER_THAN_CHARSET
%nonassoc <str> CHARSET
%right <str> UNIQUE KEY
%left <str> OR PIPE_CONCAT
%left <str> XOR
%left <str> AND
%right <str> NOT '!'
%left <str> BETWEEN CASE WHEN THEN ELSE END ELSEIF
%nonassoc LOWER_THAN_EQ
%left <str> '=' '<' '>' LE GE NE NULL_SAFE_EQUAL IS LIKE REGEXP IN ASSIGNMENT ILIKE
%left <str> '|'
%left <str> '&'
%left <str> SHIFT_LEFT SHIFT_RIGHT
%left <str> '+' '-'
%left <str> '*' '/' DIV '%' MOD
%left <str> '^'
%right <str> '~' UNARY
%left <str> COLLATE
%right <str> BINARY UNDERSCORE_BINARY
%right <str> INTERVAL
%nonassoc <str> '.' ','

%token <str> OUT INOUT

// Transaction
%token <str> BEGIN START TRANSACTION COMMIT ROLLBACK WORK CONSISTENT SNAPSHOT
%token <str> CHAIN NO RELEASE PRIORITY QUICK

// Type
%token <str> BIT TINYINT SMALLINT MEDIUMINT INT INTEGER BIGINT INTNUM
%token <str> REAL DOUBLE FLOAT_TYPE DECIMAL NUMERIC DECIMAL_VALUE
%token <str> TIME TIMESTAMP DATETIME YEAR
%token <str> CHAR VARCHAR BOOL CHARACTER VARBINARY NCHAR
%token <str> TEXT TINYTEXT MEDIUMTEXT LONGTEXT DATALINK
%token <str> BLOB TINYBLOB MEDIUMBLOB LONGBLOB JSON ENUM UUID VECF32 VECF64
%token <str> GEOMETRY POINT LINESTRING POLYGON GEOMETRYCOLLECTION MULTIPOINT MULTILINESTRING MULTIPOLYGON
%token <str> INT1 INT2 INT3 INT4 INT8 S3OPTION STAGEOPTION

// Select option
%token <str> SQL_SMALL_RESULT SQL_BIG_RESULT SQL_BUFFER_RESULT
%token <str> LOW_PRIORITY HIGH_PRIORITY DELAYED

// Create Table
%token <str> CREATE ALTER DROP RENAME ANALYZE ADD RETURNS
%token <str> SCHEMA TABLE SEQUENCE INDEX VIEW TO IGNORE IF PRIMARY COLUMN CONSTRAINT SPATIAL FULLTEXT FOREIGN KEY_BLOCK_SIZE
%token <str> SHOW DESCRIBE EXPLAIN DATE ESCAPE REPAIR OPTIMIZE TRUNCATE
%token <str> MAXVALUE PARTITION REORGANIZE LESS THAN PROCEDURE TRIGGER
%token <str> STATUS VARIABLES ROLE PROXY AVG_ROW_LENGTH STORAGE DISK MEMORY
%token <str> CHECKSUM COMPRESSION DATA DIRECTORY DELAY_KEY_WRITE ENCRYPTION ENGINE
%token <str> MAX_ROWS MIN_ROWS PACK_KEYS ROW_FORMAT STATS_AUTO_RECALC STATS_PERSISTENT STATS_SAMPLE_PAGES
%token <str> DYNAMIC COMPRESSED REDUNDANT COMPACT FIXED COLUMN_FORMAT AUTO_RANDOM ENGINE_ATTRIBUTE SECONDARY_ENGINE_ATTRIBUTE INSERT_METHOD
%token <str> RESTRICT CASCADE ACTION PARTIAL SIMPLE CHECK ENFORCED
%token <str> RANGE LIST ALGORITHM LINEAR PARTITIONS SUBPARTITION SUBPARTITIONS CLUSTER
%token <str> TYPE ANY SOME EXTERNAL LOCALFILE URL
%token <str> PREPARE DEALLOCATE RESET
%token <str> EXTENSION

// Sequence
%token <str> INCREMENT CYCLE MINVALUE
// publication
%token <str> PUBLICATION SUBSCRIPTIONS PUBLICATIONS


// MO table option
%token <str> PROPERTIES

// Secondary Index
%token <str> PARSER VISIBLE INVISIBLE BTREE HASH RTREE BSI IVFFLAT MASTER
%token <str> ZONEMAP LEADING BOTH TRAILING UNKNOWN LISTS OP_TYPE REINDEX


// Alter
%token <str> EXPIRE ACCOUNT ACCOUNTS UNLOCK DAY NEVER PUMP MYSQL_COMPATIBILITY_MODE UNIQUE_CHECK_ON_AUTOINCR
%token <str> MODIFY CHANGE

// Time
%token <str> SECOND ASCII COALESCE COLLATION HOUR MICROSECOND MINUTE MONTH QUARTER REPEAT
%token <str> REVERSE ROW_COUNT WEEK

// Revoke
%token <str> REVOKE FUNCTION PRIVILEGES TABLESPACE EXECUTE SUPER GRANT OPTION REFERENCES REPLICATION
%token <str> SLAVE CLIENT USAGE RELOAD FILE TEMPORARY ROUTINE EVENT SHUTDOWN

// Type Modifiers
%token <str> NULLX AUTO_INCREMENT APPROXNUM SIGNED UNSIGNED ZEROFILL ENGINES LOW_CARDINALITY AUTOEXTEND_SIZE

// Account
%token <str> ADMIN_NAME RANDOM SUSPEND ATTRIBUTE HISTORY REUSE CURRENT OPTIONAL FAILED_LOGIN_ATTEMPTS PASSWORD_LOCK_TIME UNBOUNDED SECONDARY RESTRICTED

// User
%token <str> USER IDENTIFIED CIPHER ISSUER X509 SUBJECT SAN REQUIRE SSL NONE PASSWORD SHARED EXCLUSIVE
%token <str> MAX_QUERIES_PER_HOUR MAX_UPDATES_PER_HOUR MAX_CONNECTIONS_PER_HOUR MAX_USER_CONNECTIONS

// Explain
%token <str> FORMAT VERBOSE CONNECTION TRIGGERS PROFILES

// Load
%token <str> LOAD INLINE INFILE TERMINATED OPTIONALLY ENCLOSED ESCAPED STARTING LINES ROWS IMPORT DISCARD JSONTYPE

// MODump
%token <str> MODUMP

// Window function
%token <str> OVER PRECEDING FOLLOWING GROUPS

// Supported SHOW tokens
%token <str> DATABASES TABLES SEQUENCES EXTENDED FULL PROCESSLIST FIELDS COLUMNS OPEN ERRORS WARNINGS INDEXES SCHEMAS NODE LOCKS ROLES
%token <str> TABLE_NUMBER COLUMN_NUMBER TABLE_VALUES TABLE_SIZE

// SET tokens
%token <str> NAMES GLOBAL PERSIST SESSION ISOLATION LEVEL READ WRITE ONLY REPEATABLE COMMITTED UNCOMMITTED SERIALIZABLE
%token <str> LOCAL EVENTS PLUGINS

// Functions
%token <str> CURRENT_TIMESTAMP DATABASE
%token <str> CURRENT_TIME LOCALTIME LOCALTIMESTAMP
%token <str> UTC_DATE UTC_TIME UTC_TIMESTAMP
%token <str> REPLACE CONVERT
%token <str> SEPARATOR TIMESTAMPDIFF
%token <str> CURRENT_DATE CURRENT_USER CURRENT_ROLE

// Time unit
%token <str> SECOND_MICROSECOND MINUTE_MICROSECOND MINUTE_SECOND HOUR_MICROSECOND
%token <str> HOUR_SECOND HOUR_MINUTE DAY_MICROSECOND DAY_SECOND DAY_MINUTE DAY_HOUR YEAR_MONTH
%token <str> SQL_TSI_HOUR SQL_TSI_DAY SQL_TSI_WEEK SQL_TSI_MONTH SQL_TSI_QUARTER SQL_TSI_YEAR
%token <str> SQL_TSI_SECOND SQL_TSI_MINUTE

// With
%token <str> RECURSIVE CONFIG DRAINER

// Source
%token <str> SOURCE STREAM HEADERS CONNECTOR CONNECTORS DAEMON PAUSE CANCEL TASK RESUME

// Match
%token <str> MATCH AGAINST BOOLEAN LANGUAGE WITH QUERY EXPANSION WITHOUT VALIDATION

// upgrade
%token <str> UPGRADE RETRY

// Built-in function
%token <str> ADDDATE BIT_AND BIT_OR BIT_XOR CAST COUNT APPROX_COUNT APPROX_COUNT_DISTINCT SERIAL_EXTRACT
%token <str> APPROX_PERCENTILE CURDATE CURTIME DATE_ADD DATE_SUB EXTRACT
%token <str> GROUP_CONCAT MAX MID MIN NOW POSITION SESSION_USER STD STDDEV MEDIAN
%token <str> CLUSTER_CENTERS KMEANS
%token <str> STDDEV_POP STDDEV_SAMP SUBDATE SUBSTR SUBSTRING SUM SYSDATE
%token <str> SYSTEM_USER TRANSLATE TRIM VARIANCE VAR_POP VAR_SAMP AVG RANK ROW_NUMBER
%token <str> DENSE_RANK BIT_CAST
%token <str> BITMAP_BIT_POSITION BITMAP_BUCKET_NUMBER BITMAP_COUNT BITMAP_CONSTRUCT_AGG BITMAP_OR_AGG

// Sequence function
%token <str> NEXTVAL SETVAL CURRVAL LASTVAL

//JSON function
%token <str> ARROW

// Insert
%token <str> ROW OUTFILE HEADER MAX_FILE_SIZE FORCE_QUOTE PARALLEL STRICT

%token <str> UNUSED BINDINGS

// Do
%token <str> DO

// Declare
%token <str> DECLARE

// Iteration
%token <str> LOOP WHILE LEAVE ITERATE UNTIL

// Call
%token <str> CALL

// Time window
%token <str> PREV SLIDING FILL

// sp_begin_sym
%token <str> SPBEGIN

%token <str> BACKEND SERVERS

// python udf
%token <str> HANDLER

// Sample Related.
%token <str> PERCENT SAMPLE

// Snapshot READ
%token <str> MO_TS

// PITR
%token <str> PITR

// CDC
%token <str> CDC

%type <statement> stmt block_stmt block_type_stmt normal_stmt
%type <statements> stmt_list stmt_list_return
%type <statement> create_stmt insert_stmt delete_stmt drop_stmt alter_stmt truncate_table_stmt alter_sequence_stmt upgrade_stmt
%type <statement> delete_without_using_stmt delete_with_using_stmt
%type <statement> drop_ddl_stmt drop_database_stmt drop_table_stmt drop_index_stmt drop_prepare_stmt drop_view_stmt drop_connector_stmt drop_function_stmt drop_procedure_stmt drop_sequence_stmt
%type <statement> drop_account_stmt drop_role_stmt drop_user_stmt
%type <statement> create_account_stmt create_user_stmt create_role_stmt
%type <statement> create_ddl_stmt create_table_stmt create_database_stmt create_index_stmt create_view_stmt create_function_stmt create_extension_stmt create_procedure_stmt create_sequence_stmt
%type <statement> create_source_stmt create_connector_stmt pause_daemon_task_stmt cancel_daemon_task_stmt resume_daemon_task_stmt
%type <statement> show_stmt show_create_stmt show_columns_stmt show_databases_stmt show_target_filter_stmt show_table_status_stmt show_grants_stmt show_collation_stmt show_accounts_stmt show_roles_stmt show_stages_stmt show_snapshots_stmt show_upgrade_stmt
%type <statement> show_tables_stmt show_sequences_stmt show_process_stmt show_errors_stmt show_warnings_stmt show_target
%type <statement> show_procedure_status_stmt show_function_status_stmt show_node_list_stmt show_locks_stmt
%type <statement> show_table_num_stmt show_column_num_stmt show_table_values_stmt show_table_size_stmt
%type <statement> show_variables_stmt show_status_stmt show_index_stmt
%type <statement> show_servers_stmt show_connectors_stmt
%type <statement> alter_account_stmt alter_user_stmt alter_view_stmt update_stmt use_stmt update_no_with_stmt alter_database_config_stmt alter_table_stmt
%type <statement> transaction_stmt begin_stmt commit_stmt rollback_stmt
%type <statement> explain_stmt explainable_stmt
%type <statement> set_stmt set_variable_stmt set_password_stmt set_role_stmt set_default_role_stmt set_transaction_stmt
%type <statement> lock_stmt lock_table_stmt unlock_table_stmt
%type <statement> revoke_stmt grant_stmt
%type <statement> load_data_stmt
%type <statement> analyze_stmt
%type <statement> prepare_stmt prepareable_stmt deallocate_stmt execute_stmt reset_stmt
%type <statement> replace_stmt
%type <statement> do_stmt
%type <statement> declare_stmt
%type <statement> values_stmt
%type <statement> call_stmt
%type <statement> mo_dump_stmt
%type <statement> load_extension_stmt
%type <statement> kill_stmt
%type <statement> backup_stmt snapshot_restore_stmt
%type <statement> create_cdc_stmt show_cdc_stmt pause_cdc_stmt drop_cdc_stmt resume_cdc_stmt restart_cdc_stmt
%type <rowsExprs> row_constructor_list
%type <exprs>  row_constructor
%type <exportParm> export_data_param_opt
%type <loadParam> load_param_opt load_param_opt_2
%type <tailParam> tail_param_opt
%type <str> json_type_opt

// case statement
%type <statement> case_stmt
%type <whenClause2> when_clause2
%type <whenClauseList2> when_clause_list2
%type <statements> else_clause_opt2

// if statement
%type <statement> if_stmt
%type <elseIfClause> elseif_clause
%type <elseIfClauseList> elseif_clause_list elseif_clause_list_opt

// iteration
%type <statement> loop_stmt iterate_stmt leave_stmt repeat_stmt while_stmt
%type <statement> create_publication_stmt drop_publication_stmt alter_publication_stmt show_publications_stmt show_subscriptions_stmt
%type <statement> create_stage_stmt drop_stage_stmt alter_stage_stmt
%type <statement> create_snapshot_stmt drop_snapshot_stmt
%type <statement> create_pitr_stmt drop_pitr_stmt show_pitr_stmt alter_pitr_stmt restore_pitr_stmt
%type <str> urlparams
%type <str> comment_opt view_list_opt view_opt security_opt view_tail check_type
%type <subscriptionOption> subscription_opt
%type <accountsSetOption> alter_publication_accounts_opt
%type <str> alter_publication_db_name_opt

%type <select> select_stmt select_no_parens
%type <selectStatement> simple_select select_with_parens simple_select_clause
%type <selectExprs> select_expression_list
%type <selectExpr> select_expression
%type <tableExprs> table_name_wild_list
%type <joinTableExpr>  table_references join_table
%type <tableExpr> into_table_name table_function table_factor table_reference escaped_table_reference
%type <direction> asc_desc_opt
%type <nullsPosition> nulls_first_last_opt
%type <order> order
%type <orderBy> order_list order_by_clause order_by_opt
%type <limit> limit_opt limit_clause
%type <str> insert_column
%type <identifierList> column_list column_list_opt partition_clause_opt partition_id_list insert_column_list accounts_list
%type <joinCond> join_condition join_condition_opt on_expression_opt
%type <selectLockInfo> select_lock_opt
%type <upgrade_target> target

%type <functionName> func_name
%type <funcArgs> func_args_list_opt func_args_list
%type <funcArg> func_arg
%type <funcArgDecl> func_arg_decl
%type <funcReturn> func_return
%type <boolVal> func_body_import
%type <str> func_lang extension_lang extension_name

%type <procName> proc_name
%type <procArgs> proc_args_list_opt proc_args_list
%type <procArg> proc_arg
%type <procArgDecl> proc_arg_decl
%type <procArgType> proc_arg_in_out_type

%type <atTimeStamp> table_snapshot_opt
%type <tableDefs> table_elem_list_opt table_elem_list
%type <tableDef> table_elem constaint_def constraint_elem index_def table_elem_2
%type <tableName> table_name table_name_opt_wild
%type <tableNames> table_name_list
%type <columnTableDef> column_def
%type <columnType> mo_cast_type mysql_cast_type
%type <columnType> column_type char_type spatial_type time_type numeric_type decimal_type int_type as_datatype_opt
%type <str> integer_opt
%type <columnAttribute> column_attribute_elem keys
%type <columnAttributes> column_attribute_list column_attribute_list_opt
%type <tableOptions> table_option_list_opt table_option_list source_option_list_opt source_option_list
%type <str> charset_name storage_opt collate_name column_format storage_media algorithm_type able_type space_type lock_type with_type rename_type algorithm_type_2 load_charset
%type <rowFormatType> row_format_options
%type <int64Val> field_length_opt max_file_size_opt
%type <matchType> match match_opt
%type <referenceOptionType> ref_opt on_delete on_update
%type <referenceOnRecord> on_delete_update_opt
%type <attributeReference> references_def
%type <alterTableOptions> alter_option_list
%type <alterTableOption> alter_option alter_table_drop alter_table_alter alter_table_rename
%type <alterPartitionOption> alter_partition_option partition_option
%type <alterColPosition> column_position
%type <alterColumnOrder> alter_column_order
%type <alterColumnOrderBy> alter_column_order_list
%type <indexVisibility> visibility
%type <PartitionNames> AllOrPartitionNameList PartitionNameList

%type <tableOption> table_option source_option
%type <connectorOption> connector_option
%type <connectorOptions> connector_option_list
%type <from> from_clause from_opt
%type <where> where_expression_opt having_opt
%type <groupBy> group_by_opt
%type <aliasedTableExpr> aliased_table_name
%type <unionTypeRecord> union_op
%type <parenTableExpr> table_subquery
%type <str> inner_join straight_join outer_join natural_join
%type <funcType> func_type_opt
%type <funcExpr> function_call_generic
%type <funcExpr> function_call_keyword
%type <funcExpr> function_call_nonkeyword
%type <funcExpr> function_call_aggregate
%type <funcExpr> function_call_window
%type <expr> sample_function_expr

%type <unresolvedName> column_name column_name_unresolved normal_ident
%type <strs> enum_values force_quote_opt force_quote_list infile_or_s3_param infile_or_s3_params credentialsparams credentialsparam create_cdc_opt create_cdc_opts
%type <str> charset_keyword db_name db_name_opt
%type <str> backup_type_opt backup_timestamp_opt
%type <str> not_keyword func_not_keyword
%type <str> non_reserved_keyword
%type <str> equal_opt column_keyword_opt
%type <str> as_opt_id name_string
%type <cstr> ident as_name_opt db_name_ident
%type <str> table_alias explain_sym prepare_sym deallocate_sym stmt_name reset_sym
%type <unresolvedObjectName> unresolved_object_name table_column_name
%type <unresolvedObjectName> table_name_unresolved
%type <comparisionExpr> like_opt
%type <fullOpt> full_opt
%type <str> database_name_opt auth_string constraint_keyword_opt constraint_keyword
%type <userMiscOption> pwd_or_lck pwd_or_lck_opt
//%type <userMiscOptions> pwd_or_lck_list

%type <expr> literal num_literal
%type <expr> predicate
%type <expr> bit_expr interval_expr
%type <expr> simple_expr else_opt
%type <expr> expression value_expression like_escape_opt boolean_primary col_tuple expression_opt
%type <exprs> expression_list_opt
%type <exprs> value_expression_list
%type <exprs> expression_list row_value window_partition_by window_partition_by_opt
%type <expr> datetime_scale_opt datetime_scale
%type <tuple> tuple_expression
%type <comparisonOp> comparison_operator and_or_some
%type <createOption> create_option
%type <createOptions> create_option_list_opt create_option_list
%type <ifNotExists> not_exists_opt
%type <defaultOptional> default_opt
%type <sourceOptional> replace_opt
%type <str> database_or_schema
%type <indexType> using_opt
%type <indexCategory> index_prefix
%type <keyParts> index_column_list index_column_list_opt
%type <keyPart> index_column
%type <indexOption> index_option_list index_option
%type <roles> role_spec_list using_roles_opt
%type <role> role_spec
%type <cstr> role_name
%type <usernameRecord> user_name
%type <user> user_spec drop_user_spec user_spec_with_identified
%type <users> user_spec_list drop_user_spec_list user_spec_list_of_create_user
//%type <tlsOptions> require_clause_opt require_clause require_list
//%type <tlsOption> require_elem
//%type <resourceOptions> conn_option_list conn_options
//%type <resourceOption> conn_option
%type <updateExpr> update_value
%type <updateExprs> update_list on_duplicate_key_update_opt
%type <completionType> completion_type
%type <str> password_opt
%type <boolVal> grant_option_opt enforce enforce_opt

%type <varAssignmentExpr> var_assignment
%type <varAssignmentExprs> var_assignment_list
%type <str> var_name equal_or_assignment
%type <strs> var_name_list
%type <expr> set_expr
//%type <setRole> set_role_opt
%type <setDefaultRole> set_default_role_opt
%type <privilege> priv_elem
%type <privileges> priv_list
%type <objectType> object_type
%type <privilegeType> priv_type
%type <privilegeLevel> priv_level
%type <unresolveNames> column_name_list
%type <partitionOption> partition_by_opt
%type <clusterByOption> cluster_by_opt
%type <partitionBy> partition_method sub_partition_method sub_partition_opt
%type <windowSpec> window_spec_opt window_spec
%type <frameClause> window_frame_clause window_frame_clause_opt
%type <frameBound> frame_bound frame_bound_start
%type <frameType> frame_type
%type <str> fields_or_columns
%type <int64Val> algorithm_opt partition_num_opt sub_partition_num_opt opt_retry pitr_value
%type <boolVal> linear_opt
%type <partition> partition
%type <partitions> partition_list_opt partition_list
%type <values> values_opt
%type <tableOptions> partition_option_list
%type <subPartition> sub_partition
%type <subPartitions> sub_partition_list sub_partition_list_opt
%type <subquery> subquery
%type <incrementByOption> increment_by_opt
%type <minValueOption> min_value_opt
%type <maxValueOption> max_value_opt
%type <startWithOption> start_with_opt
%type <cycleOption> alter_cycle_opt
%type <alterTypeOption> alter_as_datatype_opt

%type <lengthOpt> length_opt length_option_opt length timestamp_option_opt
%type <lengthScaleOpt> float_length_opt decimal_length_opt
%type <unsignedOpt> unsigned_opt header_opt parallel_opt strict_opt
%type <zeroFillOpt> zero_fill_opt
%type <boolVal> global_scope exists_opt distinct_opt temporary_opt cycle_opt drop_table_opt
%type <item> pwd_expire clear_pwd_opt
%type <str> name_confict distinct_keyword separator_opt kmeans_opt
%type <insert> insert_data
%type <replace> replace_data
%type <rowsExprs> values_list
%type <str> name_datetime_scale braces_opt name_braces
%type <str> std_dev_pop extended_opt
%type <expr> expr_or_default
%type <exprs> data_values data_opt row_value

%type <boolVal> local_opt
%type <duplicateKey> duplicate_opt
%type <fields> load_fields field_item export_fields
%type <fieldsList> field_item_list
%type <str> field_terminator starting_opt lines_terminated_opt starting lines_terminated
%type <lines> load_lines export_lines_opt
%type <int64Val> ignore_lines
%type <varExpr> user_variable variable system_variable
%type <varExprs> variable_list
%type <loadColumn> columns_or_variable
%type <loadColumns> columns_or_variable_list columns_or_variable_list_opt
%type <updateExpr> load_set_item
%type <updateExprs> load_set_list load_set_spec_opt
%type <strs> index_name_and_type_opt index_name_list
%type <str> index_name index_type key_or_index_opt key_or_index insert_method_options
// type <str> mo_keywords
%type <properties> properties_list
%type <property> property_elem
%type <assignments> set_value_list
%type <assignment> set_value
%type <str> row_opt substr_option
%type <str> time_unit time_stamp_unit
%type <whenClause> when_clause
%type <whenClauseList> when_clause_list
%type <withClause> with_clause
%type <cte> common_table_expr
%type <cteList> cte_list

%type <epxlainOptions> utility_option_list
%type <epxlainOption> utility_option_elem
%type <str> utility_option_name utility_option_arg
%type <str> explain_option_key select_option_opt
%type <str> explain_foramt_value trim_direction
%type <str> priority_opt priority quick_opt ignore_opt wild_opt

%type <str> account_name account_role_name
%type <expr> account_name_or_param account_admin_name
%type <accountAuthOption> account_auth_option
%type <alterAccountAuthOption> alter_account_auth_option
%type <accountIdentified> account_identified
%type <accountStatus> account_status_option
%type <accountComment> account_comment_opt
%type <accountCommentOrAttribute> user_comment_or_attribute_opt
%type <stageComment> stage_comment_opt
%type <stageStatus> stage_status_opt
%type <stageUrl> stage_url_opt
%type <stageCredentials> stage_credentials_opt
%type <userIdentified> user_identified user_identified_opt
%type <accountRole> default_role_opt
%type <snapshotObject> snapshot_object_opt
%type <allCDCOption> all_cdc_opt

%type <indexHintType> index_hint_type
%type <indexHintScope> index_hint_scope
%type <indexHint> index_hint
%type <indexHintList> index_hint_list index_hint_list_opt

%token <str> KILL
%type <killOption> kill_opt
%token <str> BACKUP FILESYSTEM PARALLELISM RESTORE
%type <statementOption> statement_id_opt
%token <str> QUERY_RESULT
%type<tableLock> table_lock_elem
%type<tableLocks> table_lock_list
%type<tableLockType> table_lock_type

%type<transactionCharacteristic> transaction_characteristic
%type<transactionCharacteristicList> transaction_characteristic_list
%type<isolationLevel> isolation_level
%type<accessMode> access_mode

%type <timeWindow> time_window_opt time_window
%type <timeInterval> interval
%type <timeSliding> sliding_opt
%type <timeFill> fill_opt
%type <fillMode> fill_mode

%start start_command

// python udf
%type<str> func_handler func_handler_opt
%%

start_command:
    stmt_type


stmt_type:
    block_stmt
    {
        yylex.(*Lexer).AppendStmt($1)
    }
|   stmt_list

stmt_list:
    stmt
    {
        if $1 != nil {
            yylex.(*Lexer).AppendStmt($1)
        }
    }
|   stmt_list ';' stmt
    {
        if $3 != nil {
            yylex.(*Lexer).AppendStmt($3)
        }
    }

block_stmt:
    SPBEGIN stmt_list_return END
    {
        $$ = tree.NewCompoundStmt($2)
    }

stmt_list_return:
    block_type_stmt
    {
        $$ = []tree.Statement{$1}
    }
|   stmt_list_return ';' block_type_stmt
    {
        $$ = append($1, $3)
    }

block_type_stmt:
    block_stmt
|   case_stmt
|   if_stmt
|   loop_stmt
|   repeat_stmt
|   while_stmt
|   iterate_stmt
|   leave_stmt
|   normal_stmt
|   declare_stmt
    {
        $$ = $1
    }
|   /* EMPTY */
    {
        $$ = tree.Statement(nil)
    }

stmt:
    normal_stmt
    {
        $$ = $1
    }
|   declare_stmt
|   transaction_stmt
    {
        $$ = $1
    }
|   /* EMPTY */
    {
        $$ = tree.Statement(nil)
    }

normal_stmt:
    create_stmt
|   upgrade_stmt
|   call_stmt
|   mo_dump_stmt
|   insert_stmt
|   replace_stmt
|   delete_stmt
|   drop_stmt
|   truncate_table_stmt
|   explain_stmt
|   prepare_stmt
|   deallocate_stmt
|   reset_stmt
|   execute_stmt
|   show_stmt
|   alter_stmt
|   analyze_stmt
|   update_stmt
|   use_stmt
|   set_stmt
|   lock_stmt
|   revoke_stmt
|   grant_stmt
|   load_data_stmt
|   load_extension_stmt
|   do_stmt
|   values_stmt
|   select_stmt
    {
        $$ = $1
    }
|   kill_stmt
|   backup_stmt
|   snapshot_restore_stmt
|   restore_pitr_stmt
|   pause_cdc_stmt
|   resume_cdc_stmt
|   restart_cdc_stmt


backup_stmt:
    BACKUP STRING FILESYSTEM STRING PARALLELISM STRING backup_type_opt backup_timestamp_opt
	{
        var timestamp = $2
        var isS3 = false
        var dir = $4
        var parallelism = $6
        var option []string
        var backuptype = $7
        var backupts = $8
        $$ = tree.NewBackupStart(timestamp, isS3, dir, parallelism, option, backuptype, backupts)
	}
    | BACKUP STRING S3OPTION '{' infile_or_s3_params '}' backup_type_opt backup_timestamp_opt
    {
        var timestamp = $2
        var isS3 = true
        var dir string
        var parallelism string
        var option = $5
        var backuptype = $7
        var backupts = $8
        $$ = tree.NewBackupStart(timestamp, isS3, dir, parallelism, option, backuptype, backupts)
    }

backup_type_opt:
    {
        $$ = ""
    }
|    TYPE STRING
    {
        $$ = $2
    }

backup_timestamp_opt:
    {
        $$ = ""
    }
|   TIMESTAMP STRING
    {
        $$ = $2
    }

create_cdc_stmt:
    CREATE CDC not_exists_opt STRING STRING STRING STRING STRING '{' create_cdc_opts '}'
    {
        $$ = &tree.CreateCDC{
             		IfNotExists: $3,
             		TaskName:    $4,
             		SourceUri:   $5,
             		SinkType:    $6,
             		SinkUri:     $7,
             		Tables:      $8,
             		Option:      $10,
             	}
    }

create_cdc_opts:
    create_cdc_opt
    {
        $$ = $1
    }
|   create_cdc_opts ',' create_cdc_opt
    {
        $$ = append($1, $3...)
    }
create_cdc_opt:
    {
        $$ = []string{}
    }
|   STRING '=' STRING
    {
        $$ = append($$, $1)
        $$ = append($$, $3)
    }

show_cdc_stmt:
    SHOW CDC STRING all_cdc_opt
    {
        $$ = &tree.ShowCDC{
                    SourceUri:   $3,
                    Option:      $4,
        }
    }

pause_cdc_stmt:
    PAUSE CDC STRING all_cdc_opt
    {
        $$ = &tree.PauseCDC{
                    SourceUri:   $3,
                    Option:      $4,
        }
    }

drop_cdc_stmt:
    DROP CDC STRING all_cdc_opt
    {
        $$ = tree.NewDropCDC($3, $4)
    }

all_cdc_opt:
    ALL
    {
        $$ = &tree.AllOrNotCDC{
                    All: true,
                    TaskName: "",
        }
    }
|   TASK STRING
    {
        $$ = &tree.AllOrNotCDC{
            All: false,
            TaskName: $2,
        }
    }

resume_cdc_stmt:
    RESUME CDC STRING TASK STRING
    {
        $$ = &tree.ResumeCDC{
                    SourceUri:   $3,
                    TaskName:    $5,
        }
    }

restart_cdc_stmt:
    RESUME CDC STRING TASK STRING STRING
    {
        $$ = &tree.RestartCDC{
                    SourceUri:   $3,
                    TaskName:    $5,
        }
    }

create_snapshot_stmt:
    CREATE SNAPSHOT not_exists_opt ident FOR snapshot_object_opt
    {
        $$ = &tree.CreateSnapShot{
            IfNotExists: $3,
            Name: tree.Identifier($4.Compare()),
            Object: $6,
        }
    }

snapshot_object_opt:
    CLUSTER
    {
        spLevel := tree.SnapshotLevelType{
            Level: tree.SNAPSHOTLEVELCLUSTER,
        }
        $$ = tree.ObjectInfo{
            SLevel: spLevel,
            ObjName: "",
        }
    }
|   ACCOUNT ident
    {
        spLevel := tree.SnapshotLevelType{
            Level: tree.SNAPSHOTLEVELACCOUNT,
        }
        $$ = tree.ObjectInfo{
            SLevel: spLevel,
            ObjName: tree.Identifier($2.Compare()),
        }
    }

create_pitr_stmt:
    CREATE PITR not_exists_opt ident RANGE pitr_value STRING
    {
        $$ = &tree.CreatePitr{
            IfNotExists: $3,
            Name: tree.Identifier($4.Compare()),
            Level: tree.PITRLEVELACCOUNT,
            PitrValue: $6,
            PitrUnit: $7,
        }
    }
|   CREATE PITR not_exists_opt ident FOR CLUSTER RANGE pitr_value STRING
    {
       $$ = &tree.CreatePitr{
            IfNotExists: $3,
            Name: tree.Identifier($4.Compare()),
            Level: tree.PITRLEVELCLUSTER,
            PitrValue: $8,
            PitrUnit: $9,
        }
    }
|   CREATE PITR not_exists_opt ident FOR ACCOUNT ident RANGE pitr_value STRING
    {
       $$ = &tree.CreatePitr{
            IfNotExists: $3,
            Name: tree.Identifier($4.Compare()),
            Level: tree.PITRLEVELACCOUNT,
            AccountName: tree.Identifier($7.Compare()),
            PitrValue: $9,
            PitrUnit: $10,
        }
    }
|   CREATE PITR not_exists_opt ident FOR DATABASE ident RANGE pitr_value STRING
    {
        $$ = &tree.CreatePitr{
            IfNotExists: $3,
            Name: tree.Identifier($4.Compare()),
            Level: tree.PITRLEVELDATABASE,
            DatabaseName: tree.Identifier($7.Compare()),
            PitrValue: $9,
            PitrUnit: $10,
        }
    }
|   CREATE PITR not_exists_opt ident FOR DATABASE ident TABLE ident RANGE pitr_value STRING
    {
        $$ = &tree.CreatePitr{
            IfNotExists: $3,
            Name: tree.Identifier($4.Compare()),
            Level: tree.PITRLEVELTABLE,
            DatabaseName: tree.Identifier($7.Compare()),
            TableName: tree.Identifier($9.Compare()),
            PitrValue: $11,
            PitrUnit: $12,
        }
    }

pitr_value:
    INTEGRAL
    {
        $$ = $1.(int64)
    }
   
    

snapshot_restore_stmt:
    RESTORE CLUSTER FROM SNAPSHOT ident
    {
        $$ = &tree.RestoreSnapShot{
            Level: tree.RESTORELEVELCLUSTER,
            SnapShotName: tree.Identifier($5.Compare()),
        }

    }
|   RESTORE ACCOUNT ident FROM SNAPSHOT ident
    {
        $$ = &tree.RestoreSnapShot{
            Level: tree.RESTORELEVELACCOUNT,
            AccountName:tree.Identifier($3.Compare()),
            SnapShotName:tree.Identifier($6.Compare()),
        }
    }
|   RESTORE ACCOUNT ident DATABASE ident FROM SNAPSHOT ident
    {
        $$ = &tree.RestoreSnapShot{
            Level: tree.RESTORELEVELDATABASE,
            AccountName: tree.Identifier($3.Compare()),
            DatabaseName: tree.Identifier($5.Compare()),
            SnapShotName: tree.Identifier($8.Compare()),
        }
    }
|   RESTORE ACCOUNT ident DATABASE ident TABLE ident FROM SNAPSHOT ident
    {
        $$ = &tree.RestoreSnapShot{
            Level: tree.RESTORELEVELTABLE,
            AccountName: tree.Identifier($3.Compare()),
            DatabaseName: tree.Identifier($5.Compare()),
            TableName: tree.Identifier($7.Compare()),
            SnapShotName: tree.Identifier($10.Compare()),
        }
    }
|   RESTORE ACCOUNT ident FROM SNAPSHOT ident TO ACCOUNT ident
    {
        $$ = &tree.RestoreSnapShot{ 
            Level: tree.RESTORELEVELACCOUNT,
            AccountName:tree.Identifier($3.Compare()),
            SnapShotName:tree.Identifier($6.Compare()),
            ToAccountName: tree.Identifier($9.Compare()),
        }
    }
|   RESTORE ACCOUNT ident DATABASE ident FROM SNAPSHOT ident TO ACCOUNT ident
    {
        $$ = &tree.RestoreSnapShot{
            Level: tree.RESTORELEVELDATABASE,
            AccountName: tree.Identifier($3.Compare()),
            DatabaseName: tree.Identifier($5.Compare()),
            SnapShotName: tree.Identifier($8.Compare()),
            ToAccountName: tree.Identifier($11.Compare()),
        }
    }
|   RESTORE ACCOUNT ident DATABASE ident TABLE ident FROM SNAPSHOT ident TO ACCOUNT ident
    {
        $$ = &tree.RestoreSnapShot{
            Level: tree.RESTORELEVELTABLE,
            AccountName: tree.Identifier($3.Compare()),
            DatabaseName: tree.Identifier($5.Compare()),
            TableName: tree.Identifier($7.Compare()),
            SnapShotName: tree.Identifier($10.Compare()),
            ToAccountName: tree.Identifier($13.Compare()),
        }
    }

restore_pitr_stmt:
   RESTORE FROM PITR ident STRING
   {
       $$ = &tree.RestorePitr{
           Level: tree.RESTORELEVELACCOUNT,
           Name: tree.Identifier($4.Compare()),
           TimeStamp: $5,
       }
   }
|  RESTORE DATABASE ident FROM PITR ident STRING
   {
       $$ = &tree.RestorePitr{
            Level: tree.RESTORELEVELDATABASE,
            DatabaseName: tree.Identifier($3.Compare()),
            Name: tree.Identifier($6.Compare()),
            TimeStamp: $7,
       }
   }
|   RESTORE DATABASE ident TABLE ident FROM PITR ident STRING
   {
      $$ = &tree.RestorePitr{
            Level: tree.RESTORELEVELTABLE,
            DatabaseName: tree.Identifier($3.Compare()),
            TableName: tree.Identifier($5.Compare()),
            Name: tree.Identifier($8.Compare()),
            TimeStamp: $9,
       }
   }

kill_stmt:
    KILL kill_opt INTEGRAL statement_id_opt
    {
        var connectionId uint64
        switch v := $3.(type) {
        case uint64:
	    connectionId = v
        case int64:
	    connectionId = uint64(v)
        default:
	    yylex.Error("parse integral fail")
		goto ret1
        }

	$$ = &tree.Kill{
            Option: $2,
            ConnectionId: connectionId,
            StmtOption:  $4,
	}
    }

kill_opt:
{
    $$ = tree.KillOption{
        Exist: false,
    }
}
| CONNECTION
{
    $$ = tree.KillOption{
	Exist: true,
	Typ: tree.KillTypeConnection,
    }
}
| QUERY
{
    $$ = tree.KillOption{
	Exist: true,
	Typ: tree.KillTypeQuery,
    }
}

statement_id_opt:
{
    $$ = tree.StatementOption{
        Exist: false,
    }
}
| STRING
{
    $$ = tree.StatementOption{
        Exist: true,
        StatementId: $1,
    }
}

call_stmt:
    CALL proc_name '(' expression_list_opt ')'
    {
        $$ = &tree.CallStmt{
            Name: $2,
            Args: $4,
        }
    }


leave_stmt:
    LEAVE ident
    {
        $$ = &tree.LeaveStmt{
            Name: tree.Identifier($2.Compare()),
        }
    }

iterate_stmt:
    ITERATE ident
    {
        $$ = &tree.IterateStmt{
            Name: tree.Identifier($2.Compare()),
        }
    }

while_stmt:
    WHILE expression DO stmt_list_return END WHILE
    {
        $$ = &tree.WhileStmt{
            Name: "",
            Cond: $2,
            Body: $4,
        }
    }
|   ident ':' WHILE expression DO stmt_list_return END WHILE ident
    {
        $$ = &tree.WhileStmt{
            Name: tree.Identifier($1.Compare()),
            Cond: $4,
            Body: $6,
        }
    }

repeat_stmt:
    REPEAT stmt_list_return UNTIL expression END REPEAT
    {
        $$ = &tree.RepeatStmt{
            Name: "",
            Body: $2,
            Cond: $4,
        }
    }
|    ident ':' REPEAT stmt_list_return UNTIL expression END REPEAT ident
    {
        $$ = &tree.RepeatStmt{
            Name: tree.Identifier($1.Compare()),
            Body: $4,
            Cond: $6,
        }
    }

loop_stmt:
    LOOP stmt_list_return END LOOP
    {
        $$ = &tree.LoopStmt{
            Name: "",
            Body: $2,
        }
    }
|   ident ':' LOOP stmt_list_return END LOOP ident
    {
        $$ = &tree.LoopStmt{
            Name: tree.Identifier($1.Compare()),
            Body: $4,
        }
    }

if_stmt:
    IF expression THEN stmt_list_return elseif_clause_list_opt else_clause_opt2 END IF
    {
        $$ = &tree.IfStmt{
            Cond: $2,
            Body: $4,
            Elifs: $5,
            Else: $6,
        }
    }

elseif_clause_list_opt:
    {
        $$ = nil
    }
|    elseif_clause_list
    {
        $$ = $1
    }

elseif_clause_list:
    elseif_clause
    {
        $$ = []*tree.ElseIfStmt{$1}
    }
|   elseif_clause_list elseif_clause
    {
        $$ = append($1, $2)
    }

elseif_clause:
    ELSEIF expression THEN stmt_list_return
    {
        $$ = &tree.ElseIfStmt{
            Cond: $2,
            Body: $4,
        }
    }

case_stmt:
    CASE expression when_clause_list2 else_clause_opt2 END CASE
    {
        $$ = &tree.CaseStmt{
            Expr: $2,
            Whens: $3,
            Else: $4,
        }
    }

when_clause_list2:
    when_clause2
    {
        $$ = []*tree.WhenStmt{$1}
    }
|   when_clause_list2 when_clause2
    {
        $$ = append($1, $2)
    }

when_clause2:
    WHEN expression THEN stmt_list_return
    {
        $$ = &tree.WhenStmt{
            Cond: $2,
            Body: $4,
        }
    }

else_clause_opt2:
    /* empty */
    {
        $$ = nil
    }
|   ELSE stmt_list_return
    {
        $$ = $2
    }

mo_dump_stmt:
   MODUMP QUERY_RESULT STRING INTO STRING export_fields export_lines_opt header_opt max_file_size_opt force_quote_opt
    {
        ep := &tree.ExportParam{
		Outfile:    true,
		QueryId:    $3,
		FilePath :  $5,
		Fields:     $6,
		Lines:      $7,
		Header:     $8,
		MaxFileSize:uint64($9)*1024,
		ForceQuote: $10,
	}
        $$ = &tree.MoDump{
            ExportParams: ep,
        }
    }




load_data_stmt:
    LOAD DATA local_opt load_param_opt duplicate_opt INTO TABLE table_name tail_param_opt parallel_opt strict_opt
    {
        $$ = &tree.Load{
            Local: $3,
            Param: $4,
            DuplicateHandling: $5,
            Table: $8,
        }
        $$.(*tree.Load).Param.Tail = $9
        $$.(*tree.Load).Param.Parallel = $10
        $$.(*tree.Load).Param.Strict = $11
    }

load_extension_stmt:
    LOAD extension_name
    {
        $$ = &tree.LoadExtension{
            Name: tree.Identifier($2),
        }
    }

load_set_spec_opt:
    {
        $$ = nil
    }
|   SET load_set_list
    {
        $$ = $2
    }

load_set_list:
    load_set_item
    {
        $$ = tree.UpdateExprs{$1}
    }
|   load_set_list ',' load_set_item
    {
        $$ = append($1, $3)
    }

load_set_item:
    normal_ident '=' DEFAULT
    {
        $$ = &tree.UpdateExpr{
            Names: []*tree.UnresolvedName{$1},
            Expr: &tree.DefaultVal{},
        }
    }
|   normal_ident '=' expression
    {
        $$ = &tree.UpdateExpr{
            Names: []*tree.UnresolvedName{$1},
            Expr: $3,
        }
    }

parallel_opt:
    {
        $$ = false
    }
|   PARALLEL STRING
    {
        str := strings.ToLower($2)
        if str == "true" {
            $$ = true
        } else if str == "false" {
            $$ = false
        } else {
            yylex.Error("error strict flag")
            goto ret1
        }
    }
strict_opt:
    {
        $$ = false
    }
|   STRICT STRING
    {
        str := strings.ToLower($2)
        if str == "true" {
            $$ = true
        } else if str == "false" {
            $$ = false
        } else {
            yylex.Error("error parallel flag")
            goto ret1
        }
    }

normal_ident:
    ident
    {
        $$ = tree.NewUnresolvedName($1)
    }
|   ident '.' ident
    {
        tblNameCStr := yylex.(*Lexer).GetDbOrTblNameCStr($1.Origin())
        $$ = tree.NewUnresolvedName(tblNameCStr, $3)
    }
|   ident '.' ident '.' ident
    {
        dbNameCStr := yylex.(*Lexer).GetDbOrTblNameCStr($1.Origin())
        tblNameCStr := yylex.(*Lexer).GetDbOrTblNameCStr($3.Origin())
        $$ = tree.NewUnresolvedName(dbNameCStr, tblNameCStr, $5)
    }

columns_or_variable_list_opt:
    {
        $$ = nil
    }
|   '(' ')'
    {
        $$ = nil
    }
|   '(' columns_or_variable_list ')'
    {
        $$ = $2
    }

columns_or_variable_list:
    columns_or_variable
    {
        switch $1.(type) {
        case *tree.UnresolvedName:
            $$ = []tree.LoadColumn{$1.(*tree.UnresolvedName)}
        case *tree.VarExpr:
            $$ = []tree.LoadColumn{$1.(*tree.VarExpr)}
        }
    }
|   columns_or_variable_list ',' columns_or_variable
    {
        switch $3.(type) {
        case *tree.UnresolvedName:
            $$ = append($1, $3.(*tree.UnresolvedName))
        case *tree.VarExpr:
            $$ = append($1, $3.(*tree.VarExpr))
        }
    }

columns_or_variable:
    column_name_unresolved
    {
        $$ = $1
    }
|   user_variable
    {
        $$ = $1
    }

variable_list:
    variable
    {
        $$ = []*tree.VarExpr{$1}
    }
|   variable_list ',' variable
    {
        $$ = append($1, $3)
    }

variable:
    system_variable
    {
        $$ = $1
    }
|   user_variable
    {
        $$ = $1
    }

system_variable:
    AT_AT_ID
    {
        vs := strings.Split($1, ".")
        var isGlobal bool
        if strings.ToLower(vs[0]) == "global" {
            isGlobal = true
        }
        var r string
        if len(vs) == 2 {
           r = vs[1]
        } else if len(vs) == 1 {
           r = vs[0]
        } else {
            yylex.Error("variable syntax error")
            goto ret1
        }
        $$ = &tree.VarExpr{
            Name: r,
            System: true,
            Global: isGlobal,
        }
    }

user_variable:
    AT_ID
    {
//        vs := strings.Split($1, ".")
//        var r string
//        if len(vs) == 2 {
//           r = vs[1]
//        } else if len(vs) == 1 {
//           r = vs[0]
//        } else {
//            yylex.Error("variable syntax error")
//            goto ret1
//        }
        $$ = &tree.VarExpr{
            Name: $1,
            System: false,
            Global: false,
        }
    }

ignore_lines:
    {
        $$ = 0
    }
|   IGNORE INTEGRAL LINES
    {
        $$ = $2.(int64)
    }
|   IGNORE INTEGRAL ROWS
    {
        $$ = $2.(int64)
    }

load_lines:
    {
        $$ = nil
    }
|   LINES starting lines_terminated_opt
    {
        $$ = &tree.Lines{
            StartingBy: $2,
            TerminatedBy: &tree.Terminated{
                Value : $3,
            },
        }
    }
|   LINES lines_terminated starting_opt
    {
        $$ = &tree.Lines{
            StartingBy: $3,
            TerminatedBy: &tree.Terminated{
                Value : $2,
	        },
        }
    }

starting_opt:
    {
        $$ = ""
    }
|   starting

starting:
    STARTING BY STRING
    {
        $$ = $3
    }

lines_terminated_opt:
    {
        $$ = "\n"
    }
|   lines_terminated

lines_terminated:
    TERMINATED BY STRING
    {
        $$ = $3
    }

load_fields:
    {
        $$ = nil
    }
|   fields_or_columns field_item_list
    {
        res := &tree.Fields{
            Terminated: &tree.Terminated{
                Value: "\t",
            },
            EnclosedBy: &tree.EnclosedBy{
                Value: byte(0),
            },
        }
        for _, f := range $2 {
            if f.Terminated != nil {
                res.Terminated = f.Terminated
            }
            if f.Optionally {
                res.Optionally = f.Optionally
            }
            if f.EnclosedBy != nil {
                res.EnclosedBy = f.EnclosedBy
            }
            if f.EscapedBy != nil {
                res.EscapedBy = f.EscapedBy
            }
        }
        $$ = res
    }

field_item_list:
    field_item
    {
        $$ = []*tree.Fields{$1}
    }
|   field_item_list field_item
    {
        $$ = append($1, $2)
    }

field_item:
    TERMINATED BY field_terminator
    {
        $$ = &tree.Fields{
            Terminated: &tree.Terminated{
                Value: $3,
            },
        }
    }
|   OPTIONALLY ENCLOSED BY field_terminator
    {
        str := $4
        if str != "\\" && len(str) > 1 {
            yylex.Error("error field terminator")
            goto ret1
        }
        var b byte
        if len(str) != 0 {
            b = byte(str[0])
        } else {
            b = 0
        }
        $$ = &tree.Fields{
            Optionally: true,
            EnclosedBy: &tree.EnclosedBy{
                Value: b,
            },
        }
    }
|   ENCLOSED BY field_terminator
    {
        str := $3
        if str != "\\" && len(str) > 1 {
            yylex.Error("error field terminator")
            goto ret1
        }
        var b byte
        if len(str) != 0 {
           b = byte(str[0])
        } else {
           b = 0
        }
        $$ = &tree.Fields{
            EnclosedBy: &tree.EnclosedBy{
                Value: b,
            },
        }
    }
|   ESCAPED BY field_terminator
    {
        str := $3
        if str != "\\" && len(str) > 1 {
            yylex.Error("error field terminator")
            goto ret1
        }
        var b byte
        if len(str) != 0 {
           b = byte(str[0])
        } else {
           b = 0
        }
        $$ = &tree.Fields{
            EscapedBy: &tree.EscapedBy{
                Value: b,
            },
        }
    }

field_terminator:
    STRING
// |   HEXNUM
// |   BIT_LITERAL

duplicate_opt:
    {
        $$ = &tree.DuplicateKeyError{}
    }
|   IGNORE
    {
        $$ = &tree.DuplicateKeyIgnore{}
    }
|   REPLACE
    {
        $$ = &tree.DuplicateKeyReplace{}
    }

local_opt:
    {
        $$ = false
    }
|   LOCAL
    {
        $$ = true
    }

grant_stmt:
    GRANT priv_list ON object_type priv_level TO role_spec_list grant_option_opt
    {
        $$ = &tree.Grant{
            Typ: tree.GrantTypePrivilege,
            GrantPrivilege: tree.GrantPrivilege{
                Privileges: $2,
                ObjType: $4,
                Level: $5,
                Roles: $7,
                GrantOption: $8,
            },
        }
    }
|   GRANT role_spec_list TO drop_user_spec_list grant_option_opt
    {
        $$ = &tree.Grant{
            Typ: tree.GrantTypeRole,
            GrantRole:tree.GrantRole{
                Roles: $2,
                Users: $4,
                GrantOption: $5,
            },
        }
    }
|   GRANT PROXY ON user_spec TO user_spec_list grant_option_opt
    {
        $$ =  &tree.Grant{
            Typ: tree.GrantTypeProxy,
            GrantProxy:tree.GrantProxy{
                ProxyUser: $4,
                Users: $6,
                GrantOption: $7,
            },
        }

    }

grant_option_opt:
    {
        $$ = false
    }
|   WITH GRANT OPTION
    {
        $$ = true
    }
// |    WITH MAX_QUERIES_PER_HOUR INTEGRAL
// |    WITH MAX_UPDATES_PER_HOUR INTEGRAL
// |    WITH MAX_CONNECTIONS_PER_HOUR INTEGRAL
// |    WITH MAX_USER_CONNECTIONS INTEGRAL

revoke_stmt:
    REVOKE exists_opt  priv_list ON object_type priv_level FROM role_spec_list
    {
        $$ = &tree.Revoke{
            Typ: tree.RevokeTypePrivilege,
            RevokePrivilege: tree.RevokePrivilege{
                IfExists: $2,
                Privileges: $3,
                ObjType: $5,
                Level: $6,
                Roles: $8,
            },
        }
    }
|   REVOKE exists_opt role_spec_list FROM user_spec_list
    {
        $$ = &tree.Revoke{
            Typ: tree.RevokeTypeRole,
            RevokeRole: tree.RevokeRole{
                IfExists: $2,
                Roles: $3,
                Users: $5,
            },
        }
    }

priv_level:
    '*'
    {
        $$ = &tree.PrivilegeLevel{
            Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
        }
    }
|   '*' '.' '*'
    {
        $$ = &tree.PrivilegeLevel{
            Level: tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR,
        }
    }
|   ident '.' '*'
    {
        tblName := yylex.(*Lexer).GetDbOrTblName($1.Origin())
        $$ = &tree.PrivilegeLevel{
            Level: tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR,
            DbName: tblName,
        }
    }
|   ident '.' ident
    {
        dbName := yylex.(*Lexer).GetDbOrTblName($1.Origin())
        tblName := yylex.(*Lexer).GetDbOrTblName($3.Origin())
        $$ = &tree.PrivilegeLevel{
            Level: tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
            DbName: dbName,
            TabName: tblName,
        }
    }
|   ident
    {
        tblName := yylex.(*Lexer).GetDbOrTblName($1.Origin())
        $$ = &tree.PrivilegeLevel{
            Level: tree.PRIVILEGE_LEVEL_TYPE_TABLE,
            TabName: tblName,
        }
    }

object_type:
    TABLE
    {
        $$ = tree.OBJECT_TYPE_TABLE
    }
|   DATABASE
    {
        $$ = tree.OBJECT_TYPE_DATABASE
    }
|   FUNCTION
    {
        $$ = tree.OBJECT_TYPE_FUNCTION
    }
|   PROCEDURE
    {
        $$ = tree.OBJECT_TYPE_PROCEDURE
    }
|   VIEW
    {
        $$ = tree.OBJECT_TYPE_VIEW
    }
|   ACCOUNT
    {
        $$ = tree.OBJECT_TYPE_ACCOUNT
    }


priv_list:
    priv_elem
    {
        $$ = []*tree.Privilege{$1}
    }
|   priv_list ',' priv_elem
    {
        $$ = append($1, $3)
    }

priv_elem:
    priv_type
    {
        $$ = &tree.Privilege{
            Type: $1,
            ColumnList: nil,
        }
    }
|   priv_type '(' column_name_list ')'
    {
        $$ = &tree.Privilege{
            Type: $1,
            ColumnList: $3,
        }
    }

column_name_list:
    column_name
    {
        $$ = []*tree.UnresolvedName{$1}
    }
|   column_name_list ',' column_name
    {
        $$ = append($1, $3)
    }

priv_type:
    ALL
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_ALL
    }
|    CREATE ACCOUNT
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_ACCOUNT
    }
|    DROP ACCOUNT
        {
            $$ = tree.PRIVILEGE_TYPE_STATIC_DROP_ACCOUNT
        }
|    ALTER ACCOUNT
        {
                $$ = tree.PRIVILEGE_TYPE_STATIC_ALTER_ACCOUNT
        }
|    UPGRADE ACCOUNT
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_UPGRADE_ACCOUNT
	}
|    ALL PRIVILEGES
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_ALL
    }
|    ALTER TABLE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_ALTER_TABLE
    }
|    ALTER VIEW
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_ALTER_VIEW
    }
|    CREATE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_CREATE
    }
|    CREATE USER
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_USER
    }
|    DROP USER
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_DROP_USER
    }
|    ALTER USER
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_ALTER_USER
    }
|    CREATE TABLESPACE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_TABLESPACE
    }
|    TRIGGER
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_TRIGGER
    }
|    DELETE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_DELETE
    }
|    DROP TABLE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_DROP_TABLE
    }
|    DROP VIEW
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_DROP_VIEW
    }
|    EXECUTE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_EXECUTE
    }
|    INDEX
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_INDEX
    }
|    INSERT
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_INSERT
    }
|    SELECT
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_SELECT
    }
|    SUPER
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_SUPER
    }
|    CREATE DATABASE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_DATABASE
    }
|    DROP DATABASE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_DROP_DATABASE
    }
|    SHOW DATABASES
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_SHOW_DATABASES
    }
|    CONNECT
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_CONNECT
    }
|    MANAGE GRANTS
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_MANAGE_GRANTS
    }
|    OWNERSHIP
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_OWNERSHIP
    }
|    SHOW TABLES
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_SHOW_TABLES
    }
|    CREATE TABLE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_TABLE
    }
|    UPDATE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_UPDATE
    }
|    GRANT OPTION
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_GRANT_OPTION
    }
|    REFERENCES
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_REFERENCES
    }
|    REFERENCE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_REFERENCE
    }
|    REPLICATION SLAVE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_REPLICATION_SLAVE
    }
|    REPLICATION CLIENT
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_REPLICATION_CLIENT
    }
|    USAGE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_USAGE
    }
|    RELOAD
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_RELOAD
    }
|    FILE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_FILE
    }
|    CREATE TEMPORARY TABLES
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_TEMPORARY_TABLES
    }
|    LOCK TABLES
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_LOCK_TABLES
    }
|    CREATE VIEW
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_VIEW
    }
|    SHOW VIEW
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_SHOW_VIEW
    }
|    CREATE ROLE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_ROLE
    }
|    DROP ROLE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_DROP_ROLE
    }
|    ALTER ROLE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_ALTER_ROLE
    }
|      CREATE ROUTINE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_ROUTINE
    }
|    ALTER ROUTINE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_ALTER_ROUTINE
    }
|    EVENT
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_EVENT
    }
|    SHUTDOWN
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_SHUTDOWN
    }
|    TRUNCATE
    {
        $$ = tree.PRIVILEGE_TYPE_STATIC_TRUNCATE
    }

set_stmt:
    set_variable_stmt
|   set_password_stmt
|   set_role_stmt
|   set_default_role_stmt
|   set_transaction_stmt

set_transaction_stmt:
    SET TRANSACTION transaction_characteristic_list
    {
	$$ = &tree.SetTransaction{
	    Global: false,
	    CharacterList: $3,
	    }
    }
|   SET GLOBAL TRANSACTION transaction_characteristic_list
    {
        $$ = &tree.SetTransaction{
            Global: true,
            CharacterList: $4,
            }
    }
|   SET SESSION TRANSACTION transaction_characteristic_list
    {
        $$ = &tree.SetTransaction{
            Global: false,
            CharacterList: $4,
            }
    }


transaction_characteristic_list:
    transaction_characteristic
    {
	$$ = []*tree.TransactionCharacteristic{ $1 }
    }
|   transaction_characteristic_list ',' transaction_characteristic
    {
	$$ = append($1, $3)
    }

transaction_characteristic:
    ISOLATION LEVEL isolation_level
    {
	$$ = &tree.TransactionCharacteristic{
	    IsLevel: true,
	    Isolation: $3,
	}
    }
|   access_mode
    {
	$$ = &tree.TransactionCharacteristic{
	    Access: $1,
	}
    }

isolation_level:
    REPEATABLE READ
    {
	$$ = tree.ISOLATION_LEVEL_REPEATABLE_READ
    }
|   READ COMMITTED
    {
	$$ = tree.ISOLATION_LEVEL_READ_COMMITTED
    }
|   READ UNCOMMITTED
    {
	$$ = tree.ISOLATION_LEVEL_READ_UNCOMMITTED
    }
|   SERIALIZABLE
    {
	$$ = tree.ISOLATION_LEVEL_SERIALIZABLE
    }

access_mode:
   READ WRITE
   {
	$$ = tree.ACCESS_MODE_READ_WRITE
   }
| READ ONLY
   {
	$$ = tree.ACCESS_MODE_READ_ONLY
   }

set_role_stmt:
    SET ROLE role_spec
    {
        $$ = &tree.SetRole{
            SecondaryRole: false,
            Role: $3,
        }
    }
|   SET SECONDARY ROLE ALL
    {
    $$ = &tree.SetRole{
            SecondaryRole: true,
            SecondaryRoleType: tree.SecondaryRoleTypeAll,
        }
    }
|   SET SECONDARY ROLE NONE
    {
    $$ = &tree.SetRole{
            SecondaryRole: true,
            SecondaryRoleType: tree.SecondaryRoleTypeNone,
        }
    }

set_default_role_stmt:
    SET DEFAULT ROLE set_default_role_opt TO user_spec_list
    {
        dr := $4
        dr.Users = $6
        $$ = dr
    }

//set_role_opt:
//    ALL EXCEPT role_spec_list
//    {
//        $$ = &tree.SetRole{Type: tree.SET_ROLE_TYPE_ALL_EXCEPT, Roles: $3}
//    }
//|   DEFAULT
//    {
//        $$ = &tree.SetRole{Type: tree.SET_ROLE_TYPE_DEFAULT, Roles: nil}
//    }
//|   NONE
//    {
//        $$ = &tree.SetRole{Type: tree.SET_ROLE_TYPE_NONE, Roles: nil}
//    }
//|   ALL
//    {
//        $$ = &tree.SetRole{Type: tree.SET_ROLE_TYPE_ALL, Roles: nil}
//    }
//|   role_spec_list
//    {
//        $$ = &tree.SetRole{Type: tree.SET_ROLE_TYPE_NORMAL, Roles: $1}
//    }

set_default_role_opt:
    NONE
    {
        $$ = &tree.SetDefaultRole{Type: tree.SET_DEFAULT_ROLE_TYPE_NONE, Roles: nil}
    }
|   ALL
    {
        $$ = &tree.SetDefaultRole{Type: tree.SET_DEFAULT_ROLE_TYPE_ALL, Roles: nil}
    }
|   role_spec_list
    {
        $$ = &tree.SetDefaultRole{Type: tree.SET_DEFAULT_ROLE_TYPE_NORMAL, Roles: $1}
    }

set_variable_stmt:
    SET var_assignment_list
    {
        $$ = &tree.SetVar{Assignments: $2}
    }

set_password_stmt:
    SET PASSWORD '=' password_opt
    {
        $$ = &tree.SetPassword{Password: $4}
    }
|   SET PASSWORD FOR user_spec '=' password_opt
    {
        $$ = &tree.SetPassword{User: $4, Password: $6}
    }

password_opt:
    STRING
|   PASSWORD '(' auth_string ')'
    {
        $$ = $3
    }

var_assignment_list:
    var_assignment
    {
        $$ = []*tree.VarAssignmentExpr{$1}
    }
|   var_assignment_list ',' var_assignment
    {
        $$ = append($1, $3)
    }

var_assignment:
    var_name equal_or_assignment set_expr
    {
        $$ = &tree.VarAssignmentExpr{
            System: true,
            Name: $1,
            Value: $3,
        }
    }
|   GLOBAL var_name equal_or_assignment set_expr
    {
        $$ = &tree.VarAssignmentExpr{
            System: true,
            Global: true,
            Name: $2,
            Value: $4,
        }
    }
|   PERSIST var_name equal_or_assignment set_expr
    {
        $$ = &tree.VarAssignmentExpr{
            System: true,
            Global: true,
            Name: $2,
            Value: $4,
        }
    }
|   SESSION var_name equal_or_assignment set_expr
    {
        $$ = &tree.VarAssignmentExpr{
            System: true,
            Name: $2,
            Value: $4,
        }
    }
|   LOCAL var_name equal_or_assignment set_expr
    {
        $$ = &tree.VarAssignmentExpr{
            System: true,
            Name: $2,
            Value: $4,
        }
    }
|   AT_ID equal_or_assignment set_expr
    {
        vs := strings.Split($1, ".")
        var isGlobal bool
        if strings.ToLower(vs[0]) == "global" {
            isGlobal = true
        }
        var r string
        if len(vs) == 2 {
            r = vs[1]
        } else if len(vs) == 1{
            r = vs[0]
        } else {
            yylex.Error("variable syntax error")
            goto ret1
        }
        $$ = &tree.VarAssignmentExpr{
            System: false,
            Global: isGlobal,
            Name: r,
            Value: $3,
        }
    }
|   AT_AT_ID equal_or_assignment set_expr
    {
        vs := strings.Split($1, ".")
        var isGlobal bool
        if strings.ToLower(vs[0]) == "global" {
            isGlobal = true
        }
        var r string
        if len(vs) == 2 {
            r = vs[1]
        } else if len(vs) == 1{
            r = vs[0]
        } else {
            yylex.Error("variable syntax error")
            goto ret1
        }
        $$ = &tree.VarAssignmentExpr{
            System: true,
            Global: isGlobal,
            Name: r,
            Value: $3,
        }
    }
|   NAMES charset_name
    {
        $$ = &tree.VarAssignmentExpr{
            Name: strings.ToLower($1),
            Value: tree.NewNumValWithType(constant.MakeString($2), $2, false, tree.P_char),
        }
    }
|   NAMES charset_name COLLATE DEFAULT
    {
        $$ = &tree.VarAssignmentExpr{
            Name: strings.ToLower($1),
            Value: tree.NewNumValWithType(constant.MakeString($2), $2, false, tree.P_char),
        }
    }
|   NAMES charset_name COLLATE name_string
    {
        $$ = &tree.VarAssignmentExpr{
            Name: strings.ToLower($1),
            Value: tree.NewNumValWithType(constant.MakeString($2), $2, false, tree.P_char),
            Reserved: tree.NewNumValWithType(constant.MakeString($4), $4, false, tree.P_char),
        }
    }
|   NAMES DEFAULT
    {
        $$ = &tree.VarAssignmentExpr{
            Name: strings.ToLower($1),
            Value: &tree.DefaultVal{},
        }
    }
|   charset_keyword charset_name
    {
        $$ = &tree.VarAssignmentExpr{
            Name: strings.ToLower($1),
            Value: tree.NewNumValWithType(constant.MakeString($2), $2, false, tree.P_char),
        }
    }
|   charset_keyword DEFAULT
    {
        $$ = &tree.VarAssignmentExpr{
            Name: strings.ToLower($1),
            Value: &tree.DefaultVal{},
        }
    }

set_expr:
    ON
    {
        $$ = tree.NewNumValWithType(constant.MakeString($1), $1, false, tree.P_char)
    }
|   BINARY
    {
        $$ = tree.NewNumValWithType(constant.MakeString($1), $1, false, tree.P_char)
    }
|   expr_or_default
    {
        $$ = $1
    }

equal_or_assignment:
    '='
    {
        $$ = string($1)
    }
|   ASSIGNMENT
    {
        $$ = $1
    }

var_name:
    ident
    {
    	$$ = $1.Compare()
    }
|   ident '.' ident
    {
        $$ = $1.Compare() + "." + $3.Compare()
    }

var_name_list:
    var_name
    {
        $$ = []string{$1}
    }
|   var_name_list ',' var_name
    {
        $$ = append($1, $3)
    }

transaction_stmt:
    begin_stmt
|   commit_stmt
|   rollback_stmt

rollback_stmt:
    ROLLBACK completion_type
    {
        $$ = &tree.RollbackTransaction{Type: $2}
    }

commit_stmt:
    COMMIT completion_type
    {
        $$ = &tree.CommitTransaction{Type: $2}
    }

completion_type:
    {
        $$ = tree.COMPLETION_TYPE_NO_CHAIN
    }
|    WORK
    {
        $$ = tree.COMPLETION_TYPE_NO_CHAIN
    }
|   AND CHAIN NO RELEASE
    {
        $$ = tree.COMPLETION_TYPE_CHAIN
    }
|   AND CHAIN
    {
        $$ = tree.COMPLETION_TYPE_CHAIN
    }
|   AND NO CHAIN RELEASE
    {
        $$ = tree.COMPLETION_TYPE_RELEASE
    }
|   RELEASE
    {
        $$ = tree.COMPLETION_TYPE_RELEASE
    }
|   AND NO CHAIN NO RELEASE
    {
        $$ = tree.COMPLETION_TYPE_NO_CHAIN
    }
|   AND NO CHAIN
    {
        $$ = tree.COMPLETION_TYPE_NO_CHAIN
    }
|   NO RELEASE
    {
        $$ = tree.COMPLETION_TYPE_NO_CHAIN
    }

begin_stmt:
    BEGIN
    {
        $$ = &tree.BeginTransaction{}
    }
|   BEGIN WORK
    {
        $$ = &tree.BeginTransaction{}
    }
|   START TRANSACTION
    {
        $$ = &tree.BeginTransaction{}
    }
|   START TRANSACTION READ WRITE
    {
        m := tree.MakeTransactionModes(tree.READ_WRITE_MODE_READ_WRITE)
        $$ = &tree.BeginTransaction{Modes: m}
    }
|   START TRANSACTION READ ONLY
    {
        m := tree.MakeTransactionModes(tree.READ_WRITE_MODE_READ_ONLY)
        $$ = &tree.BeginTransaction{Modes: m}
    }
|   START TRANSACTION WITH CONSISTENT SNAPSHOT
    {
        $$ = &tree.BeginTransaction{}
    }

use_stmt:
    USE db_name_ident
    {
        name := $2
        secondaryRole := false
        var secondaryRoleType tree.SecondaryRoleType = 0
        var role *tree.Role
        $$ = tree.NewUse(
            name,
            secondaryRole,
            secondaryRoleType,
            role,
        )
    }
|   USE
    {
        var name *tree.CStr
        secondaryRole := false
        var secondaryRoleType tree.SecondaryRoleType = 0
        var role *tree.Role
        $$ = tree.NewUse(
            name,
            secondaryRole,
            secondaryRoleType,
            role,
        )
    }
|   USE ROLE role_spec
    {
        var name *tree.CStr
        secondaryRole := false
        var secondaryRoleType tree.SecondaryRoleType = 0
        role := $3
        $$ = tree.NewUse(
            name,
            secondaryRole,
            secondaryRoleType,
            role,
        )
    }
|   USE SECONDARY ROLE ALL
    {
        var name *tree.CStr
        secondaryRole := true
        secondaryRoleType := tree.SecondaryRoleTypeAll
        var role *tree.Role
        $$ = tree.NewUse(
            name,
            secondaryRole,
            secondaryRoleType,
            role,
        )
    }
|   USE SECONDARY ROLE NONE
    {
        var name *tree.CStr
        secondaryRole := true
        secondaryRoleType := tree.SecondaryRoleTypeNone
        var role *tree.Role
        $$ = tree.NewUse(
            name,
            secondaryRole,
            secondaryRoleType,
            role,
        )
    }

update_stmt:
    update_no_with_stmt
|    with_clause update_no_with_stmt
    {
        $2.(*tree.Update).With = $1
        $$ = $2
    }

update_no_with_stmt:
    UPDATE priority_opt ignore_opt table_reference SET update_list where_expression_opt order_by_opt limit_opt
    {
        // Single-table syntax
        $$ = &tree.Update{
            Tables: tree.TableExprs{$4},
            Exprs: $6,
            Where: $7,
            OrderBy: $8,
            Limit: $9,
        }
    }
|    UPDATE priority_opt ignore_opt table_references SET update_list where_expression_opt
    {
        // Multiple-table syntax
        $$ = &tree.Update{
            Tables: tree.TableExprs{$4},
            Exprs: $6,
            Where: $7,
        }
    }

update_list:
    update_value
    {
        $$ = tree.UpdateExprs{$1}
    }
|   update_list ',' update_value
    {
        $$ = append($1, $3)
    }

update_value:
    column_name '=' expr_or_default
    {
        $$ = &tree.UpdateExpr{Names: []*tree.UnresolvedName{$1}, Expr: $3}
    }

lock_stmt:
    lock_table_stmt
|   unlock_table_stmt

lock_table_stmt:
    LOCK TABLES table_lock_list
    {
        $$ = &tree.LockTableStmt{TableLocks:$3}
    }

table_lock_list:
    table_lock_elem
    {
       $$ = []tree.TableLock{$1}
    }
|   table_lock_list ',' table_lock_elem
    {
       $$ = append($1, $3)
    }

table_lock_elem:
    table_name table_lock_type
    {
        $$ = tree.TableLock{Table: *$1, LockType: $2}
    }

table_lock_type:
    READ
    {
        $$ = tree.TableLockRead
    }
|   READ LOCAL
    {
        $$ = tree.TableLockReadLocal
    }
|   WRITE
    {
        $$ = tree.TableLockWrite
    }
|   LOW_PRIORITY WRITE
    {
        $$ = tree.TableLockLowPriorityWrite
    }

unlock_table_stmt:
    UNLOCK TABLES
    {
       $$ = &tree.UnLockTableStmt{}
    }

prepareable_stmt:
    create_stmt
|   insert_stmt
|   delete_stmt
|   drop_stmt
|   show_stmt
|   update_stmt
|   alter_account_stmt
|   select_stmt
    {
        $$ = $1
    }

prepare_stmt:
    prepare_sym stmt_name FROM prepareable_stmt
    {
        $$ = tree.NewPrepareStmt(tree.Identifier($2), $4)
    }
|   prepare_sym stmt_name FROM STRING
    {
        $$ = tree.NewPrepareString(tree.Identifier($2), $4)
    }

execute_stmt:
    execute_sym stmt_name
    {
        $$ = tree.NewExecute(tree.Identifier($2))
    }
|   execute_sym stmt_name USING variable_list
    {
        $$ = tree.NewExecuteWithVariables(tree.Identifier($2), $4)
    }

deallocate_stmt:
    deallocate_sym PREPARE stmt_name
    {
        $$ = tree.NewDeallocate(tree.Identifier($3), false)
    }

reset_stmt:
    reset_sym PREPARE stmt_name
    {
        $$ = tree.NewReset(tree.Identifier($3))
    }

explainable_stmt:
    delete_stmt
|   load_data_stmt
|   insert_stmt
|   replace_stmt
|   update_stmt
|   select_stmt
    {
        $$ = $1
    }

explain_stmt:
    explain_sym unresolved_object_name
    {
        $$ = &tree.ShowColumns{Table: $2}
    }
|   explain_sym unresolved_object_name column_name
    {
        $$ = &tree.ShowColumns{Table: $2, ColName: $3}
    }
|   explain_sym FOR CONNECTION INTEGRAL
    {
        $$ = tree.NewExplainFor("", uint64($4.(int64)))
    }
|   explain_sym FORMAT '=' STRING FOR CONNECTION INTEGRAL
    {
        $$ = tree.NewExplainFor($4, uint64($7.(int64)))
    }
|   explain_sym explainable_stmt
    {
        $$ = tree.NewExplainStmt($2, "text")
    }
|   explain_sym VERBOSE explainable_stmt
    {
        explainStmt := tree.NewExplainStmt($3, "text")
        optionElem := tree.MakeOptionElem("verbose", "NULL")
        options := tree.MakeOptions(optionElem)
        explainStmt.Options = options
        $$ = explainStmt
    }
|   explain_sym ANALYZE explainable_stmt
    {
        explainStmt := tree.NewExplainAnalyze($3, "text")
        optionElem := tree.MakeOptionElem("analyze", "NULL")
        options := tree.MakeOptions(optionElem)
        explainStmt.Options = options
        $$ = explainStmt
    }
|   explain_sym ANALYZE VERBOSE explainable_stmt
    {
        explainStmt := tree.NewExplainAnalyze($4, "text")
        optionElem1 := tree.MakeOptionElem("analyze", "NULL")
        optionElem2 := tree.MakeOptionElem("verbose", "NULL")
        options := tree.MakeOptions(optionElem1)
        options = append(options, optionElem2)
        explainStmt.Options = options
        $$ = explainStmt
    }
|   explain_sym '(' utility_option_list ')' explainable_stmt
    {
        if tree.IsContainAnalyze($3) {
            explainStmt := tree.NewExplainAnalyze($5, "text")
            explainStmt.Options = $3
            $$ = explainStmt
        } else {
            explainStmt := tree.NewExplainStmt($5, "text")
            explainStmt.Options = $3
            $$ = explainStmt
        }
    }
|   explain_sym FORCE execute_stmt
{
    $$ = tree.NewExplainStmt($3, "text")
}
|   explain_sym VERBOSE FORCE execute_stmt
    {
        explainStmt := tree.NewExplainStmt($4, "text")
        optionElem := tree.MakeOptionElem("verbose", "NULL")
        options := tree.MakeOptions(optionElem)
        explainStmt.Options = options
        $$ = explainStmt
    }
|   explain_sym ANALYZE FORCE execute_stmt
    {
        explainStmt := tree.NewExplainAnalyze($4, "text")
        optionElem := tree.MakeOptionElem("analyze", "NULL")
        options := tree.MakeOptions(optionElem)
        explainStmt.Options = options
        $$ = explainStmt
    }
|   explain_sym ANALYZE VERBOSE FORCE execute_stmt
    {
        explainStmt := tree.NewExplainAnalyze($5, "text")
        optionElem1 := tree.MakeOptionElem("analyze", "NULL")
        optionElem2 := tree.MakeOptionElem("verbose", "NULL")
        options := tree.MakeOptions(optionElem1)
        options = append(options, optionElem2)
        explainStmt.Options = options
        $$ = explainStmt
    }

explain_option_key:
    ANALYZE
|   VERBOSE
|   FORMAT

explain_foramt_value:
    JSON
|   TEXT


prepare_sym:
    PREPARE

deallocate_sym:
    DEALLOCATE

execute_sym:
    EXECUTE

reset_sym:
    RESET

explain_sym:
    EXPLAIN
|   DESCRIBE
|   DESC

utility_option_list:
    utility_option_elem
    {
        $$ = tree.MakeOptions($1)
    }
|     utility_option_list ',' utility_option_elem
    {
        $$ = append($1, $3);
    }

utility_option_elem:
    utility_option_name utility_option_arg
    {
        $$ = tree.MakeOptionElem($1, $2)
    }

utility_option_name:
    explain_option_key
    {
         $$ = $1
    }

utility_option_arg:
    TRUE                    { $$ = "true" }
|   FALSE                        { $$ = "false" }
|   explain_foramt_value                    { $$ = $1 }


analyze_stmt:
    ANALYZE TABLE table_name '(' column_list ')'
    {
        $$ = tree.NewAnalyzeStmt($3, $5)
    }

upgrade_stmt:
    UPGRADE ACCOUNT target opt_retry
    {
        $$ = &tree.UpgradeStatement{
        	Target: $3,
        	Retry: $4,
        }
    }

target:
    STRING
    {
        $$ = &tree.Target{
        	AccountName: $1,
        	IsALLAccount: false,
        }
    }
    | ALL
    {
        $$ = &tree.Target{
        	AccountName: "",
        	IsALLAccount: true,
        }
    }

opt_retry:
    {
        $$ = -1
    }
|   WITH RETRY INTEGRAL
    {
    	res := $3.(int64)
    	if res <= 0 {
            yylex.Error("retry value can not less than 0")
            $$ = -1
        }
        $$ = res
    }


alter_stmt:
    alter_user_stmt
|   alter_account_stmt
|   alter_database_config_stmt
|   alter_view_stmt
|   alter_table_stmt
|   alter_publication_stmt
|   alter_stage_stmt
|   alter_sequence_stmt
|   alter_pitr_stmt
// |    alter_ddl_stmt

alter_sequence_stmt:
    ALTER SEQUENCE exists_opt table_name alter_as_datatype_opt increment_by_opt min_value_opt max_value_opt start_with_opt alter_cycle_opt
    {
        var ifExists = $3
        var name = $4
        var typ = $5
        var incrementBy = $6
        var minValue = $7
        var maxValue = $8
        var startWith = $9
        var cycle = $10
        $$ = tree.NewAlterSequence(
            ifExists,
            name,
            typ,
            incrementBy,
            minValue,
            maxValue,
            startWith,
            cycle,
        )
    }

alter_view_stmt:
    ALTER VIEW exists_opt table_name column_list_opt AS select_stmt
    {
        var ifExists = $3
        var name = $4
        var colNames = $5
        var asSource = $7
        $$ = tree.NewAlterView(ifExists, name, colNames, asSource)
    }

alter_table_stmt:
    ALTER TABLE table_name alter_option_list
    {
        var table = $3
        alterTable := tree.NewAlterTable(table)
        alterTable.Options = $4
        $$ = alterTable
    }
|   ALTER TABLE table_name alter_partition_option
    {
        var table = $3
        alterTable := tree.NewAlterTable(table)
        alterTable.PartitionOption = $4
        $$ = alterTable
    }

alter_option_list:
    alter_option
    {
        $$ = []tree.AlterTableOption{$1}
    }
|   alter_option_list ',' alter_option
    {
        $$ = append($1, $3)
    }

alter_partition_option:
    partition_option
    {
	    $$ = $1
    }
|   PARTITION BY partition_method partition_num_opt sub_partition_opt partition_list_opt
    {
     	$3.Num = uint64($4)
        var PartBy =     $3
	    var SubPartBy =  $5
	    var Partitions = $6

     	partitionDef := tree.NewPartitionOption(
	        PartBy,
	        SubPartBy,
	        Partitions,
        )

        var typ tree.AlterPartitionOptionType

	    opt := tree.NewAlterPartitionRedefinePartitionClause(
            typ,
	        partitionDef,
	    )

	    $$ = tree.AlterPartitionOption(opt)
    }

alter_pitr_stmt:
    ALTER PITR exists_opt ident RANGE pitr_value STRING
    {
       var ifExists = $3
       var name = tree.Identifier($4.Compare())
       var pitrValue = $6
       var pitrUnit = $7
       $$ = tree.NewAlterPitr(ifExists, name, pitrValue, pitrUnit)
    }

partition_option:
    ADD PARTITION partition_list_opt
    {
        var typ = tree.AlterPartitionAddPartition
        var partitions = $3
	    opt := tree.NewAlterPartitionAddPartitionClause(
            typ,
	        partitions,
	    )
        $$ = tree.AlterPartitionOption(opt)
    }
|    DROP PARTITION AllOrPartitionNameList
    {
        var typ = tree.AlterPartitionDropPartition
        var partitionNames = $3
        opt := tree.NewAlterPartitionDropPartitionClause(
            typ,
            partitionNames,
        )

        if $3 == nil {
            opt.OnAllPartitions = true
        } else {
            opt.PartitionNames = $3
        }
        $$ = tree.AlterPartitionOption(opt)
    }
|     TRUNCATE PARTITION AllOrPartitionNameList
    {
        var typ = tree.AlterPartitionTruncatePartition
        var partitionNames = $3
        opt := tree.NewAlterPartitionTruncatePartitionClause(
            typ,
            partitionNames,
        )

        if $3 == nil {
            opt.OnAllPartitions = true
        } else {
            opt.PartitionNames = $3
        }
        $$ = tree.AlterPartitionOption(opt)
    }

AllOrPartitionNameList:
	ALL
	{
		$$ = nil
	}
|	PartitionNameList %prec LOWER_THAN_COMMA
    {
        $$ = $1
    }

PartitionNameList:
	ident
	{
        $$ = tree.IdentifierList{tree.Identifier($1.Compare())}
	}
|	PartitionNameList ',' ident
	{
		$$ = append($1 , tree.Identifier($3.Compare()))
	}

alter_option:
    ADD table_elem_2
    {
        var def = $2
        opt := tree.NewAlterOptionAdd(def)
        $$ = tree.AlterTableOption(opt)
    }
|    MODIFY column_keyword_opt column_def column_position
    {
        var typ  = tree.AlterTableModifyColumn
        var newColumn  = $3
        var position = $4
        opt := tree.NewAlterTableModifyColumnClause(typ, newColumn, position)
	    $$ = tree.AlterTableOption(opt)
    }
|   CHANGE column_keyword_opt column_name column_def column_position
    {
        // Type OldColumnName NewColumn Position
        var typ = tree.AlterTableChangeColumn
        var oldColumnName = $3
        var newColumn = $4
        var position = $5
        opt := tree.NewAlterTableChangeColumnClause(typ, oldColumnName, newColumn, position)
    	$$ = tree.AlterTableOption(opt)
    }
|  RENAME COLUMN column_name TO column_name
    {
        var typ = tree.AlterTableRenameColumn
        var oldColumnName = $3
        var newColumnName = $5
        opt := tree.NewAlterTableRenameColumnClause(typ, oldColumnName, newColumnName)
	    $$ = tree.AlterTableOption(opt)
    }
|  ALTER column_keyword_opt column_name SET DEFAULT bit_expr
    {
        var typ = tree.AlterTableAlterColumn
        var columnName = $3
        var defaultExpr = tree.NewAttributeDefault($6)
        var visibility tree.VisibleType
        var optionType = tree.AlterColumnOptionSetDefault
        opt := tree.NewAlterTableAlterColumnClause(typ, columnName, defaultExpr, visibility, optionType)
	    $$ = tree.AlterTableOption(opt)
    }
|  ALTER column_keyword_opt column_name SET visibility
    {
        var typ = tree.AlterTableAlterColumn
        var columnName = $3
        var defaultExpr = tree.NewAttributeDefault(nil)
        var visibility = $5
        var optionType = tree.AlterColumnOptionSetVisibility
        opt := tree.NewAlterTableAlterColumnClause(typ, columnName, defaultExpr, visibility, optionType)
	    $$ = tree.AlterTableOption(opt)
    }
|  ALTER column_keyword_opt column_name DROP DEFAULT
    {
        var typ = tree.AlterTableAlterColumn
        var columnName = $3
        var defaultExpr = tree.NewAttributeDefault(nil)
        var visibility tree.VisibleType
        var optionType = tree.AlterColumnOptionDropDefault
        opt := tree.NewAlterTableAlterColumnClause(typ, columnName, defaultExpr, visibility, optionType)
	    $$ = tree.AlterTableOption(opt)
    }
|  ORDER BY alter_column_order_list %prec LOWER_THAN_ORDER
    {
        var orderByClauseType = tree.AlterTableOrderByColumn
        var orderByColumnList = $3
        opt := tree.NewAlterTableOrderByColumnClause(orderByClauseType, orderByColumnList)
	    $$ = tree.AlterTableOption(opt)
    }
|   DROP alter_table_drop
    {
        $$ = tree.AlterTableOption($2)
    }
|   ALTER alter_table_alter
    {
    	$$ = tree.AlterTableOption($2)
    }
|   table_option
    {
        $$ = tree.AlterTableOption($1)
    }
|   RENAME rename_type alter_table_rename
    {
        $$ = tree.AlterTableOption($3)
    }
|   ADD column_keyword_opt column_def column_position
    {
        var column = $3
        var position = $4
        opt := tree.NewAlterAddCol(column, position)
        $$ = tree.AlterTableOption(opt)
    }
|   ALGORITHM equal_opt algorithm_type
    {
        var checkType = $1
        var enforce bool
        $$ = tree.NewAlterOptionAlterCheck(checkType, enforce)
    }
|   default_opt charset_keyword equal_opt charset_name COLLATE equal_opt charset_name
    {
        $$ = tree.NewTableOptionCharset($4)
    }
|   CONVERT TO CHARACTER SET charset_name
    {
        $$ = tree.NewTableOptionCharset($5)
    }
|   CONVERT TO CHARACTER SET charset_name COLLATE equal_opt charset_name
    {
        $$ = tree.NewTableOptionCharset($5)
    }
|   able_type KEYS
    {
        $$ = tree.NewTableOptionCharset($1)
    }
|   space_type TABLESPACE
    {
        $$ = tree.NewTableOptionCharset($1)
    }
|   FORCE
    {
        $$ = tree.NewTableOptionCharset($1)
    }
|   LOCK equal_opt lock_type
    {
        $$ = tree.NewTableOptionCharset($1)
    }
|   with_type VALIDATION
    {
        $$ = tree.NewTableOptionCharset($1)
    }

rename_type:
    {
        $$ = ""
    }
|   TO
|   AS

algorithm_type:
    DEFAULT
|   INSTANT
|   INPLACE
|   COPY

able_type:
    DISABLE
|   ENABLE

space_type:
    DISCARD
|   IMPORT

lock_type:
    DEFAULT
|   NONE
|   SHARED
|   EXCLUSIVE

with_type:
    WITHOUT
|   WITH

column_keyword_opt:
    {
	$$ = ""
    }
|   COLUMN
    {
        $$ = string("COLUMN")
    }

column_position:
    {
        var typ = tree.ColumnPositionNone;
        var relativeColumn *tree.UnresolvedName
        $$ = tree.NewColumnPosition(typ, relativeColumn);
    }
|   FIRST
    {
        var typ = tree.ColumnPositionFirst;
        var relativeColumn *tree.UnresolvedName
        $$ = tree.NewColumnPosition(typ, relativeColumn);
    }
|   AFTER column_name
    {
        var typ = tree.ColumnPositionAfter;
        var relativeColumn = $2
        $$ = tree.NewColumnPosition(typ, relativeColumn);
    }

alter_column_order_list:
     alter_column_order
     {
	    $$ = []*tree.AlterColumnOrder{$1}
     }
|   alter_column_order_list ',' alter_column_order
    {
	    $$ = append($1, $3)
    }

alter_column_order:
    column_name asc_desc_opt
    {
        var column = $1
        var direction = $2
	    $$ = tree.NewAlterColumnOrder(column, direction)
    }

alter_table_rename:
    table_name_unresolved
    {
        var name = $1
        $$ = tree.NewAlterOptionTableName(name)
    }

alter_table_drop:
    INDEX ident
    {
        var dropType = tree.AlterTableDropIndex;
        var name = tree.Identifier($2.Compare());
        $$ = tree.NewAlterOptionDrop(dropType, name);
    }
|   KEY ident
    {
        var dropType = tree.AlterTableDropKey;
        var name = tree.Identifier($2.Compare());
        $$ = tree.NewAlterOptionDrop(dropType, name);
    }
|   ident
    {
        var dropType = tree.AlterTableDropColumn;
        var name = tree.Identifier($1.Compare());
        $$ = tree.NewAlterOptionDrop(dropType, name);
    }
|   COLUMN ident
    {
        var dropType = tree.AlterTableDropColumn;
        var name = tree.Identifier($2.Compare());
        $$ = tree.NewAlterOptionDrop(dropType, name);
    }
|   FOREIGN KEY ident
    {
        var dropType = tree.AlterTableDropForeignKey;
        var name = tree.Identifier($3.Compare());
        $$ = tree.NewAlterOptionDrop(dropType, name);

    }
|   CONSTRAINT ident
        {
            $$ = &tree.AlterOptionDrop{
                Typ:  tree.AlterTableDropForeignKey,
                Name: tree.Identifier($2.Compare()),
            }
        }
|   PRIMARY KEY
    {
        var dropType = tree.AlterTableDropPrimaryKey;
        var name = tree.Identifier("");
        $$ = tree.NewAlterOptionDrop(dropType, name);
    }

alter_table_alter:
    INDEX ident visibility
    {
        var indexName = tree.Identifier($2.Compare())
        var visibility = $3
        $$ = tree.NewAlterOptionAlterIndex(indexName, visibility)
    }
| REINDEX ident IVFFLAT LISTS equal_opt INTEGRAL
    {
    	val := int64($6.(int64))
    	if val <= 0 {
		yylex.Error("LISTS should be greater than 0")
		return 1
    	}
        var keyType = tree.INDEX_TYPE_IVFFLAT
        var algoParamList = val
        var name = tree.Identifier($2.Compare())
        $$ = tree.NewAlterOptionAlterReIndex(name, keyType, algoParamList)
    }
|   CHECK ident enforce
    {
        var checkType = $1
        var enforce = $3
        $$ = tree.NewAlterOptionAlterCheck(checkType, enforce)
    }
|   CONSTRAINT ident enforce
    {
        var checkType = $1
        var enforce = $3
        $$ = tree.NewAlterOptionAlterCheck(checkType, enforce)
    }

visibility:
    VISIBLE
    {
        $$ = tree.VISIBLE_TYPE_VISIBLE
    }
|   INVISIBLE
    {
   	    $$ = tree.VISIBLE_TYPE_INVISIBLE
    }


alter_account_stmt:
    ALTER ACCOUNT exists_opt account_name_or_param alter_account_auth_option account_status_option account_comment_opt
    {
        var ifExists = $3
        var name = $4
        var authOption = $5
        var statusOption = $6
        var comment = $7

        $$ = tree.NewAlterAccount(
            ifExists,
            name,
            authOption,
            statusOption,
            comment,
        )
    }

alter_database_config_stmt:
    ALTER DATABASE db_name SET MYSQL_COMPATIBILITY_MODE '=' STRING
    {
        var accountName = ""
        var dbName = $3
        var isAccountLevel = false
        var updateConfig = $7

        $$ = tree.NewAlterDataBaseConfig(
            accountName,
            dbName,
            isAccountLevel,
            tree.MYSQL_COMPATIBILITY_MODE,
            updateConfig,
        )
    }
|   ALTER DATABASE db_name SET UNIQUE_CHECK_ON_AUTOINCR '=' STRING
    {
        var accountName = ""
        var dbName = $3
        var isAccountLevel = false
        var updateConfig = $7

        $$ = tree.NewAlterDataBaseConfig(
            accountName,
            dbName,
            isAccountLevel,
            tree.UNIQUE_CHECK_ON_AUTOINCR,
            updateConfig,
        )
    }
|   ALTER ACCOUNT CONFIG account_name SET MYSQL_COMPATIBILITY_MODE '=' STRING
    {
        var accountName = $4
        var dbName = ""
        var isAccountLevel = true
        var updateConfig = $8

        $$ = tree.NewAlterDataBaseConfig(
            accountName,
            dbName,
            isAccountLevel,
            tree.MYSQL_COMPATIBILITY_MODE,
            updateConfig,
        )
    }
|   ALTER ACCOUNT CONFIG SET MYSQL_COMPATIBILITY_MODE  var_name equal_or_assignment set_expr
    {
        assignments := []*tree.VarAssignmentExpr{
            &tree.VarAssignmentExpr{
                System: true,
                Global: true,
                Name: $6,
                Value: $8,
            },
        }
        $$ = &tree.SetVar{Assignments: assignments}
    }

alter_account_auth_option:
{
    $$ = tree.AlterAccountAuthOption{
       Exist: false,
    }
}
| ADMIN_NAME equal_opt account_admin_name account_identified
{
    $$ = tree.AlterAccountAuthOption{
        Exist: true,
        Equal:$2,
        AdminName:$3,
        IdentifiedType:$4,
    }
}

alter_user_stmt:
    ALTER USER exists_opt user_spec_list_of_create_user default_role_opt pwd_or_lck_opt user_comment_or_attribute_opt
    {
        // Create temporary variables with meaningful names
        ifExists := $3
        users := $4
        role := $5
        miscOpt := $6
        commentOrAttribute := $7

        // Use the temporary variables to call the function
        $$ = tree.NewAlterUser(ifExists, users, role, miscOpt, commentOrAttribute)
    }

default_role_opt:
    {
        $$ = nil
    }
|   DEFAULT ROLE account_role_name
    {
        var UserName = $3
        $$ = tree.NewRole(
            UserName,
        )
    }

exists_opt:
    {
        $$ = false
    }
|   IF EXISTS
    {
        $$ = true
    }

pwd_or_lck_opt:
    {
        $$ = nil
    }
|   pwd_or_lck
    {
        $$ = $1
    }

//pwd_or_lck_list:
//    pwd_or_lck
//    {
//        $$ = []tree.UserMiscOption{$1}
//    }
//|   pwd_or_lck_list pwd_or_lck
//    {
//        $$ = append($1, $2)
//    }

pwd_or_lck:
    UNLOCK
    {
        $$ = tree.NewUserMiscOptionAccountUnlock()
    }
|   LOCK
    {
        $$ = tree.NewUserMiscOptionAccountLock()
    }
|   pwd_expire
    {
        $$ = tree.NewUserMiscOptionPasswordExpireNone()
    }
|   pwd_expire INTERVAL INTEGRAL DAY
    {
        var Value = $3.(int64)
        $$ = tree.NewUserMiscOptionPasswordExpireInterval(
            Value,
        )
    }
|   pwd_expire NEVER
    {
        $$ = tree.NewUserMiscOptionPasswordExpireNever()
    }
|   pwd_expire DEFAULT
    {
        $$ = tree.NewUserMiscOptionPasswordExpireDefault()
    }
|   PASSWORD HISTORY DEFAULT
    {
        $$ = tree.NewUserMiscOptionPasswordHistoryDefault()
    }
|   PASSWORD HISTORY INTEGRAL
    {
        var Value = $3.(int64)
        $$ = tree.NewUserMiscOptionPasswordHistoryCount(
            Value,
        )
    }
|   PASSWORD REUSE INTERVAL DEFAULT
    {
        $$ = tree.NewUserMiscOptionPasswordReuseIntervalDefault()
    }
|   PASSWORD REUSE INTERVAL INTEGRAL DAY
    {
        var Value = $4.(int64)
        $$ = tree.NewUserMiscOptionPasswordReuseIntervalCount(
            Value,
        )
    }
|   PASSWORD REQUIRE CURRENT
    {
        $$ = tree.NewUserMiscOptionPasswordRequireCurrentNone()
    }
|   PASSWORD REQUIRE CURRENT DEFAULT
    {
        $$ = tree.NewUserMiscOptionPasswordRequireCurrentDefault()
    }
|   PASSWORD REQUIRE CURRENT OPTIONAL
    {
        $$ = tree.NewUserMiscOptionPasswordRequireCurrentOptional()
    }
|   FAILED_LOGIN_ATTEMPTS INTEGRAL
    {
        var Value = $2.(int64)
        $$ = tree.NewUserMiscOptionFailedLoginAttempts(
            Value,
        )
    }
|   PASSWORD_LOCK_TIME INTEGRAL
    {
        var Value = $2.(int64)
        $$ = tree.NewUserMiscOptionPasswordLockTimeCount(
            Value,
        )
    }
|   PASSWORD_LOCK_TIME UNBOUNDED
    {
        $$ = tree.NewUserMiscOptionPasswordLockTimeUnbounded()
    }

pwd_expire:
    PASSWORD EXPIRE clear_pwd_opt
    {
        $$ = nil
    }

clear_pwd_opt:
    {
        $$ = nil
    }

auth_string:
    STRING

show_stmt:
    show_create_stmt
|   show_columns_stmt
|   show_databases_stmt
|   show_tables_stmt
|   show_sequences_stmt
|   show_process_stmt
|   show_errors_stmt
|   show_warnings_stmt
|   show_variables_stmt
|   show_status_stmt
|   show_index_stmt
|   show_target_filter_stmt
|   show_table_status_stmt
|   show_grants_stmt
|   show_roles_stmt
|   show_collation_stmt
|   show_function_status_stmt
|   show_procedure_status_stmt
|   show_node_list_stmt
|   show_locks_stmt
|   show_table_num_stmt
|   show_column_num_stmt
|   show_table_values_stmt
|   show_table_size_stmt
|   show_accounts_stmt
|   show_upgrade_stmt
|   show_publications_stmt
|   show_subscriptions_stmt
|   show_servers_stmt
|   show_stages_stmt
|   show_connectors_stmt
|   show_snapshots_stmt
|   show_pitr_stmt
|   show_cdc_stmt

show_collation_stmt:
    SHOW COLLATION like_opt where_expression_opt
    {
        $$ = &tree.ShowCollation{
            Like: $3,
            Where: $4,
        }
    }

show_stages_stmt:
    SHOW STAGES like_opt
    {
        $$ = &tree.ShowStages{
            Like: $3,
        }
    }

show_snapshots_stmt:
    SHOW SNAPSHOTS  where_expression_opt
    {
        $$ = &tree.ShowSnapShots{
            Where: $3,
        }
    }

show_pitr_stmt:
    SHOW PITR where_expression_opt
    {
        $$ = &tree.ShowPitr{
            Where: $3,
        }
    }

show_grants_stmt:
    SHOW GRANTS
    {
        $$ = &tree.ShowGrants{ShowGrantType: tree.GrantForUser}
    }
|   SHOW GRANTS FOR user_name using_roles_opt
    {
        $$ = &tree.ShowGrants{Username: $4.Username, Hostname: $4.Hostname, Roles: $5, ShowGrantType: tree.GrantForUser}
    }
|   SHOW GRANTS FOR ROLE ident
    {
        s := &tree.ShowGrants{}
        roles := []*tree.Role{
            &tree.Role{UserName: $5.Compare()},
        }
        s.Roles = roles
        s.ShowGrantType = tree.GrantForRole
        $$ = s
    }

using_roles_opt:
    {
        $$ = nil
    }
|    USING role_spec_list
    {
        $$ = $2
    }

show_table_status_stmt:
    SHOW TABLE STATUS from_or_in_opt db_name_opt like_opt where_expression_opt
    {
        $$ = &tree.ShowTableStatus{DbName: $5, Like: $6, Where: $7}
    }

from_or_in_opt:
    {}
|    from_or_in

db_name_opt:
    {}
|    db_name

show_function_status_stmt:
    SHOW FUNCTION STATUS like_opt where_expression_opt
    {
       $$ = &tree.ShowFunctionOrProcedureStatus{
            Like: $4,
            Where: $5,
            IsFunction: true,
        }
    }

show_procedure_status_stmt:
    SHOW PROCEDURE STATUS like_opt where_expression_opt
    {
        $$ = &tree.ShowFunctionOrProcedureStatus{
            Like: $4,
            Where: $5,
            IsFunction: false,
        }
    }

show_roles_stmt:
    SHOW ROLES like_opt
    {
        $$ = &tree.ShowRolesStmt{
            Like: $3,
        }
    }

show_node_list_stmt:
    SHOW NODE LIST
    {
       $$ = &tree.ShowNodeList{}
    }

show_locks_stmt:
    SHOW LOCKS
    {
       $$ = &tree.ShowLocks{}
    }

show_table_num_stmt:
    SHOW TABLE_NUMBER from_or_in_opt db_name_opt
    {
      $$ = &tree.ShowTableNumber{DbName: $4}
    }

show_column_num_stmt:
    SHOW COLUMN_NUMBER table_column_name database_name_opt
    {
       $$ = &tree.ShowColumnNumber{Table: $3, DbName: $4}
    }

show_table_values_stmt:
    SHOW TABLE_VALUES table_column_name database_name_opt
    {
       $$ = &tree.ShowTableValues{Table: $3, DbName: $4}
    }

show_table_size_stmt:
    SHOW TABLE_SIZE table_column_name database_name_opt
    {
       $$ = &tree.ShowTableSize{Table: $3, DbName: $4}
    }

show_target_filter_stmt:
    SHOW show_target like_opt where_expression_opt
    {
        s := $2.(*tree.ShowTarget)
        s.Like = $3
        s.Where = $4
        $$ = s
    }

show_target:
    CONFIG
    {
        $$ = &tree.ShowTarget{Type: tree.ShowConfig}
    }
|    charset_keyword
    {
        $$ = &tree.ShowTarget{Type: tree.ShowCharset}
    }
|    ENGINES
    {
        $$ = &tree.ShowTarget{Type: tree.ShowEngines}
    }
|    TRIGGERS from_or_in_opt db_name_opt
    {
        $$ = &tree.ShowTarget{DbName: $3, Type: tree.ShowTriggers}
    }
|    EVENTS from_or_in_opt db_name_opt
    {
        $$ = &tree.ShowTarget{DbName: $3, Type: tree.ShowEvents}
    }
|    PLUGINS
    {
        $$ = &tree.ShowTarget{Type: tree.ShowPlugins}
    }
|    PRIVILEGES
    {
        $$ = &tree.ShowTarget{Type: tree.ShowPrivileges}
    }
|    PROFILES
    {
        $$ = &tree.ShowTarget{Type: tree.ShowProfiles}
    }

show_index_stmt:
    SHOW extended_opt index_kwd table_column_name database_name_opt where_expression_opt
    {
        $$ = &tree.ShowIndex{
            TableName: $4,
            DbName: $5,
            Where: $6,
        }
    }

extended_opt:
    {}
|    EXTENDED
    {}

index_kwd:
    INDEX
|   INDEXES
|   KEYS

show_variables_stmt:
    SHOW global_scope VARIABLES like_opt where_expression_opt
    {
        $$ = &tree.ShowVariables{
            Global: $2,
            Like: $4,
            Where: $5,
        }
    }

show_status_stmt:
    SHOW global_scope STATUS like_opt where_expression_opt
    {
        $$ = &tree.ShowStatus{
            Global: $2,
            Like: $4,
            Where: $5,
        }
    }

global_scope:
    {
        $$ = false
    }
|   GLOBAL
    {
        $$ = true
    }
|   SESSION
    {
        $$ = false
    }

show_warnings_stmt:
    SHOW WARNINGS limit_opt
    {
        $$ = &tree.ShowWarnings{}
    }

show_errors_stmt:
    SHOW ERRORS limit_opt
    {
        $$ = &tree.ShowErrors{}
    }

show_process_stmt:
    SHOW full_opt PROCESSLIST
    {
        $$ = &tree.ShowProcessList{Full: $2}
    }

show_sequences_stmt:
    SHOW SEQUENCES database_name_opt where_expression_opt
    {
        $$ = &tree.ShowSequences{
           DBName: $3,
           Where: $4,
        }
    }

show_tables_stmt:
    SHOW full_opt TABLES database_name_opt like_opt where_expression_opt table_snapshot_opt
    {
        $$ = &tree.ShowTables{
            Open: false,
            Full: $2,
            DBName: $4,
            Like: $5,
            Where: $6,
            AtTsExpr: $7,
        }
    }
|   SHOW OPEN full_opt TABLES database_name_opt like_opt where_expression_opt
    {
        $$ = &tree.ShowTables{
            Open: true,
            Full: $3,
            DBName: $5,
            Like: $6,
            Where: $7,
        }
    }

show_databases_stmt:
    SHOW DATABASES like_opt where_expression_opt table_snapshot_opt
    {
        $$ = &tree.ShowDatabases{
            Like: $3, 
            Where: $4,
            AtTsExpr: $5,
        }
    }
|   SHOW SCHEMAS like_opt where_expression_opt
    {
        $$ = &tree.ShowDatabases{Like: $3, Where: $4}
    }

show_columns_stmt:
    SHOW full_opt fields_or_columns table_column_name database_name_opt like_opt where_expression_opt
    {
        $$ = &tree.ShowColumns{
            Ext: false,
            Full: $2,
            Table: $4,
            // colName: $3,
            DBName: $5,
            Like: $6,
            Where: $7,
        }
    }
|   SHOW EXTENDED full_opt fields_or_columns table_column_name database_name_opt like_opt where_expression_opt
    {
        $$ = &tree.ShowColumns{
            Ext: true,
            Full: $3,
            Table: $5,
            // colName: $3,
            DBName: $6,
            Like: $7,
            Where: $8,
        }
    }

show_accounts_stmt:
    SHOW ACCOUNTS like_opt
    {
        $$ = &tree.ShowAccounts{Like: $3}
    }

show_publications_stmt:
    SHOW PUBLICATIONS like_opt
    {
	$$ = &tree.ShowPublications{Like: $3}
    }

show_upgrade_stmt:
    SHOW UPGRADE
    {
    	$$ = &tree.ShowAccountUpgrade{}
    }


show_subscriptions_stmt:
    SHOW SUBSCRIPTIONS like_opt
    {
	$$ = &tree.ShowSubscriptions{Like: $3}
    }
|   SHOW SUBSCRIPTIONS ALL like_opt
    {
	$$ = &tree.ShowSubscriptions{All: true, Like: $4}
    }

like_opt:
    {
        $$ = nil
    }
|   LIKE simple_expr
    {
        $$ = tree.NewComparisonExpr(tree.LIKE, nil, $2)
    }
|   ILIKE simple_expr
    {
        $$ = tree.NewComparisonExpr(tree.ILIKE, nil, $2)
    }

database_name_opt:
    {
        $$ = ""
    }
|   from_or_in ident
    {
        $$ = $2.Compare()
    }

table_column_name:
    from_or_in unresolved_object_name
    {
        $$ = $2
    }



from_or_in:
    FROM
|   IN

fields_or_columns:
    FIELDS
|   COLUMNS

full_opt:
    {
        $$ = false
    }
|   FULL
    {
        $$ = true
    }

show_create_stmt:
    SHOW CREATE TABLE table_name_unresolved table_snapshot_opt
    {
        $$ = &tree.ShowCreateTable{
            Name: $4,
            AtTsExpr: $5,
        }
    }
|
    SHOW CREATE VIEW table_name_unresolved table_snapshot_opt
    {
        $$ = &tree.ShowCreateView{
            Name: $4,
            AtTsExpr: $5,
        }
    }
|   SHOW CREATE DATABASE not_exists_opt db_name
    {
        $$ = &tree.ShowCreateDatabase{IfNotExists: $4, Name: $5}
    }
|   SHOW CREATE PUBLICATION db_name
    {
	    $$ = &tree.ShowCreatePublications{Name: $4}
    }

show_servers_stmt:
    SHOW BACKEND SERVERS
    {
        $$ = &tree.ShowBackendServers{}
    }

table_name_unresolved:
    ident
    {
        tblName := yylex.(*Lexer).GetDbOrTblName($1.Origin())
        $$ = tree.NewUnresolvedObjectName(tblName)
    }
|   ident '.' ident
    {
        dbName := yylex.(*Lexer).GetDbOrTblName($1.Origin())
        tblName := yylex.(*Lexer).GetDbOrTblName($3.Origin())
        $$ = tree.NewUnresolvedObjectName(dbName, tblName)
    }

db_name:
    ident
    {
		$$ = yylex.(*Lexer).GetDbOrTblName($1.Origin())
    }

unresolved_object_name:
    ident
    {
        tblName := yylex.(*Lexer).GetDbOrTblName($1.Origin())
        $$ = tree.NewUnresolvedObjectName(tblName)
    }
|   ident '.' ident
    {
        dbName := yylex.(*Lexer).GetDbOrTblName($1.Origin())
        tblName := yylex.(*Lexer).GetDbOrTblName($3.Origin())
        $$ = tree.NewUnresolvedObjectName(dbName, tblName)
    }
|   ident '.' ident '.' ident
    {
        $$ = tree.NewUnresolvedObjectName($1.Compare(), $3.Compare(), $5.Compare())
    }

truncate_table_stmt:
    TRUNCATE table_name
    {
    	$$ = tree.NewTruncateTable($2)
    }
|   TRUNCATE TABLE table_name
    {
	    $$ = tree.NewTruncateTable($3)
    }

drop_stmt:
    drop_ddl_stmt

drop_ddl_stmt:
    drop_database_stmt
|   drop_prepare_stmt
|   drop_table_stmt
|   drop_view_stmt
|   drop_index_stmt
|   drop_role_stmt
|   drop_user_stmt
|   drop_account_stmt
|   drop_function_stmt
|   drop_sequence_stmt
|   drop_publication_stmt
|   drop_procedure_stmt
|   drop_stage_stmt
|   drop_connector_stmt
|   drop_snapshot_stmt
|   drop_pitr_stmt
|   drop_cdc_stmt

drop_sequence_stmt:
    DROP SEQUENCE exists_opt table_name_list
    {
        var ifExists = $3
        var name = $4
        $$ = tree.NewDropSequence(ifExists, name)
    }

drop_account_stmt:
    DROP ACCOUNT exists_opt account_name_or_param
    {
        var ifExists = $3
        var name = $4
        $$ = tree.NewDropAccount(ifExists, name)
    }

drop_user_stmt:
    DROP USER exists_opt drop_user_spec_list
    {
        var ifExists = $3
        var users = $4
        $$ = tree.NewDropUser(ifExists, users)
    }

drop_user_spec_list:
    drop_user_spec
    {
        $$ = []*tree.User{$1}
    }
|   drop_user_spec_list ',' drop_user_spec
    {
        $$ = append($1, $3)
    }

drop_user_spec:
    user_name
    {
        var Username = $1.Username
        var Hostname = $1.Hostname
        var AuthOption *tree.AccountIdentified
        $$ = tree.NewUser(
            Username,
            Hostname,
            AuthOption,
        )
    }

drop_role_stmt:
    DROP ROLE exists_opt role_spec_list
    {
        var ifExists = $3
        var roles = $4
        $$ = tree.NewDropRole(ifExists, roles)
    }

drop_index_stmt:
    DROP INDEX exists_opt ident ON table_name
    {
        var name = tree.Identifier($4.Compare())
        var tableName = $6
        var ifExists = $3
        $$ = tree.NewDropIndex(name, tableName, ifExists)
    }

drop_table_stmt:
    DROP TABLE temporary_opt exists_opt table_name_list drop_table_opt
    {
        var ifExists = $4
        var names = $5
        $$ = tree.NewDropTable(ifExists, names)
    }
|   DROP SOURCE exists_opt table_name_list
    {
        var ifExists = $3
        var names = $4
        $$ = tree.NewDropTable(ifExists, names)
    }

drop_connector_stmt:
    DROP CONNECTOR exists_opt table_name_list
    {
        var ifExists = $3
        var names = $4
        $$ = tree.NewDropConnector(ifExists, names)
    }

drop_view_stmt:
    DROP VIEW exists_opt table_name_list
    {
        var ifExists = $3
        var names = $4
        $$ = tree.NewDropView(ifExists, names)
    }

drop_database_stmt:
    DROP DATABASE exists_opt db_name_ident
    {
        var name = tree.Identifier($4.Compare())
        var ifExists = $3
        $$ = tree.NewDropDatabase(name, ifExists)
    }
|   DROP SCHEMA exists_opt db_name_ident
    {
        var name = tree.Identifier($4.Compare())
        var ifExists = $3
        $$ = tree.NewDropDatabase(name, ifExists)
    }

drop_prepare_stmt:
    DROP PREPARE stmt_name
    {
        $$ = tree.NewDeallocate(tree.Identifier($3), true)
    }

drop_function_stmt:
    DROP FUNCTION func_name '(' func_args_list_opt ')'
    {
        var name = $3
        var args = $5
        $$ = tree.NewDropFunction(name, args)
    }

drop_procedure_stmt:
    DROP PROCEDURE proc_name
    {
        var name = $3
        var ifExists = false
        $$ = tree.NewDropProcedure(name, ifExists)
    }
|    DROP PROCEDURE IF EXISTS proc_name
    {
        var name = $5
        var ifExists = true
        $$ = tree.NewDropProcedure(name, ifExists)
    }

delete_stmt:
    delete_without_using_stmt
|    delete_with_using_stmt
|    with_clause delete_with_using_stmt
    {
        $2.(*tree.Delete).With = $1
        $$ = $2
    }
|    with_clause delete_without_using_stmt
    {
        $2.(*tree.Delete).With = $1
        $$ = $2
    }

delete_without_using_stmt:
    DELETE priority_opt quick_opt ignore_opt FROM table_name partition_clause_opt as_opt_id where_expression_opt order_by_opt limit_opt
    {
        // Single-Table Syntax
        t := &tree.AliasedTableExpr {
            Expr: $6,
            As: tree.AliasClause{
                Alias: tree.Identifier($8),
            },
        }
        $$ = &tree.Delete{
            Tables: tree.TableExprs{t},
            Where: $9,
            OrderBy: $10,
            Limit: $11,
        }
    }
|    DELETE priority_opt quick_opt ignore_opt table_name_wild_list FROM table_references where_expression_opt
    {
        // Multiple-Table Syntax
        $$ = &tree.Delete{
            Tables: $5,
            Where: $8,
            TableRefs: tree.TableExprs{$7},
        }
    }



delete_with_using_stmt:
    DELETE priority_opt quick_opt ignore_opt FROM table_name_wild_list USING table_references where_expression_opt
    {
        // Multiple-Table Syntax
        $$ = &tree.Delete{
            Tables: $6,
            Where: $9,
            TableRefs: tree.TableExprs{$8},
        }
    }

table_name_wild_list:
    table_name_opt_wild
    {
        $$ = tree.TableExprs{$1}
    }
|    table_name_wild_list ',' table_name_opt_wild
    {
        $$ = append($1, $3)
    }

table_name_opt_wild:
    ident wild_opt
    {
        tblName := yylex.(*Lexer).GetDbOrTblName($1.Origin())
        prefix := tree.ObjectNamePrefix{ExplicitSchema: false}
        $$ = tree.NewTableName(tree.Identifier(tblName), prefix, nil)
    }
|    ident '.' ident wild_opt
    {
        dbName := yylex.(*Lexer).GetDbOrTblName($1.Origin())
        tblName := yylex.(*Lexer).GetDbOrTblName($3.Origin())
        prefix := tree.ObjectNamePrefix{SchemaName: tree.Identifier(dbName), ExplicitSchema: true}
        $$ = tree.NewTableName(tree.Identifier(tblName), prefix, nil)
    }

wild_opt:
    %prec EMPTY
    {}
|    '.' '*'
    {}

priority_opt:
    {}
|    priority

priority:
    LOW_PRIORITY
|    HIGH_PRIORITY
|    DELAYED

quick_opt:
    {}
|    QUICK

ignore_opt:
    {}
|    IGNORE

replace_stmt:
    REPLACE into_table_name partition_clause_opt replace_data
    {
    	rep := $4
    	rep.Table = $2
    	rep.PartitionNames = $3
    	$$ = rep
    }

replace_data:
    VALUES values_list
    {
        vc := tree.NewValuesClause($2)
        $$ = &tree.Replace{
            Rows: tree.NewSelect(vc, nil, nil),
        }
    }
|   select_stmt
    {
        $$ = &tree.Replace{
            Rows: $1,
        }
    }
|   '(' insert_column_list ')' VALUES values_list
    {
        vc := tree.NewValuesClause($5)
        $$ = &tree.Replace{
            Columns: $2,
            Rows: tree.NewSelect(vc, nil, nil),
        }
    }
|   '(' ')' VALUES values_list
    {
        vc := tree.NewValuesClause($4)
        $$ = &tree.Replace{
            Rows: tree.NewSelect(vc, nil, nil),
        }
    }
|   '(' insert_column_list ')' select_stmt
    {
        $$ = &tree.Replace{
            Columns: $2,
            Rows: $4,
        }
    }
|	SET set_value_list
	{
		if $2 == nil {
			yylex.Error("the set list of replace can not be empty")
			goto ret1
		}
		var identList tree.IdentifierList
		var valueList tree.Exprs
		for _, a := range $2 {
			identList = append(identList, a.Column)
			valueList = append(valueList, a.Expr)
		}
		vc := tree.NewValuesClause([]tree.Exprs{valueList})
		$$ = &tree.Replace{
			Columns: identList,
			Rows: tree.NewSelect(vc, nil, nil),
		}
	}

insert_stmt:
    INSERT into_table_name partition_clause_opt insert_data on_duplicate_key_update_opt
    {
        ins := $4
        ins.Table = $2
        ins.PartitionNames = $3
        ins.OnDuplicateUpdate = $5
        $$ = ins
    }
|   INSERT IGNORE into_table_name partition_clause_opt insert_data
    {
        ins := $5
        ins.Table = $3
        ins.PartitionNames = $4
        ins.OnDuplicateUpdate = []*tree.UpdateExpr{nil}
        $$ = ins
    }

accounts_list:
    account_name
    {
        $$ = tree.IdentifierList{tree.Identifier($1)}
    }
|   accounts_list ',' account_name
    {
        $$ = append($1, tree.Identifier($3))
    }

insert_data:
    VALUES values_list
    {
        vc := tree.NewValuesClause($2)
        $$ = &tree.Insert{
            Rows: tree.NewSelect(vc, nil, nil),
        }
    }
|   select_stmt
    {
        $$ = &tree.Insert{
            Rows: $1,
        }
    }
|   '(' insert_column_list ')' VALUES values_list
    {
        vc := tree.NewValuesClause($5)
        $$ = &tree.Insert{
            Columns: $2,
            Rows: tree.NewSelect(vc, nil, nil),
        }
    }
|   '(' ')' VALUES values_list
    {
        vc := tree.NewValuesClause($4)
        $$ = &tree.Insert{
            Rows: tree.NewSelect(vc, nil, nil),
        }
    }
|   '(' insert_column_list ')' select_stmt
    {
        $$ = &tree.Insert{
            Columns: $2,
            Rows: $4,
        }
    }
|   SET set_value_list
    {
        if $2 == nil {
            yylex.Error("the set list of insert can not be empty")
            goto ret1
        }
        var identList tree.IdentifierList
        var valueList tree.Exprs
        for _, a := range $2 {
            identList = append(identList, a.Column)
            valueList = append(valueList, a.Expr)
        }
        vc := tree.NewValuesClause([]tree.Exprs{valueList})
        $$ = &tree.Insert{
            Columns: identList,
            Rows: tree.NewSelect(vc, nil, nil),
        }
    }

on_duplicate_key_update_opt:
    {
		$$ = []*tree.UpdateExpr{}
    }
|   ON DUPLICATE KEY UPDATE update_list
    {
      	$$ = $5
    }
|   ON DUPLICATE KEY IGNORE
    {
      	$$ = []*tree.UpdateExpr{nil}
    }

set_value_list:
    {
        $$ = nil
    }
|    set_value
    {
        $$ = []*tree.Assignment{$1}
    }
|    set_value_list ',' set_value
    {
        $$ = append($1, $3)
    }

set_value:
    insert_column '=' expr_or_default
    {
        $$ = &tree.Assignment{
            Column: tree.Identifier($1),
            Expr: $3,
        }
    }

insert_column_list:
    insert_column
    {
        $$ = tree.IdentifierList{tree.Identifier($1)}
    }
|   insert_column_list ',' insert_column
    {
        $$ = append($1, tree.Identifier($3))
    }

insert_column:
    ident
    {
        $$ = yylex.(*Lexer).GetDbOrTblName($1.Origin())
    }
|   ident '.' ident
    {
        $$ = yylex.(*Lexer).GetDbOrTblName($3.Origin())
    }

values_list:
    row_value
    {
        $$ = []tree.Exprs{$1}
    }
|   values_list ',' row_value
    {
        $$ = append($1, $3)
    }

row_value:
    row_opt '(' data_opt ')'
    {
        $$ = $3
    }

row_opt:
    {}
|    ROW

data_opt:
    {
        $$ = nil
    }
|   data_values

data_values:
    expr_or_default
    {
        $$ = tree.Exprs{$1}
    }
|   data_values ',' expr_or_default
    {
        $$ = append($1, $3)
    }

expr_or_default:
    expression
|   DEFAULT
    {
        $$ = &tree.DefaultVal{}
    }

partition_clause_opt:
    {
        $$ = nil
    }
|   PARTITION '(' partition_id_list ')'
    {
        $$ = $3
    }

partition_id_list:
    ident
    {
        $$ = tree.IdentifierList{tree.Identifier($1.Compare())}
    }
|   partition_id_list ',' ident
    {
        $$ = append($1 , tree.Identifier($3.Compare()))
    }

into_table_name:
    INTO table_name
    {
        $$ = $2
    }
|   table_name
    {
        $$ = $1
    }

export_data_param_opt:
    {
        $$ = nil
    }
|   INTO OUTFILE STRING export_fields export_lines_opt header_opt max_file_size_opt force_quote_opt
    {
        $$ = &tree.ExportParam{
            Outfile:    true,
            FilePath :  $3,
            Fields:     $4,
            Lines:      $5,
            Header:     $6,
            MaxFileSize:uint64($7)*1024,
            ForceQuote: $8,
        }
    }

export_fields:
    {
        $$ = &tree.Fields{
            Terminated: &tree.Terminated{
                Value: ",",
            },
            EnclosedBy:  &tree.EnclosedBy{
                Value: '"',
            },
        }
    }
|   FIELDS TERMINATED BY STRING
    {
        $$ = &tree.Fields{
            Terminated: &tree.Terminated{
                Value: $4,
            },
            EnclosedBy:  &tree.EnclosedBy{
                Value: '"',
            },
        }
    }
|   FIELDS TERMINATED BY STRING ENCLOSED BY field_terminator
    {
        str := $7
        if str != "\\" && len(str) > 1 {
            yylex.Error("export1 error field terminator")
            goto ret1
        }
        var b byte
        if len(str) != 0 {
           b = byte(str[0])
        } else {
           b = 0
        }
        $$ = &tree.Fields{
            Terminated: &tree.Terminated{
                Value: $4,
            },
            EnclosedBy:  &tree.EnclosedBy{
                Value: b,
            },
        }
    }
|   FIELDS ENCLOSED BY field_terminator
    {
        str := $4
        if str != "\\" && len(str) > 1 {
            yylex.Error("export2 error field terminator")
            goto ret1
        }
        var b byte
        if len(str) != 0 {
           b = byte(str[0])
        } else {
           b = 0
        }
        $$ = &tree.Fields{
            Terminated: &tree.Terminated{
                Value: ",",
            },
            EnclosedBy:  &tree.EnclosedBy{
                Value: b,
            },
        }
    }

export_lines_opt:
    {
        $$ = &tree.Lines{
            TerminatedBy: &tree.Terminated{
                Value: "\n",
            },
        }
    }
|   LINES lines_terminated_opt
    {
        $$ = &tree.Lines{
            TerminatedBy: &tree.Terminated{
                Value: $2,
            },
        }
    }

header_opt:
    {
        $$ = true
    }
|   HEADER STRING
    {
        str := strings.ToLower($2)
        if str == "true" {
            $$ = true
        } else if str == "false" {
            $$ = false
        } else {
            yylex.Error("error header flag")
            goto ret1
        }
    }

max_file_size_opt:
    {
        $$ = 0
    }
|   MAX_FILE_SIZE INTEGRAL
    {
        $$ = $2.(int64)
    }

force_quote_opt:
    {
        $$ = []string{}
    }
|   FORCE_QUOTE '(' force_quote_list ')'
    {
        $$ = $3
    }


force_quote_list:
    ident
    {
        $$ = make([]string, 0, 4)
        $$ = append($$, $1.Compare())
    }
|   force_quote_list ',' ident
    {
        $$ = append($1, $3.Compare())
    }

select_stmt:
    select_no_parens
|   select_with_parens
    {
        $$ = &tree.Select{Select: $1}
    }

select_no_parens:
    simple_select time_window_opt order_by_opt limit_opt export_data_param_opt select_lock_opt
    {
        $$ = &tree.Select{Select: $1, TimeWindow: $2, OrderBy: $3, Limit: $4, Ep: $5, SelectLockInfo: $6}
    }
|   select_with_parens time_window_opt order_by_clause export_data_param_opt
    {
        $$ = &tree.Select{Select: $1, TimeWindow: $2, OrderBy: $3, Ep: $4}
    }
|   select_with_parens time_window_opt order_by_opt limit_clause export_data_param_opt
    {
        $$ = &tree.Select{Select: $1, TimeWindow: $2, OrderBy: $3, Limit: $4, Ep: $5}
    }
|   with_clause simple_select time_window_opt order_by_opt limit_opt export_data_param_opt select_lock_opt
    {
        $$ = &tree.Select{Select: $2, TimeWindow: $3, OrderBy: $4, Limit: $5, Ep: $6, SelectLockInfo:$7, With: $1}
    }
|   with_clause select_with_parens order_by_clause export_data_param_opt
    {
        $$ = &tree.Select{Select: $2, OrderBy: $3, Ep: $4, With: $1}
    }
|   with_clause select_with_parens order_by_opt limit_clause export_data_param_opt
    {
        $$ = &tree.Select{Select: $2, OrderBy: $3, Limit: $4, Ep: $5, With: $1}
    }

time_window_opt:
	{
		$$ = nil
	}
|	time_window
	{
		$$ = $1
	}

time_window:
	interval sliding_opt fill_opt
	{
		$$ = &tree.TimeWindow{
			Interval: $1,
			Sliding: $2,
			Fill: $3,
		}
	}

interval:
	INTERVAL '(' column_name ',' INTEGRAL ',' time_unit ')'
	{
		str := fmt.Sprintf("%v", $5)
		v, errStr := util.GetInt64($5)
        if errStr != "" {
           yylex.Error(errStr)
           goto ret1
        }
		$$ = &tree.Interval{
			Col: $3,
			Val: tree.NewNumValWithType(constant.MakeInt64(v), str, false, tree.P_int64),
			Unit: $7,
		}
	}

sliding_opt:
	{
		$$ = nil
	}
|	SLIDING '(' INTEGRAL ',' time_unit ')'
	{
		str := fmt.Sprintf("%v", $3)
        v, errStr := util.GetInt64($3)
        if errStr != "" {
            yylex.Error(errStr)
            goto ret1
        }
		$$ = &tree.Sliding{
        	Val: tree.NewNumValWithType(constant.MakeInt64(v), str, false, tree.P_int64),
        	Unit: $5,
        }
	}

fill_opt:
	{
		$$ = nil
	}
|	FILL '(' fill_mode ')'
	{
		$$ = &tree.Fill{
        	Mode: $3,
        }
	}
|	FILL '(' VALUE ','  expression ')'
	{
		$$ = &tree.Fill{
			Mode: tree.FillValue,
			Val: $5,
		}
	}

fill_mode:
	PREV
	{
		$$ = tree.FillPrev
	}
|	NEXT
	{
		$$ = tree.FillNext
	}
|	NONE
	{
		$$ = tree.FillNone
	}
|	NULL
	{
		$$ = tree.FillNull
	}
|	LINEAR
	{
		$$ = tree.FillLinear
	}

with_clause:
    WITH cte_list
    {
        $$ = &tree.With{
            IsRecursive: false,
            CTEs: $2,
        }
    }
|    WITH RECURSIVE cte_list
    {
        $$ = &tree.With{
            IsRecursive: true,
            CTEs: $3,
        }
    }

cte_list:
    common_table_expr
    {
        $$ = []*tree.CTE{$1}
    }
|    cte_list ',' common_table_expr
    {
        $$ = append($1, $3)
    }

common_table_expr:
    ident column_list_opt AS '(' stmt ')'
    {
        $$ = &tree.CTE{
            Name: &tree.AliasClause{Alias: tree.Identifier($1.Compare()), Cols: $2},
            Stmt: $5,
        }
    }

column_list_opt:
    {
        $$ = nil
    }
|    '(' column_list ')'
    {
        $$ = $2
    }

limit_opt:
    {
        $$ = nil
    }
|   limit_clause
    {
        $$ = $1
    }

limit_clause:
    LIMIT expression
    {
        $$ = &tree.Limit{Count: $2}
    }
|   LIMIT expression ',' expression
    {
        $$ = &tree.Limit{Offset: $2, Count: $4}
    }
|   LIMIT expression OFFSET expression
    {
        $$ = &tree.Limit{Offset: $4, Count: $2}
    }

order_by_opt:
    {
        $$ = nil
    }
|   order_by_clause
    {
        $$ = $1
    }

order_by_clause:
    ORDER BY order_list
    {
        $$ = $3
    }

order_list:
    order
    {
        $$ = tree.OrderBy{$1}
    }
|   order_list ',' order
    {
        $$ = append($1, $3)
    }

order:
    expression asc_desc_opt nulls_first_last_opt
    {
        $$ = &tree.Order{Expr: $1, Direction: $2, NullsPosition: $3}
    }

asc_desc_opt:
    {
        $$ = tree.DefaultDirection
    }
|   ASC
    {
        $$ = tree.Ascending
    }
|   DESC
    {
        $$ = tree.Descending
    }

nulls_first_last_opt:
    {
        $$ = tree.DefaultNullsPosition
    }
|   NULLS FIRST
    {
        $$ = tree.NullsFirst
    }
|   NULLS LAST
    {
        $$ = tree.NullsLast
    }

select_lock_opt:
    {
        $$ = nil
    }
|   FOR UPDATE
    {
        $$ = &tree.SelectLockInfo{
            LockType:tree.SelectLockForUpdate,
        }
    }

select_with_parens:
    '(' select_no_parens ')'
    {
        $$ = &tree.ParenSelect{Select: $2}
    }
|   '(' select_with_parens ')'
    {
        $$ = &tree.ParenSelect{Select: &tree.Select{Select: $2}}
    }
|   '(' values_stmt ')'
    {
        valuesStmt := $2.(*tree.ValuesStatement);
        $$ = &tree.ParenSelect{Select: &tree.Select {
            Select: &tree.ValuesClause {
                Rows: valuesStmt.Rows,
                RowWord: true,
            },
            OrderBy: valuesStmt.OrderBy,
            Limit:   valuesStmt.Limit,
        }}
    }

simple_select:
    simple_select_clause
    {
        $$ = $1
    }
|   simple_select union_op simple_select_clause
    {
        $$ = &tree.UnionClause{
            Type: $2.Type,
            Left: $1,
            Right: $3,
            All: $2.All,
            Distinct: $2.Distinct,
        }
    }
|   select_with_parens union_op simple_select_clause
    {
        $$ = &tree.UnionClause{
            Type: $2.Type,
            Left: $1,
            Right: $3,
            All: $2.All,
            Distinct: $2.Distinct,
        }
    }
|   simple_select union_op select_with_parens
    {
        $$ = &tree.UnionClause{
            Type: $2.Type,
            Left: $1,
            Right: $3,
            All: $2.All,
            Distinct: $2.Distinct,
        }
    }
|   select_with_parens union_op select_with_parens
    {
        $$ = &tree.UnionClause{
            Type: $2.Type,
            Left: $1,
            Right: $3,
            All: $2.All,
            Distinct: $2.Distinct,
        }
    }

union_op:
    UNION
    {
        $$ = &tree.UnionTypeRecord{
            Type: tree.UNION,
            All: false,
            Distinct: false,
        }
    }
|   UNION ALL
    {
        $$ = &tree.UnionTypeRecord{
            Type: tree.UNION,
            All: true,
            Distinct: false,
        }
    }
|   UNION DISTINCT
    {
        $$ = &tree.UnionTypeRecord{
            Type: tree.UNION,
            All: false,
            Distinct: true,
        }
    }
|
    EXCEPT
    {
        $$ = &tree.UnionTypeRecord{
            Type: tree.EXCEPT,
            All: false,
            Distinct: false,
        }
    }
|   EXCEPT ALL
    {
        $$ = &tree.UnionTypeRecord{
            Type: tree.EXCEPT,
            All: true,
            Distinct: false,
        }
    }
|   EXCEPT DISTINCT
    {
        $$ = &tree.UnionTypeRecord{
            Type: tree.EXCEPT,
            All: false,
            Distinct: true,
        }
    }
|    INTERSECT
    {
        $$ = &tree.UnionTypeRecord{
            Type: tree.INTERSECT,
            All: false,
            Distinct: false,
        }
    }
|   INTERSECT ALL
    {
        $$ = &tree.UnionTypeRecord{
            Type: tree.INTERSECT,
            All: true,
            Distinct: false,
        }
    }
|   INTERSECT DISTINCT
    {
        $$ = &tree.UnionTypeRecord{
            Type: tree.INTERSECT,
            All: false,
            Distinct: true,
        }
    }
|    MINUS
    {
        $$ = &tree.UnionTypeRecord{
            Type: tree.UT_MINUS,
            All: false,
            Distinct: false,
        }
    }
|   MINUS ALL
    {
        $$ = &tree.UnionTypeRecord{
            Type: tree.UT_MINUS,
            All: true,
            Distinct: false,
        }
    }
|    MINUS DISTINCT
    {
        $$ = &tree.UnionTypeRecord{
            Type: tree.UT_MINUS,
            All: false,
            Distinct: true,
        }
    }

simple_select_clause:
    SELECT distinct_opt select_expression_list from_opt where_expression_opt group_by_opt having_opt
    {
        $$ = &tree.SelectClause{
            Distinct: $2,
            Exprs: $3,
            From: $4,
            Where: $5,
            GroupBy: $6,
            Having: $7,
        }
    }
|    SELECT select_option_opt select_expression_list from_opt where_expression_opt group_by_opt having_opt
    {
        $$ = &tree.SelectClause{
            Distinct: false,
            Exprs: $3,
            From: $4,
            Where: $5,
            GroupBy: $6,
            Having: $7,
            Option: $2,
        }
    }

select_option_opt:
    SQL_SMALL_RESULT
    {
    	$$ = strings.ToLower($1)
    }
|	SQL_BIG_RESULT
	{
       $$ = strings.ToLower($1)
    }
|   SQL_BUFFER_RESULT
	{
    	$$ = strings.ToLower($1)
    }

distinct_opt:
    {
        $$ = false
    }
|   ALL
    {
        $$ = false
    }
|   distinct_keyword
    {
        $$ = true
    }

distinct_keyword:
    DISTINCT
|   DISTINCTROW

having_opt:
    {
        $$ = nil
    }
|   HAVING expression
    {
        $$ = &tree.Where{Type: tree.AstHaving, Expr: $2}
    }

group_by_opt:
    {
        $$ = nil
    }
|   GROUP BY expression_list
    {
        $$ = tree.GroupBy($3)
    }

where_expression_opt:
    {
        $$ = nil
    }
|   WHERE expression
    {
        $$ = &tree.Where{Type: tree.AstWhere, Expr: $2}
    }

select_expression_list:
    select_expression
    {
        $$ = tree.SelectExprs{$1}
    }
|   select_expression_list ',' select_expression
    {
        $$ = append($1, $3)
    }

select_expression:
    '*' %prec '*'
    {
        $$ = tree.SelectExpr{Expr: tree.StarExpr()}
    }
|   expression as_name_opt
    {
        $$ = tree.SelectExpr{Expr: $1, As: $2}
    }
|   ident '.' '*' %prec '*'
    {
        $$ = tree.SelectExpr{Expr: tree.NewUnresolvedNameWithStar($1)}
    }
|   ident '.' ident '.' '*' %prec '*'
    {
        $$ = tree.SelectExpr{Expr: tree.NewUnresolvedNameWithStar($1, $3)}
    }

from_opt:
    {
        prefix := tree.ObjectNamePrefix{ExplicitSchema: false}
        tn := tree.NewTableName(tree.Identifier(""), prefix, nil)
        $$ = &tree.From{
            Tables: tree.TableExprs{&tree.AliasedTableExpr{Expr: tn}},
        }
    }
|   from_clause
    {
        $$ = $1
    }

from_clause:
    FROM table_references
    {
        $$ = &tree.From{
            Tables: tree.TableExprs{$2},
        }
    }

table_references:
    escaped_table_reference
   	{
   		if t, ok := $1.(*tree.JoinTableExpr); ok {
   			$$ = t
   		} else {
   			$$ = &tree.JoinTableExpr{Left: $1, Right: nil, JoinType: tree.JOIN_TYPE_CROSS}
   		}
    }
|   table_references ',' escaped_table_reference
    {
        $$ = &tree.JoinTableExpr{Left: $1, Right: $3, JoinType: tree.JOIN_TYPE_CROSS}
    }

escaped_table_reference:
    table_reference %prec LOWER_THAN_SET

table_reference:
    table_factor
|   join_table
	{
		$$ = $1
	}

join_table:
    table_reference inner_join table_factor join_condition_opt
    {
        $$ = &tree.JoinTableExpr{
            Left: $1,
            JoinType: $2,
            Right: $3,
            Cond: $4,
        }
    }
|   table_reference straight_join table_factor on_expression_opt
    {
        $$ = &tree.JoinTableExpr{
            Left: $1,
            JoinType: $2,
            Right: $3,
            Cond: $4,
        }
    }
|   table_reference outer_join table_factor join_condition
    {
        $$ = &tree.JoinTableExpr{
            Left: $1,
            JoinType: $2,
            Right: $3,
            Cond: $4,
        }
    }
|   table_reference natural_join table_factor
    {
        $$ = &tree.JoinTableExpr{
            Left: $1,
            JoinType: $2,
            Right: $3,
        }
    }

natural_join:
    NATURAL JOIN
    {
        $$ = tree.JOIN_TYPE_NATURAL
    }
|   NATURAL outer_join
    {
        if $2 == tree.JOIN_TYPE_LEFT {
            $$ = tree.JOIN_TYPE_NATURAL_LEFT
        } else {
            $$ = tree.JOIN_TYPE_NATURAL_RIGHT
        }
    }

outer_join:
    LEFT JOIN
    {
        $$ = tree.JOIN_TYPE_LEFT
    }
|   LEFT OUTER JOIN
    {
        $$ = tree.JOIN_TYPE_LEFT
    }
|   RIGHT JOIN
    {
        $$ = tree.JOIN_TYPE_RIGHT
    }
|   RIGHT OUTER JOIN
    {
        $$ = tree.JOIN_TYPE_RIGHT
    }

values_stmt:
    VALUES row_constructor_list order_by_opt limit_opt
    {
        $$ = &tree.ValuesStatement{
            Rows: $2,
            OrderBy: $3,
            Limit: $4,
        }
    }

row_constructor_list:
    row_constructor
    {
        $$ = []tree.Exprs{$1}
    }
|   row_constructor_list ',' row_constructor
    {
        $$ = append($1, $3)
    }

row_constructor:
    ROW '(' data_values ')'
    {
        $$ = $3
    }

on_expression_opt:
    %prec JOIN
    {
        $$ = nil
    }
|   ON expression
    {
        $$ = &tree.OnJoinCond{Expr: $2}
    }

straight_join:
    STRAIGHT_JOIN
    {
        $$ = tree.JOIN_TYPE_STRAIGHT
    }

inner_join:
    JOIN
    {
        $$ = tree.JOIN_TYPE_INNER
    }
|   INNER JOIN
    {
        $$ = tree.JOIN_TYPE_INNER
    }
|   CROSS JOIN
    {
        $$ = tree.JOIN_TYPE_CROSS
    }
|   CROSS_L2 JOIN
    {
        $$ = tree.JOIN_TYPE_CROSS_L2
    }

join_condition_opt:
    %prec JOIN
    {
        $$ = nil
    }
|   join_condition
    {
        $$ = $1
    }

join_condition:
    ON expression
    {
        $$ = &tree.OnJoinCond{Expr: $2}
    }
|   USING '(' column_list ')'
    {
        $$ = &tree.UsingJoinCond{Cols: $3}
    }

column_list:
    ident
    {
        $$ = tree.IdentifierList{tree.Identifier($1.Compare())}
    }
|   column_list ',' ident
    {
        $$ = append($1, tree.Identifier($3.Compare()))
    }

table_factor:
    aliased_table_name
    {
        $$ = $1
    }
|   table_subquery as_opt_id column_list_opt
    {
        $$ = &tree.AliasedTableExpr{
            Expr: $1,
            As: tree.AliasClause{
                Alias: tree.Identifier($2),
                Cols: $3,
            },
        }
    }
|   table_function as_opt_id
    {
        if $2 != "" {
            $$ = &tree.AliasedTableExpr{
                Expr: $1,
                As: tree.AliasClause{
                    Alias: tree.Identifier($2),
                },
            }
        } else {
            $$ = $1
        }
    }
|   '(' table_references ')'
	{
		$$ = $2
	}

table_subquery:
    select_with_parens %prec SUBQUERY_AS_EXPR
    {
    	$$ = &tree.ParenTableExpr{Expr: $1.(*tree.ParenSelect).Select}
    }

table_function:
    ident '(' expression_list_opt ')'
    {
        name := tree.NewUnresolvedName($1)
        $$ = &tree.TableFunction{
       	        Func: &tree.FuncExpr{
        	        Func: tree.FuncName2ResolvableFunctionReference(name),
                    FuncName: $1,
        	        Exprs: $3,
        	        Type: tree.FUNC_TYPE_TABLE,
                },
        }
    }

aliased_table_name:
    table_name as_opt_id index_hint_list_opt
    {
        $$ = &tree.AliasedTableExpr{
            Expr: $1,
            As: tree.AliasClause{
                Alias: tree.Identifier($2),
            },
            IndexHints: $3,
        }
    }

index_hint_list_opt:
	{
		$$ = nil
	}
|	index_hint_list

index_hint_list:
	index_hint
	{
		$$ = []*tree.IndexHint{$1}
	}
|	index_hint_list index_hint
	{
		$$ = append($1, $2)
	}

index_hint:
	index_hint_type index_hint_scope '(' index_name_list ')'
	{
		$$ = &tree.IndexHint{
			IndexNames: $4,
			HintType: $1,
			HintScope: $2,
		}
	}

index_hint_type:
	USE key_or_index
	{
		$$ = tree.HintUse
	}
|	IGNORE key_or_index
	{
		$$ = tree.HintIgnore
	}
|	FORCE key_or_index
	{
		$$ = tree.HintForce
	}

index_hint_scope:
	{
		$$ = tree.HintForScan
	}
|	FOR JOIN
	{
		$$ = tree.HintForJoin
	}
|	FOR ORDER BY
	{
		$$ = tree.HintForOrderBy
	}
|	FOR GROUP BY
	{
		$$ = tree.HintForGroupBy
	}

index_name_list:
	{
		$$ = nil
	}
|	ident
	{
		$$ = []string{$1.Compare()}
	}
|	index_name_list ',' ident
	{
		$$ = append($1, $3.Compare())
	}
|	PRIMARY
	{
		$$ = []string{$1}
	}
|	index_name_list ',' PRIMARY
	{
		$$ = append($1, $3)
	}

as_opt_id:
    {
        $$ = ""
    }
|   table_alias
    {
        $$ = $1
    }
|   AS table_alias
    {
        $$ = $2
    }

table_alias:
    ident
    {
	    $$ = yylex.(*Lexer).GetDbOrTblName($1.Origin())
    }
|   STRING
    {
	    $$ = yylex.(*Lexer).GetDbOrTblName($1)
    }

as_name_opt:
    {
        $$ = tree.NewCStr("", 1)
    }
|   ident
    {
        $$ = $1
    }
|   AS ident
    {
        $$ = $2
    }
|   STRING
    {
        $$ = tree.NewCStr($1, 1)
    }
|   AS STRING
    {
        $$ = tree.NewCStr($2, 1)
    }

stmt_name:
    ident
    {
    	$$ = $1.Compare()
    }

//table_id:
//    id_or_var
//|   non_reserved_keyword

//id_or_var:
//    ID
//|	QUOTE_ID
//|   AT_ID
//|   AT_AT_ID

create_stmt:
    create_ddl_stmt
|   create_role_stmt
|   create_user_stmt
|   create_account_stmt
|   create_publication_stmt
|   create_stage_stmt
|   create_snapshot_stmt
|   create_pitr_stmt
|   create_cdc_stmt

create_ddl_stmt:
    create_table_stmt
|   create_database_stmt
|   create_index_stmt
|   create_view_stmt
|   create_function_stmt
|   create_extension_stmt
|   create_sequence_stmt
|   create_procedure_stmt
|   create_source_stmt
|   create_connector_stmt
|   pause_daemon_task_stmt
|   cancel_daemon_task_stmt
|   resume_daemon_task_stmt

create_extension_stmt:
    CREATE EXTENSION extension_lang AS extension_name FILE STRING
    {
        var Language = $3
        var Name = tree.Identifier($5)
        var Filename = tree.Identifier($7)
        $$ = tree.NewCreateExtension(
            Language,
            Name,
            Filename,
        )
    }

extension_lang:
    ident
    {
        $$ = $1.Compare()
    }

extension_name:
    ident
    {
        $$ = $1.Compare()
    }

create_procedure_stmt:
    CREATE PROCEDURE proc_name '(' proc_args_list_opt ')' STRING
    {
        var Name = $3
        var Args = $5
        var Body = $7
        $$ = tree.NewCreateProcedure(
            Name,
            Args,
            Body,
        )
    }

proc_name:
    ident
    {
        prefix := tree.ObjectNamePrefix{ExplicitSchema: false}
        $$ = tree.NewProcedureName(tree.Identifier($1.Compare()), prefix)
    }
|   ident '.' ident
    {
        dbName := yylex.(*Lexer).GetDbOrTblName($1.Origin())
        prefix := tree.ObjectNamePrefix{SchemaName: tree.Identifier(dbName), ExplicitSchema: true}
        $$ = tree.NewProcedureName(tree.Identifier($3.Compare()), prefix)
    }

proc_args_list_opt:
    {
        $$ = tree.ProcedureArgs(nil)
    }
|   proc_args_list

proc_args_list:
    proc_arg
    {
        $$ = tree.ProcedureArgs{$1}
    }
|   proc_args_list ',' proc_arg
    {
        $$ = append($1, $3)
    }

proc_arg:
    proc_arg_decl
    {
        $$ = tree.ProcedureArg($1)
    }

proc_arg_decl:
    proc_arg_in_out_type column_name column_type
    {
        $$ = tree.NewProcedureArgDecl($1, $2, $3)
    }

proc_arg_in_out_type:
    {
        $$ = tree.TYPE_IN
    }
|   IN
    {
        $$ = tree.TYPE_IN
    }
|   OUT
    {
        $$ = tree.TYPE_OUT
    }
|   INOUT
    {
        $$ = tree.TYPE_INOUT
    }


create_function_stmt:
    CREATE replace_opt FUNCTION func_name '(' func_args_list_opt ')' RETURNS func_return LANGUAGE func_lang func_body_import STRING func_handler_opt
    {
    	if $13 == "" {
            yylex.Error("no function body error")
            goto ret1
        }
        if $11 == "python" && $14 == "" {
            yylex.Error("no handler error")
            goto ret1
        }

        var Replace = $2
        var Name = $4
        var Args = $6
        var ReturnType = $9
        var Language = $11
        var Import = $12
        var Body = $13
        var Handler = $14

        $$ = tree.NewCreateFunction(
            Replace,
            Name,
            Args,
            ReturnType,
            Language,
            Import,
            Body,
            Handler,
        )
    }

func_name:
    ident
    {
        prefix := tree.ObjectNamePrefix{ExplicitSchema: false}
        $$ = tree.NewFuncName(tree.Identifier($1.Compare()), prefix)
    }
|   ident '.' ident
    {
        dbName := yylex.(*Lexer).GetDbOrTblName($1.Origin())
        prefix := tree.ObjectNamePrefix{SchemaName: tree.Identifier(dbName), ExplicitSchema: true}
        $$ = tree.NewFuncName(tree.Identifier($3.Compare()), prefix)
    }

func_args_list_opt:
    {
        $$ = tree.FunctionArgs(nil)
    }
|   func_args_list

func_args_list:
    func_arg
    {
        $$ = tree.FunctionArgs{$1}
    }
|   func_args_list ',' func_arg
    {
        $$ = append($1, $3)
    }

func_arg:
    func_arg_decl
    {
        $$ = tree.FunctionArg($1)
    }

func_arg_decl:
    column_type
    {
        $$ = tree.NewFunctionArgDecl(nil, $1, nil)
    }
|   column_name column_type
    {
        $$ = tree.NewFunctionArgDecl($1, $2, nil)
    }
|   column_name column_type DEFAULT literal
    {
        $$ = tree.NewFunctionArgDecl($1, $2, $4)
    }

func_lang:
    ident
    {
        $$ = $1.Compare()
    }

func_return:
    column_type
    {
        $$ = tree.NewReturnType($1)
    }

func_body_import:
    AS
    {
    	$$ = false
    }
|   IMPORT
    {
    	$$ = true
    }


func_handler_opt:
    {
    	$$ = ""
    }
|   func_handler

func_handler:
    HANDLER STRING
    {
    	$$ = $2
    }

create_view_stmt:
    CREATE view_list_opt VIEW not_exists_opt table_name column_list_opt AS select_stmt view_tail
    {
        var Replace bool
        var Name = $5
        var ColNames = $6
        var AsSource = $8
        var IfNotExists = $4
        $$ = tree.NewCreateView(
            Replace,
            Name,
            ColNames,
            AsSource,
            IfNotExists,
        )
    }
|   CREATE replace_opt VIEW not_exists_opt table_name column_list_opt AS select_stmt view_tail
    {
        var Replace = $2
        var Name = $5
        var ColNames = $6
        var AsSource = $8
        var IfNotExists = $4
        $$ = tree.NewCreateView(
            Replace,
            Name,
            ColNames,
            AsSource,
            IfNotExists,
        )
    }

create_account_stmt:
    CREATE ACCOUNT not_exists_opt account_name_or_param account_auth_option account_status_option account_comment_opt
    {
        var IfNotExists = $3
        var Name = $4
        var AuthOption = $5
        var StatusOption = $6
        var Comment = $7
   		$$ = tree.NewCreateAccount(
        	IfNotExists,
            Name,
            AuthOption,
            StatusOption,
            Comment,
    	)
    }

view_list_opt:
    view_opt
    {
        $$ = $1
    }
|   view_list_opt view_opt
    {
        $$ = $$ + $2
    }

view_opt:
    ALGORITHM '=' algorithm_type_2
    {
        $$ = "ALGORITHM = " + $3
    }
|   DEFINER '=' user_name
    {
        $$ = "DEFINER = "
    }
|   SQL SECURITY security_opt
    {
        $$ = "SQL SECURITY " + $3
    }

view_tail:
    {
        $$ = ""
    }
|   WITH check_type CHECK OPTION
    {
        $$ = "WITH " + $2 + " CHECK OPTION"
    }

algorithm_type_2:
    UNDEFINED
|   MERGE
|   TEMPTABLE

security_opt:
    DEFINER
|   INVOKER

check_type:
    {
        $$ = ""
    }
|   CASCADED
|   LOCAL

account_name:
    ident
    {
    	$$ = $1.Compare()
    }

account_name_or_param:
    ident
    {
        var Str = $1.Compare()
        $$ = tree.NewNumValWithType(constant.MakeString(Str), Str, false, tree.P_char)
    }
|   VALUE_ARG
    {
        $$ = tree.NewParamExpr(yylex.(*Lexer).GetParamIndex())
    }

account_auth_option:
    ADMIN_NAME equal_opt account_admin_name account_identified
    {
        var Equal = $2
        var AdminName = $3
        var IdentifiedType = $4
        $$ = *tree.NewAccountAuthOption(
            Equal,
            AdminName,
            IdentifiedType,
        )
    }

account_admin_name:
    STRING
    {
        var Str = $1
        $$ = tree.NewNumValWithType(constant.MakeString(Str), Str, false, tree.P_char)
    }
|	ident
	{
		var Str = $1.Compare()
        $$ = tree.NewNumValWithType(constant.MakeString(Str), Str, false, tree.P_char)
	}
|   VALUE_ARG
    {
        $$ = tree.NewParamExpr(yylex.(*Lexer).GetParamIndex())
    }

account_identified:
    IDENTIFIED BY STRING
    {
        $$ = *tree.NewAccountIdentified(
            tree.AccountIdentifiedByPassword,
            tree.NewNumValWithType(constant.MakeString($3), $3, false, tree.P_char),
        )
    }
|   IDENTIFIED BY VALUE_ARG
    {
        $$ = *tree.NewAccountIdentified(
            tree.AccountIdentifiedByPassword,
            tree.NewParamExpr(yylex.(*Lexer).GetParamIndex()),
        )
    }
|   IDENTIFIED BY RANDOM PASSWORD
    {
        $$ = *tree.NewAccountIdentified(
            tree.AccountIdentifiedByRandomPassword,
            nil,
        )
    }
|   IDENTIFIED WITH STRING
    {
        $$ = *tree.NewAccountIdentified(
            tree.AccountIdentifiedWithSSL,
            tree.NewNumValWithType(constant.MakeString($3), $3, false, tree.P_char),
        )
    }
|   IDENTIFIED WITH VALUE_ARG
    {
        $$ = *tree.NewAccountIdentified(
            tree.AccountIdentifiedWithSSL,
            tree.NewParamExpr(yylex.(*Lexer).GetParamIndex()),
        )
    }

account_status_option:
    {
        as := tree.NewAccountStatus()
        as.Exist = false
        $$ = *as
    }
|   OPEN
    {
        as := tree.NewAccountStatus()
        as.Exist = true
        as.Option = tree.AccountStatusOpen
        $$ = *as
    }
|   SUSPEND
    {
        as := tree.NewAccountStatus()
        as.Exist = true
        as.Option = tree.AccountStatusSuspend
        $$ = *as
    }
|   RESTRICTED
    {
        as := tree.NewAccountStatus()
        as.Exist = true
        as.Option = tree.AccountStatusRestricted
        $$ = *as
    }

account_comment_opt:
    {
        ac := tree.NewAccountComment()
        ac.Exist = false
        $$ = *ac
    }
|   COMMENT_KEYWORD STRING
    {
        ac := tree.NewAccountComment()
        ac.Exist = true
        ac.Comment = $2
        $$ = *ac
    }

create_user_stmt:
    CREATE USER not_exists_opt user_spec_list_of_create_user default_role_opt pwd_or_lck_opt user_comment_or_attribute_opt
    {
        var IfNotExists = $3
        var Users = $4
        var Role = $5
        var MiscOpt = $6
        var CommentOrAttribute = $7
        $$ = tree.NewCreateUser(
            IfNotExists,
            Users,
            Role,
            MiscOpt,
            CommentOrAttribute,
        )
    }

create_publication_stmt:
    CREATE PUBLICATION not_exists_opt ident DATABASE ident alter_publication_accounts_opt comment_opt
    {
        var IfNotExists = $3
        var Name = tree.Identifier($4.Compare())
        var Database = tree.Identifier($6.Compare())
        var Table = tree.Identifier("")
        var AccountsSet = $7
        var Comment = $8
        $$ = tree.NewCreatePublication(
            IfNotExists,
            Name,
            Database,
            Table,
            AccountsSet,
            Comment,
        )
    }
|   CREATE PUBLICATION not_exists_opt ident TABLE ident alter_publication_accounts_opt comment_opt
    {
        var IfNotExists = $3
        var Name = tree.Identifier($4.Compare())
        var Database = tree.Identifier("")
        var Table = tree.Identifier($6.Compare())
        var AccountsSet = $7
        var Comment = $8
        $$ = tree.NewCreatePublication(
            IfNotExists,
            Name,
            Database,
            Table,
            AccountsSet,
            Comment,
        )
    }

create_stage_stmt:
    CREATE STAGE not_exists_opt ident urlparams stage_credentials_opt stage_status_opt stage_comment_opt
    {
        var IfNotExists = $3
        var Name = tree.Identifier($4.Compare())
        var Url = $5
        var Credentials = $6
        var Status = $7
        var Comment = $8
        $$ = tree.NewCreateStage(
            IfNotExists,
            Name,
            Url,
            Credentials,
            Status,
            Comment,
        )
    }

stage_status_opt:
    {
        $$ = tree.StageStatus{
            Exist: false,
        }
    }
|   ENABLE '=' TRUE
    {
        $$ = tree.StageStatus{
            Exist: true,
            Option: tree.StageStatusEnabled,
        }
    }
|   ENABLE '=' FALSE
    {
        $$ = tree.StageStatus{
            Exist: true,
            Option: tree.StageStatusDisabled,
        }
    }

stage_comment_opt:
    {
        $$ = tree.StageComment{
            Exist: false,
        }
    }
|   COMMENT_KEYWORD '=' STRING
    {
        $$ = tree.StageComment{
            Exist: true,
            Comment: $3,
        }
    }

stage_url_opt:
    {
        $$ = tree.StageUrl{
            Exist: false,
        }
    }
|   URL '=' STRING
    {
        $$ = tree.StageUrl{
            Exist: true,
            Url: $3,
        }
    }

stage_credentials_opt:
    {
        $$ = tree.StageCredentials {
            Exist:false,
        }
    }
|   CREDENTIALS '=' '{' credentialsparams '}'
    {
        $$ = tree.StageCredentials {
            Exist:true,
            Credentials:$4,
        }
    }

credentialsparams:
    credentialsparam
    {
        $$ = $1
    }
|   credentialsparams ',' credentialsparam
    {
        $$ = append($1, $3...)
    }

credentialsparam:
    {
        $$ = []string{}
    }
|   STRING '=' STRING
    {
        $$ = append($$, $1)
        $$ = append($$, $3)
    }

urlparams:
    URL '=' STRING
    {
        $$ = $3
    }

comment_opt:
    {
        $$ = ""
    }
|   COMMENT_KEYWORD STRING
    {
        $$ = $2
    }

alter_stage_stmt:
    ALTER STAGE exists_opt ident SET stage_url_opt stage_credentials_opt stage_status_opt stage_comment_opt
    {
        var ifNotExists = $3
        var name = tree.Identifier($4.Compare())
        var urlOption = $6
        var credentialsOption = $7
        var statusOption = $8
        var comment = $9
        $$ = tree.NewAlterStage(ifNotExists, name, urlOption, credentialsOption, statusOption, comment)
    }


alter_publication_stmt:
    ALTER PUBLICATION exists_opt ident alter_publication_accounts_opt alter_publication_db_name_opt comment_opt
    {
        var ifExists = $3
        var name = tree.Identifier($4.Compare())
        var accountsSet = $5
        var dbName = $6
        var comment = $7
        $$ = tree.NewAlterPublication(ifExists, name, accountsSet, dbName, comment)
    }

alter_publication_accounts_opt:
    {
	    $$ = nil
    }
    | ACCOUNT ALL
    {
	    $$ = &tree.AccountsSetOption{
	        All: true,
	    }
    }
    | ACCOUNT accounts_list
    {
    	$$ = &tree.AccountsSetOption{
	        SetAccounts: $2,
	    }
    }
    | ACCOUNT ADD accounts_list
    {
    	$$ = &tree.AccountsSetOption{
	        AddAccounts: $3,
	    }
    }
    | ACCOUNT DROP accounts_list
    {
    	$$ = &tree.AccountsSetOption{
	        DropAccounts: $3,
	    }
    }

alter_publication_db_name_opt:
    {
	    $$ = ""
    }
    | DATABASE db_name
    {
	    $$ = $2
    }

drop_publication_stmt:
    DROP PUBLICATION exists_opt ident
    {
        var ifExists = $3
        var name = tree.Identifier($4.Compare())
        $$ = tree.NewDropPublication(ifExists, name)
    }

drop_stage_stmt:
    DROP STAGE exists_opt ident
    {
        var ifNotExists = $3
        var name = tree.Identifier($4.Compare())
        $$ = tree.NewDropStage(ifNotExists, name)
    }

drop_snapshot_stmt:
   DROP SNAPSHOT exists_opt ident
   {
        var ifExists = $3
        var name = tree.Identifier($4.Compare())
        $$ = tree.NewDropSnapShot(ifExists, name)
    }

drop_pitr_stmt:
   DROP PITR exists_opt ident
   {
       var ifExists = $3
       var name = tree.Identifier($4.Compare())
       $$ = tree.NewDropPitr(ifExists, name)
   }

account_role_name:
    ident
    {
   		$$ = $1.Compare()
    }

user_comment_or_attribute_opt:
    {
        var Exist = false
        var IsComment bool
        var Str string
        $$ = *tree.NewAccountCommentOrAttribute(
            Exist,
            IsComment,
            Str,
        )

    }
|   COMMENT_KEYWORD STRING
    {
        var Exist = true
        var IsComment = true
        var Str = $2
        $$ = *tree.NewAccountCommentOrAttribute(
            Exist,
            IsComment,
            Str,
        )
    }
|   ATTRIBUTE STRING
    {
        var Exist = true
        var IsComment = false
        var Str = $2
        $$ = *tree.NewAccountCommentOrAttribute(
            Exist,
            IsComment,
            Str,
        )
    }

//conn_options:
//    {
//        $$ = nil
//    }
//|   WITH conn_option_list
//    {
//        $$ = $2
//    }
//
//conn_option_list:
//    conn_option
//    {
//        $$ = []tree.ResourceOption{$1}
//    }
//|   conn_option_list conn_option
//    {
//        $$ = append($1, $2)
//    }
//
//conn_option:
//    MAX_QUERIES_PER_HOUR INTEGRAL
//    {
//        $$ = &tree.ResourceOptionMaxQueriesPerHour{Count: $2.(int64)}
//    }
//|   MAX_UPDATES_PER_HOUR INTEGRAL
//    {
//        $$ = &tree.ResourceOptionMaxUpdatesPerHour{Count: $2.(int64)}
//    }
//|   MAX_CONNECTIONS_PER_HOUR INTEGRAL
//    {
//        $$ = &tree.ResourceOptionMaxConnectionPerHour{Count: $2.(int64)}
//    }
//|   MAX_USER_CONNECTIONS INTEGRAL
//    {
//        $$ = &tree.ResourceOptionMaxUserConnections{Count: $2.(int64)}
//    }


//require_clause_opt:
//    {
//        $$ = nil
//    }
//|   require_clause
//
//require_clause:
//    REQUIRE NONE
//    {
//        t := &tree.TlsOptionNone{}
//        $$ = []tree.TlsOption{t}
//    }
//|   REQUIRE SSL
//    {
//        t := &tree.TlsOptionSSL{}
//        $$ = []tree.TlsOption{t}
//    }
//|   REQUIRE X509
//    {
//        t := &tree.TlsOptionX509{}
//        $$ = []tree.TlsOption{t}
//    }
//|   REQUIRE require_list
//    {
//        $$ = $2
//    }
//
//require_list:
//    require_elem
//    {
//        $$ = []tree.TlsOption{$1}
//    }
//|   require_list AND require_elem
//    {
//        $$ = append($1, $3)
//    }
//|   require_list require_elem
//    {
//        $$ = append($1, $2)
//    }
//
//require_elem:
//    ISSUER STRING
//    {
//        $$ = &tree.TlsOptionIssuer{Issuer: $2}
//    }
//|   SUBJECT STRING
//    {
//        $$ = &tree.TlsOptionSubject{Subject: $2}
//    }
//|   CIPHER STRING
//    {
//        $$ = &tree.TlsOptionCipher{Cipher: $2}
//    }
//|   SAN STRING
//    {
//        $$ = &tree.TlsOptionSan{San: $2}
//    }
user_spec_list_of_create_user:
    user_spec_with_identified
    {
        $$ = []*tree.User{$1}
    }
|   user_spec_list_of_create_user ',' user_spec_with_identified
    {
        $$ = append($1, $3)
    }

user_spec_with_identified:
    user_name user_identified
    {
        var Username = $1.Username
        var Hostname = $1.Hostname
        var AuthOption = $2
        $$ = tree.NewUser(
            Username,
            Hostname,
            AuthOption,
        )
    }

user_spec_list:
    user_spec
    {
        $$ = []*tree.User{$1}
    }
|   user_spec_list ',' user_spec
    {
        $$ = append($1, $3)
    }

user_spec:
    user_name user_identified_opt
    {
        var Username = $1.Username
        var Hostname = $1.Hostname
        var AuthOption = $2
        $$ = tree.NewUser(
            Username,
            Hostname,
            AuthOption,
        )
    }

user_name:
    name_string
    {
        $$ = &tree.UsernameRecord{Username: $1, Hostname: "%"}
    }
|   name_string '@' name_string
    {
        $$ = &tree.UsernameRecord{Username: $1, Hostname: $3}
    }
|   name_string AT_ID
    {
        $$ = &tree.UsernameRecord{Username: $1, Hostname: $2}
    }

user_identified_opt:
    {
        $$ = nil
    }
|   user_identified
    {
        $$ = $1
    }

user_identified:
    IDENTIFIED BY STRING
    {
    $$ = &tree.AccountIdentified{
        Typ: tree.AccountIdentifiedByPassword,
        Str: tree.NewNumValWithType(constant.MakeString($3), $3, false, tree.P_char),
    }
    }
|   IDENTIFIED BY RANDOM PASSWORD
    {
    $$ = &tree.AccountIdentified{
        Typ: tree.AccountIdentifiedByRandomPassword,
    }
    }
|   IDENTIFIED WITH STRING
    {
    $$ = &tree.AccountIdentified{
        Typ: tree.AccountIdentifiedWithSSL,
        Str: tree.NewNumValWithType(constant.MakeString($3), $3, false, tree.P_char),
    }
    }

name_string:
    ident
    {
    	$$ = $1.Compare()
    }
|   STRING

create_role_stmt:
    CREATE ROLE not_exists_opt role_spec_list
    {
        var IfNotExists = $3
        var Roles = $4
        $$ = tree.NewCreateRole(
            IfNotExists,
            Roles,
        )
    }

role_spec_list:
    role_spec
    {
        $$ = []*tree.Role{$1}
    }
|   role_spec_list ',' role_spec
    {
        $$ = append($1, $3)
    }

role_spec:
    role_name
    {
        var UserName = $1.Compare()
        $$ = tree.NewRole(
            UserName,
        )
    }

role_name:
    ID
	{
		$$ = tree.NewCStr($1, 1)
    }
|   QUOTE_ID
	{
		$$ = tree.NewCStr($1, 1)
    }
|   STRING
	{
    	$$ = tree.NewCStr($1, 1)
    }

index_prefix:
    {
        $$ = tree.INDEX_CATEGORY_NONE
    }
|   FULLTEXT
    {
        $$ = tree.INDEX_CATEGORY_FULLTEXT
    }
|   SPATIAL
    {
        $$ = tree.INDEX_CATEGORY_SPATIAL
    }
|   UNIQUE
    {
        $$ = tree.INDEX_CATEGORY_UNIQUE
    }

create_index_stmt:
    CREATE index_prefix INDEX ident using_opt ON table_name '(' index_column_list ')' index_option_list
    {
        var io *tree.IndexOption = nil
        if $11 == nil && $5 != tree.INDEX_TYPE_INVALID {
            io = tree.NewIndexOption()
            io.IType = $5
        } else if $11 != nil{
            io = $11
            io.IType = $5
        } else {
            io = tree.NewIndexOption()
            io.IType = tree.INDEX_TYPE_INVALID
	    }
        var Name = tree.Identifier($4.Compare())
        var Table = $7
        var ifNotExists = false
        var IndexCat = $2
        var KeyParts = $9
        var IndexOption = io
        var MiscOption []tree.MiscOption
        $$ = tree.NewCreateIndex(
            Name,
            Table,
            ifNotExists,
            IndexCat,
            KeyParts,
            IndexOption,
            MiscOption,
        )
    }

index_option_list:
    {
        $$ = nil
    }
|   index_option_list index_option
    {
        // Merge the options
        if $1 == nil {
            $$ = $2
        } else {
            opt1 := $1
            opt2 := $2
            if len(opt2.Comment) > 0 {
                opt1.Comment = opt2.Comment
            } else if opt2.KeyBlockSize > 0 {
                opt1.KeyBlockSize = opt2.KeyBlockSize
            } else if len(opt2.ParserName) > 0 {
                opt1.ParserName = opt2.ParserName
            } else if opt2.Visible != tree.VISIBLE_TYPE_INVALID {
                opt1.Visible = opt2.Visible
            } else if opt2.AlgoParamList > 0 {
	      opt1.AlgoParamList = opt2.AlgoParamList
	    } else if len(opt2.AlgoParamVectorOpType) > 0 {
	      opt1.AlgoParamVectorOpType = opt2.AlgoParamVectorOpType
	    }
            $$ = opt1
        }
    }

index_option:
    KEY_BLOCK_SIZE equal_opt INTEGRAL
    {
        io := tree.NewIndexOption()
        io.KeyBlockSize = uint64($3.(int64))
        $$ = io
    }
|   LISTS equal_opt INTEGRAL
    {
    	val:= int64($3.(int64))
    	if val <= 0 {
    		yylex.Error("LISTS should be greater than 0")
    		return 1
    	}

	io := tree.NewIndexOption()
	io.AlgoParamList = val
	$$ = io
    }
|   OP_TYPE STRING
    {
        io := tree.NewIndexOption()
        io.AlgoParamVectorOpType = $2
        $$ = io
    }
|   COMMENT_KEYWORD STRING
    {
        io := tree.NewIndexOption()
        io.Comment = $2
        $$ = io
    }
|   WITH PARSER ident
    {
        io := tree.NewIndexOption()
        io.ParserName = $3.Compare()
        $$ = io
    }
|   VISIBLE
    {
        io := tree.NewIndexOption()
        io.Visible = tree.VISIBLE_TYPE_VISIBLE
        $$ = io
    }
|   INVISIBLE
    {
        io := tree.NewIndexOption()
        io.Visible = tree.VISIBLE_TYPE_INVISIBLE
        $$ = io
    }

index_column_list:
    index_column
    {
        $$ = []*tree.KeyPart{$1}
    }
|   index_column_list ',' index_column
    {
        $$ = append($1, $3)
    }

index_column:
    column_name length_opt asc_desc_opt
    {
        // Order is parsed but just ignored as MySQL dtree.
        var ColName = $1
        var Length = int($2)
        var Direction = $3
        var Expr tree.Expr
        $$ = tree.NewKeyPart(
            ColName,
            Length, 
            Direction,
            Expr,
        )
    }
|   '(' expression ')' asc_desc_opt
    {
        var ColName *tree.UnresolvedName
        var Length int
        var Expr = $2
        var Direction = $4
        $$ = tree.NewKeyPart(
            ColName,
            Length,
            Direction,
            Expr,
        )
    }

using_opt:
    {
        $$ = tree.INDEX_TYPE_INVALID
    }
|   USING BTREE
    {
        $$ = tree.INDEX_TYPE_BTREE
    }
|   USING IVFFLAT
    {
	$$ = tree.INDEX_TYPE_IVFFLAT
    }
|   USING MASTER
    {
	$$ = tree.INDEX_TYPE_MASTER
    }
|   USING HASH
    {
        $$ = tree.INDEX_TYPE_HASH
    }
|   USING RTREE
    {
        $$ = tree.INDEX_TYPE_RTREE
    }
|   USING BSI
    {
        $$ = tree.INDEX_TYPE_BSI
    }

create_database_stmt:
    CREATE database_or_schema not_exists_opt db_name subscription_opt create_option_list_opt
    {
        var IfNotExists = $3
        var Name = tree.Identifier($4)
        var SubscriptionOption = $5
        var CreateOptions = $6
        $$ = tree.NewCreateDatabase(
            IfNotExists,
            Name,
            SubscriptionOption,
            CreateOptions,
        )
    }
// CREATE comment_opt database_or_schema comment_opt not_exists_opt ident

subscription_opt:
    {
	$$ = nil
    }
|   FROM account_name PUBLICATION ident
    {
        var From = tree.Identifier($2)
        var Publication = tree.Identifier($4.Compare())
   	    $$ = tree.NewSubscriptionOption(From, Publication)
    }

database_or_schema:
    DATABASE
|   SCHEMA

not_exists_opt:
    {
        $$ = false
    }
|   IF NOT EXISTS
    {
        $$ = true
    }

create_option_list_opt:
    {
        $$ = nil
    }
|   create_option_list
    {
        $$ = $1
    }

create_option_list:
    create_option
    {
        $$ = []tree.CreateOption{$1}
    }
|   create_option_list create_option
    {
        $$ = append($1, $2)
    }

create_option:
    default_opt charset_keyword equal_opt charset_name
    {
        var IsDefault = $1
        var Charset = $4
        $$ = tree.NewCreateOptionCharset(
            IsDefault,
            Charset,
        )
    }
|   default_opt COLLATE equal_opt collate_name
    {
        var IsDefault = $1
        var Collate = $4
        $$ = tree.NewCreateOptionCollate(
            IsDefault, 
            Collate,
        )
    }
|   default_opt ENCRYPTION equal_opt STRING
    {
        var Encrypt = $4
        $$ = tree.NewCreateOptionEncryption(Encrypt)
    }

default_opt:
    {
        $$ = false
    }
|   DEFAULT
    {
        $$ = true
    }

create_connector_stmt:
    CREATE CONNECTOR FOR table_name WITH '(' connector_option_list ')'
    {
        var TableName = $4
        var Options = $7
        $$ = tree.NewCreateConnector(
            TableName,
            Options,
        )
    }

show_connectors_stmt:
    SHOW CONNECTORS
    {
	$$ = &tree.ShowConnectors{}
    }

pause_daemon_task_stmt:
    PAUSE DAEMON TASK INTEGRAL
    {
        var taskID uint64
        switch v := $4.(type) {
        case uint64:
    	    taskID = v
        case int64:
    	    taskID = uint64(v)
        default:
    	    yylex.Error("parse integral fail")
    	    goto ret1
        }
        $$ = &tree.PauseDaemonTask{
            TaskID: taskID,
        }
    }

cancel_daemon_task_stmt:
    CANCEL DAEMON TASK INTEGRAL
    {
        var taskID uint64
        switch v := $4.(type) {
        case uint64:
    	    taskID = v
        case int64:
    	    taskID = uint64(v)
        default:
    	    yylex.Error("parse integral fail")
    	    goto ret1
        }
        $$ = &tree.CancelDaemonTask{
            TaskID: taskID,
        }
    }

resume_daemon_task_stmt:
    RESUME DAEMON TASK INTEGRAL
    {
        var taskID uint64
        switch v := $4.(type) {
        case uint64:
    	    taskID = v
        case int64:
    	    taskID = uint64(v)
        default:
    	    yylex.Error("parse integral fail")
    	    goto ret1
        }
        $$ = &tree.ResumeDaemonTask{
            TaskID: taskID,
        }
    }

create_source_stmt:
    CREATE replace_opt SOURCE not_exists_opt table_name '(' table_elem_list_opt ')' source_option_list_opt
    {
        var Replace = $2
        var IfNotExists = $4
        var SourceName = $5
        var Defs = $7
        var Options = $9
        $$ = tree.NewCreateSource(
            Replace,
            IfNotExists,
            SourceName,
            Defs,
            Options,
        )
    }

replace_opt:
    {
        $$ = false
    }
|   OR REPLACE
    {
        $$ = true
    }




create_table_stmt:
    CREATE temporary_opt TABLE not_exists_opt table_name '(' table_elem_list_opt ')' table_option_list_opt partition_by_opt cluster_by_opt
    {
        t := tree.NewCreateTable()
        t.Temporary = $2
        t.IfNotExists = $4
        t.Table = *$5
        t.Defs = $7
        t.Options = $9
        t.PartitionOption =$10
        t.ClusterByOption =$11
        $$ = t
    }
|   CREATE EXTERNAL TABLE not_exists_opt table_name '(' table_elem_list_opt ')' load_param_opt_2
    {
        t := tree.NewCreateTable()
        t.IfNotExists = $4
        t.Table = *$5
        t.Defs = $7
        t.Param = $9
        $$ = t
    }
|   CREATE CLUSTER TABLE not_exists_opt table_name '(' table_elem_list_opt ')' table_option_list_opt partition_by_opt cluster_by_opt
    {
        t := tree.NewCreateTable()
        t.IsClusterTable = true
        t.IfNotExists = $4
        t.Table = *$5
        t.Defs = $7
        t.Options = $9
        t.PartitionOption = $10
        t.ClusterByOption = $11
        $$ = t
    }
|   CREATE DYNAMIC TABLE not_exists_opt table_name AS select_stmt source_option_list_opt
    {
        t := tree.NewCreateTable()
        t.IsDynamicTable = true
        t.IfNotExists = $4
        t.Table = *$5
        t.AsSource = $7
        t.DTOptions = $8
        $$ = t
    }
|   CREATE temporary_opt TABLE not_exists_opt table_name select_stmt
    {
        t := tree.NewCreateTable()
        t.IsAsSelect = true
        t.Temporary = $2
        t.IfNotExists = $4
        t.Table = *$5
        t.AsSource = $6
        $$ = t
    }
|   CREATE temporary_opt TABLE not_exists_opt table_name '(' table_elem_list_opt ')' select_stmt
    {
        t := tree.NewCreateTable()
        t.IsAsSelect = true
        t.Temporary = $2
        t.IfNotExists = $4
        t.Table = *$5
        t.Defs = $7
        t.AsSource = $9
        $$ = t
    }
|   CREATE temporary_opt TABLE not_exists_opt table_name AS select_stmt
    {
        t := tree.NewCreateTable()
        t.IsAsSelect = true
        t.Temporary = $2
        t.IfNotExists = $4
        t.Table = *$5
        t.AsSource = $7
        $$ = t
    }
|   CREATE temporary_opt TABLE not_exists_opt table_name '(' table_elem_list_opt ')' AS select_stmt
    {
        t := tree.NewCreateTable()
        t.IsAsSelect = true
        t.Temporary = $2
        t.IfNotExists = $4
        t.Table = *$5
        t.Defs = $7
        t.AsSource = $10
        $$ = t
    }
|   CREATE temporary_opt TABLE not_exists_opt table_name LIKE table_name
    {
        t := tree.NewCreateTable()
        t.IsAsLike = true
        t.Table = *$5
        t.LikeTableName = *$7
        $$ = t
    }
|   CREATE temporary_opt TABLE not_exists_opt table_name subscription_opt
    {
        t := tree.NewCreateTable()
        t.Temporary = $2
        t.IfNotExists = $4
        t.Table = *$5
        t.SubscriptionOption = $6
        $$ = t
    }

load_param_opt_2:
    load_param_opt tail_param_opt
    {
        $$ = $1
        $$.Tail = $2
    }

load_param_opt:
    INFILE STRING
    {
        $$ = &tree.ExternParam{
            ExParamConst: tree.ExParamConst{
                Filepath: $2,
                CompressType: tree.AUTO,
                Format: tree.CSV,
            },
        }
    }
|   INLINE  FORMAT '=' STRING ','  DATA '=' STRING  json_type_opt
    {
        $$ = &tree.ExternParam{
            ExParamConst: tree.ExParamConst{
                ScanType: tree.INLINE,
                Format: $4,
                Data: $8,
            },
            ExParam:tree.ExParam{
                JsonData:$9,
            },
        }
    }
|   INFILE '{' infile_or_s3_params '}'
    {
        $$ = &tree.ExternParam{
            ExParamConst: tree.ExParamConst{
                Option: $3,
            },
        }
    }
|   URL S3OPTION '{' infile_or_s3_params '}'
    {
        $$ = &tree.ExternParam{
            ExParamConst: tree.ExParamConst{
                ScanType: tree.S3,
                Option: $4,
            },
        }
    }
|   URL STAGEOPTION ident
    {
        $$ = &tree.ExternParam{
            ExParamConst: tree.ExParamConst{
                StageName: tree.Identifier($3.Compare()),
            },
        }
    }

json_type_opt:
    {
        $$ = ""
    }
|    ',' JSONTYPE '=' STRING 
    {
        $$ = $4
    }

infile_or_s3_params:
    infile_or_s3_param
    {
        $$ = $1
    }
|   infile_or_s3_params ',' infile_or_s3_param
    {
        $$ = append($1, $3...)
    }

infile_or_s3_param:
    {
        $$ = []string{}
    }
|   STRING '=' STRING
    {
        $$ = append($$, $1)
        $$ = append($$, $3)
    }

tail_param_opt:
    load_charset load_fields load_lines ignore_lines columns_or_variable_list_opt load_set_spec_opt
    {
        $$ = &tree.TailParameter{
            Charset: $1,
            Fields: $2,
            Lines: $3,
            IgnoredLines: uint64($4),
            ColumnList: $5,
            Assignments: $6,
        }
    }

load_charset:
    {
        $$ = ""
    }
|   charset_keyword charset_name
    {
    	$$ = $2
    }

create_sequence_stmt:
    CREATE SEQUENCE not_exists_opt table_name as_datatype_opt increment_by_opt min_value_opt max_value_opt start_with_opt cycle_opt
    {
        var Name = $4
        var Type = $5
        var IfNotExists = $3
        var IncrementBy = $6
        var MinValue = $7
        var MaxValue = $8
        var StartWith = $9
        var Cycle = $10
        $$ = tree.NewCreateSequence(
            Name,
            Type,
            IfNotExists,
            IncrementBy,
            MinValue,
            MaxValue,
            StartWith,
            Cycle,
        )
    }
as_datatype_opt:
    {
        locale := ""
        fstr := "bigint"
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: fstr,
                Width:  64,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_LONGLONG),
            },
        }
    }
|   AS column_type
    {
        $$ = $2
    }
alter_as_datatype_opt:
    {
        $$ = nil
    }
|   AS column_type
    {
        $$ = &tree.TypeOption{
            Type: $2,
        }
    }
increment_by_opt:
    {
        $$ = nil
    }
|   INCREMENT BY INTEGRAL
    {
        $$ = &tree.IncrementByOption{
            Minus: false,
            Num: $3,
        }
    }
|   INCREMENT INTEGRAL
    {
        $$ = &tree.IncrementByOption{
            Minus: false,
            Num: $2,
        }
    }
|   INCREMENT BY '-' INTEGRAL
    {
        $$ = &tree.IncrementByOption{
            Minus: true,
            Num: $4,
        }
    }
|   INCREMENT '-' INTEGRAL
    {
        $$ = &tree.IncrementByOption {
            Minus: true,
            Num: $3,
        }
    }
cycle_opt:
    {
        $$ = false
    }
|   NO CYCLE
    {
        $$ = false
    }
|   CYCLE
    {
        $$ = true
    }
min_value_opt:
    {
        $$ = nil
    }
|   MINVALUE INTEGRAL
    {
        $$ = &tree.MinValueOption{
            Minus: false,
            Num: $2,
        }
    }
|   MINVALUE '-' INTEGRAL
    {
        $$ = &tree.MinValueOption{
            Minus: true,
            Num: $3,
        }
    }
max_value_opt:
    {
        $$ = nil
    }
|   MAXVALUE INTEGRAL
    {
        $$ = &tree.MaxValueOption{
            Minus: false,
            Num: $2,
        }
    }
|   MAXVALUE '-' INTEGRAL
    {
        $$ = &tree.MaxValueOption{
            Minus: true,
            Num: $3,
        }
    }
alter_cycle_opt:
    {
        $$ = nil
    }
|   NO CYCLE
    {
        $$ = &tree.CycleOption{
            Cycle: false,
        }
    }
|   CYCLE
    {
        $$ = &tree.CycleOption{
            Cycle: true,
        }
    }
start_with_opt:
    {
        $$ = nil
    }
|   START WITH INTEGRAL
    {
        $$ = &tree.StartWithOption{
            Minus: false,
            Num: $3,
        }
    }
|   START INTEGRAL
    {
        $$ = &tree.StartWithOption{
            Minus:  false,
            Num: $2,
        }
    }
|   START WITH '-' INTEGRAL
    {
        $$ = &tree.StartWithOption{
            Minus: true,
            Num: $4,
        }
    }
|   START '-' INTEGRAL
    {
        $$ = &tree.StartWithOption{
            Minus: true,
            Num: $3,
        }
    }
temporary_opt:
    {
        $$ = false
    }
|   TEMPORARY
    {
        $$ = true
    }

drop_table_opt:
    {
        $$ = true
    }
|   RESTRICT
    {
        $$ = true
    }
|   CASCADE
    {
        $$ = true
    }

partition_by_opt:
    {
        $$ = nil
    }
|   PARTITION BY partition_method partition_num_opt sub_partition_opt partition_list_opt
    {
        $3.Num = uint64($4)
        var PartBy = $3
        var SubPartBy = $5
        var Partitions = $6
        $$ = tree.NewPartitionOption(
            PartBy,
            SubPartBy,
            Partitions,
        )
    }

cluster_by_opt:
    {
        $$ = nil
    }
|   CLUSTER BY column_name
    {
        var ColumnList = []*tree.UnresolvedName{$3}
        $$ = tree.NewClusterByOption(
            ColumnList,
        )

    }
    | CLUSTER BY '(' column_name_list ')'
    {
        var ColumnList = $4
        $$ = tree.NewClusterByOption(
            ColumnList,
        )
    }

sub_partition_opt:
    {
        $$ = nil
    }
|   SUBPARTITION BY sub_partition_method sub_partition_num_opt
    {
        var IsSubPartition = true
        var PType = $3
        var Num = uint64($4)
        $$ = tree.NewPartitionBy2(
            IsSubPartition,
            PType,
            Num,
        )
    }

partition_list_opt:
    {
        $$ = nil
    }
|   '(' partition_list ')'
    {
        $$ = $2
    }

partition_list:
    partition
    {
        $$ = []*tree.Partition{$1}
    }
|   partition_list ',' partition
    {
        $$ = append($1, $3)
    }

partition:
    PARTITION ident values_opt sub_partition_list_opt
    {
        var Name = tree.Identifier($2.Compare())
        var Values = $3
        var Options []tree.TableOption
        var Subs = $4
        $$ = tree.NewPartition(
            Name,
            Values,
            Options,
            Subs,
        )
    }
|   PARTITION ident values_opt partition_option_list sub_partition_list_opt
    {
        var Name = tree.Identifier($2.Compare())
        var Values = $3
        var Options = $4
        var Subs = $5
        $$ = tree.NewPartition(
            Name,
            Values,
            Options,
            Subs,
        )
    }

sub_partition_list_opt:
    {
        $$ = nil
    }
|   '(' sub_partition_list ')'
    {
        $$ = $2
    }

sub_partition_list:
    sub_partition
    {
        $$ = []*tree.SubPartition{$1}
    }
|   sub_partition_list ',' sub_partition
    {
        $$ = append($1, $3)
    }

sub_partition:
    SUBPARTITION ident
    {
        var Name = tree.Identifier($2.Compare())
        var Options []tree.TableOption
        $$ = tree.NewSubPartition(
            Name,
            Options,
        )
    }
|   SUBPARTITION ident partition_option_list
    {
        var Name = tree.Identifier($2.Compare())
        var Options = $3
        $$ = tree.NewSubPartition(
            Name,
            Options,
        )
    }

partition_option_list:
    table_option
    {
        $$ = []tree.TableOption{$1}
    }
|   partition_option_list table_option
    {
        $$ = append($1, $2)
    }

values_opt:
    {
        $$ = nil
    }
|   VALUES LESS THAN MAXVALUE
    {
        expr := tree.NewMaxValue()
        var valueList = tree.Exprs{expr}
        $$ = tree.NewValuesLessThan(valueList)
    }
|   VALUES LESS THAN '(' value_expression_list ')'
    {
        var valueList = $5
        $$ = tree.NewValuesLessThan(valueList)
    }
|   VALUES IN '(' value_expression_list ')'
    {
        var valueList = $4
        $$ = tree.NewValuesIn(
            valueList,
        )
    }

sub_partition_num_opt:
    {
        $$ = 0
    }
|   SUBPARTITIONS INTEGRAL
    {
        res := $2.(int64)
        if res == 0 {
            yylex.Error("partitions can not be 0")
            goto ret1
        }
        $$ = res
    }

partition_num_opt:
    {
        $$ = 0
    }
|   PARTITIONS INTEGRAL
    {
        res := $2.(int64)
        if res == 0 {
            yylex.Error("partitions can not be 0")
            goto ret1
        }
        $$ = res
    }

partition_method:
    RANGE '(' bit_expr ')'
    {
        rangeTyp := tree.NewRangeType()
        rangeTyp.Expr = $3
        $$ = tree.NewPartitionBy(
            rangeTyp,
        )
    }
|   RANGE fields_or_columns '(' column_name_list ')'
    {
        rangeTyp := tree.NewRangeType()
        rangeTyp.ColumnList = $4
        $$ = tree.NewPartitionBy(
            rangeTyp,
        )
    }
|   LIST '(' bit_expr ')'
    {
        listTyp := tree.NewListType()
        listTyp.Expr = $3
        $$ = tree.NewPartitionBy(
            listTyp,
        )
    }
|   LIST fields_or_columns '(' column_name_list ')'
    {
        listTyp := tree.NewListType()
        listTyp.ColumnList = $4
        $$ = tree.NewPartitionBy(
            listTyp,
        )
    }
|   sub_partition_method

sub_partition_method:
    linear_opt KEY algorithm_opt '(' ')'
    {
        keyTyp := tree.NewKeyType()
        keyTyp.Linear = $1
        keyTyp.Algorithm = $3
        $$ = tree.NewPartitionBy(
            keyTyp,
        )
    }
|   linear_opt KEY algorithm_opt '(' column_name_list ')'
    {
        keyTyp := tree.NewKeyType()
        keyTyp.Linear = $1
        keyTyp.Algorithm = $3
        keyTyp.ColumnList = $5
        $$ = tree.NewPartitionBy(
            keyTyp,
        )
    }
|   linear_opt HASH '(' bit_expr ')'
    {
        Linear := $1
        Expr := $4
        hashTyp := tree.NewHashType(Linear, Expr)
        $$ = tree.NewPartitionBy(
            hashTyp,
        )
    }

algorithm_opt:
    {
        $$ = 2
    }
|   ALGORITHM '=' INTEGRAL
    {
        $$ = $3.(int64)
    }

linear_opt:
    {
        $$ = false
    }
|   LINEAR
    {
        $$ = true
    }

connector_option_list:
	connector_option
	{
		$$ = []*tree.ConnectorOption{$1}
	}
|	connector_option_list ',' connector_option
	{
		$$ = append($1, $3)
	}

connector_option:
	ident equal_opt literal
    {
        var Key = tree.Identifier($1.Compare())
        var Val = $3
        $$ = tree.NewConnectorOption(
            Key, 
            Val,
        )
    }
    |   STRING equal_opt literal
        {
            var Key = tree.Identifier($1)
            var Val = $3
            $$ = tree.NewConnectorOption(
                Key, 
                Val,
            )
        }

source_option_list_opt:
    {
        $$ = nil
    }
|	WITH '(' source_option_list ')'
	{
		$$ = $3
	}

source_option_list:
	source_option
	{
		$$ = []tree.TableOption{$1}
	}
|	source_option_list ',' source_option
	{
		$$ = append($1, $3)
	}

source_option:
	ident equal_opt literal
    {
        var Key = tree.Identifier($1.Compare())
        var Val = $3
        $$ = tree.NewCreateSourceWithOption(
            Key,
            Val,
        )
    }
|   STRING equal_opt literal
    {
        var Key = tree.Identifier($1)
        var Val = $3
        $$ = tree.NewCreateSourceWithOption(
            Key,
            Val,
        )
    }

table_option_list_opt:
    {
        $$ = nil
    }
|   table_option_list
    {
        $$ = $1
    }

table_option_list:
    table_option
    {
        $$ = []tree.TableOption{$1}
    }
|   table_option_list ',' table_option
    {
        $$ = append($1, $3)
    }
|   table_option_list table_option
    {
        $$ = append($1, $2)
    }

table_option:
    AUTOEXTEND_SIZE equal_opt INTEGRAL
    {
        $$ = tree.NewTableOptionAUTOEXTEND_SIZE(uint64($3.(int64)))
    }
|   AUTO_INCREMENT equal_opt INTEGRAL
    {
        $$ = tree.NewTableOptionAutoIncrement(uint64($3.(int64)))
    }
|   AVG_ROW_LENGTH equal_opt INTEGRAL
    {
        $$ = tree.NewTableOptionAvgRowLength(uint64($3.(int64)))
    }
|   default_opt charset_keyword equal_opt charset_name
    {
        $$ = tree.NewTableOptionCharset($4)
    }
|   default_opt COLLATE equal_opt charset_name
    {
        $$ = tree.NewTableOptionCollate($4)
    }
|   CHECKSUM equal_opt INTEGRAL
    {
        $$ = tree.NewTableOptionChecksum(uint64($3.(int64)))
    }
|   COMMENT_KEYWORD equal_opt STRING
    {
    	str := util.DealCommentString($3)
        $$ = tree.NewTableOptionComment(str)
    }
|   COMPRESSION equal_opt STRING
    {
        $$ = tree.NewTableOptionCompression($3)
    }
|   CONNECTION equal_opt STRING
    {
        $$ = tree.NewTableOptionConnection($3)
    }
|   DATA DIRECTORY equal_opt STRING
    {
        $$ = tree.NewTableOptionDataDirectory($4)
    }
|   INDEX DIRECTORY equal_opt STRING
    {
        $$ = tree.NewTableOptionIndexDirectory($4)
    }
|   DELAY_KEY_WRITE equal_opt INTEGRAL
    {
        $$ = tree.NewTableOptionDelayKeyWrite(uint64($3.(int64)))
    }
|   ENCRYPTION equal_opt STRING
    {
        $$ = tree.NewTableOptionEncryption($3)
    }
|   ENGINE equal_opt table_alias
    {
        $$ = tree.NewTableOptionEngine($3)
    }
|   ENGINE_ATTRIBUTE equal_opt STRING
    {
        $$ = tree.NewTableOptionEngineAttr($3)
    }
|   INSERT_METHOD equal_opt insert_method_options
    {
        $$ = tree.NewTableOptionInsertMethod($3)
    }
|   KEY_BLOCK_SIZE equal_opt INTEGRAL
    {
        $$ = tree.NewTableOptionKeyBlockSize(uint64($3.(int64)))
    }
|   MAX_ROWS equal_opt INTEGRAL
    {
        $$ = tree.NewTableOptionMaxRows(uint64($3.(int64)))
    }
|   MIN_ROWS equal_opt INTEGRAL
    {
        $$ = tree.NewTableOptionMinRows(uint64($3.(int64)))
    }
|   PACK_KEYS equal_opt INTEGRAL
    {
        t := tree.NewTableOptionPackKeys()
        t.Value = $3.(int64)
        $$ = t
    }
|   PACK_KEYS equal_opt DEFAULT
    {
        t := tree.NewTableOptionPackKeys()
        t.Default = true
        $$ = t
    }
|   PASSWORD equal_opt STRING
    {
        $$ = tree.NewTableOptionPassword($3)
    }
|   ROW_FORMAT equal_opt row_format_options
    {
        $$ = tree.NewTableOptionRowFormat($3)
    }
|   START TRANSACTION
    {
        $$ = tree.NewTTableOptionStartTrans(true)
    }
|   SECONDARY_ENGINE_ATTRIBUTE equal_opt STRING
    {
        $$ = tree.NewTTableOptionSecondaryEngineAttr($3)
    }
|   STATS_AUTO_RECALC equal_opt INTEGRAL
    {
        t := tree.NewTableOptionStatsAutoRecalc()
        t.Value = uint64($3.(int64))
        $$ = t
    }
|   STATS_AUTO_RECALC equal_opt DEFAULT
    {
        t := tree.NewTableOptionStatsAutoRecalc()
        t.Default = true
        $$ = t
    }
|   STATS_PERSISTENT equal_opt INTEGRAL
    {
        t := tree.NewTableOptionStatsPersistent()
        t.Value = uint64($3.(int64))
        $$ = t
    }
|   STATS_PERSISTENT equal_opt DEFAULT
    {
        t := tree.NewTableOptionStatsPersistent()
        t.Default = true
        $$ = t
    }
|   STATS_SAMPLE_PAGES equal_opt INTEGRAL
    {
        t := tree.NewTableOptionStatsSamplePages()
        t.Value = uint64($3.(int64))
        $$ = t
    }
|   STATS_SAMPLE_PAGES equal_opt DEFAULT
    {
        t := tree.NewTableOptionStatsSamplePages()
        t.Default = true
        $$ = t
    }
|   TABLESPACE equal_opt ident
    {
        $$ = tree.NewTableOptionTablespace($3.Compare(), "")
    }
|   storage_opt
    {
        $$ = tree.NewTableOptionTablespace("", $1)
    }
|   UNION equal_opt '(' table_name_list ')'
    {
        $$ = tree.NewTableOptionUnion($4)
    }
|   PROPERTIES '(' properties_list ')'
    {
        var Preperties = $3
        $$ = tree.NewTableOptionProperties(Preperties)
    }

properties_list:
    property_elem
    {
        $$ = []tree.Property{$1}
    }
|    properties_list ',' property_elem
    {
        $$ = append($1, $3)
    }

property_elem:
    STRING '=' STRING
    {
        var Key = $1
        var Value = $3
        $$ = *tree.NewProperty(
            Key, 
            Value,
        )
    }

storage_opt:
    STORAGE DISK
    {
        $$ = " " + $1 + " " + $2
    }
|   STORAGE MEMORY
    {
        $$ = " " + $1 + " " + $2
    }

row_format_options:
    DEFAULT
    {
        $$ = tree.ROW_FORMAT_DEFAULT
    }
|   DYNAMIC
    {
        $$ = tree.ROW_FORMAT_DYNAMIC
    }
|   FIXED
    {
        $$ = tree.ROW_FORMAT_FIXED
    }
|   COMPRESSED
    {
        $$ = tree.ROW_FORMAT_COMPRESSED
    }
|   REDUNDANT
    {
        $$ = tree.ROW_FORMAT_REDUNDANT
    }
|   COMPACT
    {
        $$ = tree.ROW_FORMAT_COMPACT
    }

charset_name:
    name_string
|   BINARY

collate_name:
    name_string
|   BINARY

table_name_list:
    table_name
    {
        $$ = tree.TableNames{$1}
    }
|   table_name_list ',' table_name
    {
        $$ = append($1, $3)
    }

// Accepted patterns:
// <table>
// <schema>.<table>
table_name:
    ident table_snapshot_opt
    {
        tblName := yylex.(*Lexer).GetDbOrTblName($1.Origin())
        prefix := tree.ObjectNamePrefix{ExplicitSchema: false}
        $$ = tree.NewTableName(tree.Identifier(tblName), prefix, $2)
    }
|   ident '.' ident table_snapshot_opt
    {
        dbName := yylex.(*Lexer).GetDbOrTblName($1.Origin())
        tblName := yylex.(*Lexer).GetDbOrTblName($3.Origin())
        prefix := tree.ObjectNamePrefix{SchemaName: tree.Identifier(dbName), ExplicitSchema: true}
        $$ = tree.NewTableName(tree.Identifier(tblName), prefix, $4)
    }

table_snapshot_opt:
    {
        $$ = nil
    }
|   '{' TIMESTAMP '=' expression '}'
    {
        $$ = &tree.AtTimeStamp{
            Type: tree.ATTIMESTAMPTIME,
            Expr: $4,
        }
    }
|   '{' SNAPSHOT '=' ident '}'
    {
        var Str = $4.Compare()
        $$ = &tree.AtTimeStamp{
            Type: tree.ATTIMESTAMPSNAPSHOT,
            SnapshotName: yylex.(*Lexer).GetDbOrTblName($4.Origin()),
            Expr: tree.NewNumValWithType(constant.MakeString(Str), Str, false, tree.P_char),
        }
    }
|   '{' SNAPSHOT '=' STRING '}'
    {
        $$ = &tree.AtTimeStamp{
           Type: tree.ATTIMESTAMPSNAPSHOT,
           SnapshotName: $4,
           Expr: tree.NewNumValWithType(constant.MakeString($4), $4, false, tree.P_char),
        }
    }
|   '{' MO_TS '=' expression '}'
    {
        $$ = &tree.AtTimeStamp{
            Type: tree.ATMOTIMESTAMP,
            Expr: $4,
        }
    }

table_elem_list_opt:
    {
        $$ = tree.TableDefs(nil)
    }
|   table_elem_list

table_elem_list:
    table_elem
    {
        $$ = tree.TableDefs{$1}
    }
|   table_elem_list ',' table_elem
    {
        $$ = append($1, $3)
    }

table_elem:
    column_def
    {
        $$ = tree.TableDef($1)
    }
|   constaint_def
    {
        $$ = $1
    }
|   index_def
    {
        $$ = $1
    }

table_elem_2:
    constaint_def
    {
        $$ = $1
    }
|   index_def
    {
        $$ = $1
    }

index_def:
    FULLTEXT key_or_index_opt index_name '(' index_column_list ')' index_option_list
    {
        var KeyParts = $5
        var Name = $3
        var Empty = true
        var IndexOption = $7
        $$ = tree.NewFullTextIndex(
            KeyParts,
            Name,
            Empty,
            IndexOption,
        )
    }
|   FULLTEXT key_or_index_opt index_name '(' index_column_list ')' USING index_type index_option_list
    {
        var KeyParts = $5
        var Name = $3
        var Empty = true
        var IndexOption = $9
        $$ = tree.NewFullTextIndex(
            KeyParts,
            Name,
            Empty,
            IndexOption,
        )
    }
|   key_or_index not_exists_opt index_name_and_type_opt '(' index_column_list ')' index_option_list
    {
        keyTyp := tree.INDEX_TYPE_INVALID
        if $3[1] != "" {
               t := strings.ToLower($3[1])
            switch t {
 	    case "btree":
            	keyTyp = tree.INDEX_TYPE_BTREE
	    case "ivfflat":
		keyTyp = tree.INDEX_TYPE_IVFFLAT
	    case "master":
	    	keyTyp = tree.INDEX_TYPE_MASTER
            case "hash":
            	keyTyp = tree.INDEX_TYPE_HASH
	    case "rtree":
	   	keyTyp = tree.INDEX_TYPE_RTREE
            case "zonemap":
                keyTyp = tree.INDEX_TYPE_ZONEMAP
            case "bsi":
                keyTyp = tree.INDEX_TYPE_BSI
            default:
                yylex.Error("Invalid the type of index")
                goto ret1
            }
        }

        var IfNotExists = $2
        var KeyParts = $5
        var Name = $3[0]
        var KeyType = keyTyp
        var IndexOption = $7
        $$ = tree.NewIndex(
            IfNotExists,
            KeyParts,
            Name,
            KeyType,
            IndexOption,
        )
    }
|   key_or_index not_exists_opt index_name_and_type_opt '(' index_column_list ')' USING index_type index_option_list
    {
        keyTyp := tree.INDEX_TYPE_INVALID
        if $3[1] != "" {
               t := strings.ToLower($3[1])
            switch t {
             case "btree":
		keyTyp = tree.INDEX_TYPE_BTREE
	     case "ivfflat":
		keyTyp = tree.INDEX_TYPE_IVFFLAT
	     case "master":
        	keyTyp = tree.INDEX_TYPE_MASTER
	     case "hash":
		keyTyp = tree.INDEX_TYPE_HASH
	     case "rtree":
		keyTyp = tree.INDEX_TYPE_RTREE
	     case "zonemap":
		keyTyp = tree.INDEX_TYPE_ZONEMAP
	     case "bsi":
		keyTyp = tree.INDEX_TYPE_BSI
            default:
                yylex.Error("Invalid type of index")
                goto ret1
            }
        }
        var IfNotExists = $2
        var KeyParts = $5
        var Name = $3[0]
        var KeyType = keyTyp
        var IndexOption = $9
        $$ = tree.NewIndex(
            IfNotExists,
            KeyParts,
            Name,
            KeyType,
            IndexOption,
        )
    }

constaint_def:
    constraint_keyword constraint_elem
    {
        if $1 != "" {
            switch v := $2.(type) {
            case *tree.PrimaryKeyIndex:
                v.ConstraintSymbol = $1
            case *tree.ForeignKey:
                v.ConstraintSymbol = $1
            case *tree.UniqueIndex:
                v.ConstraintSymbol = $1
            }
        }
        $$ = $2
    }
|   constraint_elem
    {
        $$ = $1
    }

constraint_elem:
    PRIMARY KEY index_name_and_type_opt '(' index_column_list ')' index_option_list
    {
        var KeyParts = $5
        var Name = $3[0]
        var Empty = $3[1] == ""
        var IndexOption = $7
        $$ = tree.NewPrimaryKeyIndex(
            KeyParts,
            Name,
            Empty,
            IndexOption,
        )
    }
|   PRIMARY KEY index_name_and_type_opt '(' index_column_list ')' USING index_type index_option_list
    {
        var KeyParts = $5
        var Name = $3[0]
        var Empty = $3[1] == ""
        var IndexOption = $9
        $$ = tree.NewPrimaryKeyIndex(
            KeyParts,
            Name,
            Empty,
            IndexOption,
        )
    }
|   UNIQUE key_or_index_opt index_name_and_type_opt '(' index_column_list ')' index_option_list
    {
        var KeyParts = $5
        var Name = $3[0]
        var Empty = $3[1] == ""
        var IndexOption = $7
        $$ = tree.NewUniqueIndex(
            KeyParts,
            Name,
            Empty,
            IndexOption,
        )
    }
|   UNIQUE key_or_index_opt index_name_and_type_opt '(' index_column_list ')' USING index_type index_option_list
    {
        var KeyParts = $5
        var Name = $3[0]
        var Empty = $3[1] == ""
        var IndexOption = $9
        $$ = tree.NewUniqueIndex(
            KeyParts,
            Name,
            Empty,
            IndexOption,
        )
    }
|   FOREIGN KEY not_exists_opt index_name '(' index_column_list ')' references_def
    {
        var IfNotExists = $3
        var KeyParts = $6
        var Name = $4
        var Refer = $8
        var Empty = true
        $$ = tree.NewForeignKey(
            IfNotExists,
            KeyParts,
            Name,
            Refer,
            Empty,
        )
    }
|   CHECK '(' expression ')' enforce_opt
    {
        var Expr = $3
        var Enforced = $5
        $$ = tree.NewCheckIndex(
            Expr,
            Enforced,
        )
    }

enforce_opt:
    {
        $$ = false
    }
|    enforce

key_or_index_opt:
    {
        $$ = ""
    }
|   key_or_index
    {
        $$ = $1
    }

key_or_index:
    KEY
|   INDEX

index_name_and_type_opt:
    index_name
    {
        $$ = make([]string, 2)
        $$[0] = $1
        $$[1] = ""
    }
|   index_name USING index_type
    {
        $$ = make([]string, 2)
        $$[0] = $1
        $$[1] = $3
    }
|   ident TYPE index_type
    {
        $$ = make([]string, 2)
        $$[0] = $1.Compare()
        $$[1] = $3
    }

index_type:
    BTREE
|   HASH
|   RTREE
|   ZONEMAP
|   IVFFLAT
|   MASTER
|   BSI

insert_method_options:
    NO
|   FIRST
|   LAST

index_name:
    {
        $$ = ""
    }
|    ident
	{
		$$ = $1.Compare()
	}

column_def:
    column_name column_type column_attribute_list_opt
    {
        $$ = tree.NewColumnTableDef($1, $2, $3)
    }

column_name_unresolved:
    ident
    {
        $$ = tree.NewUnresolvedName($1)
    }
|   ident '.' ident
    {
        tblNameCStr := yylex.(*Lexer).GetDbOrTblNameCStr($1.Origin())
        $$ = tree.NewUnresolvedName(tblNameCStr, $3)
    }
|   ident '.' ident '.' ident
    {
        dbNameCStr := yylex.(*Lexer).GetDbOrTblNameCStr($1.Origin())
        tblNameCStr := yylex.(*Lexer).GetDbOrTblNameCStr($3.Origin())
        $$ = tree.NewUnresolvedName(dbNameCStr, tblNameCStr, $5)
    }

ident:
    ID
    {
		$$ = tree.NewCStr($1, 1)
    }
|	QUOTE_ID
	{
    	$$ = tree.NewCStr($1, 1)
    }
|   not_keyword
	{
    	$$ = tree.NewCStr($1, 1)
    }
|   non_reserved_keyword
	{
    	$$ = tree.NewCStr($1, 1)
    }

db_name_ident:
    ident
    {
    	$$ = yylex.(*Lexer).GetDbOrTblNameCStr($1.Origin())
    }

column_name:
    ident
    {
        $$ = tree.NewUnresolvedName($1)
    }
|   ident '.' ident
    {
        tblNameCStr := yylex.(*Lexer).GetDbOrTblNameCStr($1.Origin())
        $$ = tree.NewUnresolvedName(tblNameCStr, $3)
    }
|   ident '.' ident '.' ident
    {
        dbNameCStr := yylex.(*Lexer).GetDbOrTblNameCStr($1.Origin())
        tblNameCStr := yylex.(*Lexer).GetDbOrTblNameCStr($3.Origin())
        $$ = tree.NewUnresolvedName(dbNameCStr, tblNameCStr, $5)
    }

column_attribute_list_opt:
    {
        $$ = nil
    }
|   column_attribute_list
    {
        $$ = $1
    }

column_attribute_list:
    column_attribute_elem
    {
        $$ = []tree.ColumnAttribute{$1}
    }
|   column_attribute_list column_attribute_elem
    {
        $$ = append($1, $2)
    }

column_attribute_elem:
    NULL
    {
        $$ = tree.NewAttributeNull(true)
    }
|   NOT NULL
    {
        $$ = tree.NewAttributeNull(false)
    }
|   DEFAULT bit_expr
    {
        $$ = tree.NewAttributeDefault($2)
    }
|   AUTO_INCREMENT
    {
        $$ = tree.NewAttributeAutoIncrement()
    }
|   keys
    {
        $$ = $1
    }
|   COMMENT_KEYWORD STRING
    {
    	str := util.DealCommentString($2)
        $$ = tree.NewAttributeComment(tree.NewNumValWithType(constant.MakeString(str), str, false, tree.P_char))
    }
|   COLLATE collate_name
    {
        $$ = tree.NewAttributeCollate($2)
    }
|   COLUMN_FORMAT column_format
    {
        $$ = tree.NewAttributeColumnFormat($2)
    }
|   SECONDARY_ENGINE_ATTRIBUTE '=' STRING
    {
        $$ = nil
    }
|   ENGINE_ATTRIBUTE '=' STRING
    {
        $$ = nil
    }
|   STORAGE storage_media
    {
        $$ = tree.NewAttributeStorage($2)
    }
|   AUTO_RANDOM field_length_opt
    {
        $$ = tree.NewAttributeAutoRandom(int($2))
   }
|   references_def
    {
        $$ = $1
    }
|   constraint_keyword_opt CHECK '(' expression ')'
    {
        $$ = tree.NewAttributeCheckConstraint($4, false, $1)
    }
|   constraint_keyword_opt CHECK '(' expression ')' enforce
    {
        $$ = tree.NewAttributeCheckConstraint($4, $6, $1)
    }
|   ON UPDATE name_datetime_scale datetime_scale_opt
    {
        name := tree.NewUnresolvedColName($3)
        var es tree.Exprs = nil
        if $4 != nil {
            es = append(es, $4)
        }
        expr := &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($3, 1),
            Exprs: es,
        }
        $$ = tree.NewAttributeOnUpdate(expr)
    }
|   LOW_CARDINALITY
    {
	    $$ = tree.NewAttributeLowCardinality()
    }
|   VISIBLE
    {
        $$ = tree.NewAttributeVisable(true)
    }
|   INVISIBLE
    {
        $$ = tree.NewAttributeVisable(false)
    }
|   default_opt CHARACTER SET equal_opt ident
    {
        $$ = nil
    }
|	HEADER '(' STRING ')'
	{
		$$ = tree.NewAttributeHeader($3)
	}
|	HEADERS
	{
		$$ = tree.NewAttributeHeaders()
	}

enforce:
    ENFORCED
    {
        $$ = true
    }
|   NOT ENFORCED
    {
        $$ = false
    }

constraint_keyword_opt:
    {
        $$ = ""
    }
 |    constraint_keyword
     {
         $$ = $1
     }

constraint_keyword:
    CONSTRAINT
    {
        $$ = ""
    }
|   CONSTRAINT ident
    {
        $$ = $2.Compare()
    }

references_def:
    REFERENCES table_name index_column_list_opt match_opt on_delete_update_opt
    {
        var TableName = $2
        var KeyParts = $3
        var Match = $4
        var OnDelete = $5.OnDelete
        var OnUpdate = $5.OnUpdate
        $$ = tree.NewAttributeReference(
            TableName,
            KeyParts,
            Match,
            OnDelete,
            OnUpdate,
        )
    }

on_delete_update_opt:
    %prec LOWER_THAN_ON
    {
        $$ = &tree.ReferenceOnRecord{
            OnDelete: tree.REFERENCE_OPTION_INVALID,
            OnUpdate: tree.REFERENCE_OPTION_INVALID,
        }
    }
|   on_delete %prec LOWER_THAN_ON
    {
        $$ = &tree.ReferenceOnRecord{
            OnDelete: $1,
            OnUpdate: tree.REFERENCE_OPTION_INVALID,
        }
    }
|   on_update %prec LOWER_THAN_ON
    {
        $$ = &tree.ReferenceOnRecord{
            OnDelete: tree.REFERENCE_OPTION_INVALID,
            OnUpdate: $1,
        }
    }
|   on_delete on_update
    {
        $$ = &tree.ReferenceOnRecord{
            OnDelete: $1,
            OnUpdate: $2,
        }
    }
|   on_update on_delete
    {
        $$ = &tree.ReferenceOnRecord{
            OnDelete: $2,
            OnUpdate: $1,
        }
    }

on_delete:
    ON DELETE ref_opt
    {
        $$ = $3
    }

on_update:
    ON UPDATE ref_opt
    {
        $$ = $3
    }

ref_opt:
    RESTRICT
    {
        $$ = tree.REFERENCE_OPTION_RESTRICT
    }
|   CASCADE
    {
        $$ = tree.REFERENCE_OPTION_CASCADE
    }
|   SET NULL
    {
        $$ = tree.REFERENCE_OPTION_SET_NULL
    }
|   NO ACTION
    {
        $$ = tree.REFERENCE_OPTION_NO_ACTION
    }
|   SET DEFAULT
    {
        $$ = tree.REFERENCE_OPTION_SET_DEFAULT
    }

match_opt:
    {
        $$ = tree.MATCH_INVALID
    }
|   match

match:
    MATCH FULL
    {
        $$ = tree.MATCH_FULL
    }
|   MATCH PARTIAL
    {
        $$ = tree.MATCH_PARTIAL
    }
|   MATCH SIMPLE
    {
        $$ = tree.MATCH_SIMPLE
    }

index_column_list_opt:
    {
        $$ = nil
    }
|   '(' index_column_list ')'
    {
        $$ = $2
    }

field_length_opt:
    {
        $$ = -1
    }
|   '(' INTEGRAL ')'
    {
        $$ = $2.(int64)
    }

storage_media:
    DEFAULT
|   DISK
|   MEMORY

column_format:
    DEFAULT
|   FIXED
|   DYNAMIC

subquery:
    select_with_parens %prec SUBQUERY_AS_EXPR
    {
        $$ = &tree.Subquery{Select: $1, Exists: false}
    }

bit_expr:
    bit_expr '&' bit_expr %prec '&'
    {
        $$ = tree.NewBinaryExpr(tree.BIT_AND, $1, $3)
    }
|   bit_expr '|' bit_expr %prec '|'
    {
        $$ = tree.NewBinaryExpr(tree.BIT_OR, $1, $3)
    }
|   bit_expr '^' bit_expr %prec '^'
    {
        $$ = tree.NewBinaryExpr(tree.BIT_XOR, $1, $3)
    }
|   bit_expr '+' bit_expr %prec '+'
    {
        $$ = tree.NewBinaryExpr(tree.PLUS, $1, $3)
    }
|   bit_expr '-' bit_expr %prec '-'
    {
        $$ = tree.NewBinaryExpr(tree.MINUS, $1, $3)
    }
|   bit_expr '*' bit_expr %prec '*'
    {
        $$ = tree.NewBinaryExpr(tree.MULTI, $1, $3)
    }
|   bit_expr '/' bit_expr %prec '/'
    {
        $$ = tree.NewBinaryExpr(tree.DIV, $1, $3)
    }
|   bit_expr DIV bit_expr %prec DIV
    {
        $$ = tree.NewBinaryExpr(tree.INTEGER_DIV, $1, $3)
    }
|   bit_expr '%' bit_expr %prec '%'
    {
        $$ = tree.NewBinaryExpr(tree.MOD, $1, $3)
    }
|   bit_expr MOD bit_expr %prec MOD
    {
        $$ = tree.NewBinaryExpr(tree.MOD, $1, $3)
    }
|   bit_expr SHIFT_LEFT bit_expr %prec SHIFT_LEFT
    {
        $$ = tree.NewBinaryExpr(tree.LEFT_SHIFT, $1, $3)
    }
|   bit_expr SHIFT_RIGHT bit_expr %prec SHIFT_RIGHT
    {
        $$ = tree.NewBinaryExpr(tree.RIGHT_SHIFT, $1, $3)
    }
|   simple_expr
    {
        $$ = $1
    }

simple_expr:
    normal_ident
    {
        $$ = $1
    }
|   variable
    {
        $$ = $1
    }
|   literal
    {
        $$ = $1
    }
|   '(' expression ')'
    {
        $$ = tree.NewParentExpr($2)
    }
|   '(' expression_list ',' expression ')'
    {
        $$ = tree.NewTuple(append($2, $4))
    }
|   '+'  simple_expr %prec UNARY
    {
        $$ = tree.NewUnaryExpr(tree.UNARY_PLUS, $2)
    }
|   '-'  simple_expr %prec UNARY
    {
        $$ = tree.NewUnaryExpr(tree.UNARY_MINUS, $2)
    }
|   '~'  simple_expr
    {
        $$ = tree.NewUnaryExpr(tree.UNARY_TILDE, $2)
    }
|   '!' simple_expr %prec UNARY
    {
        $$ = tree.NewUnaryExpr(tree.UNARY_MARK, $2)
    }
|   '{'  ident expression '}'
    {   
        hint := strings.ToLower($2.Compare())
        switch hint {
		case "d":
            locale := ""
            t := &tree.T{
                InternalType: tree.InternalType{
                   Family: tree.TimestampFamily,
                   FamilyString: "DATETIME",
                   Locale: &locale,
                   Oid: uint32(defines.MYSQL_TYPE_DATETIME),
                },
            }
            $$ = tree.NewCastExpr($3, t)
        case "t":
            locale := ""
            t := &tree.T{
                InternalType: tree.InternalType{
                   Family: tree.TimeFamily,
                   FamilyString: "TIME",
                   Locale: &locale,
                   Oid: uint32(defines.MYSQL_TYPE_TIME),
               },
            }    
            $$ = tree.NewCastExpr($3, t)
        case "ts":
            locale := ""
            t := &tree.T{
                InternalType: tree.InternalType{
                    Family: tree.TimestampFamily,
                    FamilyString: "TIMESTAMP",
                    Locale:  &locale,
                    Oid: uint32(defines.MYSQL_TYPE_TIMESTAMP),
                },
            }
            $$ = tree.NewCastExpr($3, t) 
        default:
            yylex.Error("Invalid type")
            return 1
        }
    }
|   interval_expr
    {
        $$ = $1
    }
|   subquery
    {
        $$ = $1
    }
|   EXISTS subquery
    {
        $2.Exists = true
        $$ = $2
    }
|   CASE expression_opt when_clause_list else_opt END
    {
        $$ = &tree.CaseExpr{
            Expr: $2,
            Whens: $3,
            Else: $4,
        }
    }
|   CAST '(' expression AS mo_cast_type ')'
    {
        $$ = tree.NewCastExpr($3, $5)
    }
|   SERIAL_EXTRACT '(' expression ',' expression AS mo_cast_type ')'
    {
	$$ = tree.NewSerialExtractExpr($3, $5, $7)
    }
|   BIT_CAST '(' expression AS mo_cast_type ')'
    {
        $$ = tree.NewBitCastExpr($3, $5)
    }
|   CONVERT '(' expression ',' mysql_cast_type ')'
    {
        $$ = tree.NewCastExpr($3, $5)
    }
|   CONVERT '(' expression USING charset_name ')'
    {
        name := tree.NewUnresolvedColName($1)
        es := tree.NewNumValWithType(constant.MakeString($5), $5, false, tree.P_char)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$3, es},
        }
    }
|   function_call_generic
    {
        $$ = $1
    }
|   function_call_keyword
    {
        $$ = $1
    }
|   function_call_nonkeyword
    {
        $$ = $1
    }
|   function_call_aggregate
    {
        $$ = $1
    }
|   function_call_window
    {
        $$ = $1
    }
|   sample_function_expr
    {
        $$ = $1
    }
|   simple_expr COLLATE collate_name
    {
        $$ = $1
    }

function_call_window:
	RANK '(' ')' window_spec
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            WindowSpec: $4,
        }
    }
|	ROW_NUMBER '(' ')' window_spec
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            WindowSpec: $4,
        }
    }
|	DENSE_RANK '(' ')' window_spec
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            WindowSpec: $4,
        }
    }

sample_function_expr:
    SAMPLE '(' '*' ',' INTEGRAL ROWS ')'
    {
	v := int($5.(int64))
	val, err := tree.NewSampleRowsFuncExpression(v, true, nil, "block")
	if err != nil {
	    yylex.Error(err.Error())
	    goto ret1
	}
	$$ = val
    }
|   SAMPLE '(' '*' ',' INTEGRAL ROWS ',' STRING ')'
        {
    	v := int($5.(int64))
    	val, err := tree.NewSampleRowsFuncExpression(v, true, nil, $8)
    	if err != nil {
    	    yylex.Error(err.Error())
    	    goto ret1
    	}
    	$$ = val
        }
|   SAMPLE '(' '*' ',' INTEGRAL PERCENT ')'
    {
	val, err := tree.NewSamplePercentFuncExpression1($5.(int64), true, nil)
	if err != nil {
	    yylex.Error(err.Error())
	    goto ret1
	}
	$$ = val
    }
|   SAMPLE '(' '*' ',' FLOAT PERCENT ')'
    {
	val, err := tree.NewSamplePercentFuncExpression2($5.(float64), true, nil)
	if err != nil {
	    yylex.Error(err.Error())
	    goto ret1
	}
	$$ = val
    }
|
    SAMPLE '(' expression_list ',' INTEGRAL ROWS ')'
    {
    	v := int($5.(int64))
    	val, err := tree.NewSampleRowsFuncExpression(v, false, $3, "block")
    	if err != nil {
    	    yylex.Error(err.Error())
    	    goto ret1
    	}
    	$$ = val
    }
|   SAMPLE '(' expression_list ',' INTEGRAL ROWS ',' STRING ')'
    {
	v := int($5.(int64))
	val, err := tree.NewSampleRowsFuncExpression(v, false, $3, $8)
	if err != nil {
	    yylex.Error(err.Error())
	    goto ret1
	}
	$$ = val
    }
|   SAMPLE '(' expression_list ',' INTEGRAL PERCENT ')'
    {
        val, err := tree.NewSamplePercentFuncExpression1($5.(int64), false, $3)
        if err != nil {
            yylex.Error(err.Error())
            goto ret1
        }
        $$ = val
    }
|   SAMPLE '(' expression_list ',' FLOAT PERCENT ')'
    {
        val, err := tree.NewSamplePercentFuncExpression2($5.(float64), false, $3)
        if err != nil {
            yylex.Error(err.Error())
            goto ret1
        }
        $$ = val
    }

else_opt:
    {
        $$ = nil
    }
|    ELSE expression
    {
        $$ = $2
    }

expression_opt:
    {
        $$ = nil
    }
|    expression
    {
        $$ = $1
    }

when_clause_list:
    when_clause
    {
        $$ = []*tree.When{$1}
    }
|    when_clause_list when_clause
    {
        $$ = append($1, $2)
    }

when_clause:
    WHEN expression THEN expression
    {
        $$ = &tree.When{
            Cond: $2,
            Val: $4,
        }
    }

mo_cast_type:
    column_type
{
   t := $$
   str := strings.ToLower(t.InternalType.FamilyString)
   if str == "binary" {
        t.InternalType.Scale = -1
   } else if str == "char" {
   	if t.InternalType.DisplayWith == -1 {
   		t.InternalType.FamilyString = "varchar"
   		t.InternalType.Oid = uint32(defines.MYSQL_TYPE_VARCHAR)
   	}
   }
}
|   SIGNED integer_opt
    {
        name := $1
        if $2 != "" {
            name = $2
        }
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: name,
                Width:  64,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_LONGLONG),
            },
        }
    }
|   UNSIGNED integer_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $2,
                Width:  64,
                Locale: &locale,
                Unsigned: true,
                Oid:    uint32(defines.MYSQL_TYPE_LONGLONG),
            },
        }
    }

mysql_cast_type:
    decimal_type
|   BINARY length_option_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.StringFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_VARCHAR),
                DisplayWith: $2,
            },
        }
    }
|   CHAR length_option_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.StringFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_VARCHAR),
                DisplayWith: $2,
            },
        }
    }
|   DATE
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.DateFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_DATE),
            },
        }
    }
|   YEAR length_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $1,
                DisplayWith: $2,
                Width:  16,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_YEAR),
            },
        }
    }
|   DATETIME timestamp_option_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family:             tree.TimestampFamily,
                Scale:          $2,
                FamilyString: $1,
                DisplayWith: $2,
                TimePrecisionIsSet: false,
                Locale:             &locale,
                Oid:                uint32(defines.MYSQL_TYPE_DATETIME),
            },
        }
    }
|   TIME length_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.TimeFamily,
                FamilyString: $1,
                DisplayWith: $2,
                Scale: $2,
                TimePrecisionIsSet: false,
                Locale: &locale,
                Oid: uint32(defines.MYSQL_TYPE_TIME),
            },
        }
    }
|   SIGNED integer_opt
    {
        name := $1
        if $2 != "" {
            name = $2
        }
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: name,
                Width:  64,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_LONGLONG),
            },
        }
    }
|   UNSIGNED integer_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $2,
                Width:  64,
                Locale: &locale,
                Unsigned: true,
                Oid:    uint32(defines.MYSQL_TYPE_LONGLONG),
            },
        }
    }

integer_opt:
    {}
|    INTEGER
|    INT

frame_bound:
	frame_bound_start
|   UNBOUNDED FOLLOWING
    {
        $$ = &tree.FrameBound{Type: tree.Following, UnBounded: true}
    }
|   num_literal FOLLOWING
    {
        $$ = &tree.FrameBound{Type: tree.Following, Expr: $1}
    }
|	interval_expr FOLLOWING
	{
		$$ = &tree.FrameBound{Type: tree.Following, Expr: $1}
	}

frame_bound_start:
    CURRENT ROW
    {
        $$ = &tree.FrameBound{Type: tree.CurrentRow}
    }
|   UNBOUNDED PRECEDING
    {
        $$ = &tree.FrameBound{Type: tree.Preceding, UnBounded: true}
    }
|   num_literal PRECEDING
    {
        $$ = &tree.FrameBound{Type: tree.Preceding, Expr: $1}
    }
|	interval_expr PRECEDING
	{
		$$ = &tree.FrameBound{Type: tree.Preceding, Expr: $1}
	}

frame_type:
    ROWS
    {
        $$ = tree.Rows
    }
|   RANGE
    {
        $$ = tree.Range
    }
|   GROUPS
    {
        $$ = tree.Groups
    }

window_frame_clause:
    frame_type frame_bound_start
    {
        $$ = &tree.FrameClause{
            Type: $1,
            Start: $2,
            End: &tree.FrameBound{Type: tree.CurrentRow},
        }
    }
|   frame_type BETWEEN frame_bound AND frame_bound
    {
        $$ = &tree.FrameClause{
            Type: $1,
            HasEnd: true,
            Start: $3,
            End: $5,
        }
    }

window_frame_clause_opt:
    {
        $$ = nil
    }
|   window_frame_clause
    {
        $$ = $1
    }


window_partition_by:
   PARTITION BY expression_list
    {
        $$ = $3
    }

window_partition_by_opt:
    {
        $$ = nil
    }
|   window_partition_by
    {
        $$ = $1
    }

separator_opt:
    {
        $$ = ","
    }
|   SEPARATOR STRING
    {
       $$ = $2
    }

kmeans_opt:
    {
        $$ = "1,vector_l2_ops,random,false"
    }
|   KMEANS STRING
    {
       $$ = $2
    }

window_spec_opt:
    {
        $$ = nil
    }
|	window_spec

window_spec:
    OVER '(' window_partition_by_opt order_by_opt window_frame_clause_opt ')'
    {
    	hasFrame := true
    	var f *tree.FrameClause
    	if $5 != nil {
    		f = $5
    	} else {
    		hasFrame = false
    		f = &tree.FrameClause{Type: tree.Range}
    		if $4 == nil {
				f.Start = &tree.FrameBound{Type: tree.Preceding, UnBounded: true}
                f.End = &tree.FrameBound{Type: tree.Following, UnBounded: true}
    		} else {
    			f.Start = &tree.FrameBound{Type: tree.Preceding, UnBounded: true}
            	f.End = &tree.FrameBound{Type: tree.CurrentRow}
    		}
    	}
        $$ = &tree.WindowSpec{
            PartitionBy: $3,
            OrderBy: $4,
            Frame: f,
            HasFrame: hasFrame,
        }
    }

function_call_aggregate:
    GROUP_CONCAT '(' func_type_opt expression_list order_by_opt separator_opt ')' window_spec_opt
    {
	    name := tree.NewUnresolvedColName($1)
	        $$ = &tree.FuncExpr{
	        Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
	        Exprs: append($4,tree.NewNumValWithType(constant.MakeString($6), $6, false, tree.P_char)),
	        Type: $3,
	        WindowSpec: $8,
            OrderBy:$5,
	    }
    }
|  CLUSTER_CENTERS '(' func_type_opt expression_list order_by_opt kmeans_opt ')' window_spec_opt
      {
  	    name := tree.NewUnresolvedColName($1)
		$$ = &tree.FuncExpr{
		Func: tree.FuncName2ResolvableFunctionReference(name),
        FuncName: tree.NewCStr($1, 1),
		Exprs: append($4,tree.NewNumValWithType(constant.MakeString($6), $6, false, tree.P_char)),
		Type: $3,
		WindowSpec: $8,
		OrderBy:$5,
	    }
      }
|   AVG '(' func_type_opt expression  ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$4},
            Type: $3,
            WindowSpec: $6,
        }
    }
|   APPROX_COUNT '(' func_type_opt expression_list ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $4,
            Type: $3,
            WindowSpec: $6,
        }
    }
|   APPROX_COUNT '(' '*' ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        es := tree.NewNumValWithType(constant.MakeString("*"), "*", false, tree.P_char)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{es},
            WindowSpec: $5,
        }
    }
|   APPROX_COUNT_DISTINCT '(' expression_list ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
            WindowSpec: $5,
        }
    }
|   APPROX_PERCENTILE '(' expression_list ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
            WindowSpec: $5,
        }
    }
|   BIT_AND '(' func_type_opt expression ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$4},
            Type: $3,
            WindowSpec: $6,
        }
    }
|   BIT_OR '(' func_type_opt expression ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$4},
            Type: $3,
            WindowSpec: $6,
        }
    }
|   BIT_XOR '(' func_type_opt expression ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$4},
            Type: $3,
            WindowSpec: $6,
        }
    }
|   COUNT '(' func_type_opt expression_list ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $4,
            Type: $3,
            WindowSpec: $6,
        }
    }
|   COUNT '(' '*' ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        es := tree.NewNumValWithType(constant.MakeString("*"), "*", false, tree.P_char)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{es},
            WindowSpec: $5,
        }
    }
|   MAX '(' func_type_opt expression ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$4},
            Type: $3,
            WindowSpec: $6,
        }
    }
|   MIN '(' func_type_opt expression ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$4},
            Type: $3,
            WindowSpec: $6,
        }
    }
|   SUM '(' func_type_opt expression ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$4},
            Type: $3,
            WindowSpec: $6,
        }
    }
|   std_dev_pop '(' func_type_opt expression ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$4},
            Type: $3,
            WindowSpec: $6,
        }
    }
|   STDDEV_SAMP '(' func_type_opt expression ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$4},
            Type: $3,
            WindowSpec: $6,
        }
    }
|   VAR_POP '(' func_type_opt expression ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$4},
            Type: $3,
            WindowSpec: $6,
        }
    }
|   VAR_SAMP '(' func_type_opt expression ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$4},
            Type: $3,
            WindowSpec: $6,
        }
    }
|   MEDIAN '(' func_type_opt expression ')' window_spec_opt
    {
	name := tree.NewUnresolvedColName($1)
	$$ = &tree.FuncExpr{
	    Func: tree.FuncName2ResolvableFunctionReference(name),
        FuncName: tree.NewCStr($1, 1),
	    Exprs: tree.Exprs{$4},
	    Type: $3,
	    WindowSpec: $6,
	    }
    }
|   BITMAP_CONSTRUCT_AGG '(' func_type_opt expression ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$4},
            Type: $3,
            WindowSpec: $6,
        }
    }
|   BITMAP_OR_AGG '(' func_type_opt expression ')' window_spec_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$4},
            Type: $3,
            WindowSpec: $6,
        }
    }

std_dev_pop:
    STD
|   STDDEV
|   STDDEV_POP

function_call_generic:
    ID '(' expression_list_opt ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
        }
    }
|   substr_option '(' expression_list_opt ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
        }
    }
|   substr_option '(' expression FROM expression ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$3, $5},
        }
    }
|   substr_option '(' expression FROM expression FOR expression ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$3, $5, $7},
        }
    }
|   EXTRACT '(' time_unit FROM expression ')'
    {
        name := tree.NewUnresolvedColName($1)
        str := strings.ToLower($3)
        timeUinit := tree.NewNumValWithType(constant.MakeString(str), str, false, tree.P_char)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{timeUinit, $5},
        }
    }
|   func_not_keyword '(' expression_list_opt ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
        }
    }
|   VARIANCE '(' func_type_opt expression ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$4},
            Type: $3,
        }
    }
|   NEXTVAL '(' expression_list ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
        }
    }
|   SETVAL '(' expression_list  ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
        }
    }
|   CURRVAL '(' expression_list  ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
        }
    }
|   LASTVAL '('')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: nil,
        }
    }
|   TRIM '(' expression ')'
    {
        name := tree.NewUnresolvedColName($1)
        arg0 := tree.NewNumValWithType(constant.MakeInt64(0), "0", false, tree.P_int64)
        arg1 := tree.NewNumValWithType(constant.MakeString("both"), "both", false, tree.P_char)
        arg2 := tree.NewNumValWithType(constant.MakeString(" "), " ", false, tree.P_char)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{arg0, arg1, arg2, $3},
        }
    }
|   TRIM '(' expression FROM expression ')'
    {
        name := tree.NewUnresolvedColName($1)
        arg0 := tree.NewNumValWithType(constant.MakeInt64(1), "1", false, tree.P_int64)
        arg1 := tree.NewNumValWithType(constant.MakeString("both"), "both", false, tree.P_char)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{arg0, arg1, $3, $5},
        }
    }
|   TRIM '(' trim_direction FROM expression ')'
    {
        name := tree.NewUnresolvedColName($1)
        arg0 := tree.NewNumValWithType(constant.MakeInt64(2), "2", false, tree.P_int64)
        str := strings.ToLower($3)
        arg1 := tree.NewNumValWithType(constant.MakeString(str), str, false, tree.P_char)
        arg2 := tree.NewNumValWithType(constant.MakeString(" "), " ", false, tree.P_char)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{arg0, arg1, arg2, $5},
        }
    }
|   TRIM '(' trim_direction expression FROM expression ')'
    {
        name := tree.NewUnresolvedColName($1)
        arg0 := tree.NewNumValWithType(constant.MakeInt64(3), "3", false, tree.P_int64)
        str := strings.ToLower($3)
        arg1 := tree.NewNumValWithType(constant.MakeString(str), str, false, tree.P_char)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{arg0, arg1, $4, $6},
        }
    }
|   VALUES '(' insert_column ')'
    {
        column := tree.NewUnresolvedColName($3)
        name := tree.NewUnresolvedColName($1)
    	$$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{column},
        }
    }


trim_direction:
    BOTH
|   LEADING
|   TRAILING

substr_option:
    SUBSTRING
|   SUBSTR
|   MID

time_unit:
    time_stamp_unit
    {
        $$ = $1
    }
|    SECOND_MICROSECOND
|    MINUTE_MICROSECOND
|    MINUTE_SECOND
|    HOUR_MICROSECOND
|    HOUR_SECOND
|    HOUR_MINUTE
|    DAY_MICROSECOND
|    DAY_SECOND
|    DAY_MINUTE
|    DAY_HOUR
|    YEAR_MONTH

time_stamp_unit:
    MICROSECOND
|    SECOND
|    MINUTE
|    HOUR
|    DAY
|    WEEK
|    MONTH
|    QUARTER
|    YEAR
|    SQL_TSI_SECOND
|    SQL_TSI_MINUTE
|    SQL_TSI_HOUR
|    SQL_TSI_DAY
|    SQL_TSI_WEEK
|    SQL_TSI_MONTH
|    SQL_TSI_QUARTER
|    SQL_TSI_YEAR

function_call_nonkeyword:
    CURTIME datetime_scale
    {
        name := tree.NewUnresolvedColName($1)
        var es tree.Exprs = nil
        if $2 != nil {
            es = append(es, $2)
        }
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: es,
        }
    }
|   SYSDATE datetime_scale
    {
        name := tree.NewUnresolvedColName($1)
        var es tree.Exprs = nil
        if $2 != nil {
            es = append(es, $2)
        }
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: es,
        }
    }
|	TIMESTAMPDIFF '(' time_stamp_unit ',' expression ',' expression ')'
	{
        name := tree.NewUnresolvedColName($1)
        str := strings.ToLower($3)
        arg1 := tree.NewNumValWithType(constant.MakeString(str), str, false, tree.P_char)
		$$ =  &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{arg1, $5, $7},
        }
	}
function_call_keyword:
    name_confict '(' expression_list_opt ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
        }
    }
|   name_braces braces_opt
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
        }
    }
|   SCHEMA '('')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
        }
    }
|   name_datetime_scale datetime_scale_opt
    {
        name := tree.NewUnresolvedColName($1)
        var es tree.Exprs = nil
        if $2 != nil {
            es = append(es, $2)
        }
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: es,
        }
    }
|   BINARY '(' expression_list ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
        }
    }
|   BINARY literal
    {
        name := tree.NewUnresolvedColName($1)
        exprs := make([]tree.Expr, 1)
        exprs[0] = $2
        $$ = &tree.FuncExpr{
           Func: tree.FuncName2ResolvableFunctionReference(name),
           FuncName: tree.NewCStr($1, 1),
           Exprs: exprs,
        }
    }
|   BINARY column_name
    {
        name := tree.NewUnresolvedColName($1)
        exprs := make([]tree.Expr, 1)
        exprs[0] = $2
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: exprs,
        }
    }
|   CHAR '(' expression_list ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
        }
    }
|   CHAR '(' expression_list USING charset_name ')'
    {
        cn := tree.NewNumValWithType(constant.MakeString($5), $5, false, tree.P_char)
        es := $3
        es = append(es, cn)
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: es,
        }
    }
|   DATE STRING
    {
        val := tree.NewNumValWithType(constant.MakeString($2), $2, false, tree.P_char)
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{val},
        }
    }
|   TIME STRING
    {
        val := tree.NewNumValWithType(constant.MakeString($2), $2, false, tree.P_char)
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{val},
        }
    }
|   INSERT '(' expression_list_opt ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
        }
    }
|   MOD '(' bit_expr ',' bit_expr ')'
    {
        es := tree.Exprs{$3}
        es = append(es, $5)
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: es,
        }
    }
|   PASSWORD '(' expression_list_opt ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
        }
    }
|   TIMESTAMP STRING
    {
        val := tree.NewNumValWithType(constant.MakeString($2), $2, false, tree.P_char)
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{val},
        }
    }
|   BITMAP_BIT_POSITION '(' expression_list_opt ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
        }
    }
|   BITMAP_BUCKET_NUMBER '(' expression_list_opt ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
        }
    }
|   BITMAP_COUNT '(' expression_list_opt ')'
    {
        name := tree.NewUnresolvedColName($1)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: $3,
        }
    }

datetime_scale_opt:
    {
        $$ = nil
    }
|   datetime_scale
    {
        $$ = $1
    }

datetime_scale:
   '(' ')'
    {
        $$ = nil
    }
|   '(' INTEGRAL ')'
    {
        ival, errStr := util.GetInt64($2)
        if errStr != "" {
            yylex.Error(errStr)
            goto ret1
        }
        str := fmt.Sprintf("%v", $2)
        $$ = tree.NewNumValWithType(constant.MakeInt64(ival), str, false, tree.P_int64)
    }

name_datetime_scale:
    CURRENT_TIME
|   CURRENT_TIMESTAMP
|   LOCALTIME
|   LOCALTIMESTAMP
|   UTC_TIME
|   UTC_TIMESTAMP

braces_opt:
    {}
|   '(' ')'
    {}

name_braces:
    CURRENT_USER
|   CURRENT_DATE
|   CURRENT_ROLE
|   UTC_DATE

name_confict:
    ASCII
|   CHARSET
|   COALESCE
|   COLLATION
|   DATE
|   DATABASE
|   DAY
|   HOUR
|   IF
|   INTERVAL
|   FORMAT
|   LEFT
|   MICROSECOND
|   MINUTE
|   MONTH
|   QUARTER
|   REPEAT
|   REPLACE
|   REVERSE
|   RIGHT
|   ROW_COUNT
|   SECOND
|   TIME
|   TIMESTAMP
|   TRUNCATE
|   USER
|   WEEK
|   YEAR
|   UUID

interval_expr:
    INTERVAL expression time_unit
    {
        name := tree.NewUnresolvedColName($1)
        str := strings.ToLower($3)
        arg2 := tree.NewNumValWithType(constant.MakeString(str), str, false, tree.P_char)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr($1, 1),
            Exprs: tree.Exprs{$2, arg2},
        }
    }

func_type_opt:
    {
        $$ = tree.FUNC_TYPE_DEFAULT
    }
|   DISTINCT
    {
        $$ = tree.FUNC_TYPE_DISTINCT
    }
|   ALL
    {
        $$ = tree.FUNC_TYPE_ALL
    }

tuple_expression:
    '(' expression_list ')'
    {
        $$ = tree.NewTuple($2)
    }

expression_list_opt:
    {
        $$ = nil
    }
|   expression_list
    {
        $$ = $1
    }

value_expression_list:
    value_expression
    {
        $$ = tree.Exprs{$1}
    }
|   value_expression_list ',' value_expression
    {
        $$ = append($1, $3)
    }

expression_list:
    expression
    {
        $$ = tree.Exprs{$1}
    }
|   expression_list ',' expression
    {
        $$ = append($1, $3)
    }

// See https://dev.mysql.com/doc/refman/8.0/en/expressions.html
expression:
    expression AND expression %prec AND
    {
        $$ = tree.NewAndExpr($1, $3)
    }
|   expression OR expression %prec OR
    {
        $$ = tree.NewOrExpr($1, $3)
    }
|   expression PIPE_CONCAT expression %prec PIPE_CONCAT
    {
        name := tree.NewUnresolvedColName("concat")
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            FuncName: tree.NewCStr("concat", 1),
            Exprs: tree.Exprs{$1, $3},
        }
    }
|   expression XOR expression %prec XOR
    {
        $$ = tree.NewXorExpr($1, $3)
    }
|   NOT expression %prec NOT
    {
        $$ = tree.NewNotExpr($2)
    }
|   boolean_primary
    {
        $$ = $1
    }

value_expression:
    expression {
        $$ = $1
    }
|   MAXVALUE
    {
        $$ = tree.NewMaxValue()
    }

boolean_primary:
    boolean_primary IS NULL %prec IS
    {
        $$ = tree.NewIsNullExpr($1)
    }
|   boolean_primary IS NOT NULL %prec IS
    {
        $$ = tree.NewIsNotNullExpr($1)
    }
|   boolean_primary IS UNKNOWN %prec IS
    {
        $$ = tree.NewIsUnknownExpr($1)
    }
|   boolean_primary IS NOT UNKNOWN %prec IS
    {
        $$ = tree.NewIsNotUnknownExpr($1)
    }
|   boolean_primary IS TRUE %prec IS
    {
        $$ = tree.NewIsTrueExpr($1)
    }
|   boolean_primary IS NOT TRUE %prec IS
    {
        $$ = tree.NewIsNotTrueExpr($1)
    }
|   boolean_primary IS FALSE %prec IS
    {
        $$ = tree.NewIsFalseExpr($1)
    }
|   boolean_primary IS NOT FALSE %prec IS
    {
        $$ = tree.NewIsNotFalseExpr($1)
    }
|   boolean_primary comparison_operator predicate %prec '='
    {
        $$ = tree.NewComparisonExpr($2, $1, $3)
    }
|   boolean_primary comparison_operator and_or_some subquery %prec '='
    {
        $$ = tree.NewSubqueryComparisonExpr($2, $3, $1, $4)
        $$ = tree.NewSubqueryComparisonExpr($2, $3, $1, $4)
    }
|   predicate

predicate:
    bit_expr IN col_tuple
    {
        $$ = tree.NewComparisonExpr(tree.IN, $1, $3)
    }
|   bit_expr NOT IN col_tuple
    {
        $$ = tree.NewComparisonExpr(tree.NOT_IN, $1, $4)
    }
|   bit_expr LIKE simple_expr like_escape_opt
    {
        $$ = tree.NewComparisonExprWithEscape(tree.LIKE, $1, $3, $4)
    }
|   bit_expr NOT LIKE simple_expr like_escape_opt
    {
        $$ = tree.NewComparisonExprWithEscape(tree.NOT_LIKE, $1, $4, $5)
    }
|   bit_expr ILIKE simple_expr like_escape_opt
    {
        $$ = tree.NewComparisonExprWithEscape(tree.ILIKE, $1, $3, $4)
    }
|   bit_expr NOT ILIKE simple_expr like_escape_opt
    {
        $$ = tree.NewComparisonExprWithEscape(tree.NOT_ILIKE, $1, $4, $5)
    }
|   bit_expr REGEXP bit_expr
    {
        $$ = tree.NewComparisonExpr(tree.REG_MATCH, $1, $3)
    }
|   bit_expr NOT REGEXP bit_expr
    {
        $$ = tree.NewComparisonExpr(tree.NOT_REG_MATCH, $1, $4)
    }
|   bit_expr BETWEEN bit_expr AND predicate
    {
        $$ = tree.NewRangeCond(false, $1, $3, $5)
    }
|   bit_expr NOT BETWEEN bit_expr AND predicate
    {
        $$ = tree.NewRangeCond(true, $1, $4, $6)
    }
|   bit_expr

like_escape_opt:
    {
        $$ = nil
    }
|   ESCAPE simple_expr
    {
        $$ = $2
    }

col_tuple:
    tuple_expression
    {
        $$ = $1
    }
|   subquery
    {
        $$ = $1
    }
// |   LIST_ARG

and_or_some:
    ALL
    {
        $$ = tree.ALL
    }
|    ANY
    {
        $$ = tree.ANY
    }
|    SOME
    {
        $$ = tree.SOME
    }

comparison_operator:
    '='
    {
        $$ = tree.EQUAL
    }
|   '<'
    {
        $$ = tree.LESS_THAN
    }
|   '>'
    {
        $$ = tree.GREAT_THAN
    }
|   LE
    {
        $$ = tree.LESS_THAN_EQUAL
    }
|   GE
    {
        $$ = tree.GREAT_THAN_EQUAL
    }
|   NE
    {
        $$ = tree.NOT_EQUAL
    }
|   NULL_SAFE_EQUAL
    {
        $$ = tree.NULL_SAFE_EQUAL
    }

keys:
    PRIMARY KEY
    {
        $$ = tree.NewAttributePrimaryKey()
    }
|   UNIQUE KEY
    {
        $$ = tree.NewAttributeUniqueKey()
    }
|   UNIQUE
    {
        $$ = tree.NewAttributeUnique()
    }
|   KEY
    {
        $$ = tree.NewAttributeKey()
    }

num_literal:
    INTEGRAL
    {
        str := fmt.Sprintf("%v", $1)
        switch v := $1.(type) {
        case uint64:
            $$ = tree.NewNumValWithType(constant.MakeUint64(v), str, false, tree.P_uint64)
        case int64:
            $$ = tree.NewNumValWithType(constant.MakeInt64(v), str, false, tree.P_int64)
        default:
            yylex.Error("parse integral fail")
            goto ret1
        }
    }
|   FLOAT
    {
        fval := $1.(float64)
        $$ = tree.NewNumValWithType(constant.MakeFloat64(fval), yylex.(*Lexer).scanner.LastToken, false, tree.P_float64)
    }
|   DECIMAL_VALUE
    {
        $$ = tree.NewNumValWithType(constant.MakeString($1), $1, false, tree.P_decimal)
    }

literal:
    STRING
    {
        $$ = tree.NewNumValWithType(constant.MakeString($1), $1, false, tree.P_char)
    }
|   INTEGRAL
    {
        str := fmt.Sprintf("%v", $1)
        switch v := $1.(type) {
        case uint64:
            $$ = tree.NewNumValWithType(constant.MakeUint64(v), str, false, tree.P_uint64)
        case int64:
            $$ = tree.NewNumValWithType(constant.MakeInt64(v), str, false, tree.P_int64)
        default:
            yylex.Error("parse integral fail")
            goto ret1
        }
    }
|   FLOAT
    {
        fval := $1.(float64)
        $$ = tree.NewNumValWithType(constant.MakeFloat64(fval), yylex.(*Lexer).scanner.LastToken, false, tree.P_float64)
    }
|   TRUE
    {
        $$ = tree.NewNumValWithType(constant.MakeBool(true), "true", false, tree.P_bool)
    }
|   FALSE
    {
        $$ = tree.NewNumValWithType(constant.MakeBool(false), "false", false, tree.P_bool)
    }
|   NULL
    {
        $$ = tree.NewNumValWithType(constant.MakeUnknown(), "null", false, tree.P_null)
    }
|   HEXNUM
    {
        $$ = tree.NewNumValWithType(constant.MakeString($1), $1, false, tree.P_hexnum)
    }
|   UNDERSCORE_BINARY HEXNUM
    {
        if strings.HasPrefix($2, "0x") {
            $2 = $2[2:]
        }
        $$ = tree.NewNumValWithType(constant.MakeString($2), $2, false, tree.P_bit)
    }
|   DECIMAL_VALUE
    {
        $$ = tree.NewNumValWithType(constant.MakeString($1), $1, false, tree.P_decimal)
    }
|   BIT_LITERAL
    {
        $$ = tree.NewNumValWithType(constant.MakeString($1), $1, false, tree.P_bit)
    }
|   VALUE_ARG
    {
        $$ = tree.NewParamExpr(yylex.(*Lexer).GetParamIndex())
    }
|   UNDERSCORE_BINARY STRING
    {
        $$ = tree.NewNumValWithType(constant.MakeString($2), $2, false, tree.P_ScoreBinary)
    }


column_type:
    numeric_type unsigned_opt zero_fill_opt
    {
        $$ = $1
        $$.InternalType.Unsigned = $2
        $$.InternalType.Zerofill = $3
    }
|   char_type
|   time_type
|   spatial_type

numeric_type:
    int_type length_opt
    {
        $$ = $1
        $$.InternalType.DisplayWith = $2
    }
|   decimal_type
    {
        $$ = $1
    }

int_type:
    BIT
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.BitFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_BIT),
            },
        }
    }
|   BOOL
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.BoolFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:uint32(defines.MYSQL_TYPE_BOOL),
            },
        }
    }
|   BOOLEAN
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.BoolFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:uint32(defines.MYSQL_TYPE_BOOL),
            },
        }
    }
|   INT1
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $1,
                Width:  8,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_TINY),
            },
        }
    }
|   TINYINT
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $1,
                Width:  8,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_TINY),
            },
        }
    }
|   INT2
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $1,
                Width:  16,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_SHORT),
            },
        }
    }
|   SMALLINT
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $1,
                Width:  16,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_SHORT),
            },
        }
    }
|   INT3
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $1,
                Width:  24,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_INT24),
            },
        }
    }
|   MEDIUMINT
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $1,
                Width:  24,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_INT24),
            },
        }
    }
|   INT4
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $1,
                Width:  32,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_LONG),
            },
        }
    }
|   INT
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $1,
                Width:  32,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_LONG),
            },
        }
    }
|   INTEGER
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $1,
                Width:  32,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_LONG),
            },
        }
    }
|   INT8
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $1,
                Width:  64,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_LONGLONG),
            },
        }
    }
|   BIGINT
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $1,
                Width:  64,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_LONGLONG),
            },
        }
    }

decimal_type:
    DOUBLE float_length_opt
    {
        locale := ""
        if $2.DisplayWith > 255 {
            yylex.Error("Display width for double out of range (max = 255)")
            goto ret1
        }
        if $2.Scale > 30 {
            yylex.Error("Display scale for double out of range (max = 30)")
            goto ret1
        }
        if $2.Scale != tree.NotDefineDec && $2.Scale > $2.DisplayWith {
            yylex.Error("For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column 'a'))")
                goto ret1
        }
        $$ = &tree.T{
            InternalType: tree.InternalType{
        		Family: tree.FloatFamily,
                FamilyString: $1,
        		Width:  64,
        		Locale: &locale,
       			Oid: uint32(defines.MYSQL_TYPE_DOUBLE),
                DisplayWith: $2.DisplayWith,
                Scale: $2.Scale,
        	},
        }
    }
|   FLOAT_TYPE float_length_opt
    {
        locale := ""
        if $2.DisplayWith > 255 {
            yylex.Error("Display width for float out of range (max = 255)")
            goto ret1
        }
        if $2.Scale > 30 {
            yylex.Error("Display scale for float out of range (max = 30)")
            goto ret1
        }
        if $2.Scale != tree.NotDefineDec && $2.Scale > $2.DisplayWith {
        	yylex.Error("For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column 'a'))")
        	goto ret1
        }
        if $2.DisplayWith >= 24 {
            $$ = &tree.T{
            	InternalType: tree.InternalType{
            		Family: tree.FloatFamily,
            		FamilyString: $1,
            		Width:  64,
            		Locale: &locale,
           			Oid:    uint32(defines.MYSQL_TYPE_DOUBLE),
            		DisplayWith: $2.DisplayWith,
            		Scale: $2.Scale,
            	},
            }
        } else {
            $$ = &tree.T{
            	InternalType: tree.InternalType{
            		Family: tree.FloatFamily,
            		FamilyString: $1,
            		Width:  32,
            		Locale: &locale,
            		Oid:    uint32(defines.MYSQL_TYPE_FLOAT),
            		DisplayWith: $2.DisplayWith,
            		Scale: $2.Scale,
            	},
            }
        }
    }

|   DECIMAL decimal_length_opt
    {
        locale := ""
        if $2.Scale != tree.NotDefineDec && $2.Scale > $2.DisplayWith {
        yylex.Error("For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column 'a'))")
        goto ret1
        }
        if $2.DisplayWith > 38 || $2.DisplayWith < 0 {
            yylex.Error("For decimal(M), M must between 0 and 38.")
                goto ret1
        } else if $2.DisplayWith <= 16 {
            $$ = &tree.T{
            InternalType: tree.InternalType{
            Family: tree.FloatFamily,
            FamilyString: $1,
            Width:  64,
            Locale: &locale,
            Oid:    uint32(defines.MYSQL_TYPE_DECIMAL),
            DisplayWith: $2.DisplayWith,
            Scale: $2.Scale,
            },
        }
        } else {
            $$ = &tree.T{
            InternalType: tree.InternalType{
            Family: tree.FloatFamily,
            FamilyString: $1,
            Width:  128,
            Locale: &locale,
            Oid:    uint32(defines.MYSQL_TYPE_DECIMAL),
            DisplayWith: $2.DisplayWith,
            Scale: $2.Scale,
            },
                }
        }
    }
|   NUMERIC decimal_length_opt
    {
        locale := ""
        if $2.Scale != tree.NotDefineDec && $2.Scale > $2.DisplayWith {
        yylex.Error("For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column 'a'))")
        goto ret1
        }
        if $2.DisplayWith > 38 || $2.DisplayWith < 0 {
            yylex.Error("For decimal(M), M must between 0 and 38.")
                goto ret1
        } else if $2.DisplayWith <= 16 {
            $$ = &tree.T{
            InternalType: tree.InternalType{
            Family: tree.FloatFamily,
            FamilyString: $1,
            Width:  64,
            Locale: &locale,
            Oid:    uint32(defines.MYSQL_TYPE_DECIMAL),
            DisplayWith: $2.DisplayWith,
            Scale: $2.Scale,
            },
        }
        } else {
            $$ = &tree.T{
            InternalType: tree.InternalType{
            Family: tree.FloatFamily,
            FamilyString: $1,
            Width:  128,
            Locale: &locale,
            Oid:    uint32(defines.MYSQL_TYPE_DECIMAL),
            DisplayWith: $2.DisplayWith,
            Scale: $2.Scale,
            },
                }
        }
    }
|   REAL float_length_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.FloatFamily,
                FamilyString: $1,
                Width:  64,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_DOUBLE),
                DisplayWith: $2.DisplayWith,
                Scale: $2.Scale,
            },
        }
    }

time_type:
    DATE
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.DateFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_DATE),
            },
        }
    }
|   TIME timestamp_option_opt
    {
        locale := ""
        if $2 < 0 || $2 > 6 {
            yylex.Error("For Time(fsp), fsp must in [0, 6]")
            goto ret1
        } else {
            $$ = &tree.T{
                InternalType: tree.InternalType{
                Family: tree.TimeFamily,
                Scale: $2,
                FamilyString: $1,
                DisplayWith: $2,
                TimePrecisionIsSet: true,
                Locale: &locale,
                Oid: uint32(defines.MYSQL_TYPE_TIME),
                },
            }
        }
    }
|   TIMESTAMP timestamp_option_opt
    {
        locale := ""
        if $2 < 0 || $2 > 6 {
            yylex.Error("For Timestamp(fsp), fsp must in [0, 6]")
            goto ret1
        } else {
            $$ = &tree.T{
                InternalType: tree.InternalType{
                Family: tree.TimestampFamily,
                Scale: $2,
                FamilyString: $1,
                DisplayWith: $2,
                TimePrecisionIsSet: true,
                Locale:  &locale,
                Oid: uint32(defines.MYSQL_TYPE_TIMESTAMP),
                },
            }
        }
    }
|   DATETIME timestamp_option_opt
    {
        locale := ""
        if $2 < 0 || $2 > 6 {
            yylex.Error("For Datetime(fsp), fsp must in [0, 6]")
            goto ret1
        } else {
            $$ = &tree.T{
                InternalType: tree.InternalType{
                Family: tree.TimestampFamily,
                Scale: $2,
                FamilyString: $1,
                DisplayWith: $2,
                TimePrecisionIsSet: true,
                Locale: &locale,
                Oid: uint32(defines.MYSQL_TYPE_DATETIME),
                },
            }
        }
    }
|   YEAR length_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.IntFamily,
                FamilyString: $1,
                DisplayWith: $2,
                Width:  16,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_YEAR),
            },
        }
    }

char_type:
    CHAR length_option_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.StringFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_STRING),
                DisplayWith: $2,
            },
        }
    }
|   VARCHAR length_option_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.StringFamily,
                Locale: &locale,
                FamilyString: $1,
                DisplayWith: $2,
                Oid:    uint32(defines.MYSQL_TYPE_VARCHAR),
            },
        }
    }
|   BINARY length_option_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.StringFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_VARCHAR),
                DisplayWith: $2,
            },
        }
    }
|   VARBINARY length_option_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.StringFamily,
                Locale: &locale,
                FamilyString: $1,
                DisplayWith: $2,
                Oid:    uint32(defines.MYSQL_TYPE_VARCHAR),
            },
        }
    }
|   DATALINK
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.BlobFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_TEXT),
            },
        }
    }
|   TEXT
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.BlobFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_TEXT),
            },
        }
    }
|   TINYTEXT
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.BlobFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_TEXT),
            },
        }
    }
|   MEDIUMTEXT
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.BlobFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_TEXT),
            },
        }
    }
|   LONGTEXT
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.BlobFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_TEXT),
            },
        }
    }
|   BLOB
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.BlobFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_BLOB),
            },
        }
    }
|   TINYBLOB
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.BlobFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_TINY_BLOB),
            },
        }
    }
|   MEDIUMBLOB
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.BlobFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_MEDIUM_BLOB),
            },
        }
    }
|   LONGBLOB
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.BlobFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:    uint32(defines.MYSQL_TYPE_LONG_BLOB),
            },
        }
    }
|   JSON
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.JsonFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:uint32(defines.MYSQL_TYPE_JSON),
            },
        }
    }
|   VECF32 length_option_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.ArrayFamily,
                Locale: &locale,
                FamilyString: $1,
                DisplayWith: $2,
                Oid:uint32(defines.MYSQL_TYPE_VARCHAR),
            },
        }
    }
|   VECF64 length_option_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.ArrayFamily,
                Locale: &locale,
                FamilyString: $1,
                DisplayWith: $2,
                Oid:uint32(defines.MYSQL_TYPE_VARCHAR),
            },
        }
    }
| ENUM '(' enum_values ')'
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.EnumFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:uint32(defines.MYSQL_TYPE_ENUM),
                EnumValues: $3,
            },
        }
    }
|   SET '(' enum_values ')'
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.SetFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:uint32(defines.MYSQL_TYPE_SET),
                EnumValues: $3,
            },
        }
    }
|  UUID
    {
       locale := ""
        $$ = &tree.T{
         InternalType: tree.InternalType{
        Family: tree.UuidFamily,
           FamilyString: $1,
        Width:  128,
        Locale: &locale,
        Oid:    uint32(defines.MYSQL_TYPE_UUID),
    },
    }
}

do_stmt:
    DO expression_list
    {
        $$ = &tree.Do {
            Exprs: $2,
        }
    }

declare_stmt:
    DECLARE var_name_list column_type
    {
        $$ = &tree.Declare {
            Variables: $2,
            ColumnType: $3,
            DefaultVal: tree.NewNumValWithType(constant.MakeUnknown(), "null", false, tree.P_null),
        }
    }
    |
    DECLARE var_name_list column_type DEFAULT expression
    {
        $$ = &tree.Declare {
            Variables: $2,
            ColumnType: $3,
            DefaultVal: $5,
        }
    }

spatial_type:
    GEOMETRY
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
                Family: tree.GeometryFamily,
                FamilyString: $1,
                Locale: &locale,
                Oid:uint32(defines.MYSQL_TYPE_GEOMETRY),
            },
        }
    }
// |   POINT
// |   LINESTRING
// |   POLYGON
// |   GEOMETRYCOLLECTION
// |   MULTIPOINT
// |   MULTILINESTRING
// |   MULTIPOLYGON

// TODO:
// need to encode SQL string
enum_values:
    STRING
    {
        $$ = make([]string, 0, 4)
        $$ = append($$, $1)
    }
|   enum_values ',' STRING
    {
        $$ = append($1, $3)
    }

length_opt:
    /* EMPTY */
    {
        $$ = 0
    }
|    length

timestamp_option_opt:
    /* EMPTY */
        {
            $$ = 0
        }
|    '(' INTEGRAL ')'
    {
        $$ = int32($2.(int64))
    }

length_option_opt:
    {
        $$ = int32(-1)
    }
|    '(' INTEGRAL ')'
    {
        $$ = int32($2.(int64))
    }

length:
   '(' INTEGRAL ')'
    {
        $$ = tree.GetDisplayWith(int32($2.(int64)))
    }

float_length_opt:
    /* EMPTY */
    {
        $$ = tree.LengthScaleOpt{
            DisplayWith: tree.NotDefineDisplayWidth,
            Scale: tree.NotDefineDec,
        }
    }
|   '(' INTEGRAL ')'
    {
        $$ = tree.LengthScaleOpt{
            DisplayWith: tree.GetDisplayWith(int32($2.(int64))),
            Scale: tree.NotDefineDec,
        }
    }
|   '(' INTEGRAL ',' INTEGRAL ')'
    {
        $$ = tree.LengthScaleOpt{
            DisplayWith: tree.GetDisplayWith(int32($2.(int64))),
            Scale: int32($4.(int64)),
        }
    }

decimal_length_opt:
    /* EMPTY */
    {
        $$ = tree.LengthScaleOpt{
            DisplayWith: 38,           // this is the default precision for decimal
            Scale: 0,
        }
    }
|   '(' INTEGRAL ')'
    {
        $$ = tree.LengthScaleOpt{
            DisplayWith: tree.GetDisplayWith(int32($2.(int64))),
            Scale: 0,
        }
    }
|   '(' INTEGRAL ',' INTEGRAL ')'
    {
        $$ = tree.LengthScaleOpt{
            DisplayWith: tree.GetDisplayWith(int32($2.(int64))),
            Scale: int32($4.(int64)),
        }
    }

unsigned_opt:
    /* EMPTY */
    {
        $$ = false
    }
|   UNSIGNED
    {
        $$ = true
    }
|   SIGNED
    {
        $$ = false
    }

zero_fill_opt:
    /* EMPTY */
    {}
|   ZEROFILL
    {
        $$ = true
    }

charset_keyword:
    CHARSET
|   CHARACTER SET
|   CHAR SET

equal_opt:
    {
        $$ = ""
    }
|   '='
    {
        $$ = string($1)
    }

//sql_id:
//    id_or_var
//|   non_reserved_keyword

//reserved_sql_id:
//    sql_id
//|   reserved_keyword

//reserved_table_id:
//    table_id
//|   reserved_keyword

//reserved_keyword:
//    ADD
//|   ALL
//|   AND
//|   AS
//|   ASC
//|   ASCII
//|   AUTO_INCREMENT
//|   BETWEEN
//|   BINARY
//|   BY
//|   CASE
//|   CHAR
//|   COLLATE
//|   COLLATION
//|   CONVERT
//|   COALESCE
//|   CREATE
//|   CROSS
//|   CURRENT_DATE
//|   CURRENT_ROLE
//|   CURRENT_USER
//|   CURRENT_TIME
//|   CURRENT_TIMESTAMP
//|   CIPHER
//|   SAN
//|   SSL
//|   SUBJECT
//|   DATABASE
//|   DATABASES
//|   DEFAULT
//|   DELETE
//|   DESC
//|   DESCRIBE
//|   DISTINCT
//|   DISTINCTROW
//|   DIV
//|   DROP
//|   ELSE
//|   END
//|   ESCAPE
//|   EXISTS
//|   EXPLAIN
//|   FALSE
//|   FIRST
//|   AFTER
//|   FOR
//|   FORCE
//|   FROM
//|   GROUP
//|   HAVING
//|   HOUR
//|   IDENTIFIED
//|   IF
//|   IGNORE
//|   IN
//|   INFILE
//|   INDEX
//|   INNER
//|   INSERT
//|   INTERVAL
//|   INTO
//|   IS
//|   ISSUER
//|   JOIN
//|   KEY
//|   LAST
//|   LEFT
//|   LIKE
//|	ILIKE
//|   LIMIT
//|   LOCALTIME
//|   LOCALTIMESTAMP
//|   LOCK
//|   LOAD
//|   IMPORT
//|   MATCH
//|   MAXVALUE
//|   MOD
//|   MICROSECOND
//|   MINUTE
//|   NATURAL
//|   NOT
//|   NONE
//|   NULL
//|   NULLS
//|   ON
//|   OR
//|   ORDER
//|   OUTER
//|   REGEXP
//|   RENAME
//|   REPLACE
//|   RIGHT
//|   REQUIRE
//|   REPEAT
//|   ROW_COUNT
//|    REFERENCES
//|   RECURSIVE
//|   REVERSE
//|   SCHEMA
//|   SCHEMAS
//|   SELECT
//|   SECOND
//|   SEPARATOR
//|   SET
//|   SHOW
//|   STRAIGHT_JOIN
//|   TABLE
//|   THEN
//|   TO
//|   TRUE
//|   TRUNCATE
//|   UNION
//|   UNIQUE
//|   UPDATE
//|   USE
//|   USING
//|   UTC_DATE
//|   UTC_TIME
//|   UTC_TIMESTAMP
//|   VALUES
//|   WHEN
//|   WHERE
//|   WEEK
//|   WITH
//|   TERMINATED
//|   OPTIONALLY
//|   ENCLOSED
//|   ESCAPED
//|   STARTING
//|   LINES
//|   ROWS
//|   INT1
//|   INT2
//|   INT3
//|   INT4
//|   INT8
//|   CHECK
//|    CONSTRAINT
//|   PRIMARY
//|   FULLTEXT
//|   FOREIGN
//|    ROW
//|   OUTFILE
//|    SQL_SMALL_RESULT
//|    SQL_BIG_RESULT
//|    LEADING
//|    TRAILING
//|   CHARACTER
//|    LOW_PRIORITY
//|    HIGH_PRIORITY
//|    DELAYED
//|   PARTITION
//|    QUICK
//|   EXCEPT
//|   ADMIN_NAME
//|   RANDOM
//|   SUSPEND
//|   REUSE
//|   CURRENT
//|   OPTIONAL
//|   FAILED_LOGIN_ATTEMPTS
//|   PASSWORD_LOCK_TIME
//|   UNBOUNDED
//|   SECONDARY
//|   DECLARE
//|   MODUMP
//|   OVER
//|   PRECEDING
//|   FOLLOWING
//|   GROUPS
//|   LOCKS
//|   TABLE_NUMBER
//|   COLUMN_NUMBER
//|   TABLE_VALUES
//|   RETURNS
//|   MYSQL_COMPATIBILITY_MODE

non_reserved_keyword:
    ACCOUNT
|   ACCOUNTS
|   AGAINST
|   AVG_ROW_LENGTH
|   AUTO_RANDOM
|   ATTRIBUTE
|   ACTION
|   ALGORITHM
|   BEGIN
|   BIGINT
|   BIT
|   BLOB
|   BOOL
|   CANCEL
|   CHAIN
|   CHECKSUM
|   CLUSTER
|   COMPRESSION
|   COMMENT_KEYWORD
|   COMMIT
|   COMMITTED
|   CHARSET
|   COLUMNS
|   CONNECTION
|   CONSISTENT
|   COMPRESSED
|   COMPACT
|   COLUMN_FORMAT
|   CONNECTOR
|   CONNECTORS
|	COLLATION
|   SECONDARY_ENGINE_ATTRIBUTE
|   STREAM
|   ENGINE_ATTRIBUTE
|   INSERT_METHOD
|   CASCADE
|   DAEMON
|   DATA
|	DAY
|   DATETIME
|   DECIMAL
|   DYNAMIC
|   DISK
|   DO
|   DOUBLE
|   DIRECTORY
|   DUPLICATE
|   DELAY_KEY_WRITE
|   ENUM
|   ENCRYPTION
|   ENGINE
|   EXPANSION
|   EXTENDED
|   EXPIRE
|   ERRORS
|   ENFORCED
|	ENABLE
|   FORMAT
|   FLOAT_TYPE
|   FULL
|   FIXED
|   FIELDS
|   GEOMETRY
|   GEOMETRYCOLLECTION
|   GLOBAL
|   PERSIST
|   GRANT
|   INT
|   INTEGER
|   INDEXES
|   ISOLATION
|   JSON
|   VECF32
|   VECF64
|   KEY_BLOCK_SIZE
|   LISTS
|   OP_TYPE
|   KEYS
|   LANGUAGE
|   LESS
|   LEVEL
|   LINESTRING
|   LONGBLOB
|   LONGTEXT
|   LOCAL
|   LINEAR
|   LIST
|   MEDIUMBLOB
|   MEDIUMINT
|   MEDIUMTEXT
|   MEMORY
|   MODE
|   MULTILINESTRING
|   MULTIPOINT
|   MULTIPOLYGON
|   MAX_QUERIES_PER_HOUR
|   MAX_UPDATES_PER_HOUR
|   MAX_CONNECTIONS_PER_HOUR
|   MAX_USER_CONNECTIONS
|   MAX_ROWS
|   MIN_ROWS
|   MONTH
|   NAMES
|   NCHAR
|   NUMERIC
|   NEVER
|   NO
|   OFFSET
|   ONLY
|   OPTIMIZE
|   OPEN
|   OPTION
|   PACK_KEYS
|   PARTIAL
|   PARTITIONS
|   POINT
|   POLYGON
|   PROCEDURE
|   PROXY
|   QUERY
|   PAUSE
|   PROFILES
|   ROLE
|   RANGE
|   READ
|   REAL
|   REORGANIZE
|   REDUNDANT
|   REPAIR
|   REPEATABLE
|   RELEASE
|   RESUME
|   REVOKE
|   REPLICATION
|   ROW_FORMAT
|   ROLLBACK
|   RESTRICT
|   SESSION
|   SERIALIZABLE
|   SHARE
|   SIGNED
|   SMALLINT
|   SNAPSHOT
|   PITR
|   SPATIAL
|   START
|   STATUS
|   STORAGE
|   STATS_AUTO_RECALC
|   STATS_PERSISTENT
|   STATS_SAMPLE_PAGES
|	SOURCE
|   SUBPARTITIONS
|   SUBPARTITION
|   SIMPLE
|   TASK
|   TEXT
|   THAN
|   TINYBLOB
|   TIME %prec LOWER_THAN_STRING
|   TINYINT
|   TINYTEXT
|   TRANSACTION
|   TRIGGER
|   UNCOMMITTED
|   UNSIGNED
|   UNUSED
|   UNLOCK
|   USER
|   VARBINARY
|   VARCHAR
|   VARIABLES
|   VIEW
|   WRITE
|   WARNINGS
|   WORK
|   X509
|   ZEROFILL
|   YEAR
|   TYPE
|   HEADER
|   MAX_FILE_SIZE
|   FORCE_QUOTE
|   QUARTER
|   UNKNOWN
|   ANY
|   SOME
|   TIMESTAMP %prec LOWER_THAN_STRING
|   DATE %prec LOWER_THAN_STRING
|   TABLES
|   SEQUENCES
|   URL
|   PASSWORD %prec LOWER_THAN_EQ
|   HASH
|   ENGINES
|   TRIGGERS
|   HISTORY
|   LOW_CARDINALITY
|   S3OPTION
|   STAGEOPTION
|   EXTENSION
|   NODE
|   ROLES
|   UUID
|   PARALLEL
|   INCREMENT
|   CYCLE
|   MINVALUE
|	PROCESSLIST
|   PUBLICATION
|   SUBSCRIPTIONS
|   PUBLICATIONS
|   PROPERTIES
|	WEEK
|   DEFINER
|   SQL
|   STAGE
|   SNAPSHOTS
|   STAGES
|   BACKUP
|   RESTORE
|   FILESYSTEM
|   PARALLELISM
|	VALUE
|	REFERENCE
|	MODIFY
|	ASCII
|	AUTO_INCREMENT
|	AUTOEXTEND_SIZE
|	BSI
|	BINDINGS
|	BOOLEAN
|   BTREE
|	IVFFLAT
|	MASTER
|	COALESCE
|	CONNECT
|	CIPHER
|	CLIENT
|	SAN
|	SUBJECT
|	INSTANT
|	INPLACE
|	COPY
|	UNDEFINED
|	MERGE
|	TEMPTABLE
|	INVOKER
|	SECURITY
|	CASCADED
|	DISABLE
|	DRAINER
|	EXECUTE
|	EVENT
|	EVENTS
|	FIRST
|	AFTER
|	FILE
|	GRANTS
|	HOUR
|	IDENTIFIED
|	INLINE
|	INVISIBLE
|	ISSUER
|	JSONTYPE
|	LAST
|	IMPORT
|	DISCARD
|	LOCKS
|	MANAGE
|	MINUTE
|	MICROSECOND
|	NEXT
|	NULLS
|	NONE
|	SHARED
|	EXCLUSIVE
|	EXTERNAL
|	PARSER
|	PRIVILEGES
|	PREV
|	PLUGINS
|	REVERSE
|	RELOAD
|	ROUTINE
|	ROW_COUNT
|	RTREE
|	SECOND
|	SHUTDOWN
|	SQL_CACHE
|	SQL_NO_CACHE
|	SLAVE
|	SLIDING
|	SUPER
|	TABLESPACE
|	TRUNCATE
|	VISIBLE
|	WITHOUT
|	VALIDATION
|	ZONEMAP
|	MEDIAN
|	PUMP
|	VERBOSE
|	SQL_TSI_MINUTE
|	SQL_TSI_SECOND
|	SQL_TSI_YEAR
|	SQL_TSI_QUARTER
|	SQL_TSI_MONTH
|	SQL_TSI_WEEK
|	SQL_TSI_DAY
|	SQL_TSI_HOUR
|	PREPARE
|	DEALLOCATE
|	RESET
|	ADMIN_NAME
|	RANDOM
|	SUSPEND
|	RESTRICTED
|	REUSE
|	CURRENT
|	OPTIONAL
|	FAILED_LOGIN_ATTEMPTS
|	PASSWORD_LOCK_TIME
|	UNBOUNDED
|	SECONDARY
|	MODUMP
|	PRECEDING
|	FOLLOWING
|	FILL
|	TABLE_NUMBER
|	TABLE_VALUES
|	TABLE_SIZE
|	COLUMN_NUMBER
|	RETURNS
|	QUERY_RESULT
|	MYSQL_COMPATIBILITY_MODE
|   UNIQUE_CHECK_ON_AUTOINCR
|	SEQUENCE
|	BACKEND
|	SERVERS
|	CREDENTIALS
|	HANDLER
|	SAMPLE
|	PERCENT
|	OWNERSHIP
|   MO_TS

func_not_keyword:
    DATE_ADD
|    DATE_SUB
|   NOW
|    ADDDATE
|   CURDATE
|   POSITION
|   SESSION_USER
|   SUBDATE
|   SYSTEM_USER
|   TRANSLATE

not_keyword:
    ADDDATE
|   BIT_AND
|   BIT_OR
|   BIT_XOR
|   CAST
|   COUNT
|   APPROX_COUNT
|   APPROX_COUNT_DISTINCT
|   APPROX_PERCENTILE
|   CURDATE
|   CURTIME
|   DATE_ADD
|   DATE_SUB
|   EXTRACT
|   GROUP_CONCAT
|   CLUSTER_CENTERS
|   KMEANS
|   MAX
|   MID
|   MIN
|   NOW
|   POSITION
|   SESSION_USER
|   STD
|   STDDEV
|   STDDEV_POP
|   STDDEV_SAMP
|   SUBDATE
|   SUBSTR
|   SUBSTRING
|   SUM
|   SYSDATE
|   SYSTEM_USER
|   TRANSLATE
|   TRIM
|   VARIANCE
|   VAR_POP
|   VAR_SAMP
|   AVG
|	TIMESTAMPDIFF
|   NEXTVAL
|   SETVAL
|   CURRVAL
|   LASTVAL
|   HEADERS
|   SERIAL_EXTRACT
|   BIT_CAST
|   BITMAP_BIT_POSITION
|   BITMAP_BUCKET_NUMBER
|   BITMAP_COUNT


//mo_keywords:
//    PROPERTIES
//  BSI
//  ZONEMAP

%%
