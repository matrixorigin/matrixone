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
    "strings"
    "go/constant"

    "matrixone/pkg/sql/parsers/tree"
    "matrixone/pkg/defines"
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

    from *tree.From
    where *tree.Where
    groupBy tree.GroupBy
    aliasedTableExpr *tree.AliasedTableExpr
    direction tree.Direction
    orderBy tree.OrderBy
    order *tree.Order
    limit *tree.Limit
    unionTypeRecord *tree.UnionTypeRecord
    parenTableExpr *tree.ParenTableExpr
    identifierList tree.IdentifierList
    joinCond tree.JoinCond

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
    partitionBy *tree.PartitionBy
    partition *tree.Partition
    partitions []*tree.Partition
    values tree.Values
    numVal *tree.NumVal
    subPartition *tree.SubPartition
    subPartitions []*tree.SubPartition

    subquery *tree.Subquery
    intervalType tree.IntervalType
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
    fullOpt bool
    boolVal bool
    int64Val int64
    strs []string

    duplicateKey tree.DuplicateKey
    fields *tree.Fields
    fieldsList []*tree.Fields
    lines *tree.Lines
    varExpr *tree.VarExpr
    loadColumn tree.LoadColumn
    loadColumns []tree.LoadColumn
    strs []string
}

%token LEX_ERROR
%left <str> UNION
%token <str> SELECT STREAM INSERT UPDATE DELETE FROM WHERE GROUP HAVING ORDER BY LIMIT OFFSET FOR
%token <str> ALL DISTINCT DISTINCTROW AS EXISTS ASC DESC INTO DUPLICATE DEFAULT SET LOCK KEYS
%token <str> VALUES LAST_INSERT_ID
%token <str> NEXT VALUE SHARE MODE
%token <str> SQL_NO_CACHE SQL_CACHE
%left <str> JOIN STRAIGHT_JOIN LEFT RIGHT INNER OUTER CROSS NATURAL USE FORCE
%left <str> ON USING
%left <str> SUBQUERY_AS_EXPR
%left <str> '(' ',' ')'
%token <str> ID AT_ID AT_AT_ID STRING VALUE_ARG LIST_ARG COMMENT COMMENT_KEYWORD
%token <item> INTEGRAL HEX HEXNUM BIT_LITERAL FLOAT
%token <str> NULL TRUE FALSE
%left EMPTY_FROM_CLAUSE
%nonassoc <str> LOWER_THAN_CHARSET
%nonassoc <str> CHARSET
%right <str> UNIQUE KEY
%left <str> OR
%left <str> XOR
%left <str> AND
%right <str> NOT '!'
%left <str> BETWEEN CASE WHEN THEN ELSE END
%left <str> '=' '<' '>' LE GE NE NULL_SAFE_EQUAL IS LIKE REGEXP IN ASSIGNMENT
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
%nonassoc <str> '.'

// Transaction
%token <str> BEGIN START TRANSACTION COMMIT ROLLBACK WORK CONSISTENT SNAPSHOT
%token <str> CHAIN NO RELEASE

// Type
%token <str> BIT TINYINT SMALLINT MEDIUMINT INT INTEGER BIGINT INTNUM
%token <str> REAL DOUBLE FLOAT_TYPE DECIMAL NUMERIC
%token <str> TIME TIMESTAMP DATETIME YEAR
%token <str> CHAR VARCHAR BOOL CHARACTER VARBINARY NCHAR
%token <str> TEXT TINYTEXT MEDIUMTEXT LONGTEXT
%token <str> BLOB TINYBLOB MEDIUMBLOB LONGBLOB JSON ENUM
%token <str> GEOMETRY POINT LINESTRING POLYGON GEOMETRYCOLLECTION MULTIPOINT MULTILINESTRING MULTIPOLYGON
%token <str> INT1 INT2 INT3 INT4 INT8

// CreateTable
%token <str> CREATE ALTER DROP RENAME ANALYZE ADD
%token <str> SCHEMA TABLE INDEX VIEW TO IGNORE IF PRIMARY COLUMN CONSTRAINT SPATIAL FULLTEXT FOREIGN KEY_BLOCK_SIZE
%token <str> SHOW DESCRIBE EXPLAIN DATE ESCAPE REPAIR OPTIMIZE TRUNCATE
%token <str> MAXVALUE PARTITION REORGANIZE LESS THAN PROCEDURE TRIGGER
%token <str> STATUS VARIABLES ROLE PROXY AVG_ROW_LENGTH STORAGE DISK MEMORY
%token <str> CHECKSUM COMPRESSION DATA DIRECTORY DELAY_KEY_WRITE ENCRYPTION ENGINE
%token <str> MAX_ROWS MIN_ROWS PACK_KEYS ROW_FORMAT STATS_AUTO_RECALC STATS_PERSISTENT STATS_SAMPLE_PAGES
%token <str> DYNAMIC COMPRESSED REDUNDANT COMPACT FIXED COLUMN_FORMAT AUTO_RANDOM
%token <str> RESTRICT CASCADE ACTION PARTIAL SIMPLE CHECK ENFORCED
%token <str> RANGE LIST ALGORITHM LINEAR PARTITIONS SUBPARTITION SUBPARTITIONS
%token <str> TYPE

// CreateIndex
%token <str> PARSER VISIBLE INVISIBLE BTREE HASH RTREE

// Alter
%token <str> EXPIRE ACCOUNT UNLOCK DAY NEVER

// Time
%token <str> SECOND ASCII COALESCE COLLATION HOUR MICROSECOND MINUTE MONTH QUARTER REPEAT
%token <str> REVERSE ROW_COUNT WEEK

// Revoke
%token <str> REVOKE FUNCTION PRIVILEGES TABLESPACE EXECUTE SUPER GRANT OPTION REFERENCES REPLICATION
%token <str> SLAVE CLIENT USAGE RELOAD FILE TEMPORARY ROUTINE EVENT SHUTDOWN

// Type Modifiers
%token <str> NULLX AUTO_INCREMENT APPROXNUM SIGNED UNSIGNED ZEROFILL

// User
%token <str> USER IDENTIFIED CIPHER ISSUER X509 SUBJECT SAN REQUIRE SSL NONE PASSWORD
%token <str> MAX_QUERIES_PER_HOUR MAX_UPDATES_PER_HOUR MAX_CONNECTIONS_PER_HOUR MAX_USER_CONNECTIONS

// Explain
%token <str> FORMAT CONNECTION

// Load
%token <str> LOAD INFILE TERMINATED OPTIONALLY ENCLOSED ESCAPED STARTING LINES

// Supported SHOW tokens
%token <str> DATABASES TABLES EXTENDED FULL PROCESSLIST FIELDS COLUMNS OPEN ERRORS WARNINGS INDEXES

// SET tokens
%token <str> NAMES GLOBAL SESSION ISOLATION LEVEL READ WRITE ONLY REPEATABLE COMMITTED UNCOMMITTED SERIALIZABLE
%token <str> LOCAL EXCEPT

// Functions
%token <str> CURRENT_TIMESTAMP DATABASE
%token <str> CURRENT_TIME LOCALTIME LOCALTIMESTAMP
%token <str> UTC_DATE UTC_TIME UTC_TIMESTAMP
%token <str> REPLACE CONVERT
%token <str> SEPARATOR
%token <str> CURRENT_DATE CURRENT_USER CURRENT_ROLE

// Match
%token <str> MATCH AGAINST BOOLEAN LANGUAGE WITH QUERY EXPANSION

// Built-in function
%token <str> ADDDATE BIT_AND BIT_OR BIT_XOR CAST COUNT APPROX_COUNT_DISTINCT
%token <str> APPROX_PERCENTILE CURDATE CURTIME DATE_ADD DATE_SUB EXTRACT
%token <str> GROUP_CONCAT MAX MID MIN NOW POSITION SESSION_USER STD STDDEV
%token <str> STDDEV_POP STDDEV_SAMP SUBDATE SUBSTR SUBSTRING SUM SYSDATE
%token <str> SYSTEM_USER TRANSLATE TRIM VARIANCE VAR_POP VAR_SAMP AVG

// MySQL reserved words that are unused by this grammar will map to this token.
%token <str> UNUSED

%type <statement> stmt
%type <statements> stmt_list
%type <statement> create_stmt insert_stmt delete_stmt drop_stmt alter_stmt
%type <statement> drop_ddl_stmt drop_database_stmt drop_table_stmt drop_index_stmt
%type <statement> drop_role_stmt drop_user_stmt
%type <statement> create_user_stmt create_role_stmt
%type <statement> create_ddl_stmt create_table_stmt create_database_stmt create_index_stmt
%type <statement> show_stmt show_create_stmt show_columns_stmt show_databases_stmt
%type <statement> show_tables_stmt show_process_stmt show_errors_stmt show_warnings_stmt
%type <statement> show_variables_stmt show_status_stmt show_index_stmt
%type <statement> alter_user_stmt update_stmt use_stmt
%type <statement> transaction_stmt begin_stmt commit_stmt rollback_stmt
%type <statement> explain_stmt explainable_stmt
%type <statement> set_stmt set_variable_stmt set_password_stmt set_role_stmt set_default_role_stmt
%type <statement> revoke_stmt grant_stmt
%type <statement> load_data_stmt

%type <select> select_stmt select_no_parens
%type <selectStatement> simple_select select_with_parens simple_select_clause
%type <selectExprs> select_expression_list
%type <selectExpr> select_expression
%type <tableExprs> table_references
%type <tableExpr> table_reference table_factor join_table into_table_name
%type <direction> asc_desc_opt
%type <order> order
%type <orderBy> order_list order_by_clause order_by_opt
%type <limit> limit_opt limit_clause
%type <str> insert_column
%type <identifierList> column_list partition_clause_opt partition_id_list insert_column_list
%type <joinCond> join_condition join_condition_opt on_expression_opt

%type <tableDefs> table_elem_list_opt table_elem_list
%type <tableDef> table_elem constaint_def constraint_elem
%type <tableName> table_name
%type <tableNames> table_name_list
%type <columnTableDef> column_def
%type <columnType> cast_type
%type <columnType> column_type char_type spatial_type time_type numeric_type decimal_type int_type
%type <str> integer_opt
%type <columnAttribute> column_attribute_elem keys
%type <columnAttributes> column_attribute_list column_attribute_list_opt
%type <tableOptions> table_option_list_opt table_option_list
%type <str> charset_name storage_opt collate_name column_format storage_media
%type <rowFormatType> row_format_options
%type <int64Val> field_length_opt
%type <matchType> match match_opt
%type <referenceOptionType> ref_opt on_delete on_update
%type <referenceOnRecord> on_delete_update_opt on_delete_update
%type <attributeReference> references_def

%type <tableOption> table_option
%type <from> from_clause from_opt
%type <where> where_expression_opt having_opt
%type <groupBy> group_by_opt
%type <aliasedTableExpr> aliased_table_name
%type <unionTypeRecord> union_op
%type <parenTableExpr> derived_table
%type <str> inner_join straight_join outer_join natural_join
%type <funcType> func_type_opt
%type <funcExpr> function_call_generic
%type <funcExpr> function_call_keyword
%type <funcExpr> function_call_nonkeyword
%type <funcExpr> function_call_aggregate

%type <unresolvedName> column_name column_name_unresolved
%type <strs> enum_values
%type <str> sql_id charset_keyword db_name
%type <str> not_keyword
%type <str> reserved_keyword non_reserved_keyword
%type <str> equal_opt reserved_sql_id reserved_table_id
%type <str> as_name_opt as_opt_id table_id id_or_var name_string ident
%type <str> database_id table_alias explain_sym
%type <unresolvedObjectName> unresolved_object_name table_column_name
%type <unresolvedObjectName> table_name_unresolved
%type <comparisionExpr> like_opt
%type <fullOpt> full_opt
%type <str> database_name_opt auth_string constraint_keyword_opt constraint_keyword
%type <userMiscOption> pwd_or_lck
%type <userMiscOptions> pwd_or_lck_opt pwd_or_lck_list

%type <expr> literal
%type <expr> predicate
%type <expr> bit_expr interval_expr
%type <expr> simple_expr
%type <expr> expression like_escape_opt boolean_primary col_tuple
%type <exprs> expression_list_opt
%type <exprs> expression_list row_value
%type <expr> datatime_precision_opt datatime_precision
%type <tuple> tuple_expression
%type <comparisonOp> comparison_operator
%type <createOption> create_option
%type <createOptions> create_option_list_opt create_option_list
%type <ifNotExists> not_exists_opt
%type <defaultOptional> default_opt
%type <str> database_or_schema
%type <indexType> using_opt
%type <indexCategory> index_prefix
%type <keyParts> index_column_list index_column_list_opt
%type <keyPart> index_column
%type <indexOption> index_option_list index_option
%type <roles> role_spec_list
%type <role> role_spec
%type <str> role_name
%type <usernameRecord> user_name
%type <authRecord> auth_option
%type <user> user_spec
%type <users> user_spec_list
%type <tlsOptions> require_clause_opt require_clause require_list
%type <tlsOption> require_elem
%type <resourceOptions> conn_option_list conn_options
%type <resourceOption> conn_option
%type <updateExpr> update_expression
%type <updateExprs> update_list
%type <completionType> completion_type
%type <str> id_prefix_at password_opt
%type <boolVal> grant_option_opt enforce enforce_opt

%type <varAssignmentExpr> var_assignment
%type <varAssignmentExprs> var_assignment_list
%type <str> var_name equal_or_assignment
%type <expr> set_expr
%type <setRole> set_role_opt
%type <setDefaultRole> set_default_role_opt
%type <privilege> priv_elem
%type <privileges> priv_list
%type <objectType> object_type
%type <privilegeType> priv_type
%type <privilegeLevel> priv_level
%type <unresolveNames> column_name_list
%type <partitionOption> partition_by_opt
%type <partitionBy> partition_method sub_partition_method sub_partition_opt
%type <str> fields_or_columns
%type <int64Val> algorithm_opt partition_num_opt sub_partition_num_opt
%type <boolVal> linear_opt
%type <partition> partition
%type <partitions> partition_list_opt partition_list
%type <values> values_opt
%type <tableOptions> partition_option_list
%type <subPartition> sub_partition
%type <subPartitions> sub_partition_list sub_partition_list_opt
%type <subquery> subquery
%type <intervalType> interval_type
%type <numVal> int_num_val

%type <lengthOpt> length_opt
%type <lengthScaleOpt> float_length_opt decimal_length_opt
%type <unsignedOpt> unsigned_opt
%type <zeroFillOpt> zero_fill_opt
%type <boolVal> global_scope exists_opt distinct_opt temporary_opt
%type <item> pwd_expire clear_pwd_opt
%type <str> name_confict distinct_keyword
%type <insert> insert_data
%type <rowsExprs> values_list
%type <str> name_datatime_precision braces_opt name_braces
%type <str> std_dev_pop
%type <expr> expr_or_default
%type <exprs> data_values data_opt row_value

%type <boolVal> local_opt
%type <duplicateKey> duplicate_opt
%type <fields> load_fields field_item
%type <fieldsList> field_item_list
%type <str> field_terminator starting_opt lines_terminated_opt
%type <lines> load_lines
%type <int64Val> ignore_lines
%type <varExpr> user_variable variable system_variable
%type <loadColumn> columns_or_variable
%type <loadColumns> columns_or_variable_list columns_or_variable_list_opt
%type <unresolvedName> normal_ident
%type <updateExpr> load_set_item
%type <updateExprs> load_set_list load_set_spec_opt
%type <strs> index_name_and_type_opt
%type <str> index_name index_type key_or_index_opt key_or_index

%start start_command

%%

start_command:
    stmt_list

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

stmt:
    create_stmt
|   insert_stmt
|   delete_stmt
|   drop_stmt
|   explain_stmt
|   show_stmt
|   alter_stmt
|   update_stmt
|   use_stmt
|   transaction_stmt
|   set_stmt
|   revoke_stmt
|   grant_stmt
|   load_data_stmt
|   select_stmt
    {
        $$ = $1
    }
|   /* EMPTY */
    {
        $$ = tree.Statement(nil)
    }

load_data_stmt:
    LOAD DATA local_opt INFILE STRING duplicate_opt INTO TABLE table_name load_fields load_lines ignore_lines columns_or_variable_list_opt load_set_spec_opt
    {
        $$ = &tree.Load{
            Local: $3,
            File: $5,
            DuplicateHandling: $6,
            Table: $9,
            Fields: $10,
            Lines: $11,
            IgnoredLines: uint64($12),
            ColumnList: $13,
            Assignments: $14,
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

normal_ident:
    ident
    {
        $$ = tree.SetUnresolvedName($1)
    }
|   ident '.' ident
    {
        $$ = tree.SetUnresolvedName($1, $3)
    }
|   ident '.' ident '.' ident
    {
        $$ = tree.SetUnresolvedName($1, $3, $5)
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
        v := strings.ToLower($1)
        isGlobal := false
        if strings.HasPrefix(v, "global.") {
            isGlobal = true
        }
        $$ = &tree.VarExpr{
            Name: $1,
            System: true,
            Global: isGlobal,
        }
    }

user_variable:
    AT_ID
    {
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

load_lines:
    {
        $$ = nil
    }
|   LINES starting_opt lines_terminated_opt
    {
        $$ = &tree.Lines{
            StartingBy: $2,
            TerminatedBy: $3,
        }
    }

starting_opt:
    {
        $$ = ""
    }
|   STARTING BY STRING
    {
        $$ = $3
    }

lines_terminated_opt:
    {
        $$ = "\n"
    }
|   TERMINATED BY STRING
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
            Terminated: "\t",
			EscapedBy:    []byte("\\")[0],
        }
        for _, f := range $2 {
            if f.Terminated != "" {
                res.Terminated = f.Terminated
            } 
            if f.Optionally {
                res.Optionally = f.Optionally
            }
            if f.EnclosedBy != 0 {
                res.EnclosedBy = f.EnclosedBy
            }
            if f.EscapedBy != 0 {
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
            Terminated: $3,
        }
    }
|   OPTIONALLY ENCLOSED BY field_terminator
    {
        str := $4
        if str != "\\" && len(str) > 1 {
            yylex.Error("error field terminator")
            return 1
        }
        $$ = &tree.Fields{
            Optionally: true,
            EscapedBy: byte(str[0]),
        }
    }
|   ENCLOSED BY field_terminator
    {
        str := $3
        if str != "\\" && len(str) > 1 {
            yylex.Error("error field terminator")
            return 1
        }
        $$ = &tree.Fields{
            EnclosedBy: byte(str[0]),
        }
    }
|   ESCAPED BY field_terminator
    {
        str := $3
        if str != "\\" && len(str) > 1 {
            yylex.Error("error field terminator")
            return 1
        }
        $$ = &tree.Fields{
            EscapedBy: byte(str[0]),
        }
    }

field_terminator:
    STRING
// |   HEX
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
    GRANT priv_list ON object_type priv_level TO user_spec_list require_clause_opt grant_option_opt
    {
        $$ = &tree.Grant{
            Privileges: $2,
            ObjType: $4,
            Level: $5,
            Users: $7,
            GrantOption: $9,
        }
    }
|   GRANT role_spec_list TO user_spec_list
    {
        $$ = &tree.Grant{
            IsGrantRole: true,
            RolesInGrantRole: $2,
            Users: $4,
        }
    }
|   GRANT PROXY ON user_spec TO user_spec_list grant_option_opt
    {
        $$ = &tree.Grant{
            IsProxy: true,
            ProxyUser: $4,
            Users: $6,
            GrantOption: $7,
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
// |	WITH MAX_QUERIES_PER_HOUR INTEGRAL
// |	WITH MAX_UPDATES_PER_HOUR INTEGRAL
// |	WITH MAX_CONNECTIONS_PER_HOUR INTEGRAL
// |	WITH MAX_USER_CONNECTIONS INTEGRAL

revoke_stmt:
    REVOKE priv_list ON object_type priv_level FROM user_spec_list
    {
        $$ = &tree.Revoke{
            Privileges: $2,
            ObjType: $4,
            Level: $5,
            Users: $7,
            Roles: nil,
        }
    }
|   REVOKE role_spec_list FROM user_spec_list
    {
        $$ = &tree.Revoke{
            IsRevokeRole: true,
            RolesInRevokeRole: $2,
            Users: $4,
        }
    }

priv_level:
    '*'
    {
        $$ = &tree.PrivilegeLevel{
            Level: tree.PRIVILEGE_LEVEL_TYPE_DATABASE,
        }
    }
|   '*' '.' '*'
    {
        $$ = &tree.PrivilegeLevel{
            Level: tree.PRIVILEGE_LEVEL_TYPE_GLOBAL,
        }
    }
|   ID '.' '*'
    {
        $$ = &tree.PrivilegeLevel{
            Level: tree.PRIVILEGE_LEVEL_TYPE_DATABASE,
            DbName: $1,
        }
    }
|   ID '.' ID
    {
        $$ = &tree.PrivilegeLevel{
            Level: tree.PRIVILEGE_LEVEL_TYPE_TABLE,
            DbName: $1,
            TabName: $3,
        }
    }
|   ID
    {
        $$ = &tree.PrivilegeLevel{
            Level: tree.PRIVILEGE_LEVEL_TYPE_TABLE,
            TabName: $1,
        }
    }

object_type:
    {
        $$ = tree.OBJECT_TYPE_NONE
    }
|   TABLE
    {
        $$ = tree.OBJECT_TYPE_TABLE
    }
|   FUNCTION
    {
        $$ = tree.OBJECT_TYPE_FUNCTION
    }
|   PROCEDURE
    {
        $$ = tree.OBJECT_TYPE_PROCEDURE
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
|	ALL PRIVILEGES
	{
	    $$ = tree.PRIVILEGE_TYPE_STATIC_ALL
	}
|	ALTER
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_ALTER
	}
|	CREATE
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_CREATE
	}
|	CREATE USER
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_USER
	}
|	CREATE TABLESPACE
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_TABLESPACE
	}
|	TRIGGER
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_TRIGGER
	}
|	DELETE
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_DELETE
	}
|	DROP
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_DROP
	}
|	EXECUTE
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_EXECUTE
	}
|	INDEX
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_INDEX
	}
|	INSERT
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_INSERT
	}
|	SELECT
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_SELECT
	}
|	SUPER
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_SUPER
	}
|	SHOW DATABASES
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_SHOW_DATABASES
	}
|	UPDATE
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_UPDATE
	}
|	GRANT OPTION
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_GRANT_OPTION
	}
|	REFERENCES
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_REFERENCES
	}
|	REPLICATION SLAVE
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_REPLICATION_SLAVE
	}
|	REPLICATION CLIENT
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_REPLICATION_CLIENT
	}
|	USAGE
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_USAGE
	}
|	RELOAD
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_RELOAD
	}
|	FILE
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_FILE
	}
|	CREATE TEMPORARY TABLES
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_TEMPORARY_TABLES
	}
|	LOCK TABLES
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_LOCK_TABLES
	}
|	CREATE VIEW
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_VIEW
	}
|	SHOW VIEW
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_SHOW_VIEW
	}
|	CREATE ROLE
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_ROLE
	}
|	DROP ROLE
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_DROP_ROLE
	}
|   CREATE ROUTINE
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_CREATE_ROUTINE
	}
|	ALTER ROUTINE
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_ALTER_ROUTINE
	}
|	EVENT
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_EVENT
	}
|	SHUTDOWN
	{
		$$ = tree.PRIVILEGE_TYPE_STATIC_SHUTDOWN
	}

set_stmt:
    set_variable_stmt
|   set_password_stmt
|   set_role_stmt
|   set_default_role_stmt

set_role_stmt:
    SET ROLE set_role_opt
    {
        $$ = $3
    }

set_default_role_stmt:
    SET DEFAULT ROLE set_default_role_opt TO user_spec_list
    {
        dr := $4
        dr.Users = $6
        $$ = dr
    }

set_role_opt:
    ALL EXCEPT role_spec_list
    {
        $$ = &tree.SetRole{Type: tree.SET_ROLE_TYPE_ALL_EXCEPT, Roles: $3}
    }
|   DEFAULT
    {
        $$ = &tree.SetRole{Type: tree.SET_ROLE_TYPE_DEFAULT, Roles: nil}
    }
|   NONE
    {
        $$ = &tree.SetRole{Type: tree.SET_ROLE_TYPE_NONE, Roles: nil}
    }
|   ALL
    {
        $$ = &tree.SetRole{Type: tree.SET_ROLE_TYPE_ALL, Roles: nil}
    }
|   role_spec_list
    {
        $$ = &tree.SetRole{Type: tree.SET_ROLE_TYPE_NORMAL, Roles: $1}
    }

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
|   id_prefix_at equal_or_assignment set_expr
    {
        $$ = &tree.VarAssignmentExpr{
            System: true,
            Name: $1,
            Value: $3,
        }
    }
|   id_prefix_at '.' ID equal_or_assignment set_expr
    {
        v := strings.ToLower($1)
        if v != "session" && v != "global" && v != "local"{
            yylex.Error("expecting session or global")
            return 1
        }
        var isGlobal bool
        if v == "global" {
            isGlobal = true
        }
        $$ = &tree.VarAssignmentExpr{
            System: true,
            Global: isGlobal,
            Name: $3,
            Value: $5,
        }
    }
|   NAMES charset_name
    {
        $$ = &tree.VarAssignmentExpr{
            Name: $1,
            Value: tree.NewNumVal(constant.MakeString($2), $2, false),
        }
    }
|   NAMES charset_name COLLATE DEFAULT
    {
        $$ = &tree.VarAssignmentExpr{
            Name: $1,
            Value: tree.NewNumVal(constant.MakeString($2), $2, false),
        }
    }
|   NAMES charset_name COLLATE name_string
    {
        $$ = &tree.VarAssignmentExpr{
            Name: $1,
            Value: tree.NewNumVal(constant.MakeString($2), $2, false),
            Reserved: tree.NewNumVal(constant.MakeString($4), $4, false),
        }
    }
|   NAMES DEFAULT
    {
        $$ = &tree.VarAssignmentExpr{
            Name: $1,
            Value: &tree.DefaultVal{},
        }
    }
|   charset_keyword charset_name
    {
        $$ = &tree.VarAssignmentExpr{
            Name: $1,
            Value: tree.NewNumVal(constant.MakeString($2), $2, false),
        }
    }
|   charset_keyword DEFAULT
    {
        $$ = &tree.VarAssignmentExpr{
            Name: $1,
            Value: &tree.DefaultVal{},
        }
    }

id_prefix_at:
    AT_ID
|   AT_AT_ID

set_expr:
    ON
    {
        $$ = tree.NewNumVal(constant.MakeString($1), $1, false)
    }
|   BINARY
    {
        $$ = tree.NewNumVal(constant.MakeString($1), $1, false)
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
|   ident '.' ident
    {
        $$ = $1 + "." + $3
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
    USE ident
    {
        $$ = &tree.Use{Name: $2}
    }
|   USE
    {
        $$ = &tree.Use{}
    }

update_stmt:
    UPDATE table_reference SET update_list where_expression_opt order_by_opt limit_opt
    {
        $$ = &tree.Update{
            Table: $2,
            Exprs: $4,
            Where: $5,
            OrderBy: $6,
            Limit: $7,
        }
    }
// |   UPDATE comment_opt ignore_opt table_references SET update_list where_expression_opt order_by_opt limit_opt 

update_list:
    update_expression
    {
        $$ = tree.UpdateExprs{$1}
    }
|   update_list ',' update_expression
    {
        $$ = append($1, $3)
    }

update_expression:
    column_name '=' expression
    {
        $$ = &tree.UpdateExpr{Names: []*tree.UnresolvedName{$1}, Expr: $3}
    }

explainable_stmt:
    delete_stmt
|   insert_stmt
|   update_stmt
|   select_stmt
    {
        $$ = $1
    }

explain_stmt:
    explain_sym unresolved_object_name
    {
        st := &tree.ShowColumns{Table: $2}
        $$ = tree.NewExplainStmt(st, "")
    }
|   explain_sym unresolved_object_name column_name
    {
        st := &tree.ShowColumns{Table: $2, ColName: $3}
        $$ = tree.NewExplainStmt(st, "")
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
        $$ = tree.NewExplainStmt($2, "row")
    }
|   explain_sym FORMAT '=' STRING explainable_stmt
    {
        $$ = tree.NewExplainStmt($5, $4)
    }
|   explain_sym ANALYZE explainable_stmt
    {
        $$ = tree.NewExplainAnalyze($3, "")
    }

explain_sym:
    EXPLAIN
|   DESCRIBE
|   DESC

alter_stmt:
    alter_user_stmt
// |    alter_ddl_stmt

alter_user_stmt:
    ALTER USER exists_opt user_spec_list require_clause_opt conn_options pwd_or_lck_opt
    {
        $$ = &tree.AlterUser{
            IfExists: $3,
            IsUserFunc: false,
            Users: $4,
            TlsOpts: $5,
            ResOpts: $6,
            MiscOpts: $7,
        }
    }
|   ALTER USER exists_opt USER '(' ')' IDENTIFIED BY auth_string
    {
        auth := &tree.User{
            AuthString: $9,
            ByAuth: true,
        }
        $$ = &tree.AlterUser{
            IfExists: $3,
            IsUserFunc: true,
            UserFunc: auth,
        }
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
|   pwd_or_lck_list
    {
        $$ = $1
    }

pwd_or_lck_list:
    pwd_or_lck
    {
        $$ = []tree.UserMiscOption{$1}
    }
|   pwd_or_lck_list pwd_or_lck
    {
        $$ = append($1, $2)
    }

pwd_or_lck:
    ACCOUNT UNLOCK
    {
        $$ = &tree.UserMiscOptionAccountUnlock{}
    }
|   ACCOUNT LOCK
    {
        $$ = &tree.UserMiscOptionAccountLock{}
    }
|   pwd_expire
    {
        $$ = &tree.UserMiscOptionPasswordExpireNone{}
    }
|   pwd_expire INTERVAL INTEGRAL DAY
    {
        $$ = &tree.UserMiscOptionPasswordExpireInterval{Value: $3.(int64)}
    }
|   pwd_expire NEVER
    {
        $$ = &tree.UserMiscOptionPasswordExpireNever{}
    }
|   pwd_expire DEFAULT
    {
        $$ = &tree.UserMiscOptionPasswordExpireDefault{}
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
|   show_process_stmt
|   show_errors_stmt
|   show_warnings_stmt
|   show_variables_stmt
|   show_status_stmt
|   show_index_stmt

show_index_stmt:
    SHOW index_kwd from_or_in table_name where_expression_opt
    {
        $$ = &tree.ShowIndex{
            TableName: *$4,
            Where: $5,
        }
    }

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
    SHOW WARNINGS
    {
        $$ = &tree.ShowWarnings{}
    }

show_errors_stmt:
    SHOW ERRORS
    {
        $$ = &tree.ShowErrors{}
    }

show_process_stmt:
    SHOW full_opt PROCESSLIST
    {
        $$ = &tree.ShowProcessList{Full: $2}
    }

show_tables_stmt:
    SHOW full_opt TABLES database_name_opt like_opt where_expression_opt
    {
        $$ = &tree.ShowTables{
            Open: false,
            Full: $2,
            DBName: $4,
            Like: $5,
            Where: $6,
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
    SHOW DATABASES like_opt where_expression_opt
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

like_opt:
    {
        $$ = nil
    }
|   LIKE simple_expr
    {
        $$ = tree.NewComparisonExpr(tree.LIKE, nil, $2)
    }

database_name_opt:
    {
        $$ = ""
    }
|   from_or_in database_id
    {
        $$ = $2
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
    SHOW CREATE TABLE table_name_unresolved
    {
        $$ = &tree.ShowCreate{Name: $4}
    }
|   SHOW CREATE DATABASE not_exists_opt db_name
    {
        $$ = &tree.ShowCreateDatabase{IfNotExists: $4, Name: $5}
    }

table_name_unresolved:
    ident
    {
        $$ = tree.SetUnresolvedObjectName(1, [3]string{$1})
    }
|   ident '.' ident
    {
        $$ = tree.SetUnresolvedObjectName(2, [3]string{$1, $3})
    }

db_name:
    ident

unresolved_object_name:
    ident
    {
        $$ = tree.SetUnresolvedObjectName(1, [3]string{$1})
    }
|   ident '.' ident
    {
        $$ = tree.SetUnresolvedObjectName(2, [3]string{$1, $3})
    }
|   ident '.' ident '.' ident
    {
        $$ = tree.SetUnresolvedObjectName(3, [3]string{$1, $3, $5})
    }

drop_stmt:
    drop_ddl_stmt

drop_ddl_stmt:
    drop_database_stmt
|   drop_table_stmt
|   drop_index_stmt
|   drop_role_stmt
|   drop_user_stmt

drop_user_stmt:
    DROP USER exists_opt user_spec_list
    {
        $$ = &tree.DropUser{
            IfExists: $3,
            Users: $4,
        }
    }

drop_role_stmt:
    DROP ROLE exists_opt role_spec_list
    {
        $$ = &tree.DropRole{
            IfExists: $3,
            Roles: $4,
        }
    } 

drop_index_stmt:
    DROP INDEX exists_opt ident ON table_name
    {
        $$ = &tree.DropIndex{
            Name: tree.Identifier($4),
            TableName: *$6,
            IfExists: $3,
        }
    }

drop_table_stmt:
    DROP TABLE exists_opt table_name_list
    {
        $$ = &tree.DropTable{IfExists: $3, Names: $4}
    }

drop_database_stmt:
    DROP DATABASE exists_opt database_id
    {
        $$ = &tree.DropDatabase{Name: tree.Identifier($4), IfExists: $3}
    }

delete_stmt:
    DELETE FROM table_reference where_expression_opt order_by_opt limit_opt
    {
        $$ = &tree.Delete{
            Table: $3,
            Where: $4,
            OrderBy: $5,
            Limit: $6,
        }
    }
//     DELETE comment_opt ignore_opt FROM table_name partition_clause_opt where_expression_opt order_by_opt limit_opt
// |   DELETE comment_opt ignore_opt FROM table_name_list USING table_references where_expression_opt
// |   DELETE comment_opt ignore_opt table_name_list from_or_using table_references where_expression_opt
// |   DELETE comment_opt ignore_opt delete_table_list from_or_using table_references where_expression_opt

insert_stmt:
    INSERT into_table_name partition_clause_opt insert_data
    {
        ins := $4
        ins.Table = $2
        ins.PartitionNames = $3
        $$ = ins
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
            Rows: tree.NewSelect($1, nil, nil),
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
            Rows: tree.NewSelect($4, nil, nil),
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
        $$ = $1
    }
|   ident '.' ident
    {
        $$ = $3
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
    '(' data_opt ')'
    {
        $$ = $2
    }

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
        $$ = tree.IdentifierList{tree.Identifier($1)}
    }
|   partition_id_list ',' ident
    {
        $$ = append($1 , tree.Identifier($3))
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

select_stmt:
    select_no_parens
|   select_with_parens
    {
        $$ = &tree.Select{Select: $1}
    }

select_no_parens:
    simple_select order_by_opt limit_opt // select_lock_opt
    {
        $$ = &tree.Select{Select: $1, OrderBy: $2, Limit: $3}
    }
|   select_with_parens order_by_clause
    {
        $$ = &tree.Select{Select: $1, OrderBy: $2}
    }
|   select_with_parens order_by_opt limit_clause
    {
        $$ = &tree.Select{Select: $1, OrderBy: $2, Limit: $3}
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
    expression asc_desc_opt
    {
        $$ = &tree.Order{Expr: $1, Direction: $2}
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


select_with_parens:
    '(' select_no_parens ')'
    {
        $$ = &tree.ParenSelect{Select: $2}
    }
|   '(' select_with_parens ')'
    {
        $$ = &tree.ParenSelect{Select: &tree.Select{Select: $2}}
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
        $$ = tree.SelectExpr{Expr: $1, As: tree.UnrestrictedIdentifier($2)}
    }
|   ident '.' '*' %prec '*'
    {
        $$ = tree.SelectExpr{Expr: tree.SetUnresolvedNameWithStar($1)}
    }
|   ident '.' ident '.' '*' %prec '*'
    {
        $$ = tree.SelectExpr{Expr: tree.SetUnresolvedNameWithStar($3, $1)}
    }

from_opt:
    %prec EMPTY_FROM_CLAUSE
    {
        prefix := tree.ObjectNamePrefix{ExplicitSchema: false}
        tn := tree.NewTableName(tree.Identifier("dual"), prefix)
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
            Tables: $2,
        }
    }

table_references:
    table_reference
    {
        $$ = tree.TableExprs{$1}
    }
|   table_references ',' table_reference
    {
        $$ = append($1, $3)
    }

table_reference:
    table_factor
|   join_table

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
// right: table_reference
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
        $$ = ""
    }
|   INNER JOIN
    {
        $$ = tree.JOIN_TYPE_INNER
    }
|   CROSS JOIN
    {
        $$ = tree.JOIN_TYPE_CROSS
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
        $$ = tree.IdentifierList{tree.Identifier($1)}
    }
|   column_list ',' ident
    {
        $$ = append($1, tree.Identifier($3))
    }

table_factor:   
    aliased_table_name
    {
        $$ = $1
    }
|   derived_table as_opt table_id
    {
        $$ = &tree.AliasedTableExpr{
            Expr: $1,
            As: tree.AliasClause{
                Alias: tree.Identifier($3),
            },
        }
    }
// |   '(' table_references ')'

derived_table:
    '(' select_no_parens ')'
    {
        $$ = &tree.ParenTableExpr{Expr: $2}
    }

as_opt:
    {}
|   AS {}

aliased_table_name:
    table_name as_opt_id // index_hint_list
    {
        $$ = &tree.AliasedTableExpr{
            Expr: $1,
            As: tree.AliasClause{
                Alias: tree.Identifier($2),
            },
        }
    }
// |   table_name PARTITION '(' partition_id_list ')' as_opt_id index_hint_list


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
    table_id
|   STRING

as_name_opt:
    {
        $$ = ""
    }
|   ident
    {
        $$ = $1
    }
|   AS ident
    {
        $$ = $2
    }

database_id:
    id_or_var
|   non_reserved_keyword

table_id:
    id_or_var
|   non_reserved_keyword

id_or_var:
    ID
|   AT_ID
|   AT_AT_ID

create_stmt:
    create_ddl_stmt
|   create_role_stmt
|   create_user_stmt

create_ddl_stmt:
    create_table_stmt
|   create_database_stmt
|   create_index_stmt

create_user_stmt:
    CREATE USER not_exists_opt user_spec_list require_clause_opt conn_options
    {
        $$ = &tree.CreateUser{
            IfNotExists: $3,
            Users: $4,
            TlsOpts: $5,
            ResOpts: $6,
        }
    }

conn_options:
    {
        $$ = nil
    }
|   WITH conn_option_list
    {
        $$ = $2
    }

conn_option_list:
    conn_option
    {
        $$ = []tree.ResourceOption{$1}
    }
|   conn_option_list conn_option
    {
        $$ = append($1, $2)
    }

conn_option:
    MAX_QUERIES_PER_HOUR INTEGRAL
    {
        $$ = &tree.ResourceOptionMaxQueriesPerHour{Count: $2.(int64)}
    }
|   MAX_UPDATES_PER_HOUR INTEGRAL
    {
        $$ = &tree.ResourceOptionMaxUpdatesPerHour{Count: $2.(int64)}
    }
|   MAX_CONNECTIONS_PER_HOUR INTEGRAL
    {
        $$ = &tree.ResourceOptionMaxConnectionPerHour{Count: $2.(int64)}
    }
|   MAX_USER_CONNECTIONS INTEGRAL
    {
        $$ = &tree.ResourceOptionMaxUserConnections{Count: $2.(int64)}
    }


require_clause_opt:
    {
        $$ = nil
    }
|   require_clause

require_clause:
    REQUIRE NONE
    {
        t := &tree.TlsOptionNone{}
        $$ = []tree.TlsOption{t}
    }
|   REQUIRE SSL
    {
        t := &tree.TlsOptionSSL{}
        $$ = []tree.TlsOption{t}
    }
|   REQUIRE X509
    {
        t := &tree.TlsOptionX509{}
        $$ = []tree.TlsOption{t}
    }
|   REQUIRE require_list
    {
        $$ = $2
    }

require_list:
    require_elem
    {
        $$ = []tree.TlsOption{$1}
    }
|   require_list AND require_elem
    {
        $$ = append($1, $3)
    }
|   require_list require_elem
    {
        $$ = append($1, $2)
    }

require_elem:
    ISSUER STRING
    {
        $$ = &tree.TlsOptionIssuer{Issuer: $2}
    }
|   SUBJECT STRING
    {
        $$ = &tree.TlsOptionSubject{Subject: $2}
    }
|   CIPHER STRING
    {
        $$ = &tree.TlsOptionCipher{Cipher: $2}
    }
|   SAN STRING
    {
        $$ = &tree.TlsOptionSan{San: $2}
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
    user_name auth_option
    {
        $$ = &tree.User{
            Username: $1.Username,
            Hostname: $1.Hostname,
            AuthPlugin: $2.AuthPlugin,
            AuthString: $2.AuthString,
            HashString: $2.HashString,
            ByAuth: $2.ByAuth,
        }
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

auth_option:
    {
        $$ = &tree.AuthRecord{}
    }
|	IDENTIFIED BY name_string
	{
		$$ = &tree.AuthRecord{
			AuthString: $3,
			ByAuth: true,
		}
	}
|	IDENTIFIED WITH name_string
	{ 
		$$ = &tree.AuthRecord{
			AuthPlugin: $3,
		}
	}
|	IDENTIFIED WITH name_string BY name_string
	{
		$$ = &tree.AuthRecord{
			AuthPlugin: $3,
			AuthString: $5,
			ByAuth: true,
		}
	}
|	IDENTIFIED WITH name_string AS name_string
	{
		$$ = &tree.AuthRecord{
			AuthPlugin: $3,
			HashString: $5,
		}
	}
|	IDENTIFIED BY PASSWORD name_string
	{
		$$ = &tree.AuthRecord{
			HashString: $4,
		}
	}

name_string:
    ident
|   STRING

create_role_stmt:
    CREATE ROLE not_exists_opt role_spec_list
    {
        $$ = &tree.CreateRole{
            IfNotExists: $3,
            Roles: $4,
        }
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
        $$ = &tree.Role{UserName: $1, HostName: "%"}
    }
|   name_string '@' name_string
    {
        $$ = &tree.Role{UserName: $1, HostName: $3}
    }
|   name_string AT_ID
    {
        $$ = &tree.Role{UserName: $1, HostName: $2}
    }

role_name:
    ID
|   STRING

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
    CREATE index_prefix INDEX id_or_var using_opt ON table_name '(' index_column_list ')' index_option_list
    {
        var io *tree.IndexOption = nil
        if $11 == nil && $5 != tree.INDEX_TYPE_INVALID {
            io = &tree.IndexOption{IType: $5}
        } else if $11 != nil{
            io = $11
            io.IType = $5
        }
        $$ = &tree.CreateIndex{
            Name: tree.Identifier($4),
            Table: *$7,
            IndexCat: $2,
            KeyParts: $9,
            IndexOption: io,
            MiscOption: nil,
        }
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
            }
            $$ = opt1
        }
    }

index_option:
    KEY_BLOCK_SIZE equal_opt INTEGRAL
    {
        $$ = &tree.IndexOption{KeyBlockSize: uint64($3.(int64))}
    }
|   COMMENT_KEYWORD STRING
    {
        $$ = &tree.IndexOption{Comment: $2}
    }
|   WITH PARSER id_or_var
    {
        $$ = &tree.IndexOption{ParserName: $3}
    }
|   VISIBLE
    {
        $$ = &tree.IndexOption{Visible: tree.VISIBLE_TYPE_VISIBLE}
    }
|   INVISIBLE
    {
        $$ = &tree.IndexOption{Visible: tree.VISIBLE_TYPE_INVISIBLE}
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
        // Order is parsed but just ignored as MySQL did.
        $$ = &tree.KeyPart{ColName: $1, Length: int($2), Direction: $3}
    }
|   '(' expression ')' asc_desc_opt
    {
        $$ = &tree.KeyPart{Expr: $2, Direction: $4}
    }

using_opt:
    {
        $$ = tree.INDEX_TYPE_INVALID
    }
|   USING BTREE
    {
        $$ = tree.INDEX_TYPE_BTREE
    }
|   USING HASH
    {
        $$ = tree.INDEX_TYPE_HASH
    }
|   USING RTREE
    {
        $$ = tree.INDEX_TYPE_RTREE
    }

create_database_stmt:
    CREATE database_or_schema not_exists_opt ident create_option_list_opt
    {
        $$ = &tree.CreateDatabase{
            IfNotExists: $3,
            Name: tree.Identifier($4),
            CreateOptions: $5,
        }
    }
// CREATE comment_opt database_or_schema comment_opt not_exists_opt ident

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
        $$ = &tree.CreateOptionCharset{IsDefault: $1, Charset: $4}
    }
|   default_opt COLLATE equal_opt collate_name
    {
        $$ = &tree.CreateOptionCollate{IsDefault: $1, Collate: $4}
    }
|   default_opt ENCRYPTION equal_opt STRING
    {
        $$ = &tree.CreateOptionEncryption{Encrypt: $4}
    }

default_opt:
    {
        $$ = false
    }
|   DEFAULT
    {
        $$ = true
    }

create_table_stmt:
    CREATE temporary_opt TABLE not_exists_opt table_name '(' table_elem_list_opt ')' table_option_list_opt partition_by_opt
    {
        $$ = &tree.CreateTable {
            Temporary: $2,
            IfNotExists: $4,
            Table: *$5,
            Defs: $7,
            Options: $9,
            PartitionOption: $10,
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

partition_by_opt:
    {
        $$ = nil
    }
|   PARTITION BY partition_method partition_num_opt sub_partition_opt partition_list_opt
    {
        $3.Num = uint64($4)
        $$ = &tree.PartitionOption{
            PartBy: *$3,
            SubPartBy: $5,
            Partitions: $6,
        }
    }

sub_partition_opt:
    {
        $$ = nil
    }
|   SUBPARTITION BY sub_partition_method sub_partition_num_opt
    {
        $$ = &tree.PartitionBy{
            IsSubPartition: true,
            PType: $3,
            Num: uint64($4),
        }
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
    PARTITION ID values_opt sub_partition_list_opt
    {
        $$ = &tree.Partition{
            Name: tree.Identifier($2),
            Values: $3,
            Options: nil,
            Subs: $4,
        }
    }
|   PARTITION ID values_opt partition_option_list sub_partition_list_opt
    {
        $$ = &tree.Partition{
            Name: tree.Identifier($2),
            Values: $3,
            Options: $4,
            Subs: $5,
        }
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
    SUBPARTITION ID
    {
        $$ = &tree.SubPartition{
            Name: tree.Identifier($2),
            Options: nil,
        }
    }
|   SUBPARTITION ID partition_option_list
    {
        $$ = &tree.SubPartition{
            Name: tree.Identifier($2),
            Options: $3,
        }
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
|   VALUES LESS THAN '(' expression_list ')'
    {
        $$ = &tree.ValuesLessThan{ValueList: $5}
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
            return 1
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
            return 1
        }
        $$ = res
    }

partition_method:
    RANGE '(' bit_expr ')'
    {
        $$ = &tree.PartitionBy{
            PType: &tree.RangeType{
                Expr: $3,
            },
        }
    }
|   RANGE fields_or_columns '(' column_name_list ')'
    {
        $$ = &tree.PartitionBy{
            PType: &tree.RangeType{
                ColumnList: $4,
            },
        }
    }
|   LIST '(' bit_expr ')'
    {
        $$ = &tree.PartitionBy{
            PType: &tree.ListType{
                Expr: $3,
            },
        }
    }
|   LIST fields_or_columns '(' column_name_list ')'
    {
        $$ = &tree.PartitionBy{
            PType: &tree.ListType{
                ColumnList: $4,
            },
        }
    }
|   sub_partition_method

sub_partition_method:
    linear_opt KEY algorithm_opt '(' column_name_list ')'
    {
        $$ = &tree.PartitionBy{
            PType: &tree.KeyType{
                Linear: $1,
                ColumnList: $5,
                Algorithm: $3,
            },
        }
    }
|   linear_opt HASH '(' bit_expr ')'
    {
        $$ = &tree.PartitionBy{
            PType: &tree.HashType{
                Linear: $1,
                Expr: $4,
            },
        }
    }

algorithm_opt:
    {
        $$ = 0
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
    AUTO_INCREMENT equal_opt INTEGRAL
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
        $$ = tree.NewTableOptionComment($3)
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
        $$ = &tree.TableOptionPackKeys{Value: $3.(int64)}
    }
|   PACK_KEYS equal_opt DEFAULT
    {
        $$ = &tree.TableOptionPackKeys{Default: true}
    }
|   PASSWORD equal_opt STRING
    {
        $$ = tree.NewTableOptionPassword($3)
    }
|   ROW_FORMAT equal_opt row_format_options
    {
        $$ = tree.NewTableOptionRowFormat($3)
    }
|   STATS_AUTO_RECALC equal_opt INTEGRAL
    {
        $$ = &tree.TableOptionStatsAutoRecalc{Value: uint64($3.(int64))}
    }
|   STATS_AUTO_RECALC equal_opt DEFAULT
    {
        $$ = &tree.TableOptionStatsAutoRecalc{Default: true}
    }
|   STATS_PERSISTENT equal_opt INTEGRAL
    {
        $$ = &tree.TableOptionStatsPersistent{Value: uint64($3.(int64))}
    }
|   STATS_PERSISTENT equal_opt DEFAULT
    {
        $$ = &tree.TableOptionStatsPersistent{Default: true}
    }
|   STATS_SAMPLE_PAGES equal_opt INTEGRAL
    {
        $$ = &tree.TableOptionStatsSamplePages{Value: uint64($3.(int64))}
    }
|   STATS_SAMPLE_PAGES equal_opt DEFAULT
    {
        $$ = &tree.TableOptionStatsSamplePages{Default: true}
    }
|   TABLESPACE equal_opt ident storage_opt
    {
        $$= tree.NewTableOptionTablespace($3, $4)
    }
|   UNION equal_opt '(' table_name_list ')'
    {
        $$= tree.NewTableOptionUnion($4)
    }
// |   INSERT_METHOD equal_opt insert_method_options

storage_opt:
    {
        $$ = ""
    }
|   STORAGE DISK
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
    id_or_var
|   STRING
|   BINARY

collate_name:
    id_or_var
|   STRING
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
    ident
    {
        prefix := tree.ObjectNamePrefix{ExplicitSchema: false}
        $$ = tree.NewTableName(tree.Identifier($1), prefix)
    }
|   ident '.' ident
    {
        prefix := tree.ObjectNamePrefix{SchemaName: tree.Identifier($1), ExplicitSchema: true}
        $$ = tree.NewTableName(tree.Identifier($3), prefix)
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

constaint_def:
	constraint_keyword constraint_elem
	{
		if $1 != "" {
			switch v := $2.(type) {
            case *tree.PrimaryKeyIndex:
            	v.Name = $1
            }
		}
		$$ = $2
	}
|	constraint_elem
	{
		$$ = $1
	}

constraint_elem:
	PRIMARY KEY index_name_and_type_opt '(' index_column_list ')' index_option_list
	{
 		$$ = &tree.PrimaryKeyIndex{
			KeyParts: $5,
			Name: $3[0],
			Empty: $3[1] == "",
			IndexOption: $7,
		}
	}
|	FULLTEXT key_or_index_opt index_name '(' index_column_list ')' index_option_list
	{
		$$ = &tree.FullTextIndex{
			KeyParts: $5,
			Name: $3,
			Empty: true,
			IndexOption: $7,
		}
	}
|	key_or_index not_exists_opt index_name_and_type_opt '(' index_column_list ')' index_option_list
	{
		$$ = &tree.Index{
			IfNotExists: $2,
			KeyParts: $5,
			Name: $3[0],
			Empty: $3[1] == "",
			IndexOption: $7,
		}
	}
|	UNIQUE key_or_index_opt index_name_and_type_opt '(' index_column_list ')' index_option_list
	{
		$$ = &tree.UniqueIndex{
			KeyParts: $5,
			Name: $3[0],
            Empty: $3[1] == "",
            IndexOption: $7,
		}
	}
|	FOREIGN KEY not_exists_opt index_name '(' index_column_list ')' references_def
	{
		$$ = &tree.ForeignKey{
			IfNotExists: $3,
			KeyParts: $6,
			Name: $4,
			Refer: $8,
			Empty: true,
		}
	}
|	CHECK '(' expression ')' enforce_opt
	{
		$$ = &tree.CheckIndex{
			Expr: $3,
			Enforced: $5,
		}
	}

enforce_opt:
	{
		$$ = false
	}
|	enforce

key_or_index_opt:
	{
		$$ = ""
	}
|	key_or_index
	{
		$$ = $1
	}

key_or_index:
	KEY
|	INDEX

index_name_and_type_opt:
	index_name
	{
		$$ = make([]string, 2)
		$$[0] = $1
		$$[1] = ""
	}
|	index_name USING index_type
	{
		$$ = make([]string, 2)
        $$[0] = $1
        $$[1] = $3
	}
|	ident TYPE index_type
	{
		$$ = make([]string, 2)
        $$[0] = $1
        $$[1] = $3
	}

index_type:
	BTREE
|	HASH
|	RTREE

index_name:
	{
		$$ = ""
	}
|	ident

column_def:
    column_name column_type column_attribute_list_opt 
    {
        $$ = tree.NewColumnTableDef($1, $2, $3)
    }

column_name_unresolved:
    ident
    {
        $$ = tree.SetUnresolvedName($1)
    }
|   ident '.' ident
    {
        $$ = tree.SetUnresolvedName($1, $3)
    }
|   ident '.' ident '.' ident
    {
        $$ = tree.SetUnresolvedName($1, $3, $5)
    }

ident:
    ID
|   not_keyword
|   non_reserved_keyword

column_name:
    ident
    {
        $$ = tree.SetUnresolvedName($1)
    }
|   ident '.' reserved_sql_id
    {
        $$ = tree.SetUnresolvedName($1, $3)
    }
|   ident '.' reserved_table_id '.' reserved_sql_id
    {
        $$ = tree.SetUnresolvedName($1, $3, $5)
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
        $$ = tree.NewAttributeComment(tree.NewNumVal(constant.MakeString($2), $2, false))
    }
|   COLLATE collate_name
    {
        $$ = tree.NewAttributeCollate($2)
    }
|   COLUMN_FORMAT column_format
    {
        $$ = tree.NewAttributeColumnFormat($2)
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
        $$ = tree.NewAttributeCheck($4, false, $1)
    }
|   constraint_keyword_opt CHECK '(' expression ')' enforce
    {
        $$ = tree.NewAttributeCheck($4, $6, $1)
    }
// |   ON UPDATE function_call_nonkeyword
//     {
//         $$ = tree.NewAttributeOnUpdate($3)
//     }

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
 |	constraint_keyword
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
        $$ = $2
    }

references_def:    
    REFERENCES table_name index_column_list_opt match_opt on_delete_update_opt
    {
        $$ = &tree.AttributeReference{
            TableName: $2,
            KeyParts: $3,
            Match: $4,
            OnDelete: $5.OnDelete,
            OnUpdate: $5.OnUpdate,
        }
    }

on_delete_update_opt:
    {
        $$ = &tree.ReferenceOnRecord{
            OnDelete: tree.REFERENCE_OPTION_INVALID,
            OnUpdate: tree.REFERENCE_OPTION_INVALID,
        }
    }
|   on_delete_update

on_delete_update:
    on_delete
    {
        $$ = &tree.ReferenceOnRecord{
            OnDelete: $1,
            OnUpdate: tree.REFERENCE_OPTION_INVALID,
        }
    }
|   on_update
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
        $$ = tree.NewParenExpr($2)
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
|   CAST '(' expression AS cast_type ')' 
    {
        $$ = tree.NewCastExpr($3, $5)
    }
|   CONVERT '(' expression ',' cast_type ')' 
    {
        $$ = tree.NewCastExpr($3, $5)
    }
|   CONVERT '(' expression USING charset_name ')' 
    {
        name := tree.SetUnresolvedName("convert")
        es := tree.NewNumVal(constant.MakeString($5), $5, false)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
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

cast_type:
    BINARY length_opt
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
|   CHAR length_opt
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
|   DATETIME length_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
		        Family:             tree.TimestampFamily,
		        Precision:          0,
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
		        Precision: 0,
		        TimePrecisionIsSet: false,
		        Locale: &locale,
		        Oid: uint32(defines.MYSQL_TYPE_TIME),
	        },
        }
    }
|   DOUBLE float_length_opt
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
                Precision: $2.Precision,
	        },
        }
    }
|   FLOAT_TYPE decimal_length_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
		        Family: tree.FloatFamily,
                FamilyString: $1,
		        Width:  32,
		        Locale: &locale,
		        Oid:    uint32(defines.MYSQL_TYPE_FLOAT),
                DisplayWith: $2.DisplayWith,
                Precision: $2.Precision,
	        },
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
                Precision: $2.Precision,
	        },
        }
    }
|   SIGNED integer_opt
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
|   UNSIGNED integer_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
		        Family: tree.IntFamily,
                FamilyString: $1,
		        Width:  64,
		        Locale: &locale,
                Unsigned: true,
		        Oid:    uint32(defines.MYSQL_TYPE_LONGLONG),
	        },
        }
    }

integer_opt:
    {}
|	INTEGER
|	INT

function_call_aggregate:
    AVG '(' func_type_opt expression  ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{$4},
            Type: $3,
        }
    }
|   APPROX_COUNT_DISTINCT '(' expression_list ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: $3,
        }
    }
|   APPROX_PERCENTILE '(' expression_list ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: $3,
        }
    }
|   BIT_AND '(' func_type_opt expression ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{$4},
            Type: $3,
        }
    }
|   BIT_OR '(' func_type_opt expression ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{$4},
            Type: $3,
        }
    }
|   BIT_XOR '(' func_type_opt expression ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{$4},
            Type: $3,
        }
    }
|   COUNT '(' func_type_opt expression_list ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: $4,
            Type: $3,
        }
    }
|   COUNT '(' '*' ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        es := tree.NewNumVal(constant.MakeString("*"), "*", false)
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{es},
        }
    }
|   MAX '(' func_type_opt expression ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{$4},
            Type: $3,
        }
    }
|   MIN '(' func_type_opt expression ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{$4},
            Type: $3,
        }
    }
|   SUM '(' func_type_opt expression ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{$4},
            Type: $3,
        }
    }
|   std_dev_pop '(' func_type_opt expression ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{$4},
            Type: $3,
        }
    }
|   STDDEV_SAMP '(' func_type_opt expression ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{$4},
            Type: $3,
        }
    }
|   VAR_POP '(' func_type_opt expression ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{$4},
            Type: $3,
        }
    }
|   VAR_SAMP '(' func_type_opt expression ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{$4},
            Type: $3,
        }
    }

std_dev_pop:
    STD
|   STDDEV
|   STDDEV_POP

function_call_generic:
    ID '(' expression_list_opt ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: $3,
        }
    }
// |   identifier '.' identifier '(' expression_list_opt ')'
//     {
//         name := tree.SetUnresolvedName(strings.ToLower($1), strings.ToLower($3))
//         $$ = &tree.FuncExpr{
//             Func: tree.FuncName2ResolvableFunctionReference(name),
//             Exprs: $5,
//         }
//     }

function_call_nonkeyword:
    CURTIME datatime_precision
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        var es tree.Exprs = nil
        if $2 != nil {
            es = append(es, $2)
        }
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: es,
        }
    }
|   SYSDATE datatime_precision
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        var es tree.Exprs = nil
        if $2 != nil {
            es = append(es, $2)
        }
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: es,
        }
    }

function_call_keyword:
    name_confict '(' expression_list_opt ')'
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: $3,
        }
    }
|   name_braces braces_opt
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
        }
    }
|   name_datatime_precision datatime_precision_opt
    {
        name := tree.SetUnresolvedName(strings.ToLower($1))
        var es tree.Exprs = nil
        if $2 != nil {
            es = append(es, $2)
        }
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: es,
        }
    }
|   CHAR '(' expression_list ')'
    {
        name := tree.SetUnresolvedName("char")
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: $3,
        }
    }
|   CHAR '(' expression_list USING charset_name ')'
    {
        cn := tree.NewNumVal(constant.MakeString($5), $5, false)
        es := $3
        es = append(es, cn)
        name := tree.SetUnresolvedName("char")
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: es,
        }
    }
|   DATE STRING
    {
        val := tree.NewNumVal(constant.MakeString($2), $2, false)
        name := tree.SetUnresolvedName("date")
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{val},
        }
    }
|   TIME STRING
    {
        val := tree.NewNumVal(constant.MakeString($2), $2, false)
        name := tree.SetUnresolvedName("time")
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{val},
        }
    }
|   TIMESTAMP STRING
    {
        val := tree.NewNumVal(constant.MakeString($2), $2, false)
        name := tree.SetUnresolvedName("timestamp")
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{val},
        }
    }
|   INSERT '(' expression_list_opt ')'
    {
        name := tree.SetUnresolvedName("insert")
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: $3,
        }
    }
|   MOD '(' bit_expr ',' bit_expr ')'
    {
        es := tree.Exprs{$3}
        es = append(es, $5)
        name := tree.SetUnresolvedName("mod")
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: es,
        }
    }
|   PASSWORD '(' expression_list_opt ')'
    {
        name := tree.SetUnresolvedName("password")
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: $3,
        }
    }
|   BINARY simple_expr %prec UNARY
    {
        name := tree.SetUnresolvedName("binary")
        $$ = &tree.FuncExpr{
            Func: tree.FuncName2ResolvableFunctionReference(name),
            Exprs: tree.Exprs{$2},
        }
    }

datatime_precision_opt:
    {
        $$ = nil
    }
|   datatime_precision
    {
        $$ = $1
    }

datatime_precision:
   '(' ')'
    {
        $$ = nil
    }
|   '(' int_num_val ')'
    {
        $$ = $2
    }

name_datatime_precision:
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

interval_expr:
    INTERVAL expression interval_type
    {
        $$ = &tree.IntervalExpr{
            Expr: $2, 
            Type: $3,
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

interval_type:
    SECOND
    {
        $$ = tree.INTERVAL_TYPE_SECOND
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

boolean_primary:
    boolean_primary IS NULL %prec IS
    {
        $$ = tree.NewIsNullExpr($1)
    }
|   boolean_primary IS NOT NULL %prec IS
    {
        $$ = tree.NewIsNotNullExpr($1)
    }
|   boolean_primary comparison_operator predicate %prec '='
    {
        $$ = tree.NewComparisonExpr($2, $1, $3)
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
// |    NULL_SAFE_EQUAL

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

int_num_val:
    INTEGRAL
    {
        ival, errStr := getInt64($1)
        if errStr != "" {
            yylex.Error(errStr)
            return 1
        }
        if ival > 0 {
            $$ = tree.NewNumValWithResInt(constant.MakeInt64(ival), yylex.(*Lexer).scanner.LastToken, false, ival)
        } else {
            $$ = tree.NewNumValWithResInt(constant.MakeInt64(ival), yylex.(*Lexer).scanner.LastToken, true, ival)
        }
    }

literal:
    STRING
    {
        $$ = tree.NewNumVal(constant.MakeString($1), $1, false)
    }
|   INTEGRAL
    {
        ival := getUint64($1)
        $$ = tree.NewNumVal(constant.MakeUint64(ival), yylex.(*Lexer).scanner.LastToken, false)
    }
|   FLOAT
    {
        fval := $1.(float64)
        $$ = tree.NewNumValWithResFoalt(constant.MakeFloat64(fval), yylex.(*Lexer).scanner.LastToken, false, fval)
    }
|   TRUE
    {
        $$ = tree.NewNumVal(constant.MakeBool(true), "", false)
    }
|   FALSE
    {
        $$ = tree.NewNumVal(constant.MakeBool(false), "", false)
    }
|   NULL
    {
        $$ = tree.NewNumVal(constant.MakeUnknown(), "", false)
    }
// |   HEX
// |   BIT_LITERAL
// |   HEXNUM
// |   VALUE_ARG

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
        $$ = &tree.T{
            InternalType: tree.InternalType{
		        Family: tree.FloatFamily,
                FamilyString: $1,
		        Width:  64,
		        Locale: &locale,
		        Oid:    uint32(defines.MYSQL_TYPE_DOUBLE),
                DisplayWith: $2.DisplayWith,
                Precision: $2.Precision,
	        },
        }
    }
|   FLOAT_TYPE decimal_length_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
		        Family: tree.FloatFamily,
                FamilyString: $1,
		        Width:  32,
		        Locale: &locale,
		        Oid:    uint32(defines.MYSQL_TYPE_FLOAT),
                DisplayWith: $2.DisplayWith,
                Precision: $2.Precision,
	        },
        }
    }
// |   DECIMAL decimal_length_opt
//     {
//         $$ = tree.TYPE_DOUBLE
//         $$.InternalType.DisplayWith = $2.DisplayWith
//         $$.InternalType.Precision = $2.Precision
//     }
// |   NUMERIC decimal_length_opt
//     {
//         $$ = tree.TYPE_DOUBLE
//         $$.InternalType.DisplayWith = $2.DisplayWith
//         $$.InternalType.Precision = $2.Precision
//     }
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
                Precision: $2.Precision,
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
|   TIME length_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
		        Family: tree.TimeFamily,
                FamilyString: $1,
                DisplayWith: $2,
		        Precision: 0,
		        TimePrecisionIsSet: false,
		        Locale: &locale,
		        Oid: uint32(defines.MYSQL_TYPE_TIME),
	        },
        }
    }
|   TIMESTAMP length_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
		        Family:             tree.TimestampFamily,
		        Precision:          0,
                FamilyString: $1,
                DisplayWith: $2,
		        TimePrecisionIsSet: false,
		        Locale:             &locale,
		        Oid:                uint32(defines.MYSQL_TYPE_TIMESTAMP),
	        },
        }
    }
|   DATETIME length_opt
    {
        locale := ""
        $$ = &tree.T{
            InternalType: tree.InternalType{
		        Family:             tree.TimestampFamily,
		        Precision:          0,
                FamilyString: $1,
                DisplayWith: $2,
		        TimePrecisionIsSet: false,
		        Locale:             &locale,
		        Oid:                uint32(defines.MYSQL_TYPE_DATETIME),
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

char_type:
    CHAR length_opt
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
|   VARCHAR length_opt
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
|   BINARY length_opt
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
|   VARBINARY length_opt
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
|   TEXT
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
|   TINYTEXT
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
|   MEDIUMTEXT
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
|   LONGTEXT
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
|   ENUM '(' enum_values ')'
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
|   '(' INTEGRAL ')'
    {
        $$ = tree.GetDisplayWith(int32($2.(int64)))
    }

float_length_opt:
    /* EMPTY */
    {
        $$ = tree.LengthScaleOpt{
            DisplayWith: 0,
            Precision:  0,
        }
    }
|   '(' INTEGRAL ',' INTEGRAL ')'
    {
        $$ = tree.LengthScaleOpt{
            DisplayWith: tree.GetDisplayWith(int32($2.(int64))),
            Precision: int32($4.(int64)),
        }
    }

decimal_length_opt:
    /* EMPTY */
    {
        $$ = tree.LengthScaleOpt{
            DisplayWith: 0,
            Precision: 0,
        }
    }
|   '(' INTEGRAL ')'
    {
        $$ = tree.LengthScaleOpt{
            DisplayWith: tree.GetDisplayWith(int32($2.(int64))),
            Precision: 0,
        }
    }
|   '(' INTEGRAL ',' INTEGRAL ')'
    {
        $$ = tree.LengthScaleOpt{
            DisplayWith: tree.GetDisplayWith(int32($2.(int64))),
            Precision: int32($4.(int64)),
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

sql_id:
    id_or_var
|   non_reserved_keyword

reserved_sql_id:
    sql_id
|   reserved_keyword

reserved_table_id:
    table_id
|   reserved_keyword

reserved_keyword:
    ACCOUNT
|   ADD
|   ALL
|   AND
|   AS
|   ASC
|   ASCII
|   AUTO_INCREMENT
|   BETWEEN
|   BINARY
|   BY
|   CASE
|   CHAR
|   COLLATE
|   COLLATION
|   CONVERT
|   COALESCE
|   CREATE
|   CROSS
|   CHARSET
|   CURRENT_DATE
|   CURRENT_ROLE
|   CURRENT_USER
|   CURRENT_TIME
|   CURRENT_TIMESTAMP
|   CIPHER
|   SAN
|   SSL
|   SUBJECT
|   DATABASE
|   DATABASES
|   DEFAULT
|   DELETE
|   DESC
|   DESCRIBE
|   DISTINCT
|   DISTINCTROW
|   DIV
|   DROP
|   DATE
|   ELSE
|   END
|   ESCAPE
|   EXISTS
|   EXPLAIN
|   FALSE
|   FOR
|   FORCE
|   FROM
|   FORMAT
|   GROUP
|   HAVING
|   HOUR
|   IDENTIFIED
|   IF
|   IGNORE
|   IN
|   INFILE
|   INDEX
|   INNER
|   INSERT
|   INTERVAL
|   INTO
|   IS
|   ISSUER
|   JOIN
|   KEY
|   LEFT
|   LIKE
|   LIMIT
|   LOCALTIME
|   LOCALTIMESTAMP
|   LOCK
|   LOAD
|   MATCH
|   MAXVALUE
|   MOD
|   MICROSECOND
|   MINUTE
|   MONTH
|   NATURAL
|   NOT
|   NONE
|   NULL
|   ON
|   OR
|   ORDER
|   OUTER
|   REGEXP
|   RENAME
|   REPLACE
|   RIGHT
|   ROLE
|   REQUIRE
|   REPEAT
|   ROW_COUNT
|   REVERSE
|   SCHEMA
|   SELECT
|   SECOND
|   SEPARATOR
|   SET
|   SHOW
|   STRAIGHT_JOIN
|   TABLE
|   TABLES
|   THEN
|   TO
|   TRUE
|   TRUNCATE
|   TIME
|   TIMESTAMP
|   UNION
|   UNIQUE
|   UPDATE
|   USE
|   USING
|   UTC_DATE
|   UTC_TIME
|   UTC_TIMESTAMP
|   VALUES
|   WHEN
|   WHERE
|   WEEK
|   QUARTER
|   PASSWORD
|   TERMINATED
|   OPTIONALLY
|   ENCLOSED
|   ESCAPED
|   STARTING
|   LINES
|   INT1
|   INT2
|   INT3
|   INT4
|   INT8
|   CHECK
|	CONSTRAINT
|   PRIMARY
|   FULLTEXT
|   FOREIGN

non_reserved_keyword:
    AGAINST
|   AVG_ROW_LENGTH
|   AUTO_RANDOM
|   ACTION
|   ALGORITHM
|   BEGIN
|   BIGINT
|   BIT
|   BLOB
|   BOOL
|   CHARACTER
|   CHAIN
|   CHECKSUM
|   COMPRESSION
|   COMMENT_KEYWORD
|   COMMIT
|   COMMITTED
|   COLUMNS
|   CONNECTION
|   CONSISTENT
|   COMPRESSED
|   COMPACT
|   COLUMN_FORMAT
|   CASCADE
|   DATA
|   DATETIME
|   DECIMAL
|   DYNAMIC
|   DISK
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
|   EXCEPT
|   ERRORS
|   ENFORCED
|   FLOAT_TYPE
|   FULL
|   FIXED
|   FIELDS
|   GEOMETRY
|   GEOMETRYCOLLECTION
|   GLOBAL
|   GRANT
|   INT
|   INTEGER
|   INDEXES
|   ISOLATION
|   JSON
|   KEY_BLOCK_SIZE
|   KEYS
|   LANGUAGE
|   LAST_INSERT_ID
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
|   PARTITION
|   PARTITIONS
|   POINT
|   POLYGON
|   PROCEDURE
|   PROXY
|   QUERY
|   RANGE
|   READ
|   REAL
|   REORGANIZE
|   REDUNDANT
|   REPAIR
|   REPEATABLE
|   RELEASE
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
|   SPATIAL
|   START
|   STATUS
|   STORAGE
|   STATS_AUTO_RECALC
|   STATS_PERSISTENT
|   STATS_SAMPLE_PAGES
|   SUBPARTITIONS
|   SUBPARTITION
|   SIMPLE
|   TEXT
|   THAN
|   TINYBLOB
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
|   WITH
|   WRITE
|   WARNINGS
|   WORK
|   X509
|   ZEROFILL
|   YEAR
|	TYPE

not_keyword:
    ADDDATE
|   BIT_AND
|   BIT_OR
|   BIT_XOR
|   CAST
|   COUNT
|   APPROX_COUNT_DISTINCT
|   APPROX_PERCENTILE
|   CURDATE
|   CURTIME
|   DATE_ADD
|   DATE_SUB
|   EXTRACT
|   GROUP_CONCAT
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

%%
