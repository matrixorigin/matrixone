// Copyright 2021 - 2022 Matrix Origin
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

package function

const (
	Distinct     = 0x8000000000000000
	DistinctMask = 0x7FFFFFFFFFFFFFFF
)

// All function IDs
const (
	EQUAL       = iota // =
	NOT_EQUAL          // <>
	GREAT_THAN         // >
	GREAT_EQUAL        // >=
	LESS_THAN          // <
	LESS_EQUAL         // <=
	BETWEEN
	UNARY_PLUS  // UNARY_PLUS +
	UNARY_MINUS // UNARY_MINUS -
	UNARY_TILDE // UNARY_TILDE ~
	PLUS        // +
	MINUS       // -
	MULTI       // *
	DIV         // /
	INTEGER_DIV // Div
	MOD         // %
	CONCAT      // ||
	AND
	OR
	XOR
	NOT
	CAST
	BIT_CAST
	IS
	ISNOT
	ISNULL
	ISNOTNULL
	ISTRUE
	ISNOTTRUE
	ISFALSE
	ISNOTFALSE
	ISEMPTY
	NOT_IN_ROWS
	OP_BIT_AND         // &
	OP_BIT_OR          // |
	OP_BIT_XOR         // ^
	OP_BIT_SHIFT_LEFT  // <<
	OP_BIT_SHIFT_RIGHT // >>

	ABS
	ACOS
	ADDDATE
	ADDTIME
	AES_DECRYPT
	AES_ENCRYPT
	ANY_VALUE
	APPROX_COUNT
	ARRAY_AGG
	ARRAY_APPEND
	ARRAY_CAT
	ARRAY_CONTAINS
	ARRAY_POSITION
	ARRAY_SIZE
	ASCII
	ASIN
	ASSERT
	ATAN
	ATAN2
	AVG
	BASE64_DECODE
	BASE64_ENCODE
	BIT_AND
	BIT_LENGTH
	BIT_NOT
	BIT_OR
	BIT_XOR
	BITAGG_AND
	BITAGG_OR
	BOOLAGG_AND
	BOOLAGG_OR
	CASE
	CEIL
	CHR
	COALESCE
	FIELD
	CONCAT_WS
	CONTAINS
	CORR
	COS
	COT
	COUNT
	COUNT_IF
	COVAR_POP
	COVAR_SAMPLE
	CONVERT_TZ
	CUME_DIST
	CURRENT_DATE
	CURRENT_TIMESTAMP
	DATE_FROM_PARTS
	DATE_PART
	DATEADD
	DATEDIFF
	TIMEDIFF
	TIMESTAMPDIFF
	DENSE_RANK
	EMPTY
	ENDSWITH
	EXP
	FINDINSET
	FIRST_VALUE
	FLOOR
	GREATEST
	GROUPING_ID
	HASH
	HASH_AGG
	HEX_DECODE
	HEX_ENCODE
	HEX
	IFF
	IFNULL
	ILIKE
	ILIKE_ALL
	ILIKE_ANY
	IN
	LAG
	LAST_VALUE
	LEAD
	LEAST
	LEFT
	LENGTH
	LENGTH_UTF8
	LIKE
	LIKE_ALL
	LIKE_ANY
	LN
	NOT_IN
	LOG
	LOG2
	LOG10
	LOWER
	LPAD
	LTRIM
	MAX
	MEDIAN
	MIN
	MODE
	MONTH
	NORMAL
	NTH_VALUE
	NTILE
	NULLIF
	PERCENT_RANK
	PI
	POSITION
	POW
	PREFIX_EQ
	PREFIX_IN
	PREFIX_BETWEEN
	RADIAN
	RANDOM
	RANK
	REGEXP
	REGEXP_INSTR
	REGEXP_LIKE
	REGEXP_REPLACE
	REGEXP_SUBSTR
	REG_MATCH
	NOT_REG_MATCH
	REPEAT
	REPLACE
	REVERSE
	RIGHT
	ROUND
	ROW_NUMBER
	RPAD
	RTRIM
	SIGN
	SIN
	SINH
	SPACE
	SPLIT
	SPLIT_PART
	SQRT
	STARCOUNT
	STARTSWITH
	STDDEV_POP
	STDDEV_SAMPLE
	SUBSTR
	SUM
	GROUP_CONCAT
	TAN
	TO_DATE
	STR_TO_DATE
	TO_INTERVAL
	TRANSLATE
	TRIM
	UNIFORM
	SHA2
	UTC_TIMESTAMP
	UNIX_TIMESTAMP
	FROM_UNIXTIME
	UPPER
	VAR_POP
	VAR_SAMPLE

	DATE
	TIME
	DAY
	DAYOFYEAR
	INTERVAL
	EXTRACT
	OCT
	SUBSTRING
	ENCODE
	DECODE
	SUBSTRING_INDEX
	WEEK
	WEEKDAY
	YEAR
	HOUR
	MINUTE
	SECOND
	TO_DAYS
	TO_SECONDS

	DATE_ADD
	DATE_SUB
	APPROX_COUNT_DISTINCT

	LOAD_FILE

	//information functions
	//Reference to : https://dev.mysql.com/doc/refman/8.0/en/information-functions.html
	DATABASE
	USER
	CONNECTION_ID
	CHARSET
	CONVERT
	CURRENT_ROLE
	FOUND_ROWS
	ICULIBVERSION
	LAST_INSERT_ID
	LAST_QUERY_ID
	LAST_UUID
	ROLES_GRAPHML
	ROW_COUNT
	VERSION
	COLLATION
	CURRENT_ACCOUNT_ID
	CURRENT_ACCOUNT_NAME
	CURRENT_ROLE_ID
	CURRENT_ROLE_NAME
	CURRENT_USER_ID
	CURRENT_USER_NAME

	TIMESTAMP
	DATE_FORMAT
	JSON_EXTRACT
	JSON_QUOTE
	JSON_UNQUOTE
	FORMAT
	SLEEP
	INSTR
	LOCATE

	UUID
	SERIAL
	SERIAL_FULL
	SERIAL_EXTRACT
	BIN

	ENABLE_FAULT_INJECTION
	DISABLE_FAULT_INJECTION
	ADD_FAULT_POINT     // Add a fault point
	REMOVE_FAULT_POINT  // Remove
	TRIGGER_FAULT_POINT // Trigger.

	MO_MEMORY_USAGE // Dump memory usage
	MO_ENABLE_MEMORY_USAGE_DETAIL
	MO_DISABLE_MEMORY_USAGE_DETAIL

	// MO_CTL is used to check some internal status, and issue some ctl commands to the service.
	// see builtin.ctl.ctl.go to get detail.
	MO_CTL

	MO_SHOW_VISIBLE_BIN // parse type/onUpdate/default []byte to visible string

	MO_TABLE_ROWS    // table rows
	MO_TABLE_SIZE    // table size
	MO_TABLE_COL_MAX // table column max value
	MO_TABLE_COL_MIN // table column min value

	MO_LOG_DATE // parse date from string, like __mo_filepath
	MO_CHECH_LEVEL
	PURGE_LOG // purge mo internal log, like rawlog, statement_info, metric
	MO_CU
	MO_CU_V1

	GIT_VERSION
	BUILD_VERSION

	// be used: insert into t1 values(1,1) on duplicate key update a=values(a)+a+1
	VALUES
	BINARY
	INTERNAL_CHAR_LENGTH
	INTERNAL_CHAR_SIZE
	INTERNAL_NUMERIC_PRECISION
	INTERNAL_NUMERIC_SCALE
	INTERNAL_DATETIME_SCALE
	INTERNAL_COLUMN_CHARACTER_SET
	INTERNAL_AUTO_INCREMENT

	// be uesed: enum
	CAST_INDEX_TO_VALUE
	CAST_VALUE_TO_INDEX
	CAST_INDEX_VALUE_TO_INDEX

	//Sequence function
	NEXTVAL
	SETVAL
	CURRVAL
	LASTVAL

	// Array Function
	SUMMATION
	L1_NORM // L1_NORMALIZATION
	L2_NORM // L2 NORMALIZATION
	INNER_PRODUCT
	COSINE_SIMILARITY
	VECTOR_DIMS     //VECTOR DIMENSIONS
	NORMALIZE_L2    //NORMALIZE L2
	L2_DISTANCE     //L2_DISTANCE
	COSINE_DISTANCE //COSINE_DISTANCE
	CLUSTER_CENTERS // CLUSTER_CENTERS
	SUB_VECTOR      // SUB_VECTOR

	PYTHON_UDF

	// observation function
	MO_CPU
	MO_MEMORY
	MO_CPU_DUMP

	// bitmap function
	BITMAP_BIT_POSITION
	BITMAP_BUCKET_NUMBER
	BITMAP_COUNT
	BITMAP_CONSTRUCT_AGG
	BITMAP_OR_AGG

	// FUNCTION_END_NUMBER is not a function, just a flag to record the max number of function.
	// TODO: every one should put the new function id in front of this one if you want to make a new function.
	FUNCTION_END_NUMBER
)

// functionIdRegister is what function we have registered already.
var functionIdRegister = map[string]int32{
	// operators
	"=":              EQUAL,
	">":              GREAT_THAN,
	">=":             GREAT_EQUAL,
	"<":              LESS_THAN,
	"<=":             LESS_EQUAL,
	"<>":             NOT_EQUAL,
	"!=":             NOT_EQUAL,
	"not":            NOT,
	"and":            AND,
	"or":             OR,
	"xor":            XOR,
	"like":           LIKE,
	"between":        BETWEEN,
	"in":             IN,
	"not_in":         NOT_IN,
	"+":              PLUS,
	"-":              MINUS,
	"*":              MULTI,
	"/":              DIV,
	"div":            INTEGER_DIV,
	"%":              MOD,
	"mod":            MOD,
	"unary_plus":     UNARY_PLUS,
	"unary_minus":    UNARY_MINUS,
	"unary_tilde":    UNARY_TILDE,
	"unary_mark":     NOT,
	"case":           CASE,
	"coalesce":       COALESCE,
	"cast":           CAST,
	"bit_cast":       BIT_CAST,
	"is":             IS,
	"is_not":         ISNOT,
	"isnot":          ISNOT,
	"is_null":        ISNULL,
	"isnull":         ISNULL,
	"ifnull":         ISNULL,
	"ilike":          ILIKE,
	"is_not_null":    ISNOTNULL,
	"isnotnull":      ISNOTNULL,
	"isunknown":      ISNULL,
	"isnotunknown":   ISNOTNULL,
	"istrue":         ISTRUE,
	"isnottrue":      ISNOTTRUE,
	"isfalse":        ISFALSE,
	"isnotfalse":     ISNOTFALSE,
	"&":              OP_BIT_AND,
	"|":              OP_BIT_OR,
	"^":              OP_BIT_XOR,
	"<<":             OP_BIT_SHIFT_LEFT,
	">>":             OP_BIT_SHIFT_RIGHT,
	"decode":         DECODE,
	"prefix_eq":      PREFIX_EQ,
	"prefix_in":      PREFIX_IN,
	"prefix_between": PREFIX_BETWEEN,
	// aggregate
	"max":                   MAX,
	"min":                   MIN,
	"sum":                   SUM,
	"group_concat":          GROUP_CONCAT,
	"avg":                   AVG,
	"count":                 COUNT,
	"starcount":             STARCOUNT,
	"bit_or":                BIT_OR,
	"bit_and":               BIT_AND,
	"bit_xor":               BIT_XOR,
	"cluster_centers":       CLUSTER_CENTERS,
	"subvector":             SUB_VECTOR,
	"std":                   STDDEV_POP,
	"stddev_pop":            STDDEV_POP,
	"variance":              VAR_POP,
	"var_pop":               VAR_POP,
	"approx_count":          APPROX_COUNT,
	"approx_count_distinct": APPROX_COUNT_DISTINCT,
	"any_value":             ANY_VALUE,
	"median":                MEDIAN,
	// count window
	"rank": RANK,
	// builtin
	// whoever edit this, please follow the lexical order, or come up with a better ordering method
	// binary functions
	"endswith":    ENDSWITH,
	"findinset":   FINDINSET,
	"find_in_set": FINDINSET,
	"power":       POW,
	"startswith":  STARTSWITH,
	"to_date":     STR_TO_DATE,
	"str_to_date": STR_TO_DATE,
	"date_format": DATE_FORMAT,
	// whoever edit this, please follow the lexical order, or come up with a better ordering method
	// variadic functions
	"ceil":              CEIL,
	"ceiling":           CEIL,
	"concat_ws":         CONCAT_WS,
	"concat":            CONCAT,
	"current_timestamp": CURRENT_TIMESTAMP,
	"now":               CURRENT_TIMESTAMP,
	"floor":             FLOOR,
	"lpad":              LPAD,
	"pi":                PI,
	"round":             ROUND,
	"rpad":              RPAD,
	"substr":            SUBSTRING,
	"substring":         SUBSTRING,
	"mid":               SUBSTRING,
	"encode":            ENCODE,
	"utc_timestamp":     UTC_TIMESTAMP,
	"unix_timestamp":    UNIX_TIMESTAMP,
	"from_unixtime":     FROM_UNIXTIME,
	"left":              LEFT,
	// unary functions
	// whoever edit this, please follow the lexical order, or come up with a better ordering method
	"abs":                            ABS,
	"acos":                           ACOS,
	"assert":                         ASSERT,
	"bit_length":                     BIT_LENGTH,
	"date":                           DATE,
	"time":                           TIME,
	"hour":                           HOUR,
	"minute":                         MINUTE,
	"second":                         SECOND,
	"sqrt":                           SQRT,
	"to_seconds":                     TO_SECONDS,
	"day":                            DAY,
	"to_days":                        TO_DAYS,
	"dayofyear":                      DAYOFYEAR,
	"exp":                            EXP,
	"empty":                          EMPTY,
	"length":                         LENGTH,
	"lengthutf8":                     LENGTH_UTF8,
	"char_length":                    LENGTH_UTF8,
	"ln":                             LN,
	"log":                            LOG,
	"log2":                           LOG2,
	"log10":                          LOG10,
	"ltrim":                          LTRIM,
	"month":                          MONTH,
	"not_in_rows":                    NOT_IN_ROWS,
	"oct":                            OCT,
	"rand":                           RANDOM,
	"reverse":                        REVERSE,
	"rtrim":                          RTRIM,
	"sin":                            SIN,
	"sinh":                           SINH,
	"space":                          SPACE,
	"tan":                            TAN,
	"week":                           WEEK,
	"weekday":                        WEEKDAY,
	"year":                           YEAR,
	"extract":                        EXTRACT,
	"if":                             IFF,
	"iff":                            IFF,
	"isempty":                        ISEMPTY,
	"date_add":                       DATE_ADD,
	"date_sub":                       DATE_SUB,
	"atan":                           ATAN,
	"cos":                            COS,
	"cot":                            COT,
	"timestamp":                      TIMESTAMP,
	"database":                       DATABASE,
	"schema":                         DATABASE,
	"user":                           USER,
	"system_user":                    USER,
	"session_user":                   USER,
	"current_user":                   USER,
	"connection_id":                  CONNECTION_ID,
	"charset":                        CHARSET,
	"convert":                        CONVERT,
	"convert_tz":                     CONVERT_TZ,
	"current_account_id":             CURRENT_ACCOUNT_ID,
	"current_account_name":           CURRENT_ACCOUNT_NAME,
	"current_role":                   CURRENT_ROLE,
	"current_role_id":                CURRENT_ROLE_ID,
	"current_role_name":              CURRENT_ROLE_NAME,
	"current_user_id":                CURRENT_USER_ID,
	"current_user_name":              CURRENT_USER_NAME,
	"found_rows":                     FOUND_ROWS,
	"icu_version":                    ICULIBVERSION,
	"last_insert_id":                 LAST_INSERT_ID,
	"last_query_id":                  LAST_QUERY_ID,
	"last_uuid":                      LAST_QUERY_ID,
	"roles_graphml":                  ROLES_GRAPHML,
	"row_count":                      ROW_COUNT,
	"row_number":                     ROW_NUMBER,
	"version":                        VERSION,
	"collation":                      COLLATION,
	"json_extract":                   JSON_EXTRACT,
	"json_quote":                     JSON_QUOTE,
	"enable_fault_injection":         ENABLE_FAULT_INJECTION,
	"disable_fault_injection":        DISABLE_FAULT_INJECTION,
	"dense_rank":                     DENSE_RANK,
	"add_fault_point":                ADD_FAULT_POINT,
	"remove_fault_point":             REMOVE_FAULT_POINT,
	"trigger_fault_point":            TRIGGER_FAULT_POINT,
	"uuid":                           UUID,
	"load_file":                      LOAD_FILE,
	"hex":                            HEX,
	"serial":                         SERIAL,
	"serial_full":                    SERIAL_FULL,
	"serial_extract":                 SERIAL_EXTRACT,
	"hash_value":                     HASH,
	"bin":                            BIN,
	"datediff":                       DATEDIFF,
	"timestampdiff":                  TIMESTAMPDIFF,
	"timediff":                       TIMEDIFF,
	"reg_match":                      REG_MATCH,
	"not_reg_match":                  NOT_REG_MATCH,
	"regexp_instr":                   REGEXP_INSTR,
	"regexp_like":                    REGEXP_LIKE,
	"regexp_replace":                 REGEXP_REPLACE,
	"regexp_substr":                  REGEXP_SUBSTR,
	"repeat":                         REPEAT,
	"mo_memory_usage":                MO_MEMORY_USAGE,
	"mo_enable_memory_usage_detail":  MO_ENABLE_MEMORY_USAGE_DETAIL,
	"mo_disable_memory_usage_detail": MO_DISABLE_MEMORY_USAGE_DETAIL,
	"mo_ctl":                         MO_CTL,
	"mo_show_visible_bin":            MO_SHOW_VISIBLE_BIN,
	"substring_index":                SUBSTRING_INDEX,
	"field":                          FIELD,
	"format":                         FORMAT,
	"sleep":                          SLEEP,
	"split_part":                     SPLIT_PART,
	"instr":                          INSTR,
	"locate":                         LOCATE,
	"curdate":                        CURRENT_DATE,
	"current_date":                   CURRENT_DATE,
	"json_unquote":                   JSON_UNQUOTE,
	"ascii":                          ASCII,
	"replace":                        REPLACE,
	"mo_table_rows":                  MO_TABLE_ROWS,
	"mo_table_size":                  MO_TABLE_SIZE,
	"mo_table_col_max":               MO_TABLE_COL_MAX,
	"mo_table_col_min":               MO_TABLE_COL_MIN,
	"trim":                           TRIM,
	"sha2":                           SHA2,
	"mo_log_date":                    MO_LOG_DATE,
	"mo_check_level":                 MO_CHECH_LEVEL,
	"purge_log":                      PURGE_LOG,
	"mo_cu":                          MO_CU,
	"mo_cu_v1":                       MO_CU_V1,
	"git_version":                    GIT_VERSION,
	"build_version":                  BUILD_VERSION,
	"values":                         VALUES,
	"binary":                         BINARY,
	"internal_char_length":           INTERNAL_CHAR_LENGTH,
	"internal_char_size":             INTERNAL_CHAR_SIZE,
	"internal_numeric_precision":     INTERNAL_NUMERIC_PRECISION,
	"internal_numeric_scale":         INTERNAL_NUMERIC_SCALE,
	"internal_datetime_scale":        INTERNAL_DATETIME_SCALE,
	"internal_column_character_set":  INTERNAL_COLUMN_CHARACTER_SET,
	"internal_auto_increment":        INTERNAL_AUTO_INCREMENT,
	"nextval":                        NEXTVAL,
	"setval":                         SETVAL,
	"currval":                        CURRVAL,
	"lastval":                        LASTVAL,
	"cast_index_to_value":            CAST_INDEX_TO_VALUE,
	"cast_value_to_index":            CAST_VALUE_TO_INDEX,
	"cast_index_value_to_index":      CAST_INDEX_VALUE_TO_INDEX,
	"to_upper":                       UPPER,
	"upper":                          UPPER,
	"ucase":                          UPPER,
	"to_lower":                       LOWER,
	"lower":                          LOWER,
	"lcase":                          LOWER,

	"summation":         SUMMATION,
	"l1_norm":           L1_NORM,
	"l2_norm":           L2_NORM,
	"inner_product":     INNER_PRODUCT,
	"cosine_similarity": COSINE_SIMILARITY,
	"vector_dims":       VECTOR_DIMS,
	"normalize_l2":      NORMALIZE_L2,
	"l2_distance":       L2_DISTANCE,
	"cosine_distance":   COSINE_DISTANCE,

	"python_user_defined_function": PYTHON_UDF,

	"mo_cpu":      MO_CPU,
	"mo_memory":   MO_MEMORY,
	"mo_cpu_dump": MO_CPU_DUMP,
	// bitmap function
	"bitmap_bit_position":  BITMAP_BIT_POSITION,
	"bitmap_bucket_number": BITMAP_BUCKET_NUMBER,
	"bitmap_count":         BITMAP_COUNT,
	"bitmap_construct_agg": BITMAP_CONSTRUCT_AGG,
	"bitmap_or_agg":        BITMAP_OR_AGG,
}
