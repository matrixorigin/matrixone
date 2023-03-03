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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

const (
	Distinct     = 0x8000000000000000
	DistinctMask = 0x7FFFFFFFFFFFFFFF
)

// All function IDs
const (
	EQUAL              = iota // =
	NOT_EQUAL                 // <>
	GREAT_THAN                // >
	GREAT_EQUAL               // >=
	LESS_THAN                 // <
	LESS_EQUAL                // <=
	BETWEEN                   // BETWEEN
	UNARY_PLUS                // UNARY_PLUS +
	UNARY_MINUS               // UNARY_MINUS -
	UNARY_TILDE               // UNARY_TILDE ~
	PLUS                      // +
	MINUS                     // -
	MULTI                     // *
	DIV                       // /
	INTEGER_DIV               // Div
	MOD                       // %
	CONCAT                    // ||
	AND                       // AND
	OR                        // OR
	XOR                       // XOR
	NOT                       // NOT
	CAST                      // CAST
	IS                        //IS
	ISNOT                     //ISNOT
	ISNULL                    //ISNULL
	ISNOTNULL                 //ISNOTNULL
	ISUNKNOWN                 //ISUNKNOWN
	ISNOTUNKNOWN              //ISNOTUNKNOWN
	ISTRUE                    //ISTRUE
	ISNOTTRUE                 //ISNOTTRUE
	ISFALSE                   //ISFALSE
	ISNOTFALSE                //ISNOTTRUE
	OP_BIT_AND                // &
	OP_BIT_OR                 // |
	OP_BIT_XOR                // ^
	OP_BIT_SHIFT_LEFT         // <<
	OP_BIT_SHIFT_RIGHT        // >>

	ABS            // ABS
	ACOS           // ACOS
	ADDDATE        // ADDDATE
	ADDTIME        // ADDTIME
	AES_DECRYPT    // AES_DECRYPT
	AES_ENCRYPT    // AES_ENCRYPT
	ANY_VALUE      // ANY_VALUE
	ARRAY_AGG      // ARRAY_AGG
	ARRAY_APPEND   // ARRAY_APPEND
	ARRAY_CAT      // ARRAY_CAT
	ARRAY_CONTAINS // ARRAY_CONTAINS
	ARRAY_POSITION // ARRAY_POSITION
	ARRAY_SIZE     // ARRAY_SIZE
	ASCII          // ASCII
	ASIN           // ASIN
	ATAN           // ATAN
	ATAN2          // ATAN2
	AVG            // AVG
	BASE64_DECODE  // BASE64_DECODE
	BASE64_ENCODE  // BASE64_ENCODE
	BIT_AND        // BIT_AND
	BIT_LENGTH     // BIT_LENGTH
	BIT_NOT        // BIT_NOT
	BIT_OR         // BIT_OR
	BIT_XOR        // BIT_XOR
	BITAGG_AND     // BITAGG_AND
	BITAGG_OR      // BITAGG_OR
	BOOLAGG_AND    // BOOLAGG_AND
	BOOLAGG_OR     // BOOLAGG_OR
	CASE           // CASE
	CEIL           // CEIL
	CHR            // CHR
	COALESCE       // COALESCE
	FIELD          // FIELD
	CONCAT_WS
	CONTAINS          // CONTAINS
	CORR              // CORR
	COS               // COS
	COT               // COT
	COUNT             // COUNT
	COUNT_IF          // COUNT_IF
	COVAR_POP         // COVAR_POP
	COVAR_SAMPLE      // COVAR_SAMPLE
	CUME_DIST         // CUME_DIST
	CURRENT_DATE      // CURRENT_DATE
	CURRENT_TIMESTAMP // CURRENT_TIMESTAMP
	DATE_FROM_PARTS   // DATE_FROM_PARTS
	DATE_PART         // DATE_PART
	DATEADD           // DATEADD
	DATEDIFF          // DATEDIFF
	TIMEDIFF          // TIMEDIFF
	TIMESTAMPDIFF     // TIMESTAMPDIFF
	DENSE_RANK        // DENSE_RANK
	EMPTY
	ENDSWITH // ENDSWITH
	EXP      // EXP
	FINDINSET
	FIRST_VALUE // FIRST_VALUE
	FLOOR       // FLOOR
	GREATEST    // GREATEST
	GROUPING_ID // GROUPING_ID
	HASH        // HASH
	HASH_AGG    // HASH_AGG
	HEX_DECODE  // HEX_DECODE
	HEX_ENCODE  // HEX_ENCODE
	HEX         // HEX
	IFF         // IFF
	IFNULL      // IFNULL
	ILIKE       // ILIKE
	ILIKE_ALL   // ILIKE_ALL
	ILIKE_ANY   // ILIKE_ANY
	IN          // IN
	LAG         // LAG
	LAST_VALUE  // LAST_VALUE
	LEAD        // LEAD
	LEAST       // LEAST
	LEFT        // LEFT
	LENGTH      // LENGTH
	LENGTH_UTF8
	LIKE     // LIKE
	LIKE_ALL // LIKE_ALL
	LIKE_ANY // LIKE_ANY
	LN       // LN
	NOT_IN   // NOT_IN
	LOG      // LOG
	LOWER    // LOWER
	LPAD     // LPAD
	LTRIM    // LTRIM
	MAX      // MAX
	MEDIAN   // MEDIAN
	MIN      // MIN
	MODE     // MODE
	MONTH
	NORMAL         // NORMAL
	NTH_VALUE      // NTH_VALUE
	NTILE          // NTILE
	NULLIF         // NULLIF
	PERCENT_RANK   // PERCENT_RANK
	PI             // PI
	POSITION       // POSITION
	POW            // POW
	RADIAN         // RADIAN
	RANDOM         // RANDOM
	RANK           // RANK
	REGEXP         // REGEXP
	REGEXP_INSTR   // REGEXP_INSTR
	REGEXP_LIKE    // REGEXP_LIKE
	REGEXP_REPLACE // REGEXP_REPLACE
	REGEXP_SUBSTR  // REGEXP_SUBSTR
	REG_MATCH      // REG_MATHCH
	NOT_REG_MATCH  // NOT_REG_MATCH
	REPEAT         // REPEAT
	REPLACE        // REPLACE
	REVERSE
	RIGHT      // RIGHT
	ROUND      // ROUND
	ROW_NUMBER // ROW_NUMBER
	RPAD       // RPAD
	RTRIM      // RTRIM
	SIGN       // SIGN
	SIN        // SIN
	SINH       //SINH
	SPACE
	SPLIT         // SPLIT
	SPLIT_PART    // SPLIT_PART
	STARCOUNT     // STARTCOUNT
	STARTSWITH    // STARTSWITH
	STDDEV_POP    // STDDEV_POP
	STDDEV_SAMPLE // STDDEV_SAMPLE
	SUBSTR        // SUBSTR
	SUM           // SUM
	GROUP_CONCAT
	TAN // TAN
	TO_DATE
	STR_TO_DATE
	TO_INTERVAL // TO_INTERVAL
	TRANSLATE   // TRANSLATE
	TRIM        // TRIM
	UNIFORM     // UNIFORM
	UTC_TIMESTAMP
	UNIX_TIMESTAMP
	FROM_UNIXTIME
	UPPER      // UPPER
	VAR_POP    // VAR_POP
	VAR_SAMPLE // VAR_SAMPLE

	EXISTS // EXISTS
	ALL    // ALL
	ANY    // ANY

	DATE      // DATE
	TIME      //TIME
	DAY       //DAY
	DAYOFYEAR // DAYOFYEAR
	INTERVAL  // INTERVAL
	EXTRACT   // EXTRACT
	OCT
	SUBSTRING       // SUBSTRING
	SUBSTRING_INDEX //SUBSTRING_INDEX
	WEEK            //WEEK
	WEEKDAY
	YEAR   // YEAR
	HOUR   // HOUR
	MINUTE // MINUTE
	SECOND // SECOND

	DATE_ADD              // DATE_ADD
	DATE_SUB              // DATE_SUB
	APPROX_COUNT_DISTINCT // APPROX_COUNT_DISTINCT, special aggregate

	LOAD_FILE // LOAD_FILE

	//information functions
	//Reference to : https://dev.mysql.com/doc/refman/8.0/en/information-functions.html
	DATABASE
	USER
	CONNECTION_ID
	CHARSET
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

	TIMESTAMP    // TIMESTAMP
	DATE_FORMAT  // DATE_FORMAT
	JSON_EXTRACT // JSON_EXTRACT
	JSON_QUOTE   // JSON_QUOTE
	JSON_UNQUOTE // JSON_UNQUOTE
	FORMAT       // FORMAT
	SLEEP        // sleep for a while
	INSTR

	UUID
	SERIAL
	BIN //BIN

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

	// be used: insert into t1 values(1,1) on duplicate key update a=values(a)+a+1
	VALUES
	BINARY
	INTERNAL_CHAR_LENGTH
	INTERNAL_CHAR_SIZE
	INTERNAL_NUMERIC_PRECISION
	INTERNAL_NUMERIC_SCALE
	INTERNAL_DATETIME_PRECISION
	INTERNAL_COLUMN_CHARACTER_SET
	INTERNAL_AUTO_INCREMENT

	// FUNCTION_END_NUMBER is not a function, just a flag to record the max number of function.
	// TODO: every one should put the new function id in front of this one if you want to make a new function.
	FUNCTION_END_NUMBER
)

// functionIdRegister is what function we have registered already.
var functionIdRegister = map[string]int32{
	// operators
	"=":            EQUAL,
	">":            GREAT_THAN,
	">=":           GREAT_EQUAL,
	"<":            LESS_THAN,
	"<=":           LESS_EQUAL,
	"<>":           NOT_EQUAL,
	"!=":           NOT_EQUAL,
	"not":          NOT,
	"and":          AND,
	"or":           OR,
	"xor":          XOR,
	"like":         LIKE,
	"between":      BETWEEN,
	"in":           IN,
	"not_in":       NOT_IN,
	"exists":       EXISTS,
	"+":            PLUS,
	"-":            MINUS,
	"*":            MULTI,
	"/":            DIV,
	"div":          INTEGER_DIV,
	"%":            MOD,
	"mod":          MOD,
	"unary_plus":   UNARY_PLUS,
	"unary_minus":  UNARY_MINUS,
	"unary_tilde":  UNARY_TILDE,
	"case":         CASE,
	"coalesce":     COALESCE,
	"cast":         CAST,
	"is":           IS,
	"is_not":       ISNOT,
	"isnot":        ISNOT,
	"is_null":      ISNULL,
	"isnull":       ISNULL,
	"ifnull":       ISNULL,
	"ilike":        ILIKE,
	"is_not_null":  ISNOTNULL,
	"isnotnull":    ISNOTNULL,
	"isunknown":    ISUNKNOWN,
	"isnotunknown": ISNOTUNKNOWN,
	"istrue":       ISTRUE,
	"isnottrue":    ISNOTTRUE,
	"isfalse":      ISFALSE,
	"isnotfalse":   ISNOTFALSE,
	"&":            OP_BIT_AND,
	"|":            OP_BIT_OR,
	"^":            OP_BIT_XOR,
	"<<":           OP_BIT_SHIFT_LEFT,
	">>":           OP_BIT_SHIFT_RIGHT,
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
	"std":                   STDDEV_POP,
	"stddev_pop":            STDDEV_POP,
	"variance":              VAR_POP,
	"approx_count_distinct": APPROX_COUNT_DISTINCT,
	"any_value":             ANY_VALUE,
	"median":                MEDIAN,
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
	"utc_timestamp":     UTC_TIMESTAMP,
	"unix_timestamp":    UNIX_TIMESTAMP,
	"from_unixtime":     FROM_UNIXTIME,
	"left":              LEFT,
	// unary functions
	// whoever edit this, please follow the lexical order, or come up with a better ordering method
	"abs":                            ABS,
	"acos":                           ACOS,
	"bit_length":                     BIT_LENGTH,
	"date":                           DATE,
	"time":                           TIME,
	"hour":                           HOUR,
	"minute":                         MINUTE,
	"second":                         SECOND,
	"day":                            DAY,
	"dayofyear":                      DAYOFYEAR,
	"exp":                            EXP,
	"empty":                          EMPTY,
	"length":                         LENGTH,
	"lengthutf8":                     LENGTH_UTF8,
	"char_length":                    LENGTH_UTF8,
	"ln":                             LN,
	"log":                            LOG,
	"ltrim":                          LTRIM,
	"month":                          MONTH,
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
	"version":                        VERSION,
	"collation":                      COLLATION,
	"json_extract":                   JSON_EXTRACT,
	"json_quote":                     JSON_QUOTE,
	"enable_fault_injection":         ENABLE_FAULT_INJECTION,
	"disable_fault_injection":        DISABLE_FAULT_INJECTION,
	"add_fault_point":                ADD_FAULT_POINT,
	"remove_fault_point":             REMOVE_FAULT_POINT,
	"trigger_fault_point":            TRIGGER_FAULT_POINT,
	"uuid":                           UUID,
	"load_file":                      LOAD_FILE,
	"hex":                            HEX,
	"serial":                         SERIAL,
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
	"mo_log_date":                    MO_LOG_DATE,
	"values":                         VALUES,
	"binary":                         BINARY,
	"internal_char_length":           INTERNAL_CHAR_LENGTH,
	"internal_char_size":             INTERNAL_CHAR_SIZE,
	"internal_numeric_precision":     INTERNAL_NUMERIC_PRECISION,
	"internal_numeric_scale":         INTERNAL_NUMERIC_SCALE,
	"internal_datetime_precision":    INTERNAL_DATETIME_PRECISION,
	"internal_column_character_set":  INTERNAL_COLUMN_CHARACTER_SET,
	"internal_auto_increment":        INTERNAL_AUTO_INCREMENT,
}

func GetFunctionIsWinfunByName(name string) bool {
	fid, exists := fromNameToFunctionIdWithoutError(name)
	if !exists {
		return false
	}
	fs := functionRegister[fid].Overloads
	return len(fs) > 0 && fs[0].TestFlag(plan.Function_WIN)
}
