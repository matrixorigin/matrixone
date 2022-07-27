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
	EQUAL           = iota // =
	NOT_EQUAL              // <>
	GREAT_THAN             // >
	GREAT_EQUAL            // >=
	LESS_THAN              // <
	LESS_EQUAL             // <=
	BETWEEN                // BETWEEN
	UNARY_PLUS             // UNARY_PLUS
	UNARY_MINUS            // UNARY_MINUS
	PLUS                   // +
	MINUS                  // -
	MULTI                  // *
	DIV                    // /
	INTEGER_DIV            // Div
	BIT_SHIFT_LEFT         // <<
	BIT_SHIFT_RIGHT        // >>
	MOD                    // %
	CONCAT                 // ||
	AND                    // AND
	OR                     // OR
	XOR                    // XOR
	NOT                    // NOT
	CAST                   // CAST
	IS                     //IS
	ISNOT                  //ISNOT
	ISNULL                 //ISNULL
	ISNOTNULL              //ISNOTNULL

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
	REGEXP_REPLACE // REGEXP_REPLACE
	REGEXP_SUBSTR  // REGEXP_SUBSTR
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
	STARCOUNT     // STARTCOUNT
	STARTSWITH    // STARTSWITH
	STDDEV_POP    // STDDEV_POP
	STDDEV_SAMPLE // STDDEV_SAMPLE
	SUBSTR        // SUBSTR
	SUM           // SUM
	TAN           // TAN
	TO_DATE
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
	DAY       //DAY
	DAYOFYEAR // DAYOFYEAR
	INTERVAL  // INTERVAL
	EXTRACT   // EXTRACT
	OCT
	SUBSTRING // SUBSTRING
	WEEK      //WEEK
	WEEKDAY
	YEAR // YEAR

	DATE_ADD              // DATE_ADD
	DATE_SUB              // DATE_SUB
	APPROX_COUNT_DISTINCT // APPROX_COUNT_DISTINCT, special aggregate

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
	ROLES_GRAPHML
	ROW_COUNT
	VERSION
	COLLATION

	TIMESTAMP   // TIMESTAMP
	DATE_FORMAT // DATE_FORMAT
	// FUNCTION_END_NUMBER is not a function, just a flag to record the max number of function.
	// TODO: every one should put the new function id in front of this one if you want to make a new function.
	FUNCTION_END_NUMBER
)

// functionIdRegister is what function we have registered already.
var functionIdRegister = map[string]int32{
	// operators
	"=":           EQUAL,
	">":           GREAT_THAN,
	">=":          GREAT_EQUAL,
	"<":           LESS_THAN,
	"<=":          LESS_EQUAL,
	"<>":          NOT_EQUAL,
	"!=":          NOT_EQUAL,
	"not":         NOT,
	"and":         AND,
	"or":          OR,
	"xor":         XOR,
	"like":        LIKE,
	"between":     BETWEEN,
	"in":          IN,
	"exists":      EXISTS,
	"+":           PLUS,
	"-":           MINUS,
	"*":           MULTI,
	"/":           DIV,
	"div":         INTEGER_DIV,
	"%":           MOD,
	"mod":         MOD,
	"unary_plus":  UNARY_PLUS,
	"unary_minus": UNARY_MINUS,
	"case":        CASE,
	"cast":        CAST,
	"is":          IS,
	"is_not":      ISNOT,
	"isnot":       ISNOT,
	"is_null":     ISNULL,
	"isnull":      ISNULL,
	"ifnull":      ISNULL,
	"is_not_null": ISNOTNULL,
	"isnotnull":   ISNOTNULL,
	// aggregate
	"max":                   MAX,
	"min":                   MIN,
	"sum":                   SUM,
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
	// builtin
	// whoever edit this, please follow the lexical order, or come up with a better ordering method
	// binary functions
	"endswith":    ENDSWITH,
	"findinset":   FINDINSET,
	"find_in_set": FINDINSET,
	"power":       POW,
	"startswith":  STARTSWITH,
	"to_date":     TO_DATE,
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
	"utc_timestamp":     UTC_TIMESTAMP,
	"unix_timestamp":    UNIX_TIMESTAMP,
	"from_unixtime":     FROM_UNIXTIME,
	// unary functions
	// whoever edit this, please follow the lexical order, or come up with a better ordering method
	"abs":            ABS,
	"acos":           ACOS,
	"bit_length":     BIT_LENGTH,
	"date":           DATE,
	"day":            DAY,
	"dayofyear":      DAYOFYEAR,
	"exp":            EXP,
	"empty":          EMPTY,
	"length":         LENGTH,
	"lengthutf8":     LENGTH_UTF8,
	"char_length":    LENGTH_UTF8,
	"ln":             LN,
	"log":            LOG,
	"ltrim":          LTRIM,
	"month":          MONTH,
	"oct":            OCT,
	"reverse":        REVERSE,
	"rtrim":          RTRIM,
	"sin":            SIN,
	"sinh":           SINH,
	"space":          SPACE,
	"tan":            TAN,
	"week":           WEEK,
	"weekday":        WEEKDAY,
	"year":           YEAR,
	"extract":        EXTRACT,
	"if":             IFF,
	"iff":            IFF,
	"date_add":       DATE_ADD,
	"date_sub":       DATE_SUB,
	"atan":           ATAN,
	"cos":            COS,
	"cot":            COT,
	"timestamp":      TIMESTAMP,
	"database":       DATABASE,
	"schema":         DATABASE,
	"user":           USER,
	"system_user":    USER,
	"session_user":   USER,
	"current_user":   USER,
	"connection_id":  CONNECTION_ID,
	"charset":        CHARSET,
	"current_role":   CURRENT_ROLE,
	"found_rows":     FOUND_ROWS,
	"icu_version":    ICULIBVERSION,
	"last_insert_id": LAST_INSERT_ID,
	"roles_graphml":  ROLES_GRAPHML,
	"row_count":      ROW_COUNT,
	"version":        VERSION,
	"collation":      COLLATION,
}

func GetFunctionIsWinfunByName(name string) bool {
	fid, err := fromNameToFunctionId(name)
	if err != nil {
		return false
	}
	fs := functionRegister[fid].Overloads
	return len(fs) > 0 && fs[0].Flag == plan.Function_WIN
}
