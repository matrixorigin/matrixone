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

	ABS               // ABS
	ACOS              // ACOS
	ADDDATE           // ADDDATE
	ADDTIME           // ADDTIME
	AES_DECRYPT       // AES_DECRYPT
	AES_ENCRYPT       // AES_ENCRYPT
	ANY_VALUE         // ANY_VALUE
	ARRAY_AGG         // ARRAY_AGG
	ARRAY_APPEND      // ARRAY_APPEND
	ARRAY_CAT         // ARRAY_CAT
	ARRAY_CONTAINS    // ARRAY_CONTAINS
	ARRAY_POSITION    // ARRAY_POSITION
	ARRAY_SIZE        // ARRAY_SIZE
	ASCII             // ASCII
	ASIN              // ASIN
	ATAN              // ATAN
	ATAN2             // ATAN2
	AVG               // AVG
	BASE64_DECODE     // BASE64_DECODE
	BASE64_ENCODE     // BASE64_ENCODE
	BIT_AND           // BIT_AND
	BIT_LENGTH        // BIT_LENGTH
	BIT_NOT           // BIT_NOT
	BIT_OR            // BIT_OR
	BIT_XOR           // BIT_XOR
	BITAGG_AND        // BITAGG_AND
	BITAGG_OR         // BITAGG_OR
	BOOLAGG_AND       // BOOLAGG_AND
	BOOLAGG_OR        // BOOLAGG_OR
	CASE              // CASE
	CEIL              // CEIL
	CHR               // CHR
	COALESCE          // COALESCE
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
	TO_INTERVAL   // TO_INTERVAL
	TRANSLATE     // TRANSLATE
	TRIM          // TRIM
	UNIFORM       // UNIFORM
	UTC_TIMESTAMP
	UPPER      // UPPER
	VAR_POP    // VAR_POP
	VAR_SAMPLE // VAR_SAMPLE

	EXISTS // EXISTS
	ALL    // ALL
	ANY    // ANY

	DATE      // DATE
	DAYOFYEAR // DAYOFYEAR
	INTERVAL  // INTERVAL
	EXTRACT   // EXTRACT
	OCT
	SUBSTRING // SUBSTRING
	WEEKDAY
	YEAR // YEAR

	DATE_ADD              // DATE_ADD
	DATE_SUB              // DATE_SUB
	APPROX_COUNT_DISTINCT // APPROX_COUNT_DISTINCT, special aggregate

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
	"stddev_pop":            STDDEV_POP,
	"variance":              VAR_POP,
	"approx_count_distinct": APPROX_COUNT_DISTINCT,
	// builtin
	// binary functions
	"endswith":   ENDSWITH,
	"findinset":  FINDINSET,
	"power":      POW,
	"startswith": STARTSWITH,
	// variadic functions
	"ceil":          CEIL,
	"ceiling":       CEIL,
	"floor":         FLOOR,
	"lpad":          LPAD,
	"pi":            PI,
	"round":         ROUND,
	"rpad":          RPAD,
	"substr":        SUBSTRING,
	"substring":     SUBSTRING,
	"utc_timestamp": UTC_TIMESTAMP,
	"abs":           ABS,
	"acos":          ACOS,
	"bit_length":    BIT_LENGTH,
	"date":          DATE,
	"dayofyear":     DAYOFYEAR,
	"exp":           EXP,
	"empty":         EMPTY,
	"length":        LENGTH,
	"lengthutf8":    LENGTH_UTF8,
	"ln":            LN,
	"log":           LOG,
	"ltrim":         LTRIM,
	"month":         MONTH,
	"oct":           OCT,
	"reverse":       REVERSE,
	"rtrim":         RTRIM,
	"sin":           SIN,
	"sinh":          SINH,
	"space":         SPACE,
	"tan":           TAN,
	"weekday":       WEEKDAY,
	"year":          YEAR,
	"extract":       EXTRACT,
	"iff":           IFF,
	"date_add":      DATE_ADD,
	"date_sub":      DATE_SUB,
	"atan":          ATAN,
	"cos":           COS,
	"cot":           COT,
}
