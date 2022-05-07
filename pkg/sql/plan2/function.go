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

package plan2

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

//
// Function signature.  ArgTypeClass and ArgType need some explaination.
// ArgTypeClass defines type equivalent class for this function.  The first
// equivlent class is the return type.   For each argument of this function
// it has a arg type, which is an index into the ArgTypeClass.   Let's
// use an example,
//     "=", STRICT, {BOOL, ANY}, {1, 1}}
// means,
//     function name is "=".
//     it is a STRICT function, that is, it will return null iff
//             it has a null arguments
//     Two type equivalent class
//             first is BOOL, means the the return type is BOOL
//             second is ANY, means any type
//     {1, 1}, means the function take two arguments.
//             first arg is of type class 1, which is ANY
//             second arg is of type class 1 too, which is ANY
//             both arg is of same class, means they must be same
//             type.
// Therefore, it means "=" is a strict function, taking two arguments
// of ANY type (but both arguments must be of SAME type), and return BOOL.
//
type FunctionSig struct {
	Name         string
	Flag         plan.Function_FuncFlag
	ArgTypeClass []plan.Type_TypeId
	ArgType      []int8
}

// Functions shipped by system.
var BuiltinFunctions = [...]*FunctionSig{
	// Operators,
	{"=", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_ANY}, []int8{1, 1}},
	{"<>", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_ANY}, []int8{1, 1}},
	{">", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_ANY}, []int8{1, 1}},
	{">=", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_ANY}, []int8{1, 1}},
	{"<", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_ANY}, []int8{1, 1}},
	{"<=", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_ANY}, []int8{1, 1}},
	{"BETWEEN", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_ANY}, []int8{1, 1, 1}},
	{"UNARY_PLUS", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYNUMBER}, []int8{0}},
	{"UNARY_MINUS", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYNUMBER}, []int8{0}},
	{"+", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYNUMBER}, []int8{0, 0}},
	{"-", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYNUMBER}, []int8{0, 0}},
	{"*", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYNUMBER}, []int8{0, 0}},
	{"/", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYNUMBER}, []int8{0, 0}},
	{"<<", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYINT, plan.Type_INT32}, []int8{0, 1}},
	{">>", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYINT, plan.Type_INT32}, []int8{0, 1}},
	{"%", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYNUMBER}, []int8{0, 0}},
	{"||", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0, 0}},
	{"AND", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL}, []int8{0, 0}},
	{"OR", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL}, []int8{0, 0}},
	{"NOT", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL}, []int8{0}},
	{"CAST", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANY, plan.Type_ANY}, []int8{1}},

	// Functions
	{"ABS", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYNUMBER}, []int8{0}},
	{"ACOS", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},
	{"ADDDATE", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYTIME, plan.Type_INTERVAL}, []int8{0, 1}},
	{"ADDTIME", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYTIME, plan.Type_TIME}, []int8{0, 1}},
	{"AES_DECRYPT", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0, 0}},
	{"AES_ENCRYPT", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0, 0}},
	{"ANY_VALUE", plan.Function_AGG, []plan.Type_TypeId{plan.Type_ANY}, []int8{0}},
	{"ARRAY_AGG", plan.Function_AGG, []plan.Type_TypeId{plan.Type_ARRAY, plan.Type_ANY}, []int8{1}},
	{"ARRAY_APPEND", plan.Function_NONE, []plan.Type_TypeId{plan.Type_ARRAY, plan.Type_ANY}, []int8{0, 1}},
	{"ARRAY_CAT", plan.Function_NONE, []plan.Type_TypeId{plan.Type_ARRAY}, []int8{0, 0}},
	{"ARRAY_CONTAINS", plan.Function_NONE, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_ARRAY, plan.Type_ANY}, []int8{1, 2}},
	{"ARRAY_POSITION", plan.Function_NONE, []plan.Type_TypeId{plan.Type_INT32, plan.Type_ARRAY, plan.Type_ANY}, []int8{1, 2}},
	{"ARRAY_SIZE", plan.Function_NONE, []plan.Type_TypeId{plan.Type_INT32, plan.Type_ARRAY}, []int8{1}},
	{"ASCII", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_INT32, plan.Type_VARCHAR}, []int8{1}},
	{"ASIN", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},
	{"ATAN", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},
	{"ATAN2", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0, 0}},
	{"AVG", plan.Function_AGG, []plan.Type_TypeId{plan.Type_FLOAT64, plan.Type_ANYNUMBER}, []int8{1}},

	{"BASE64_DECODE", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARBINARY, plan.Type_VARCHAR}, []int8{1}},
	{"BASE64_ENCODE", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR, plan.Type_VARBINARY}, []int8{1}},
	{"BIT_AND", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYINT}, []int8{1, 1}},
	{"BIT_NOT", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYINT}, []int8{1}},
	{"BIT_OR", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYINT}, []int8{1, 1}},
	{"BIT_XOR", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYINT}, []int8{1, 1}},
	{"BITAGG_AND", plan.Function_AGG, []plan.Type_TypeId{plan.Type_ANYINT}, []int8{1}},
	{"BITAGG_OR", plan.Function_AGG, []plan.Type_TypeId{plan.Type_ANYINT}, []int8{1}},
	{"BOOLAGG_AND", plan.Function_AGG, []plan.Type_TypeId{plan.Type_BOOL}, []int8{1}},
	{"BOOLAGG_OR", plan.Function_AGG, []plan.Type_TypeId{plan.Type_BOOL}, []int8{1}},

	{"CASE", plan.Function_NONE, []plan.Type_TypeId{plan.Type_ANY, plan.Type_ANY, plan.Type_ARRAY, plan.Type_ARRAY}, []int8{1, 2, 3}},
	{"CEIL", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYNUMBER, plan.Type_INT32}, []int8{0, -1}},
	{"CHR", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR, plan.Type_INT32}, []int8{1}},
	{"COALESCE", plan.Function_VARARG, []plan.Type_TypeId{plan.Type_ANY}, []int8{0}},
	{"CONTAINS", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_VARCHAR}, []int8{1, 1}},
	{"CORR", plan.Function_AGG, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0, 0}},
	{"COS", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},
	{"COT", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},
	{"COUNT", plan.Function_AGG, []plan.Type_TypeId{plan.Type_INT64, plan.Type_ANY}, []int8{1}},
	{"COUNT_IF", plan.Function_AGG, []plan.Type_TypeId{plan.Type_INT64, plan.Type_ANY, plan.Type_BOOL}, []int8{1, 2}},
	{"COVAR_POP", plan.Function_AGG, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0, 0}},
	{"COVAR_SAMPLE", plan.Function_AGG, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0, 0}},
	{"CUME_DIST", plan.Function_WIN, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{}},
	{"CURRENT_DATE", plan.Function_STABLE, []plan.Type_TypeId{plan.Type_DATE}, []int8{}},
	{"CURRENT_TIMESTAMP", plan.Function_STABLE, []plan.Type_TypeId{plan.Type_TIMESTAMP}, []int8{}},

	{"DATE_FROM_PARTS", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_DATE, plan.Type_INT32}, []int8{1, 1, 1}},
	{"DATE_PART", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_INT32, plan.Type_VARCHAR, plan.Type_ANYTIME}, []int8{1, 2}},
	{"DATEADD", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYTIME, plan.Type_VARCHAR, plan.Type_INT64}, []int8{1, 2, 0}},
	{"DATEDIFF", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_INT64, plan.Type_VARCHAR, plan.Type_ANYTIME}, []int8{1, 2, 2}},
	{"DENSE_RANK", plan.Function_WIN, []plan.Type_TypeId{plan.Type_INT32}, []int8{}},

	{"ENDSWITH", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_VARCHAR}, []int8{1, 1}},
	{"EXP", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},

	{"FIRST_VALUE", plan.Function_AGG, []plan.Type_TypeId{plan.Type_ANY}, []int8{0}},
	{"FLOOR", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYNUMBER, plan.Type_INT32}, []int8{0, -1}},

	{"GREATEST", plan.Function_VARARG, []plan.Type_TypeId{plan.Type_ANY}, []int8{0}},
	{"GROUPING_ID", plan.Function_VARARG, []plan.Type_TypeId{plan.Type_INT64, plan.Type_ANY}, []int8{-1}},

	{"HASH", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_INT64, plan.Type_ANY}, []int8{1}},
	{"HASH_AGG", plan.Function_AGG, []plan.Type_TypeId{plan.Type_INT64, plan.Type_ANY}, []int8{1}},
	{"HEX_DECODE", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARBINARY, plan.Type_VARCHAR}, []int8{1}},
	{"HEX_ENCODE", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR, plan.Type_VARBINARY}, []int8{1}},

	{"IFF", plan.Function_NONE, []plan.Type_TypeId{plan.Type_ANY, plan.Type_BOOL}, []int8{1, 0, 0}},
	{"IFNULL", plan.Function_NONE, []plan.Type_TypeId{plan.Type_ANY}, []int8{0}},
	{"ILIKE", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_VARCHAR}, []int8{1, 1}},
	{"ILIKE_ALL", plan.Function_STRICT | plan.Function_VARARG, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0, 0}},
	{"ILIKE_ANY", plan.Function_STRICT | plan.Function_VARARG, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0, 0}},
	{"IN", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_ANY, plan.Type_ARRAY}, []int8{1, 2}},

	{"LAG", plan.Function_WIN, []plan.Type_TypeId{plan.Type_ANY, plan.Type_INT32, plan.Type_ANY}, []int8{1, 2}},
	{"LAST_VALUE", plan.Function_AGG, []plan.Type_TypeId{plan.Type_ANY}, []int8{0}},
	{"LEAD", plan.Function_WIN, []plan.Type_TypeId{plan.Type_ANY, plan.Type_INT32, plan.Type_ANY}, []int8{1, 2}},
	{"LEAST", plan.Function_VARARG, []plan.Type_TypeId{plan.Type_ANY}, []int8{0}},
	{"LEFT", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR, plan.Type_INT32}, []int8{0, 1}},
	{"LENGTH", plan.Function_VARARG, []plan.Type_TypeId{plan.Type_INT32, plan.Type_VARCHAR}, []int8{1}},
	{"LIKE", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_VARCHAR}, []int8{1, 1}},
	{"LIKE_ALL", plan.Function_STRICT | plan.Function_VARARG, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0, 0}},
	{"LIKE_ANY", plan.Function_STRICT | plan.Function_VARARG, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0, 0}},
	{"LN", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},
	{"LOG", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},
	{"LOWER", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0}},
	{"LPAD", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR, plan.Type_INT32}, []int8{0, 1, 0}},
	{"LTRIM", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0, 0}},

	{"MAX", plan.Function_AGG, []plan.Type_TypeId{plan.Type_ANY}, []int8{0}},
	{"MEDIAN", plan.Function_AGG, []plan.Type_TypeId{plan.Type_ANY}, []int8{0}},
	{"MIN", plan.Function_AGG, []plan.Type_TypeId{plan.Type_ANY}, []int8{0}},
	{"MODE", plan.Function_AGG, []plan.Type_TypeId{plan.Type_ANY}, []int8{0}},

	{"NORMAL", plan.Function_VOLATILE, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0, 0}},
	{"NTH_VALUE", plan.Function_AGG, []plan.Type_TypeId{plan.Type_ANY, plan.Type_INT32}, []int8{0, 1}},
	{"NTILE", plan.Function_WIN, []plan.Type_TypeId{plan.Type_INT32}, []int8{0}},
	{"NULLIF", plan.Function_WIN, []plan.Type_TypeId{plan.Type_ANY}, []int8{0, 0}},

	{"PERCENT_RANK", plan.Function_WIN, []plan.Type_TypeId{plan.Type_INT32}, []int8{}},
	{"POSITION", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_INT32, plan.Type_VARCHAR}, []int8{1, 1, 0}},
	{"POW", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0, 0}},

	{"RADIAN", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},
	{"RANDOM", plan.Function_VOLATILE, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},
	{"RANK", plan.Function_WIN, []plan.Type_TypeId{plan.Type_INT32}, []int8{}},
	{"REGEXP", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_VARCHAR}, []int8{1, 1}},
	{"REGEXP_REPLACE", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0, 0}},
	{"REGEXP_SUBSTR", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0, 0}},
	{"REPEAT", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR, plan.Type_INT32}, []int8{0, 1}},
	{"REPLACE", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0, 0}},
	{"RIGHT", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR, plan.Type_INT32}, []int8{0, 1}},
	{"ROUND", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ANYNUMBER, plan.Type_INT32}, []int8{0, -1}},
	{"ROW_NUMBER", plan.Function_WIN, []plan.Type_TypeId{plan.Type_INT32}, []int8{}},
	{"RPAD", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR, plan.Type_INT32}, []int8{0, 1, 0}},
	{"RTRIM", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0, 0}},

	{"SIGN", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_INT32, plan.Type_ANYNUMBER}, []int8{1}},
	{"SIN", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},
	{"SPLIT", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_ARRAY, plan.Type_VARCHAR}, []int8{1, 1}},
	{"STARTSWITH", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_VARCHAR}, []int8{1, 1}},
	{"STDDEV_POP", plan.Function_AGG, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},
	{"STDDEV_SAMPLE", plan.Function_AGG, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},
	{"SUBSTR", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR, plan.Type_INT32}, []int8{0, 0, -1}},
	{"SUM", plan.Function_AGG, []plan.Type_TypeId{plan.Type_ANYNUMBER, plan.Type_ANYNUMBER}, []int8{1}},

	{"TAN", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},
	{"TO_INTERVAL", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_INTERVAL, plan.Type_INT32, plan.Type_VARCHAR}, []int8{1, 2}},
	{"TRANSLATE", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0, 0}},
	{"TRIM", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0, 0}},

	{"UNIFORM", plan.Function_VOLATILE, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0, 0}},
	{"UPPER", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_VARCHAR}, []int8{0}},

	{"VAR_POP", plan.Function_AGG, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},
	{"VAR_SAMPLE", plan.Function_AGG, []plan.Type_TypeId{plan.Type_FLOAT64}, []int8{0}},

	//add for subquery
	{"EXISTS", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_ARRAY}, []int8{1}},
	{"ALL", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_ARRAY}, []int8{1}},
	{"ANY", plan.Function_STRICT, []plan.Type_TypeId{plan.Type_BOOL, plan.Type_ARRAY}, []int8{1}},
}
