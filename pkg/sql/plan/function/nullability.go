// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// nonNullResultProof 只证明成功求值不会产生 SQL NULL，不保证无错误、确定性或可重排求值。
// 未注册证明的函数/overload 保持 nullable；STRICT 不能代替此契约。
type nonNullResultProof func([]*plan.Expr) bool

func nonNullIfAllArgs(args []*plan.Expr) bool {
	for _, arg := range args {
		if arg == nil || !arg.Typ.NotNullable {
			return false
		}
	}
	return true
}

func nonNullIfAnyArg(args []*plan.Expr) bool {
	for _, arg := range args {
		if arg != nil && arg.Typ.NotNullable {
			return true
		}
	}
	return false
}

func nonNullAlways(_ []*plan.Expr) bool { return true }

func nonNullCase(args []*plan.Expr) bool {
	if len(args) < 3 || len(args)%2 != 1 || !args[len(args)-1].Typ.NotNullable {
		return false
	}
	for i := 1; i < len(args)-1; i += 2 {
		if !args[i].Typ.NotNullable {
			return false
		}
	}
	return true
}

func nonNullIf(args []*plan.Expr) bool {
	return len(args) == 3 && args[1].Typ.NotNullable && args[2].Typ.NotNullable
}

func nonNullLeadLag(args []*plan.Expr) bool {
	// window.processValueFunc 在越界、负 offset、NULL offset 时使用 default。
	return len(args) == 3 && args[0].Typ.NotNullable && args[2].Typ.NotNullable
}

func nonNullCast(args []*plan.Expr) bool {
	if len(args) != 2 || !args[0].Typ.NotNullable {
		return false
	}
	from, to := types.T(args[0].Typ.Id), types.T(args[1].Typ.Id)
	if CastPreservesNonNull(from, to) {
		return true
	}
	lit := args[0].GetLit()
	if lit == nil || lit.Isnull || !IfTypeCastSupported(from, to) ||
		(from != types.T_char && from != types.T_varchar && from != types.T_text) {
		return false
	}
	value, ok := lit.Value.(*plan.Literal_Sval)
	if !ok {
		return false
	}
	// 只检查已经绑定的字符串值，不求值表达式、不抛出/吞掉执行错误。
	// strToTime/Datetime/Timestamp 唯一的非空输入 NULL 分支是空字符串。
	if to == types.T_time || to == types.T_datetime || to == types.T_timestamp {
		return len(value.Sval) > 0
	}
	if to == types.T_date {
		_, err := types.ParseDateCast(value.Sval)
		return value.Sval != "" && err == nil
	}
	if to == types.T_year {
		_, err := types.ParseMoYear(value.Sval)
		return err == nil
	}
	return false
}

// CastPreservesNonNull 是已审计的“非空输入 -> 值或错误”转换契约。
// 它不证明精度保持或求值安全；例如 BIT -> TIMESTAMP 可以产生 NULL。
func CastPreservesNonNull(from, to types.T) bool {
	if !IfTypeCastSupported(from, to) {
		return false
	}
	textResult := to == types.T_char || to == types.T_varchar || to == types.T_text ||
		to == types.T_binary || to == types.T_varbinary || to == types.T_blob || to == types.T_datalink
	numericInput := from.IsInteger() || from.IsFloat() || from.IsDecimal()
	numericResult := to.IsInteger() || to.IsFloat() || to.IsDecimal()
	if from == types.T_bool {
		return to.IsInteger() || to == types.T_bool || to == types.T_bit || to == types.T_year || textResult
	}
	if from == types.T_bit {
		return numericResult || to == types.T_bool || to == types.T_bit || to == types.T_time ||
			to == types.T_enum || textResult
	}
	if numericInput {
		return numericResult || textResult || to == types.T_bit || to == types.T_enum ||
			to == types.T_time || (to == types.T_bool && !from.IsDecimal()) ||
			(from.IsDecimal() && (to == types.T_timestamp || to == types.T_year))
	}
	if from == types.T_char || from == types.T_varchar || from == types.T_text ||
		from == types.T_binary || from == types.T_varbinary || from == types.T_blob || from == types.T_datalink {
		// strToFloat/strToSigned 等返回值或错误；空字符串到时间类型、
		// 无效字符串到 DATE/YEAR 则会产生 NULL，不能套用字符串通用规则。
		return numericResult || textResult || to == types.T_bool || to == types.T_bit ||
			to == types.T_json || to == types.T_uuid || to.IsArrayRelate() ||
			to == types.T_datalink || to == types.T_geometry || to == types.T_geometry32 || to == types.T_TS
	}
	if from == types.T_date || from == types.T_datetime || from == types.T_timestamp || from == types.T_time {
		return (numericResult && to != types.T_decimal256) || textResult || to == types.T_date ||
			to == types.T_datetime || to == types.T_timestamp || to == types.T_time || to == types.T_year || to == types.T_bit
	}
	if from == types.T_year {
		return to.IsInteger() || to.IsFloat() || textResult || to == types.T_year
	}
	if from == types.T_json {
		// JSON literal null 是非空 JSON 值，但 jsonToNumeric 会生成 SQL NULL。
		return textResult || to == types.T_json
	}
	if from == types.T_uuid || from == types.T_enum || from == types.T_geometry || from == types.T_geometry32 {
		return textResult || to == from || (from == types.T_enum && to.IsUnsignedInt()) ||
			to == types.T_geometry || to == types.T_geometry32
	}
	if from.IsArrayRelate() {
		return to.IsArrayRelate()
	}
	return from == types.T_Rowid && to == types.T_Rowid ||
		from == types.T_TS && (to == types.T_TS || to == types.T_varchar || to == types.T_timestamp || to == types.T_int64)
}

// 在现有函数注册表内保存证明，绑定、重映射和所有下游消费者共用。
// 每组以执行实现/模板为依据，不能把未知函数自动加入任一安全组。
func registerNullabilityProofs() {
	for _, fid := range []int{
		// 比较、逻辑、算术和位运算：除输入 NULL 外，返回值或错误。
		EQUAL, GREAT_THAN, GREAT_EQUAL, LESS_THAN, LESS_EQUAL, NOT_EQUAL, BETWEEN,
		NOT, AND, OR, XOR, LIKE, ILIKE, IN, NOT_IN,
		PLUS, MINUS, MULTI, UNARY_PLUS, UNARY_MINUS, UNARY_TILDE,
		OP_BIT_AND, OP_BIT_OR, OP_BIT_XOR, OP_BIT_SHIFT_LEFT, OP_BIT_SHIFT_RIGHT,
		IS, ISNOT,
		// 字节/字符串模板和显式循环：不使用 NullOnError 或 domain-NULL 分支。
		ASCII, BINARY, ORD, BIT_LENGTH, CHAR, CONCAT, CONCAT_WS, CONVERT, EMPTY, FORMAT,
		ENDSWITH, FIELD, FINDINSET, INSTR, LEAST, GREATEST, MAKE_SET, EXPORT_SET, EXTRACT,
		LEFT, RIGHT, LENGTH, LENGTH_UTF8, LTRIM, RTRIM, REVERSE,
		REPLACE, INSERT, REG_MATCH, NOT_REG_MATCH, REGEXP_INSTR, REGEXP_LIKE, REGEXP_REPLACE,
		SERIAL, SERIAL_FULL, STARTSWITH, PREFIX_EQ, PREFIX_IN, PREFIX_BETWEEN, IN_RANGE, PREFIX_IN_RANGE,
		TO_BASE64, VECF32_FROM_BASE64, VECF64_FROM_BASE64,
		STRCMP, SUBSTRING, SUBSTRING_INDEX, TRIM, LOWER, UPPER, LOCATE, POSITION, QUOTE,
		JSON_QUOTE, JSON_UNQUOTE, JSON_VALID, JSON_TYPE, JSON_PRETTY,
		JSON_SCHEMA_VALID, JSON_SCHEMA_VALID_REPORT, ENCODE, DECODE,
		JQ, WASM, LLM_CHAT, LLM_EMBEDDING,
		MD5, SHA1, CRC32, VALIDATE_PASSWORD_STRENGTH,
		// 数学模板只返回值或传播错误；SQRT/ACOS/ASIN/LOG 家族明确不在此组。
		ABS, SIGN, ATAN, ATAN2, DEGREES, RADIANS, BIN, BIT_COUNT, CEIL, FLOOR,
		COS, EXP, HEX, OCT, PI, POW, ROUND, TRUNCATE, SIN, SINH, TAN,
		// 日期分解/格式化及直接转换；domain-NULL 转换单独审计。
		DATE, DAY, DAYOFYEAR, HOUR, MINUTE, SECOND, DAYNAME, MONTHNAME, DAYOFMONTH,
		WEEK, WEEKOFYEAR, WEEKDAY, DAYOFWEEK, QUARTER, YEAR, DATE_FORMAT, TIME_FORMAT,
		DATEDIFF, TO_DAYS, TO_SECONDS, TS_TO_TIME,
		// 数组模板中的错误继续向上传播，不等同于 SQL NULL。
		SUMMATION, L1_NORM, L2_NORM, VECTOR_DIMS, INNER_PRODUCT, L2_DISTANCE, L2_DISTANCE_SQ,
		SUB_VECTOR, COSINE_SIMILARITY, COSINE_DISTANCE, NORMALIZE_L2, L2_DISTANCE_XC, L2_DISTANCE_SQ_XC,
		SPACE, SOUNDEX, TIME_TO_SEC, TIME, TIMESTAMP,
		// 几何转换/运算使用非 NullOnError 的字节模板；错误继续向上传播。
		ST_ASTEXT, ST_ASWKB, ST_GEOMFROMWKB, ST_GEOMFROMTEXT, ST_GEOMETRYTYPE,
		ST_POINT, ST_POINT32, ST_POINTFROMTEXT, ST_LINEFROMTEXT, ST_POLYFROMTEXT,
		ST_MPOINTFROMTEXT, ST_MLINEFROMTEXT, ST_MPOLYFROMTEXT, ST_GEOMCOLLFROMTEXT,
		ST_POINTFROMWKB, ST_LINEFROMWKB, ST_POLYFROMWKB,
		ST_MPOINTFROMWKB, ST_MLINEFROMWKB, ST_MPOLYFROMWKB, ST_GEOMCOLLFROMWKB,
		ST_X, ST_Y, ST_LONGITUDE, ST_LATITUDE, ST_GEOMETRYN, ST_POINTN, ST_INTERIORRINGN, ST_SRID,
		ST_DISTANCE, ST_DISTANCE_SPHERE, ST_LENGTH, ST_AREA, ST_FRECHETDISTANCE, ST_HAUSDORFFDISTANCE,
		ST_GEOHASH, ST_POINTFROMGEOHASH, ST_GEOMFROMGEOJSON, ST_BUFFER,
		ST_UNION, ST_INTERSECTION, ST_DIFFERENCE, ST_SYMDIFFERENCE,
		MBRCONTAINS, MBRCOVEREDBY, MBRCOVERS, MBRDISJOINT, MBREQUALS,
		MBRINTERSECTS, MBROVERLAPS, MBRTOUCHES, MBRWITHIN,
		ST_SWAPXY, ST_MAKEENVELOPE, ST_LATFROMGEOHASH, ST_LONGFROMGEOHASH,
		ST_ASGEOJSON, ST_CONVEXHULL, ST_SIMPLIFY, ST_COLLECT,
		ST_LINEINTERPOLATEPOINT, ST_LINEINTERPOLATEPOINTS, ST_POINTATDISTANCE,
		ST_ISEMPTY, ST_CONTAINS, ST_WITHIN, ST_INTERSECTS, ST_DISJOINT,
		ST_TOUCHES, ST_CROSSES, ST_OVERLAPS, ST_EQUALS, ST_COVERS, ST_COVEREDBY,
		ST_STARTPOINT, ST_ENDPOINT, ST_EXTERIORRING, ST_NUMINTERIORRINGS, ST_NUMPOINTS,
		ST_ISCLOSED, ST_ISCOLLECTION, ST_DIMENSION, ST_ISSIMPLE, ST_ISRING,
		ST_ENVELOPE, ST_CENTROID, ST_BOUNDARY, ST_ISVALID, ST_POINTONSURFACE,
		// S2/H3 的共享模板只对被跳过的行或 NULL 输入写 NULL，非法值返回错误。
		S2_CELLID, S2_CELLID_LEVEL, S2_CELLID_CENTER, S2_CELLID_AREA, S2_CELLID_PARENT,
		S2_CELLID_EDGENEIGHBORS, S2_CELLID_ALLNEIGHBORS, S2_CELLID_ARENEIGHBORS,
		H3_H3INDEX, H3_H3INDEX_RESOLUTION, H3_H3INDEX_CENTER, H3_H3INDEX_BOUNDARY,
		H3_H3INDEX_PARENT, H3_H3INDEX_NEIGHBORS, H3_H3INDEX_ARENEIGHBORS,
		// 内部格式转换和基数查询，同样使用值/错误模板。
		CAST_GEOMETRY_TO_SUBTYPE, CAST_JSON_TO_ARRAY, CAST_RANGE_VALUE_UNIT,
		CAST_INDEX_TO_VALUE, CAST_VALUE_TO_INDEX, CAST_INDEX_VALUE_TO_INDEX,
		CAST_INDEX_TO_SET_VALUE, CAST_SET_VALUE_TO_INDEX, CAST_SET_INDEX_VALUE_TO_INDEX,
		INET_NTOA, IS_IPV4, IS_IPV6, IS_IPV4_COMPAT, IS_IPV4_MAPPED,
		VALUES, NAME_CONST, IS_UUID, UUID_TO_BIN, BIN_TO_UUID, BIT_CAST,
		BITMAP_BIT_POSITION, BITMAP_BUCKET_NUMBER, BITMAP_COUNT, HLL_CARDINALITY,
		MO_MEMORY_USAGE, MO_MEMORY, MO_CPU, MO_CPU_DUMP, MO_SHOW_COL_UNIQUE, REMOVE_FAULT_POINT,
		PURGE_LOG, MO_ADMIN_NAME, MO_FEATURE_REGISTRY_UPSERT, MO_FEATURE_LIMIT_UPSERT,
		ADD_FAULT_POINT, MO_ENABLE_MEMORY_USAGE_DETAIL, MO_DISABLE_MEMORY_USAGE_DETAIL,
		SLEEP, GET_LOCK, IS_FREE_LOCK, MO_WIN_TRUNCATE, MO_WIN_DIVISOR,
		HASH, HASH_PARTITION, GROUPING, CAST_NANO_TO_TIMESTAMP, MO_TABLE_ROWS, MO_TABLE_SIZE,
		SAVE_FILE, NEXTVAL, SETVAL, CURRVAL, MO_CTL, FAULT_INJECT, ASSERT, ISEMPTY, NOT_IN_ROWS,
		// 未被索引改写的全文函数只返回错误；改写后的列使用实际关系输入刷新。
		FULLTEXT_MATCH, FULLTEXT_MATCH_SCORE,
	} {
		allSupportedFunctions[fid].nonNullResult = nonNullIfAllArgs
	}
	for _, fid := range []int{
		RANK, ROW_NUMBER, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE,
		APPROX_COUNT, APPROX_COUNT_DISTINCT,
		RANDOM, CURRENT_DATE, UTC_DATE, CURRENT_TIMESTAMP, LOCALTIME, SYSDATE, CURRENT_TIME,
		UTC_TIME, UTC_TIMESTAMP,
		BUILD_VERSION, CHARSET, COLLATION, CONNECTION_ID, CURRENT_ACCOUNT_ID, CURRENT_ACCOUNT_NAME,
		CURRENT_ROLE, CURRENT_ROLE_ID, CURRENT_USER_ID, CURRENT_ROLE_NAME, CURRENT_USER_NAME,
		DATABASE, FOUND_ROWS, GIT_VERSION, LAST_INSERT_ID, ROW_COUNT, USER, UUID, VERSION,
		ICULIBVERSION, ROLES_GRAPHML, LASTVAL, RELEASE_ALL_LOCKS,
		DISABLE_FAULT_INJECTION, ENABLE_FAULT_INJECTION,
	} {
		allSupportedFunctions[fid].nonNullResult = nonNullAlways
	}
	// 同名函数的文本解析 overload 可因非法输入产生 NULL，不能共享类型化 overload 的证明。
	for _, pair := range [][2]int{
		{MONTH, 0}, {MONTH, 1},
		{MICROSECOND, 0}, {MICROSECOND, 1}, {MICROSECOND, 2},
		{TIMEDIFF, 0}, {TIMEDIFF, 1},
		{TIMESTAMPDIFF, 0}, {TIMESTAMPDIFF, 1}, {TIMESTAMPDIFF, 2},
		{TIMESTAMPDIFF, 4}, {TIMESTAMPDIFF, 5}, {TIMESTAMPDIFF, 6},
		{TIMESTAMPDIFF, 10}, {TIMESTAMPDIFF, 11},
		{YEARWEEK, 0}, {YEARWEEK, 1}, {YEARWEEK, 2},
		{YEARWEEK, 3}, {YEARWEEK, 4}, {YEARWEEK, 5},
		{DATE_ADD, 4}, {DATE_ADD, 5}, {DATE_SUB, 0}, {DATE_SUB, 6},
		{UNIX_TIMESTAMP, 0},
	} {
		allSupportedFunctions[pair[0]].Overloads[pair[1]].nonNullResult = nonNullIfAllArgs
	}
	// CONV 的数值路径只格式化整数；文本路径解析失败时返回 NULL。
	allSupportedFunctions[CONV].nonNullResult = func(args []*plan.Expr) bool {
		return len(args) == 3 && nonNullIfAllArgs(args) &&
			(types.T(args[0].Typ.Id).IsInteger() || types.T(args[0].Typ.Id).IsFloat())
	}
	allSupportedFunctions[COALESCE].nonNullResult = nonNullIfAnyArg
	allSupportedFunctions[CASE].nonNullResult = nonNullCase
	allSupportedFunctions[IFF].nonNullResult = nonNullIf
	allSupportedFunctions[LAG].nonNullResult = nonNullLeadLag
	allSupportedFunctions[LEAD].nonNullResult = nonNullLeadLag
	allSupportedFunctions[CAST].Overloads[0].nonNullResult = nonNullCast
	allSupportedFunctions[CAST_STRICT].Overloads[0].nonNullResult = nonNullCast
}

// DeduceWindowNotNullable 补充需要 frame 信息的证明。FIRST/LAST_VALUE 在空 frame
// 上输出 NULL；只有能保证包含当前行时，才允许继承源表达式的非空性。
func DeduceWindowNotNullable(w *plan.WindowSpec) bool {
	if w == nil || w.WindowFunc == nil || w.WindowFunc.GetF() == nil {
		return false
	}
	fn := w.WindowFunc.GetF()
	fid, _ := DecodeOverloadID(fn.Func.Obj)
	if fid == FIRST_VALUE || fid == LAST_VALUE {
		frame := w.Frame
		return len(fn.Args) == 1 && fn.Args[0].Typ.NotNullable && frame != nil &&
			frame.Start != nil && frame.End != nil &&
			(frame.Start.Type == plan.FrameBound_PRECEDING && frame.Start.UnBounded || frame.Start.Type == plan.FrameBound_CURRENT_ROW) &&
			(frame.End.Type == plan.FrameBound_FOLLOWING && frame.End.UnBounded || frame.End.Type == plan.FrameBound_CURRENT_ROW)
		// 有界 RANGE 可带负偏移，ROWS 的 uint64 偏移也可能在执行时溢出 int。
		// 在未证明偏移值及执行算术安全前，不仅凭 PRECEDING/FOLLOWING 方向推断非空。
	}
	return DeduceNotNullable(fn.Func.Obj, fn.Args)
}
