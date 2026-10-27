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

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var allSupportedFunctions [1000]FuncNew

// register all supported functions.
func initAllSupportedFunctions() {
	for _, fn := range supportedOperators {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedStringBuiltIns {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedDateAndTimeBuiltIns {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedMathBuiltIns {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedArrayOperations {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedControlBuiltIns {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedOthersBuiltIns {
		allSupportedFunctions[fn.functionId] = fn
	}

	for _, fn := range supportedWindowInNewFramework {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedAggInNewFramework {
		allSupportedFunctions[fn.functionId] = fn
	}
}

func GetFunctionIsAggregateByName(name string) bool {
	fid, exists := getFunctionIdByNameWithoutErr(name)
	if !exists {
		return false
	}
	f := allSupportedFunctions[fid]
	return f.isAggregate()
}

func GetFunctionIsWinFunByName(name string) bool {
	fid, exists := getFunctionIdByNameWithoutErr(name)
	if !exists {
		return false
	}
	f := allSupportedFunctions[fid]
	return f.isWindow()
}

func GetFunctionIsWinOrderFunByName(name string) bool {
	fid, exists := getFunctionIdByNameWithoutErr(name)
	if !exists {
		return false
	}
	f := allSupportedFunctions[fid]
	return f.isWindowOrder()
}

func GetFunctionIsWinValueFunByName(name string) bool {
	fid, exists := getFunctionIdByNameWithoutErr(name)
	if !exists {
		return false
	}
	f := allSupportedFunctions[fid]
	return f.isWindowValue()
}

// GetFunctionIsWinIndicatorFunByName reports whether name is one of the
// technical-indicator window kernels (issue #29949): WIN_VALUE-class
// registrations that the window operator evaluates through the indicator
// state machine instead of the value/aggregate paths. They are never
// aggregates and never carry aggexec kernels.
func GetFunctionIsWinIndicatorFunByName(name string) bool {
	fid, exists := getFunctionIdByNameWithoutErr(name)
	return exists && GetFunctionIsWinIndicatorFunById(fid)
}

// GetFunctionIsWinIndicatorFunById is the ID-set membership of the indicator
// kernel family, shared by the executor dispatch, the frame-ignoring window
// binding and the binder's section-4 surface validation.
func GetFunctionIsWinIndicatorFunById(fid int32) bool {
	switch fid {
	case SMA, WMA, TRIMA, EMA, WILDER, KAMA, DEMA, TEMA, T3, MMAXDRAWDOWN, CUMPOSITIVESTREAK:
		return true
	}
	return false
}

// GetFunctionIgnoresWindowFrameByName reports whether a window function
// operates on partition-relative row positions instead of the current frame.
func GetFunctionIgnoresWindowFrameByName(name string) bool {
	fid, exists := getFunctionIdByNameWithoutErr(name)
	if !exists {
		return false
	}
	if fid == LAG || fid == LEAD {
		return true
	}
	// The indicator kernels define extent through the period parameter;
	// a frame-less OVER clause binds with the default synthetic frame and
	// explicit frames are rejected by the window binder.
	return GetFunctionIsWinIndicatorFunById(fid)
}

func GetFunctionIsVolatileOrRealTimeRelatedByName(name string) bool {
	fid, exists := getFunctionIdByNameWithoutErr(name)
	if !exists {
		return false
	}
	for _, ov := range allSupportedFunctions[fid].Overloads {
		if ov.IsVolatile() || ov.IsRealTimeRelated() {
			return true
		}
	}
	return false
}

// GetFunctionValueUnstableByID reports the resolved function's own instability
// within one execution. Callers must inspect its arguments separately.
// Execution stability does not relax folding or diagnostic-count restrictions.
func GetFunctionValueUnstableByID(overloadID int64) (unstable, known bool) {
	ov, known := GetFunctionByIdWithoutError(overloadID)
	if !known {
		return false, false
	}
	return ov.IsVolatile() && !ov.executionStable, true
}

// provenPureValueFunctions lists every SQL builtin registration that is
// positively proven to evaluate as a pure value transformation: an
// in-process computation over its own argument vectors with no effects
// outside the expression tree - no external service, no process-global or
// session state, no catalog access, no randomness. The guarded fold of a
// metadata-only publication-binding probe admits a nested function call
// only through this list, never through the absence of other flags:
// llm_chat is registered STRICT without the internal-machinery class bit
// and without a volatile or real-time-related overload, and a review
// counterexample showed those negative predicates alone admitting it and
// its executor issuing the HTTP request during publication and
// subscription validation. Add a name only after auditing every overload
// the registration carries; proven_pure_foldable_test.go pins each listed
// name to a non-internal class and the registry-scan admission rule, and every
// registration outside this set keeps its rejection whatever its flags
// declare. The round-14 follow-up review completed the classification for
// ordinary SQL constant expressions: every writable builtin whose clean
// overloads are a deterministic in-process value transformation of their own
// argument vectors is listed, so a valid stored definition folds in the
// guarded probe exactly as ordinary binding folds it. The audit keeps OUT,
// deliberately and with its names pinned by the same test: the
// environment, session and build readers (charset, collation, version,
// current_* identity and validate_password_strength), the clock- and
// zone-dependent transforms whose result varies with the session time
// zone rather than the operands (from_unixtime, convert_tz), the
// randomness sources (random_bytes), the program interpreters and service
// callers (llm_chat, llm_embedding, starlark, try_starlark, jq, try_jq),
// the fulltext match machinery that reads query and index context,
// aggregates and window functions, and plan-internal helpers a persisted
// definition never names (serial*, physical_*, to_interval*,
// __mo_json_* and the range-prefix predicates).
var provenPureValueFunctions = map[string]struct{}{
	// Pure boolean comparison, logic, truth-value and membership predicates
	// of operand values.
	"=": {}, "<=>": {}, ">": {}, ">=": {}, "<": {}, "<=": {}, "<>": {}, "!=": {},
	"and": {}, "or": {}, "xor": {}, "not": {},
	"is": {}, "isnot": {}, "isnotnull": {}, "istrue": {}, "isnottrue": {},
	"isfalse": {}, "isnotfalse": {},
	"between": {}, "in": {}, "not_in": {}, "like": {}, "ilike": {},
	// Pure bitwise value operators.
	"&": {}, "|": {}, "^": {}, "<<": {}, ">>": {},
	// Pure numeric and arithmetic value transforms.
	"abs": {}, "acos": {}, "asin": {}, "atan": {}, "atan2": {}, "bit_count": {},
	"ceil": {}, "ceiling": {}, "cos": {}, "cot": {}, "degrees": {}, "exp": {},
	"floor": {}, "ln": {}, "log": {}, "log2": {}, "log10": {}, "mod": {},
	"pi": {}, "pow": {}, "power": {}, "radians": {}, "round": {}, "sign": {},
	"sin": {}, "sqrt": {}, "tan": {}, "truncate": {},
	"sinh": {}, "cosh": {}, "tanh": {}, "asinh": {}, "acosh": {}, "atanh": {},
	"format_bytes": {}, "format_pico_time": {},
	// Pure textual value transforms of constant operands.
	"ascii": {}, "bin": {}, "bit_length": {}, "char_length": {}, "character_length": {},
	"concat": {}, "concat_ws": {}, "conv": {}, "crc32": {}, "elt": {}, "export_set": {},
	"field": {}, "find_in_set": {}, "hex": {}, "left": {}, "length": {}, "locate": {},
	"lower": {}, "lpad": {}, "ltrim": {}, "make_set": {}, "md5": {}, "mid": {},
	"oct": {}, "octet_length": {}, "ord": {}, "position": {}, "quote": {},
	"repeat": {}, "replace": {}, "reverse": {}, "right": {}, "rpad": {}, "rtrim": {},
	"sha1": {}, "sha2": {}, "soundex": {}, "space": {}, "strcmp": {}, "substr": {},
	"substring": {}, "substring_index": {}, "trim": {}, "unhex": {}, "upper": {},
	"binary": {}, "char": {}, "empty": {}, "endswith": {}, "startswith": {},
	"insert": {}, "instr": {}, "reg_match": {}, "not_reg_match": {},
	"regexp_instr": {}, "regexp_like": {}, "regexp_replace": {}, "regexp_substr": {},
	"split_part": {}, "compress": {}, "encode": {}, "decode": {},
	"to_base64": {}, "from_base64": {},
	// Pure date and time value transforms of constant operands: coercions of
	// date parts, calendar parts, pure arithmetic and formatting keyed only
	// by their own arguments.
	"addtime": {}, "subtime": {},
	"date": {}, "time": {}, "timestamp": {},
	"year": {}, "month": {}, "day": {}, "hour": {}, "minute": {}, "second": {},
	"microsecond": {}, "quarter": {}, "weekofyear": {}, "weekday": {}, "yearweek": {},
	"dayname": {}, "monthname": {}, "last_day": {}, "extract": {},
	"date_format": {}, "time_format": {}, "date_trunc": {}, "get_format": {},
	"dayofmonth": {}, "dayofweek": {}, "dayofyear": {},
	"to_days": {}, "to_seconds": {}, "from_days": {},
	"sec_to_time": {}, "time_to_sec": {}, "makedate": {}, "maketime": {},
	"period_add": {}, "period_diff": {}, "timediff": {}, "timestampdiff": {},
	"timestampadd": {}, "date_add": {}, "date_sub": {}, "interval": {},
	// Pure JSON value-tree transforms of constant operands: readers,
	// extractors, validators and tree editors that never leave the process
	// and key only on their own argument values.
	"json_extract": {}, "json_extract_string": {}, "json_extract_float64": {},
	"json_quote": {}, "json_unquote": {}, "json_row": {}, "json_value": {},
	"json_length": {}, "json_depth": {}, "json_type": {}, "json_valid": {},
	"json_keys": {}, "json_pretty": {}, "json_contains": {}, "json_contains_path": {},
	"json_overlaps": {}, "json_storage_size": {}, "json_storage_free": {},
	"json_schema_valid": {}, "json_schema_validation_report": {},
	"json_array": {}, "json_object": {}, "json_set": {}, "json_insert": {},
	"json_replace": {}, "json_remove": {}, "json_merge": {}, "json_merge_preserve": {},
	"json_merge_patch": {}, "json_array_append": {}, "json_array_insert": {},
	"member of": {},
	// Pure UUID and network-address value transforms.
	"is_uuid": {}, "uuid_to_bin": {}, "bin_to_uuid": {},
	"uuid_extract_version": {}, "uuid_extract_timestamp": {},
	"inet_aton": {}, "inet_ntoa": {}, "inet6_aton": {}, "inet6_ntoa": {},
	"is_ipv4": {}, "is_ipv6": {}, "is_ipv4_compat": {}, "is_ipv4_mapped": {},
	// Pure geometry value functions of their own arguments: constructors,
	// readers, measurements and predicates over constant geometry values.
	"st_astext": {}, "st_geomfromtext": {}, "st_asbinary": {},
	"st_geometrytype": {}, "st_x": {}, "st_y": {},
	"st_numgeometries": {}, "st_geometryn": {}, "st_isempty": {},
	"st_distance": {}, "st_srid": {}, "st_length": {}, "st_area": {},
	"st_contains": {}, "st_within": {}, "st_startpoint": {}, "st_endpoint": {},
	"st_pointn": {}, "st_exteriorring": {}, "st_numinteriorring": {},
	"st_interiorringn": {}, "st_numpoints": {}, "st_isclosed": {},
	"st_iscollection": {}, "st_dimension": {}, "st_issimple": {}, "st_isring": {},
	"st_envelope": {}, "st_centroid": {}, "st_boundary": {}, "st_isvalid": {},
	"st_pointonsurface": {}, "st_intersects": {}, "st_disjoint": {},
	"st_touches": {}, "st_crosses": {}, "st_overlaps": {}, "st_equals": {},
	"st_covers": {}, "st_coveredby": {}, "st_geomfromwkb": {},
	"st_pointfromtext": {}, "st_linefromtext": {}, "st_polyfromtext": {},
	"st_mpointfromtext": {}, "st_mlinefromtext": {}, "st_mpolyfromtext": {},
	"st_geomcollfromtext": {}, "st_pointfromwkb": {}, "st_linefromwkb": {},
	"st_polyfromwkb": {}, "st_mpointfromwkb": {}, "st_mlinefromwkb": {},
	"st_mpolyfromwkb": {}, "st_geomcollfromwkb": {},
	"st_longitude": {}, "st_latitude": {}, "st_swapxy": {}, "st_validate": {},
	"st_makeenvelope": {}, "st_distance_sphere": {}, "st_geohash": {},
	"st_latfromgeohash": {}, "st_longfromgeohash": {}, "st_pointfromgeohash": {},
	"mbrcontains": {}, "mbrcoveredby": {}, "mbrcovers": {}, "mbrdisjoint": {},
	"mbrequals": {}, "mbrintersects": {}, "mbroverlaps": {}, "mbrtouches": {},
	"mbrwithin": {}, "st_asgeojson": {}, "st_geomfromgeojson": {},
	"st_convexhull": {}, "st_simplify": {}, "st_collect": {},
	"st_lineinterpolatepoint": {}, "st_lineinterpolatepoints": {},
	"st_pointatdistance": {}, "st_frechetdistance": {}, "st_hausdorffdistance": {},
	"st_union": {}, "st_intersection": {}, "st_difference": {},
	"st_symdifference": {}, "st_buffer": {}, "point": {}, "st_point32": {},
	// Pure bitmap, hyperloglog, vector and geo-cell scalar value transforms.
	"hll_cardinality": {}, "bitmap_bit_position": {}, "bitmap_bucket_number": {},
	"bitmap_count": {},
	"summation":    {}, "l1_norm": {}, "l2_norm": {}, "inner_product": {},
	"cosine_similarity": {}, "vector_dims": {}, "normalize_l2": {},
	"l2_distance": {}, "l2_distance_sq": {}, "cosine_distance": {}, "l1_distance": {},
	"l2_distance_xc": {}, "l2_distance_sq_xc": {},
	"vecf32_from_base64": {}, "vecf64_from_base64": {}, "vecbf16_from_base64": {},
	"vecf16_from_base64": {}, "vecint8_from_base64": {}, "vecuint8_from_base64": {},
	"s2_cellid": {}, "s2_cellid_level": {}, "s2_cellid_center": {},
	"s2_cellid_area": {}, "s2_cellid_parent": {},
	"s2_cellid_edgeneighbors": {}, "s2_cellid_edgeneighbours": {},
	"s2_cellid_allneighbors": {}, "s2_cellid_allneighbours": {},
	"s2_cellid_areneighbors": {}, "s2_cellid_areneighbours": {},
	"h3_h3index": {}, "h3_h3index_resolution": {}, "h3_h3index_center": {},
	"h3_h3index_boundary": {}, "h3_h3index_parent": {},
	"h3_h3index_neighbors": {}, "h3_h3index_neighbours": {},
	"h3_h3index_areneighbors": {}, "h3_h3index_areneighbours": {},
	// Pure constant branch selection and reduction.
	"case": {}, "coalesce": {}, "greatest": {}, "iff": {}, "ifnull": {}, "least": {},
	"name_const": {},
}

// provenPureValueFunctionIds is the resolved id view of
// provenPureValueFunctions. Every listed name must resolve at package
// init: a silent drop would silently weaken the guarded fold's gate.
var provenPureValueFunctionIds map[int32]struct{}

func init() {
	provenPureValueFunctionIds = make(map[int32]struct{}, len(provenPureValueFunctions))
	for name := range provenPureValueFunctions {
		fid, ok := functionIdRegister[name]
		if !ok {
			panic(fmt.Sprintf("proven pure builtin %q is not a registered function", name))
		}
		provenPureValueFunctionIds[fid] = struct{}{}
	}
}

// GetFunctionIsProvenPureFoldableById reports, for the resolved overload
// overloadID, whether the guarded fold of a metadata-only binding probe
// may evaluate it for a constant tree: the overload's function belongs to
// the curated provenPureValueFunctions set, the internal-machinery class
// bit never qualifies (plain or combined with other class flags -
// python_user_defined_function is INTERNAL|STRICT and carries no volatile
// flag, so the rejection must test the bit through testFlag, never compare
// the whole word, or the caller's guarded fold would execute it right into
// UdfService.Run), and the resolved overload carries neither the volatile
// (CannotFold) nor the real-time-related flag. Membership is the purity
// proof: absence of the excluded flags establishes nothing - llm_chat
// carries none of them and its executor still performs an HTTP request -
// so a registration outside the curated set is rejected and the probe
// keeps that node un-evaluated.
func GetFunctionIsProvenPureFoldableById(overloadID int64) bool {
	fid, oIndex := DecodeOverloadID(overloadID)
	if !validFunctionOverloadID(fid, oIndex) {
		return false
	}
	if _, proven := provenPureValueFunctionIds[fid]; !proven {
		return false
	}
	if allSupportedFunctions[fid].testFlag(plan.Function_INTERNAL) {
		return false
	}
	ov := allSupportedFunctions[fid].Overloads[oIndex]
	return !ov.CannotFold() && !ov.IsRealTimeRelated()
}

func GetFunctionIsWinOrderFunById(overloadID int64) bool {
	fid, oIndex := DecodeOverloadID(overloadID)
	if !validFunctionOverloadID(fid, oIndex) {
		return false
	}
	return allSupportedFunctions[fid].isWindowOrder()
}

func validFunctionOverloadID(fid, oIndex int32) bool {
	return fid >= 0 && int(fid) < len(allSupportedFunctions) &&
		int(fid) == allSupportedFunctions[fid].functionId &&
		oIndex >= 0 && int(oIndex) < len(allSupportedFunctions[fid].Overloads)
}

// IsConstant applies the existing folding policy, including prepare-time
// parameter and statement-time handling, at the function semantic owner.
func IsConstant(expr *plan.Expr, varAndParamIsConst bool) bool {
	return isConstant(expr, varAndParamIsConst, varAndParamIsConst, constantForFolding)
}

// IsFoldCandidate admits deterministic expressions for speculative evaluation.
// Unlike IsConstant, this is not permission to relocate or eliminate effects:
// the folder must isolate diagnostics and publish only a warning-free result.
func IsFoldCandidate(expr *plan.Expr, varAndParamIsConst bool) bool {
	return isConstant(expr, varAndParamIsConst, varAndParamIsConst, constantForFoldCandidate)
}

// IsStatementConstant conservatively admits speculative evaluation and endpoint
// CONST provenance. Parameters and variables must already be materialized.
func IsStatementConstant(expr *plan.Expr) bool {
	return isConstant(expr, false, true, constantForFolding)
}

// IsRuntimeConstant admits one row-independent value for a scan. Parameters
// retain their runtime binding promise; lazy selectors stay unevaluated.
func IsRuntimeConstant(expr *plan.Expr) bool {
	return isConstant(expr, true, true, constantForRuntime)
}

type constantPurpose uint8

const (
	constantForFolding constantPurpose = iota
	constantForRuntime
	constantForFoldCandidate
)

func isConstant(expr *plan.Expr, allowParameters, currentExecution bool, purpose constantPurpose) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Lit, *plan.Expr_T, *plan.Expr_Vec:
		return true
	case *plan.Expr_Fold:
		return e.Fold != nil && e.Fold.IsConst
	case *plan.Expr_P, *plan.Expr_V:
		return allowParameters
	case *plan.Expr_List:
		if e.List == nil {
			return false
		}
		for _, arg := range e.List.List {
			if !isConstant(arg, allowParameters, currentExecution, purpose) {
				return false
			}
		}
		return true
	case *plan.Expr_F:
		if e.F == nil || e.F.Func == nil {
			return false
		}
		fid, _ := DecodeOverloadID(e.F.Func.GetObj())
		if fid == CASE && purpose != constantForRuntime {
			return false
		}
		f, ok := GetFunctionByIdWithoutError(e.F.Func.GetObj())
		if !ok || f.IsVolatile() ||
			(f.HasRowScopedDiagnostics() && purpose != constantForFoldCandidate) ||
			(f.IsRealTimeRelated() && !currentExecution) {
			return false
		}
		if purpose == constantForRuntime && (allSupportedFunctions[fid].isAggregate() || allSupportedFunctions[fid].isWindow()) {
			return false
		}
		for _, arg := range e.F.Args {
			if !isConstant(arg, allowParameters, currentExecution, purpose) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// CanUseZoneMapComparison checks whether physical byte ordering represents the
// bound SQL comparison domain. CHAR comparisons ignore trailing spaces, which
// the persisted raw-byte min/max and bloom metadata do not represent.
func CanUseZoneMapComparison(overloadID int64, args []*plan.Expr) bool {
	fid, _ := DecodeOverloadID(overloadID)
	// Persisted zone maps and bloom filters still store the original bytes.
	// Native Unicode collations compare UCA keys instead, so admitting these
	// predicates would allow a bytewise metadata probe to prune a matching row.
	// Keep the admission owner conservative until the persisted summary domain
	// carries the same collation identity and probe encoding.
	for _, arg := range args {
		if arg != nil && types.IsUnicodeCollation(uint8(arg.Typ.Charset)) {
			return false
		}
	}
	switch fid {
	case IN, NOT_IN:
		if len(args) != 2 || args[1] == nil {
			return false
		}
		// List execution uses its first item type; a folded tuple can retain
		// the outer T_tuple declaration, so encoded vectors own their type.
		switch tuple := args[1].Expr.(type) {
		case *plan.Expr_List:
			if tuple.List == nil || len(tuple.List.List) == 0 {
				return false
			}
			for _, item := range tuple.List.List {
				if item == nil || types.T(item.Typ.Id) == types.T_char || types.IsUnicodeCollation(uint8(item.Typ.Charset)) {
					return false
				}
			}
			return true
		case *plan.Expr_Vec:
			// The actual consumer validates and decodes the carrier once. Its
			// outer tuple declaration cannot prove physical membership order.
			return tuple.Vec != nil
		case *plan.Expr_Fold:
			return tuple.Fold != nil && !tuple.Fold.IsConst
		default:
			return false
		}
	case EQUAL, NULL_SAFE_EQUAL, NOT_EQUAL, GREAT_THAN, GREAT_EQUAL, LESS_THAN, LESS_EQUAL, BETWEEN:
		if len(args) == 2 && args[0] != nil && args[1] != nil && IsJSONNumericComparison(types.T(args[0].Typ.Id), types.T(args[1].Typ.Id)) {
			return false
		}
		if len(args) == 0 {
			return false
		}
		for _, arg := range args {
			if arg == nil {
				return false
			}
		}
		// The registered comparison kernels dispatch on operand zero. A raw
		// Sval literal uses the executor's canonical VARCHAR container even
		// when the plan declares CHAR (notably the empty string).
		if types.T(args[0].Typ.Id) == types.T_char {
			lit := args[0].GetLit()
			if lit == nil || lit.Isnull {
				return false
			}
			_, varcharLiteral := lit.Value.(*plan.Literal_Sval)
			return varcharLiteral
		}
	}
	return true
}

func GetFunctionIsZonemappableById(ctx context.Context, overloadID int64) (bool, error) {
	fid, oIndex := DecodeOverloadID(overloadID)
	if !validFunctionOverloadID(fid, oIndex) {
		return false, moerr.NewInvalidInput(ctx, "function overload id not found")
	}
	f := allSupportedFunctions[fid]
	if f.Overloads[oIndex].CannotFold() {
		return false, nil
	}
	return f.testFlag(plan.Function_ZONEMAPPABLE), nil
}

func GetFunctionById(ctx context.Context, overloadID int64) (f overload, err error) {
	fid, oIndex := DecodeOverloadID(overloadID)
	if !validFunctionOverloadID(fid, oIndex) {
		return overload{}, moerr.NewInvalidInput(ctx, "function overload id not found")
	}
	return allSupportedFunctions[fid].Overloads[oIndex], nil
}

func GetLayoutById(ctx context.Context, overloadID int64) (FuncExplainLayout, error) {
	fid, oIndex := DecodeOverloadID(overloadID)
	if !validFunctionOverloadID(fid, oIndex) {
		return 0, moerr.NewInvalidInput(ctx, "function overload id not found")
	}
	return allSupportedFunctions[fid].layout, nil
}

func GetFunctionByIdWithoutError(overloadID int64) (f overload, exists bool) {
	fid, oIndex := DecodeOverloadID(overloadID)
	if !validFunctionOverloadID(fid, oIndex) {
		return overload{}, false
	}
	return allSupportedFunctions[fid].Overloads[oIndex], true
}

func GetFunctionByName(ctx context.Context, name string, args []types.Type) (r FuncGetResult, err error) {
	return getFunctionByName(ctx, name, args, nil)
}

// StringDomainCheckMode separates an operand's current string domain from the
// provenance rules that decide whether it participates in regexp charset
// compatibility. A single boolean cannot represent both PREPARE-time
// uncertainty and MySQL's execute-time parameter-marker exception.
type StringDomainCheckMode uint8

const (
	// StringDomainCheckKnown applies the operand's current text/binary domain.
	StringDomainCheckKnown StringDomainCheckMode = iota
	// StringDomainCheckDeferred omits a PREPARE-time runtime-owned domain. The
	// concrete execution must bind the operand again with a non-deferred mode.
	StringDomainCheckDeferred
	// StringDomainCheckParamMarker keeps the marker's current domain for
	// compatibility with a fixed binary operand, but a binary marker does not
	// itself trigger MySQL's static-binary restriction.
	StringDomainCheckParamMarker
	// StringDomainCheckDomainless omits a bare, untyped NULL literal.
	StringDomainCheckDomainless
	// StringDomainCheckBinaryCast makes CAST AS BINARY a static binary VARCHAR
	// trigger, unlike a physical BINARY column. Its execution type is unchanged.
	StringDomainCheckBinaryCast
	// StringDomainCheckBinaryBlob keeps an expression's logical BLOB domain
	// compatible without turning its execution type into a VARCHAR trigger.
	StringDomainCheckBinaryBlob
)

// GetFunctionByNameWithStringDomainCheckModes resolves a function while
// preserving the provenance needed by regexp charset checks. Ordinary argument
// types remain authoritative for overload selection, casts, result metadata,
// and the executor's effective text/binary domain.
func GetFunctionByNameWithStringDomainCheckModes(
	ctx context.Context, name string, args []types.Type, modes []StringDomainCheckMode,
) (r FuncGetResult, err error) {
	if len(modes) != len(args) {
		return r, moerr.NewInternalErrorf(
			ctx, "string domain check mode count %d does not match argument count %d",
			len(modes), len(args))
	}
	return getFunctionByName(ctx, name, args, modes)
}

func getFunctionByName(
	ctx context.Context, name string, args []types.Type, stringDomainModes []StringDomainCheckMode,
) (r FuncGetResult, err error) {
	r.fid, err = getFunctionIdByName(ctx, name)
	if err != nil {
		return r, err
	}
	f := allSupportedFunctions[r.fid]
	if len(f.Overloads) == 0 || f.checkFn == nil {
		return r, moerr.NewNYIf(ctx, "should implement the function %s", name)
	}

	check := f.checkArgumentTypes(args, stringDomainModes)
	if legacyIntegerDecimalCoercions(ctx) {
		check = legacyIntegerDecimalCheck(r.fid, args, check)
	}
	if r.fid == UNIX_TIMESTAMP && LegacyUnixTimestampDatetime(ctx) {
		check = fixedTypeMatch(f.Overloads[:4], args)
	}
	if legacy, ok := legacyIncompleteTemporalCheck(ctx, r.fid, f.Overloads, args); ok {
		check = legacy
	}
	if r.fid == INTEGER_DIV && LegacyDecimalIntegerDiv(ctx) {
		check = legacyIntegerDivCheck(args)
	}
	if (r.fid == FORMAT || r.fid == MAKEDATE || r.fid == MAKETIME) && LegacySpecialConsumers(ctx) {
		if legacy, ok := legacySpecialConsumerCheck(r.fid, f.Overloads, args); ok {
			check = legacy
		}
	}
	if r.fid == MINUS && signedUnsignedSubtraction(ctx, args) {
		check = newCheckResultWithCast(3, integerDomainOperands(args))
	}
	switch check.status {
	case succeedMatched:
		r.overloadId = int32(check.idx)
		r.retType = f.Overloads[r.overloadId].retType(args)
		r.cannotRunInParallel = f.Overloads[r.overloadId].cannotParallel

	case succeedWithCast:
		r.overloadId = int32(check.idx)
		r.needCast = true
		r.targetTypes = check.finalType
		r.retType = f.Overloads[r.overloadId].retType(r.targetTypes)
		r.cannotRunInParallel = f.Overloads[r.overloadId].cannotParallel

	case failedFunctionParametersWrong:
		if check.characterSetMismatch[0] != "" {
			err = moerr.NewCharacterSetMismatch(
				ctx, check.characterSetMismatch[0], check.characterSetMismatch[1], name)
		} else if check.invalidJSONArgumentIndex != 0 {
			err = moerr.NewInvalidTypeForJSON(ctx, check.invalidJSONArgumentIndex, name)
		} else if f.isFunction() {
			err = moerr.NewInvalidArg(ctx, fmt.Sprintf("function %s", name), args)
		} else {
			err = moerr.NewInvalidArg(ctx, fmt.Sprintf("operator %s", name), args)
		}

	case failedAggParametersWrong:
		err = moerr.NewInvalidArg(ctx, fmt.Sprintf("aggregate function %s", name), args)

	case failedBitwiseAggregateOperandsSize:
		err = moerr.NewInvalidBitwiseAggregateOperandsSize(ctx)

	case failedTooManyFunctionMatched:
		err = moerr.NewInvalidArg(ctx, fmt.Sprintf("too many overloads matched %s", name), args)
	}
	if err == nil {
		r.applyDivPrecisionIncrement(ctx, args)
		r.applyDecimalModMetadata(args)
		if !LegacyDecimalIntegerDiv(ctx) {
			r.applyDecimalIntegerDivMetadata(args)
		}
	}

	return r, err
}

// GetFunctionByNameWithoutError tries to resolve a function overload without
// constructing an error for an expected mismatch. It is intended for
// speculative planner checks where unsupported candidate types are normal and
// the caller only needs the successful resolution metadata.
func GetFunctionByNameWithoutError(name string, args []types.Type) (r FuncGetResult, ok bool) {
	r.fid, ok = getFunctionIdByNameWithoutErr(name)
	if !ok || r.fid < 0 || int(r.fid) >= len(allSupportedFunctions) {
		return FuncGetResult{}, false
	}

	f := allSupportedFunctions[r.fid]
	if len(f.Overloads) == 0 || f.checkFn == nil {
		return FuncGetResult{}, false
	}

	check := f.checkArgumentTypes(args, nil)
	switch check.status {
	case succeedMatched:
		r.overloadId = int32(check.idx)
		r.retType = f.Overloads[r.overloadId].retType(args)
		r.cannotRunInParallel = f.Overloads[r.overloadId].cannotParallel
		r.applyDivPrecisionIncrement(context.Background(), args)
		r.applyDecimalModMetadata(args)
		r.applyDecimalIntegerDivMetadata(args)
		return r, true

	case succeedWithCast:
		r.overloadId = int32(check.idx)
		r.needCast = true
		r.targetTypes = check.finalType
		r.retType = f.Overloads[r.overloadId].retType(r.targetTypes)
		r.cannotRunInParallel = f.Overloads[r.overloadId].cannotParallel
		r.applyDivPrecisionIncrement(context.Background(), args)
		r.applyDecimalModMetadata(args)
		r.applyDecimalIntegerDivMetadata(args)
		return r, true

	default:
		return FuncGetResult{}, false
	}
}

// GetFunctionByNameWithOverload validates the arguments using the function's
// normal type checker, then selects a specific overload. It is intended for
// planner-only variants that must keep the same SQL function name and layout.
func GetFunctionByNameWithOverload(
	ctx context.Context, name string, args []types.Type, overloadID int32,
) (r FuncGetResult, err error) {
	originalArgs := args
	if name == "cast" && IsIntegerArgumentCastOverload(overloadID) {
		if !integerArgumentCastSignature(overloadID, args) {
			return FuncGetResult{}, moerr.NewInvalidInputf(ctx, "invalid integer argument cast signature %v", args)
		}
		return FuncGetResult{fid: CAST, overloadId: overloadID, retType: args[1]}, nil
	}
	r, err = GetFunctionByName(ctx, name, args)
	if err != nil {
		return r, err
	}
	f := allSupportedFunctions[r.fid]
	if overloadID < 0 || int(overloadID) >= len(f.Overloads) {
		return FuncGetResult{}, moerr.NewInvalidInputf(ctx, "function overload %s.%d not found", name, overloadID)
	}
	if !f.bindsOverload(int(overloadID)) {
		return FuncGetResult{}, moerr.NewInvalidInputf(ctx, "function overload %s.%d is legacy execution only", name, overloadID)
	}
	if r.fid == INTEGER_DIV && overloadID == DecimalIntegerDivUnsignedOverload {
		if _, scoped := DecimalIntegerDivReturnType(originalArgs); !scoped {
			return FuncGetResult{}, moerr.NewInvalidInputf(ctx, "invalid decimal integer division signature %v", originalArgs)
		}
		r.targetTypes = decimalIntegerDivOperandTypes(originalArgs)
		r.needCast = !originalArgs[0].Eq(r.targetTypes[0]) || !originalArgs[1].Eq(r.targetTypes[1])
	}
	r.overloadId = overloadID
	if r.needCast {
		args = r.targetTypes
	}
	r.retType = f.Overloads[overloadID].retType(args)
	r.cannotRunInParallel = f.Overloads[overloadID].cannotParallel
	r.applyDivPrecisionIncrement(ctx, originalArgs)
	r.applyDecimalModMetadata(originalArgs)
	r.applyDecimalIntegerDivMetadata(originalArgs)
	return r, nil
}

// RunFunctionDirectly runs a function directly without any protections.
// It is dangerous and should be used only when you are sure that the overloadID is correct and the inputs are valid.
func RunFunctionDirectly(proc *process.Process, overloadID int64, inputs []*vector.Vector, length int) (*vector.Vector, error) {
	f, err := GetFunctionById(proc.Ctx, overloadID)
	if err != nil {
		return nil, err
	}

	mp := proc.Mp()
	inputTypes := make([]types.Type, len(inputs))
	for i := range inputTypes {
		inputTypes[i] = *inputs[i].GetType()
	}

	result := vector.NewFunctionResultWrapper(f.retType(inputTypes), mp)

	fold := !f.IsVolatile() && !f.IsRealTimeRelated() && (!f.HasRowScopedDiagnostics() || length > 0)
	evaluateLength := length
	if fold {
		for _, param := range inputs {
			if !param.IsConst() {
				fold = false
			}
		}
		if fold {
			evaluateLength = 1
		}
	}
	// Value constancy survives diagnostics; only effect-free folding may reduce
	// the evaluation length. Keep this proof if the probe rejects folding.
	scalarValue := fold

	if err = result.PreExtendAndReset(evaluateLength); err != nil {
		result.Free()
		return nil, err
	}
	exec, _, execFree, _ := f.GetExecuteMethod()
	if execFree != nil {
		defer func() { _ = execFree() }()
	}
	evalProc := proc
	var probe *process.WarningProbe
	if fold && f.HasRowScopedDiagnostics() {
		probe = &process.WarningProbe{}
		evalProc = proc.NewNoContextChildProc(0)
		evalProc.Ctx = proc.Ctx
		evalProc.WarningSink = probe
	}
	err = exec(inputs, result, evalProc, evaluateLength, nil)
	if probe != nil && (err != nil || probe.Warned()) {
		// The probe has no statement-visible effects. Re-evaluate at the
		// requested cardinality so the caller owns diagnostics and errors.
		if err = result.PreExtendAndReset(length); err == nil {
			err = exec(inputs, result, proc, length, nil)
		}
	}
	if err != nil {
		result.Free()
		return nil, err
	}

	vec := result.GetResultVector()
	if scalarValue {
		vec.ToConst()
		vec.SetLength(length)
	}
	return vec, nil
}

func GetAggFunctionNameByID(overloadID int64) string {
	f, exist := GetFunctionByIdWithoutError(overloadID)
	if !exist {
		return "unknown function"
	}
	return f.aggName
}

// DeduceNotNullable reports whether a function result is guaranteed to be
// non-NULL. STRICT functions normally preserve an all-non-NULL argument
// guarantee, except for functions that can synthesize NULL from valid values.
func DeduceNotNullable(overloadID int64, args []*plan.Expr) bool {
	return deduceNotNullable(context.Background(), overloadID, args)
}

// DeduceNotNullableForBinding shares the current inference owner while allowing
// complete historical catalog origins to reproduce their published metadata.
func DeduceNotNullableForBinding(ctx context.Context, overloadID int64, args []*plan.Expr) bool {
	return deduceNotNullable(ctx, overloadID, args)
}

func deduceNotNullable(ctx context.Context, overloadID int64, args []*plan.Expr) bool {
	fid, oid := DecodeOverloadID(overloadID)
	switch fid {
	case DATABASE:
		// DATABASE and its SCHEMA alias can observe an unset default database.
		return false
	case SMA, WMA, TRIMA, EMA, WILDER, KAMA, DEMA, TEMA, T3, MMAXDRAWDOWN:
		// Indicator kernels emit leading warmup NULLs (issue #29949,
		// design section 4); never proven not-nullable regardless of
		// argument nullability.
		return false
	case CUMPOSITIVESTREAK:
		// Dense output: NULL observations emit 0, never NULL.
		return true
	case EXTRACT:
		// Numeric EXTRACT synthesizes NULL for invalid text/fields and zero
		// calendar weeks. Released overloads 0..4 retain their physical ABI.
		if oid >= 5 && oid <= 9 {
			return false
		}
	case YEAR, MONTH, QUARTER, DAY, DAYOFMONTH, HOUR, MINUTE, SECOND, MICROSECOND:
		// Tolerant string extractors may reject a non-NULL value. Typed field
		// extraction remains non-NULL, including its zero-calendar fields.
		if len(args) == 1 && types.T(args[0].Typ.Id).IsMySQLString() {
			return false
		}
	case LAST_DAY, WEEK, WEEKOFYEAR, WEEKDAY, YEARWEEK, DAYOFWEEK, DAYOFYEAR, DAYNAME, MONTHNAME, FROM_DAYS:
		// Calendar calculations reject zero dates; FROM_DAYS rejects values
		// above the representable calendar even when the input is NOT NULL.
		return false
	case TO_DAYS, TO_SECONDS:
		// String calendar totals reject incomplete dates even when the input
		// column is NOT NULL. Preserve the legacy typed overload's contract.
		if oid == 1 {
			return false
		}
	case DATEDIFF:
		// Either string operand can be rejected and synthesize NULL.
		if oid >= 1 && oid <= 3 {
			return false
		}
	case DATE:
		// Typed zero dates can become NULL under NO_ZERO_DATE, and string
		// conversions synthesize NULL for malformed input.
		if oid == 0 || oid == 2 || ((oid == 3 || oid == 4) && !LegacyTemporalNullability(ctx)) {
			return false
		}
	case CAST:
		if (oid == 0 || oid == 1) && len(args) == 2 &&
			types.T(args[0].Typ.Id).IsMySQLString() && types.T(args[1].Typ.Id) == types.T_datetime && !LegacyTemporalNullability(ctx) {
			return false
		}
		// Explicit typed DATE casts share the DATE conversion policy. Legacy
		// implicit and assignment casts keep their existing nullability.
		if oid == 1 && len(args) == 2 &&
			types.T(args[1].Typ.Id) == types.T_date &&
			(types.T(args[0].Typ.Id) == types.T_date || types.T(args[0].Typ.Id) == types.T_datetime) {
			return false
		}
	case OCT:
		// New string executors produce NULL for empty non-NULL input.
		// Preserve the persisted legacy and numeric overload contracts.
		if oid >= OctStringOverloadStart && len(args) == 1 {
			switch types.T(args[0].Typ.Id) {
			case types.T_char, types.T_varchar, types.T_text,
				types.T_binary, types.T_varbinary, types.T_blob:
				return false
			}
		}
	case CASE:
		if caseHasTemporalPromotion(args) {
			return false
		}
		for _, arg := range args {
			if !arg.Typ.NotNullable {
				return false
			}
		}
		return true
	case TIMESTAMP:
		// Calendar parsing and zero-date handling can produce NULL even for
		// non-NULL operands, in both unary and pair forms.
		return false
	case ADDTIME:
		// These overloads can return NULL for an out-of-range TIME result
		// even when every operand is NOT NULL. The intervening legacy
		// DATETIME overloads retain their released contract.
		if oid <= 5 || (oid >= 9 && oid <= 11) {
			return false
		}
	case SUBTIME:
		if oid <= 5 || (oid >= 11 && oid <= 15) {
			return false
		}
	case TIMEDIFF:
		if oid <= 8 {
			return false
		}
	case DATE_ADD, DATE_SUB:
		if oid <= 15 {
			return false
		}
	case MAKETIME:
		if oid <= 38 {
			return false
		}
	case TIMESTAMPADD:
		if oid <= 7 {
			return false
		}
	case COALESCE:
		for _, arg := range args {
			if arg.Typ.NotNullable {
				return true
			}
		}
		return false
	case GREATEST, LEAST:
		return false
	case GREAT_THAN, GREAT_EQUAL, LESS_THAN, LESS_EQUAL:
		if len(args) == 2 && IsJSONNumericComparison(types.T(args[0].Typ.Id), types.T(args[1].Typ.Id)) {
			return false
		}
	case EQUAL, NOT_EQUAL:
		if len(args) == 2 && IsJSONNumericComparison(types.T(args[0].Typ.Id), types.T(args[1].Typ.Id)) {
			return false
		}
		// Direct JSON/BOOL equality preserves the JSON scalar category. A
		// physically non-NULL JSON value can still contain JSON null, which the
		// comparison maps to SQL UNKNOWN. Do not infer a non-NULL result merely
		// from the two vector-level argument declarations.
		if len(args) == 2 && isJSONBooleanComparison(
			types.T(args[0].Typ.Id).ToType(), types.T(args[1].Typ.Id).ToType()) {
			return false
		}
	// Value window functions can synthesize NULLs even when every input is
	// NOT NULL. LAG/LEAD do so outside the partition unless an explicit,
	// non-NULL default is present. FIRST_VALUE/LAST_VALUE can observe an empty
	// frame, and NTH_VALUE can also miss the requested row. The frame is not
	// available here, so keep those contracts conservative.
	case FIRST_VALUE, LAST_VALUE, NTH_VALUE:
		return false
	case LAG, LEAD:
		if len(args) != 3 {
			return false
		}
		for _, arg := range args {
			if !arg.Typ.NotNullable {
				return false
			}
		}
		return true
	// These STRICT functions can synthesize NULL from non-NULL arguments.
	// The UUID extractors do so for non-RFC-4122 variants, and
	// uuid_extract_timestamp also for versions without a time source (e.g. v4).
	case DIV, INTEGER_DIV, MOD,
		POW, EXP, COT, ACOSH, ATANH,
		JSON_EXTRACT, JSON_EXTRACT_STRING, JSON_EXTRACT_FLOAT64,
		REGEXP_SUBSTR,
		INET6_ATON, INET_ATON, INET6_NTOA, ELT, UNHEX, CONV, MAKEDATE,
		SHA2, AES_ENCRYPT, AES_DECRYPT, COMPRESS, UNCOMPRESS, EXTRACTVALUE, UPDATEXML,
		DATE_FORMAT, TIME_FORMAT,
		UUID_EXTRACT_VERSION, UUID_EXTRACT_TIMESTAMP,
		TO_INTERVAL, TO_INTERVAL_MICROSECOND, TO_CALENDAR_INTERVAL, TIMESTAMPADD_SECOND_INTERVAL:
		return false
	}
	if ProducesNoNull(overloadID) {
		return true
	}
	for _, arg := range args {
		if !arg.Typ.NotNullable {
			return false
		}
	}
	return true
}

func caseHasTemporalPromotion(args []*plan.Expr) bool {
	for i := 1; i < len(args); i += 2 {
		if isTemporalPromotion(args[i]) {
			return true
		}
	}
	// CASE arguments are condition/value pairs followed by ELSE. The ELSE
	// expression is at the final even index and needs the same check.
	if len(args)%2 == 1 && isTemporalPromotion(args[len(args)-1]) {
		return true
	}
	return false
}

func isTemporalPromotion(arg *plan.Expr) bool {
	fn := arg.GetF()
	if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) == 0 {
		return false
	}
	source := types.T(fn.Args[0].Typ.Id)
	target := types.T(arg.Typ.Id)
	return source.IsDateRelate() && target.IsDateRelate() && source != target
}

// ProducesNoNull reports whether a function's contract guarantees a non-NULL
// result independently of its argument values. This is stronger than
// DeduceNotNullable: STRICT functions such as json_extract can still return
// SQL NULL for non-NULL inputs when a requested value is absent.
func ProducesNoNull(overloadID int64) bool {
	fid, _ := DecodeOverloadID(overloadID)
	return fid >= 0 &&
		int(fid) < len(allSupportedFunctions) &&
		int(fid) == allSupportedFunctions[fid].functionId &&
		allSupportedFunctions[fid].testFlag(plan.Function_PRODUCE_NO_NULL)
}

// HasExecutableCTASTypeDefault reports whether the SQL type default is a valid
// value for a materialized function result when an INSERT omits that column.
// This contract is deliberately independent of ProducesNoNull: domain types
// such as HLL sketches never return NULL, but their zero-value byte string is
// not a valid encoded sketch.
func HasExecutableCTASTypeDefault(overloadID int64) bool {
	fid, _ := DecodeOverloadID(overloadID)
	return fid >= 0 &&
		int(fid) < len(allSupportedFunctions) &&
		int(fid) == allSupportedFunctions[fid].functionId &&
		allSupportedFunctions[fid].hasExecutableCTASTypeDefault
}

type FuncGetResult struct {
	fid        int32
	overloadId int32
	retType    types.Type

	cannotRunInParallel bool

	needCast    bool
	targetTypes []types.Type
}

func (fr *FuncGetResult) GetEncodedOverloadID() (overloadID int64) {
	return encodeOverloadID(fr.fid, fr.overloadId)
}

func (fr *FuncGetResult) ShouldDoImplicitTypeCast() (typs []types.Type, should bool) {
	return fr.targetTypes, fr.needCast
}

func (fr *FuncGetResult) GetReturnType() types.Type {
	return fr.retType
}

func (fr *FuncGetResult) CannotRunInParallel() bool {
	return fr.cannotRunInParallel
}

func encodeOverloadID(fid, overloadId int32) (overloadID int64) {
	overloadID = int64(fid)
	overloadID = overloadID << 32
	overloadID |= int64(overloadId)
	return
}

func DecodeOverloadID(overloadID int64) (fid int32, oIndex int32) {
	base := overloadID
	oIndex = int32(overloadID)
	fid = int32(base >> 32)
	return fid, oIndex
}

func IsUserLevelLockFunctionID(fid int32) bool {
	switch fid {
	case GET_LOCK, RELEASE_LOCK, IS_FREE_LOCK, IS_USED_LOCK, RELEASE_ALL_LOCKS:
		return true
	default:
		return false
	}
}

func getFunctionIdByName(ctx context.Context, name string) (int32, error) {
	if fid, ok := functionIdRegister[name]; ok {
		return fid, nil
	}
	return -1, moerr.NewNotSupportedf(ctx, "function or operator '%s'", name)
}

func getFunctionIdByNameWithoutErr(name string) (int32, bool) {
	fid, exist := functionIdRegister[name]
	return fid, exist
}

// FuncNew stores all information about a function.
// including the unique id that marks the function, the class which the function belongs to,
// and all overloads of the function.
type FuncNew struct {
	// unique id of function.
	functionId int

	// function type.
	class plan.Function_FuncFlag

	// Whether the SQL type default is a valid executable default after CTAS
	// materializes this function's result as a table column.
	hasExecutableCTASTypeDefault bool

	// All execution overloads, including identities retained for old plans.
	Overloads []overload

	// Integer contexts and canonical binding identities are independent of the
	// source numeric type. An empty bindingOverloads list keeps legacy binding.
	integerParameters []integerParameter
	bindingOverloads  []int

	// checkFn was used to check whether the input type can match the requirement of the function.
	// if matched, return the corresponding id of overload. If type conversion was required,
	// the required type should be returned at the same time.
	checkFn func(overloads []overload, inputs []types.Type) checkResult

	// stringDomainCheckFn is the optional second-stage checker for functions
	// whose string compatibility depends on operand provenance as well as the
	// current text/binary type.
	stringDomainCheckFn func(overloads []overload, inputs []types.Type, modes []StringDomainCheckMode) checkResult

	// layout was used for `explain SQL`.
	layout FuncExplainLayout
}

type executeLogicOfOverload func(parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process, length int,
	selectList *FunctionSelectList) error

// executeFreeOfOverload is used to free the resources allocated by the execution logic.
// It is mainly used in SERIAL and SERIAL_FULL.
// NOTE: right now, we are not throwing an error when the free logic failed. However, it is still included
// in case we need it in the future.
type executeFreeOfOverload func() error

// executeResetOfOverload is used to reset the resources allocated by the execution logic.
// It is mainly used in SERIAL and SERIAL_FULL.
// NOTE: right now, we are not throwing an error when the reset logic failed. However, it is still included
// in case we need it in the future.
type executeResetOfOverload func() error

// executeRetainedBytesOfOverload reports non-vector backing allocations kept
// alive by a stateful function operator. It is optional: ordinary functions
// whose complete retained state is represented by executor vectors omit it.
type executeRetainedBytesOfOverload func() uint64

// an overload of a function.
// stores all information about execution logic.
type overload struct {
	overloadId int

	// args records some type information about this overload.
	// in most case, it records, in order, which parameter types the overload required.
	// For example,
	//		args can be `{int64, int64}` of one overload for the `pow` function.
	//		this means the overload can accept {int64, int64} as its input.
	// but it was not necessarily the type directly required by the overload.
	// what it is depends on the logic of function's checkFn.
	args []types.T

	// return type of the overload.
	// parameters are the params actually received when the overload is executed.
	retType func(parameters []types.Type) types.Type

	// the execution logic.
	newOp func() executeLogicOfOverload

	// the execution logic and free logic.
	// NOTE: use either newOp or newOpWithFree.
	newOpWithFree func() (
		executeLogicOfOverload,
		executeResetOfOverload,
		executeFreeOfOverload,
		executeRetainedBytesOfOverload,
	)

	// in fact, the function framework does not directly run aggregate functions and window functions.
	// we use two flags to mark whether function is one of them.
	isAgg bool
	isWin bool

	// aggName is used in aggregate-related error messages.
	aggName string

	// if true, overload was unable to run in parallel.
	// For example,
	//		rand(1) cannot run in parallel because it should use the same rand seed.
	//
	// TODO: there is not a good place to use that in plan now. the attribute is not effective.
	cannotParallel bool

	// Legacy volatile policy also protects execution-time diagnostics and session
	// settings from folding. It need not imply different values on replay.
	volatile bool
	// A positive proof of repeatable values for fixed arguments and execution
	// context. Folding and diagnostic-count restrictions remain in force.
	// Unannotated volatile overloads retain conservative identity requirements.
	executionStable bool
	// Deterministic values may still emit diagnostics once per evaluated row.
	// Folding or changing their evaluation count would lose those effects.
	rowScopedDiagnostics bool
	// if realTimeRelated, overload cannot be folded when `Prepare`.
	realTimeRelated bool
}

// IsVolatile retains the legacy conservative evaluation policy. For value
// repeatability within one execution, use GetFunctionValueUnstableByID.
func (ov *overload) IsVolatile() bool {
	return ov.volatile
}

// CannotFold conservatively protects effects when no warning-free evaluation
// proof is available. Speculative folders use IsFoldCandidate and a warning probe.
func (ov *overload) CannotFold() bool {
	return ov.volatile || ov.rowScopedDiagnostics
}

func (ov *overload) HasRowScopedDiagnostics() bool {
	return ov.rowScopedDiagnostics
}

func (ov *overload) IsRealTimeRelated() bool {
	return ov.realTimeRelated
}

func (ov *overload) IsAgg() bool {
	return ov.isAgg
}

func (ov *overload) CannotExecuteInParallel() bool {
	return ov.cannotParallel
}

func (ov *overload) GetExecuteMethod() (
	executeLogicOfOverload,
	executeResetOfOverload,
	executeFreeOfOverload,
	executeRetainedBytesOfOverload,
) {
	if ov.newOpWithFree != nil {
		return ov.newOpWithFree()
	}

	fn := ov.newOp()
	return fn, nil, nil, nil
}

// GetArgumentType returns one element of the registered signature, whose meaning
// is defined by the function's type checker, without exposing its mutable slice.
func (ov *overload) GetArgumentType(index int) (types.T, bool) {
	if index < 0 || index >= len(ov.args) {
		return types.T_any, false
	}
	return ov.args[index], true
}

func (ov *overload) GetReturnTypeMethod() func(parameters []types.Type) types.Type {
	return ov.retType
}

func (ov *overload) IsWin() bool {
	return ov.isWin
}

func (fn *FuncNew) isFunction() bool {
	return fn.layout == STANDARD_FUNCTION || fn.layout >= NOPARAMETER_FUNCTION
}

func (fn *FuncNew) isAggregate() bool {
	return fn.testFlag(plan.Function_AGG)
}

func (fn *FuncNew) isWindow() bool {
	return fn.testFlag(plan.Function_WIN_ORDER) || fn.testFlag(plan.Function_WIN_VALUE) || fn.testFlag(plan.Function_AGG)
}

func (fn *FuncNew) isWindowOrder() bool {
	return fn.testFlag(plan.Function_WIN_ORDER)
}

func (fn *FuncNew) isWindowValue() bool {
	return fn.testFlag(plan.Function_WIN_VALUE)
}

func (fn *FuncNew) testFlag(funcFlag plan.Function_FuncFlag) bool {
	return fn.class&funcFlag != 0
}

type overloadCheckSituation int

const (
	succeedMatched                     overloadCheckSituation = 0
	succeedWithCast                    overloadCheckSituation = -1
	failedFunctionParametersWrong      overloadCheckSituation = -2
	failedAggParametersWrong           overloadCheckSituation = -3
	failedTooManyFunctionMatched       overloadCheckSituation = -4
	failedBitwiseAggregateOperandsSize overloadCheckSituation = -5
)

type checkResult struct {
	status overloadCheckSituation

	// if matched
	idx                      int
	finalType                []types.Type
	invalidJSONArgumentIndex int
	characterSetMismatch     [2]string
}

func newCheckResultWithSuccess(overloadId int) checkResult {
	return checkResult{status: succeedMatched, idx: overloadId}
}

func newCheckResultWithFailure(status overloadCheckSituation) checkResult {
	return checkResult{status: status}
}

func newCheckResultWithInvalidJSONArgument(argumentIndex int) checkResult {
	return checkResult{
		status:                   failedFunctionParametersWrong,
		invalidJSONArgumentIndex: argumentIndex,
	}
}

func newCheckResultWithCharacterSetMismatch(left, right string) checkResult {
	return checkResult{
		status:               failedFunctionParametersWrong,
		characterSetMismatch: [2]string{left, right},
	}
}

func newCheckResultWithCast(overloadId int, castType []types.Type) checkResult {
	return checkResult{
		status:    succeedWithCast,
		idx:       overloadId,
		finalType: castType,
	}
}

type FunctionSelectList struct {
	AnyNull    bool
	AllNull    bool
	SelectList []bool
}

func (selectList *FunctionSelectList) ShouldEvalAllRow() bool {
	if selectList == nil {
		return true
	}
	return !selectList.AnyNull
}

func (selectList *FunctionSelectList) IgnoreAllRow() bool {
	if selectList == nil {
		return false
	}
	return selectList.AllNull
}

func (selectList *FunctionSelectList) Contains(row uint64) bool {
	if selectList == nil || len(selectList.SelectList) <= int(row) {
		return false
	}
	return !selectList.SelectList[row]
}

var EncodeOverloadID = encodeOverloadID
var GetFunctionIdByName = getFunctionIdByName
