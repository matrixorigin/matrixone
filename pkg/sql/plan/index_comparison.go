// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// indexComparisonDomain describes how SQL bounds relate to physical keys.
// Only exact domains may be serialized as an ordinary lookup operand.
type indexComparisonDomain uint8

const (
	indexComparisonUnsupported indexComparisonDomain = iota
	indexComparisonRaw
	indexComparisonNormalized
	indexComparisonInverse
)

func (d indexComparisonDomain) exact() bool {
	return d == indexComparisonRaw || d == indexComparisonNormalized
}

// indexComparisonMayNeedInverse is the comparison owner's capability check.
// An unknown bound is only a conservative lifetime question; it must not
// admit an executable index range.
func indexComparisonMayNeedInverse(key, bound types.T) bool {
	if bound == types.T_any {
		return isDatetimeTimestampTypePair(key, types.T_datetime) ||
			isDatetimeTimestampTypePair(key, types.T_timestamp)
	}
	return isDatetimeTimestampTypePair(key, bound)
}

func sameIndexComparisonDomain(key, value types.Type) bool {
	// CHAR IN compares PAD SPACE keys, whereas SERIAL preserves bytes. The
	// raw-vector runtime-filter contract invokes that IN kernel and therefore
	// does not prove that a byte predicate over a persisted CHAR key is safe.
	if key.Oid == types.T_char || value.Oid == types.T_char {
		return false
	}
	return keycodec.SupportsExactRawRuntimeFilterPair(key, value) ||
		key.Oid.IsMySQLString() && value.Oid.IsMySQLString() &&
			!types.IsUnicodeCollation(key.Charset) && !types.IsUnicodeCollation(value.Charset)
}

func analyzeIndexComparisonBound(ctx context.Context, bound *plan.Expr, keyType plan.Type) indexComparisonDomain {
	if bound == nil || !isRuntimeConstExpr(bound) {
		return indexComparisonUnsupported
	}
	if lit := bound.GetLit(); lit != nil && lit.Isnull {
		// SERIAL propagates NULL, as SQL comparisons do. Stored SERIAL_FULL
		// keys can still require a decoded residual; the lookup owns that rule.
		return indexComparisonRaw
	}
	key, value := makeTypeByPlan2Type(keyType), makeTypeByPlan2Expr(bound)
	if sameIndexComparisonDomain(key, value) {
		return indexComparisonRaw
	}
	if key.Oid.IsDecimal() && value.Oid.IsDecimal() && losslessIndexDecimalBound(ctx, bound, key) {
		return indexComparisonNormalized
	}
	if value.Oid != types.T_any && indexComparisonMayNeedInverse(key.Oid, value.Oid) {
		return indexComparisonInverse
	}
	// In particular, DATETIME/TIMESTAMP comparison converts the DATETIME in
	// the session zone and truncates it to the TIMESTAMP precision. A cast of
	// the bound is not its inverse (precision buckets and DST folds/gaps).
	return indexComparisonUnsupported
}

func losslessIndexDecimalBound(ctx context.Context, bound *plan.Expr, key types.Type) bool {
	literal := bound.GetLit()
	if literal == nil || literal.Isnull {
		return false // A runtime value or a cast source is not a value proof.
	}
	if bound.Typ.Scale > key.Scale {
		zero, proven := decimalTrailingZerosStatus(bound, makeTypeByPlan2Expr(bound), key.Scale)
		if !zero || !proven {
			return false
		}
	}
	var text string
	switch value := literal.Value.(type) {
	case *plan.Literal_Decimal64Val:
		text = types.Decimal64(value.Decimal64Val.A).Format(bound.Typ.Scale)
	case *plan.Literal_Decimal128Val:
		text = (types.Decimal128{B0_63: uint64(value.Decimal128Val.A), B64_127: uint64(value.Decimal128Val.B)}).Format(bound.Typ.Scale)
	case *plan.Literal_Sval:
		if types.T(bound.Typ.Id) != types.T_decimal256 {
			return false
		}
		text = value.Sval
	default:
		return false
	}
	// Suffix zeros alone do not prove that an assignment cast fits the target
	// precision/carrier. Check that domain too, without evaluating an expression
	// or publishing any conversion error from this optional optimization.
	var err error
	switch key.Oid {
	case types.T_decimal64:
		_, err = types.ParseDecimal64WithContext(ctx, text, key.Width, key.Scale)
	case types.T_decimal128:
		_, err = types.ParseDecimal128WithContext(ctx, text, key.Width, key.Scale)
	case types.T_decimal256:
		_, err = types.ParseDecimal256WithContext(ctx, text, key.Width, key.Scale)
	default:
		return false
	}
	return err == nil
}

// normalizeSerializedIndexComparison is also used at construction, so a
// caller cannot bypass admission and serialize an incompatible or unnormalized
// operand. SQL filters remain untouched when optional binding fails.
func normalizeSerializedIndexComparison(ctx context.Context, filter *plan.Expr, keyType plan.Type) (*plan.Expr, error) {
	if filter == nil || !serializedIndexComparisonUsable(ctx, filter.GetF(), keyType) {
		return nil, moerr.NewInternalError(ctx, "unsupported serialized index comparison domain")
	}
	result := DeepCopyExpr(filter)
	fn := result.GetF()
	if fn.Func.ObjName == "or" {
		for i, arg := range fn.Args {
			var err error
			fn.Args[i], err = normalizeSerializedIndexComparison(ctx, arg, keyType)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	var positions []int
	switch fn.Func.ObjName {
	case "=", "<", "<=", ">", ">=":
		pos := 1
		if fn.Args[1].GetCol() != nil {
			pos = 0
		}
		positions = []int{pos}
	case "between", "in_range":
		positions = []int{1, 2}
	case "in":
		if list := fn.Args[1].GetList(); list != nil {
			for i, bound := range list.List {
				var err error
				list.List[i], err = normalizeIndexComparisonBound(ctx, bound, keyType)
				if err != nil {
					return nil, err
				}
			}
			fn.Args[1].Typ = keyType
		}
	}
	for _, pos := range positions {
		var err error
		fn.Args[pos], err = normalizeIndexComparisonBound(ctx, fn.Args[pos], keyType)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func normalizeIndexComparisonBound(ctx context.Context, bound *plan.Expr, keyType plan.Type) (*plan.Expr, error) {
	decision := analyzeIndexComparisonBound(ctx, bound, keyType)
	if !decision.exact() {
		return nil, moerr.NewInternalError(ctx, "unsupported serialized index comparison domain")
	}
	if decision == indexComparisonNormalized {
		return forceCastExpr(ctx, bound, keyType)
	}
	return bound, nil
}

// indexComparisonColumn returns the component compared with runtime bounds.
// It does not commute or otherwise mutate the original SQL expression.
func indexComparisonColumn(fn *plan.Function) *plan.Expr {
	if fn == nil || fn.Func == nil || len(fn.Args) < 2 {
		return nil
	}
	if fn.Args[0] != nil && fn.Args[0].GetCol() != nil {
		return fn.Args[0]
	}
	switch fn.Func.ObjName {
	case "=", "<", "<=", ">", ">=":
		if fn.Args[1] != nil && fn.Args[1].GetCol() != nil {
			return fn.Args[1]
		}
	}
	return nil
}

// serializedIndexComparisonUsable proves that every bound is in the stored
// component's comparison domain. Prefix/NULL exactness is a separate property
// of the complete lookup; this is the no-false-negatives admission boundary.
func serializedIndexComparisonUsable(ctx context.Context, fn *plan.Function, keyType plan.Type) bool {
	if fn == nil || fn.Func == nil {
		return false
	}
	if fn.Func.ObjName == "or" {
		if len(fn.Args) == 0 {
			return false
		}
		for _, arm := range fn.Args {
			if arm == nil || !serializedIndexComparisonUsable(ctx, arm.GetF(), keyType) {
				return false
			}
		}
		return true
	}
	column := indexComparisonColumn(fn)
	if column == nil || column.Typ.Id != keyType.Id ||
		types.T(keyType.Id).IsDecimal() && column.Typ.Scale != keyType.Scale {
		return false
	}
	usable := func(bound *plan.Expr) bool { return analyzeIndexComparisonBound(ctx, bound, keyType).exact() }
	switch fn.Func.ObjName {
	case "=", "<", "<=", ">", ">=":
		if len(fn.Args) != 2 {
			return false
		}
		bound := fn.Args[1]
		if column == bound {
			bound = fn.Args[0]
		}
		return usable(bound)
	case "in_range":
		if len(fn.Args) != 4 || fn.Args[3] == nil {
			return false
		}
		return usable(fn.Args[1]) && usable(fn.Args[2])
	case "between":
		return len(fn.Args) == 3 && usable(fn.Args[1]) && usable(fn.Args[2])
	case "in":
		return serializedIndexInUsable(ctx, fn.Args[1], keyType)
	default:
		return false
	}
}

func serializedIndexInUsable(ctx context.Context, rhs *plan.Expr, keyType plan.Type) (usable bool) {
	if rhs == nil {
		return false
	}
	if list := rhs.GetList(); list != nil {
		for _, bound := range list.List {
			if !analyzeIndexComparisonBound(ctx, bound, keyType).exact() {
				return false
			}
		}
		return true
	}
	if rhs.GetVec() != nil {
		// Checked decoding validates executable vector metadata without
		// allocating a scalar expression for every member.
		vec, _, ok := decodeLiteralVec(rhs.GetVec())
		return ok && types.T(keyType.Id) != types.T_char && vec.GetType().Oid != types.T_char &&
			keycodec.SupportsExactRawRuntimeFilterPair(makeTypeByPlan2Type(keyType), *vec.GetType())
	}
	return analyzeIndexComparisonBound(ctx, rhs, keyType).exact()
}

func mixedTemporalIndexComparison(fn *plan.Function) bool {
	column := indexComparisonColumn(fn)
	if column == nil {
		return false
	}
	for _, arg := range fn.Args {
		if arg == nil {
			continue
		}
		left, right := types.T(column.Typ.Id), types.T(arg.Typ.Id)
		if left == types.T_datetime && right == types.T_timestamp || left == types.T_timestamp && right == types.T_datetime {
			return true
		}
	}
	return false
}

func (builder *QueryBuilder) regularIndexEqualityUsable(idxDef *IndexDef, table *plan.TableDef, fn *plan.Function, part int) bool {
	if !indexTableStoresSerializedKey(idxDef) {
		return true // Keep the original typed scalar comparison on a direct key.
	}
	if part < 0 || part >= len(idxDef.Parts) {
		return false
	}
	pos, ok := table.Name2ColIndex[catalog.ResolveAlias(idxDef.Parts[part])]
	return ok && pos >= 0 && int(pos) < len(table.Cols) &&
		serializedIndexComparisonUsable(builder.GetContext(), fn, table.Cols[pos].Typ)
}

func (builder *QueryBuilder) compositeKeyComparisonUsable(fn *plan.Function, table *plan.TableDef) bool {
	if fn != nil && fn.Func != nil && (fn.Func.ObjName == "or" || fn.Func.ObjName == "and") {
		// Each recursively lowered arm/conjunct owns its component proof.
		return true
	}
	column := indexComparisonColumn(fn)
	if column == nil || column.GetCol().ColPos < 0 || int(column.GetCol().ColPos) >= len(table.Cols) {
		return false
	}
	return serializedIndexComparisonUsable(builder.GetContext(), fn, table.Cols[column.GetCol().ColPos].Typ)
}

// Ordering may skip a fixed component only when SQL equality identifies at
// most one physical value. Cross-width integer equality has this property,
// even though its operands cannot be serialized in different integer domains.
func indexEqualityPinsSingleValue(key, value plan.Type) bool {
	k, v := makeTypeByPlan2Type(key), makeTypeByPlan2Type(value)
	return sameIndexComparisonDomain(k, v) || k.IsIntOrUint() && v.IsIntOrUint()
}

type indexComparison struct {
	column *plan.Expr
	bound  *plan.Expr
	op     types.ComparisonOp
	scale  int32
	domain indexComparisonDomain
}

// observationMatches checks only the execution state used to observe this
// comparison. A raw integer literal has no clock or time-zone dependency.
func (comparison indexComparison) observationMatches(intent ExecutionIntent, proc *process.Process) bool {
	if comparison.domain == indexComparisonInverse {
		return intent.MatchesProcess(proc)
	}
	return ProcessAllowsIndexObservation(proc)
}

// costRanges describes the physical values that may satisfy the comparison.
// These numeric arms are only a cost approximation, never an execution filter.
// It writes at most four arms into the caller's buffer and returns their count.
func (comparison indexComparison) costRanges(ctx context.Context, scalar indexCostScalar, zone *time.Location, spans *[4][2]float64) (int, bool, error) {
	if comparison.domain == indexComparisonInverse {
		if scalar.null || len(scalar.value) != 8 {
			return 0, false, nil
		}
		kind := types.T(comparison.column.Typ.Id)
		candidate, err := types.BuildTemporalCandidate(ctx, kind, comparison.scale, types.DecodeInt64(scalar.value), false, comparison.op, zone)
		if err != nil {
			return 0, false, err
		}
		count, ok := temporalIndexCostRanges(kind, candidate, spans)
		return count, ok, nil
	}
	if comparison.domain != indexComparisonRaw {
		return 0, false, nil
	}
	value, ok := indexIntegerCostValue(scalar)
	if !ok {
		return 0, false, nil
	}
	lo, hi := math.Inf(-1), math.Inf(1)
	switch comparison.op {
	case types.ComparisonEqual:
		lo, hi = value, value
	case types.ComparisonLess:
		hi = value - 1
	case types.ComparisonLessEqual:
		hi = value
	case types.ComparisonGreater:
		lo = value + 1
	case types.ComparisonGreaterEqual:
		lo = value
	default:
		return 0, false, nil
	}
	spans[0] = [2]float64{lo, hi}
	return 1, true, nil
}

func temporalIndexCostRanges(kind types.T, candidate types.TemporalCandidate, spans *[4][2]float64) (int, bool) {
	if (kind != types.T_datetime && kind != types.T_timestamp) || candidate.Full || candidate.Empty || candidate.Lo > candidate.Hi {
		return 0, false
	}
	spans[0] = [2]float64{float64(candidate.Lo), float64(candidate.Hi)}
	if kind == types.T_datetime {
		spans[1] = [2]float64{float64(candidate.Sentinel), float64(candidate.Sentinel)}
		spans[2] = [2]float64{math.MinInt64, -2}
		spans[3] = [2]float64{float64(int64(types.DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999)) + 1), math.MaxInt64}
		return 4, true
	}
	return 1, true
}

func indexIntegerCostValue(scalar indexCostScalar) (float64, bool) {
	if scalar.null || !scalar.typ.IsIntOrUint() || len(scalar.value) != scalar.typ.Oid.FixedLength() {
		return 0, false
	}
	var value int64
	switch scalar.typ.Oid {
	case types.T_uint8:
		return float64(types.DecodeUint8(scalar.value)), true
	case types.T_uint16:
		return float64(types.DecodeUint16(scalar.value)), true
	case types.T_uint32:
		return float64(types.DecodeUint32(scalar.value)), true
	case types.T_uint64:
		v := types.DecodeUint64(scalar.value)
		return float64(v), v < 1<<53
	case types.T_int8:
		value = int64(types.DecodeInt8(scalar.value))
	case types.T_int16:
		value = int64(types.DecodeInt16(scalar.value))
	case types.T_int32:
		value = int64(types.DecodeInt32(scalar.value))
	case types.T_int64:
		value = types.DecodeInt64(scalar.value)
	default:
		return 0, false
	}
	return float64(value), value > -(1<<53) && value < 1<<53
}

// The current necessary predicate has at most seven bound endpoints. Encoded
// leaves copy endpoint trees; reserve those copies and wrapper nodes before
// construction. The later structure walk charges its own work separately.
func (comparison indexComparison) prepayNecessaryPredicate(ctx context.Context, budget *int) bool {
	bound := comparison.bound
	if comparison.domain != indexComparisonInverse {
		return false
	}
	nodes := 0
	if !indexCostExpressionFits(ctx, bound, 0, &nodes, budget) {
		return false
	}
	return spendIndexCostWork(ctx, budget, 16*(nodes+64)+16*bound.ProtoSize()) && *budget > 0
}

func (comparison indexComparison) necessaryPredicate(ctx context.Context, column, physical *plan.Expr) (*plan.Expr, error) {
	if comparison.domain != indexComparisonInverse {
		return nil, nil
	}
	return buildTemporalCandidate(ctx, column, comparison.bound, physical, comparison.op, comparison.scale)
}

// necessaryRangeAllowed proves producer-specific execution conditions. The
// generic observation and compile owners only check their captured context.
func (comparison indexComparison) necessaryRangeAllowed(zone *time.Location) bool {
	if comparison.domain != indexComparisonInverse {
		return false
	}
	offset, fixed := types.FixedZoneOffset(zone)
	return fixed && int64(offset) >= math.MinInt32 && int64(offset) <= math.MaxInt32
}

// analyzeIndexComparison identifies a single oriented comparison. It is
// only a shape and eager-bound proof, not the required query-level admission.
func analyzeIndexComparison(ctx context.Context, expr *plan.Expr) (indexComparison, bool) {
	if expr == nil || !indexPredicateEvaluationSafe(expr) {
		return indexComparison{}, false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil {
		return indexComparison{}, false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	var op types.ComparisonOp
	switch id {
	case function.EQUAL:
		op = types.ComparisonEqual
	case function.LESS_THAN:
		op = types.ComparisonLess
	case function.LESS_EQUAL:
		op = types.ComparisonLessEqual
	case function.GREAT_THAN:
		op = types.ComparisonGreater
	case function.GREAT_EQUAL:
		op = types.ComparisonGreaterEqual
	default:
		return indexComparison{}, false
	}
	column := indexComparisonColumn(fn)
	if column == nil {
		return indexComparison{}, false
	}
	bound := fn.Args[1]
	if column == fn.Args[1] {
		bound = fn.Args[0]
		switch op {
		case types.ComparisonLess:
			op = types.ComparisonGreater
		case types.ComparisonLessEqual:
			op = types.ComparisonGreaterEqual
		case types.ComparisonGreater:
			op = types.ComparisonLess
		case types.ComparisonGreaterEqual:
			op = types.ComparisonLessEqual
		}
	}
	domain := analyzeIndexComparisonBound(ctx, bound, column.Typ)
	if domain == indexComparisonUnsupported || !indexBoundEvaluationSafe(bound) {
		return indexComparison{}, false
	}
	scale := column.Typ.Scale
	if domain == indexComparisonInverse && types.T(bound.Typ.Id) == types.T_timestamp {
		scale = bound.Typ.Scale
		if clock := bound.GetF(); clock != nil {
			wanted := int32(0)
			if len(clock.Args) == 1 {
				wanted, _ = temporalFunctionFSPFromPlanExpr(clock.Args[0])
			}
			if scale != wanted {
				return indexComparison{}, false
			}
		}
	}
	if domain == indexComparisonInverse && (scale < 0 || scale > 6) {
		return indexComparison{}, false
	}
	return indexComparison{column: column, bound: bound, op: op, scale: scale, domain: domain}, true
}

// indexBoundEvaluationSafe is a positive proof for optional eager evaluation.
// Prepared parameter provenance must be checked separately before using it.
func indexBoundEvaluationSafe(expr *plan.Expr) bool {
	if expr == nil || indexExpressionHasUnprovedSource(expr) {
		return false
	}
	if literal := expr.GetLit(); literal != nil {
		// A materialized parameter may have literal syntax. The query-level
		// admission gate must reject unknown or unproved provenance separately.
		return literal.Src == nil
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || types.T(expr.Typ.Id) != types.T_timestamp {
		return false
	}
	id, overload := function.DecodeOverloadID(fn.Func.Obj)
	if id != function.CURRENT_TIMESTAMP || overload != 0 {
		return false
	}
	if unstable, known := function.GetFunctionValueUnstableByID(fn.Func.Obj); !known || unstable || !function.IsRuntimeConstant(expr) {
		return false
	}
	switch len(fn.Args) {
	case 0:
		return true
	case 1:
		_, ok := temporalFunctionFSPFromPlanExpr(fn.Args[0])
		return ok
	default:
		return false
	}
}

// indexPredicateEvaluationSafe proves each original predicate, not merely
// the bound used by an optional candidate. It does not authorize a candidate by
// itself: the scan, ancestor and prepared-provenance gates are also required.
func indexPredicateEvaluationSafe(expr *plan.Expr) bool {
	if expr == nil || types.T(expr.Typ.Id) != types.T_bool || indexExpressionHasUnprovedSource(expr) {
		return false
	}
	if isTruncationSafePredicateExpr(expr) {
		return true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	if _, ok := function.GetFunctionByIdWithoutError(fn.Func.Obj); !ok {
		return false
	}
	if id == function.AND || id == function.OR {
		return len(fn.Args) == 2 && indexPredicateEvaluationSafe(fn.Args[0]) && indexPredicateEvaluationSafe(fn.Args[1])
	}
	if !isTruncationSafeEquality(id) && !isTruncationSafeOrdering(id) || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil {
		return false
	}
	left, right := fn.Args[0], fn.Args[1]
	if left.GetCol() == nil {
		left, right = right, left
	}
	return left.GetCol() != nil &&
		comparisonEvaluationIsTotal(id, left.Typ, right.Typ) && indexBoundEvaluationSafe(right)
}

// A literal's retained source may carry parameter or diagnostic provenance.
// Until those sources are proved per execution, do not narrow around them.
func indexExpressionHasUnprovedSource(expr *plan.Expr) bool {
	if expr == nil || expr.GetP() != nil {
		return true
	}
	if lit := expr.GetLit(); lit != nil && lit.Src != nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if indexExpressionHasUnprovedSource(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if indexExpressionHasUnprovedSource(item) {
				return true
			}
		}
	}
	return false
}

// indexScanExpressionsSafe is only the scan-owner part of the
// proof. The caller must also prove all ancestor paths and query-wide prepared
// provenance before creating a hint. Pagination is declined until its separate
// order proof is connected.
func indexScanExpressionsSafe(node *plan.Node) bool {
	if node == nil || node.NodeType != plan.Node_TABLE_SCAN || node.Limit != nil || node.Offset != nil {
		return false
	}
	for _, predicate := range node.FilterList {
		if !indexPredicateEvaluationSafe(predicate) {
			return false
		}
	}
	for _, expr := range node.ProjectList {
		if expr == nil || indexExpressionHasUnprovedSource(expr) || !isTruncationSafeRowExpr(expr) {
			return false
		}
	}
	return true
}

// indexCandidatePathSafe checks the current rewritten path. The
// original-owner proof and final catalog/publication proof are separate.
func indexCandidatePathSafe(query *plan.Query, scanID int32) bool {
	path, ok := traceIndexCandidatePath(query)
	return ok && path.ids[path.len-1] == scanID && indexPathExpressionsSafe(query, path)
}

// temporalInteriorTimestampMargin includes the fixed-zone offset envelope,
// the largest precision quantum and strict-bound arithmetic. Values inside
// this domain cannot trigger the calendar interval's lo>hi reset for a ray.
const temporalInteriorTimestampMargin = (int64(1)<<31+72*types.SecsPerHour+2)*types.MicroSecsPerSec + types.MicroSecsPerSec + 1

// Classify an immutable expression domain, never the observed statement clock.
// CURRENT_TIMESTAMP maps every int64 Unix nanosecond input into 1677..2262,
// including precision rounding, well inside this calendar margin. Other
// functions and unproved parameter/source values retain the original union.
func temporalCandidateInteriorTimestampBound(bound *plan.Expr) bool {
	if bound == nil || types.T(bound.Typ.Id) != types.T_timestamp || bound.Typ.Scale < 0 || bound.Typ.Scale > 6 || bound.AuxId < 0 || bound.PreparedNumeric != nil || len(bound.XXX_unrecognized) != 0 || len(bound.Typ.XXX_unrecognized) != 0 {
		return false
	}
	switch value := bound.Expr.(type) {
	case *plan.Expr_Lit:
		if value == nil || value.Lit == nil || value.Lit.Isnull || value.Lit.Src != nil || len(value.Lit.XXX_unrecognized) != 0 {
			return false
		}
		stamp, ok := value.Lit.Value.(*plan.Literal_Timestampval)
		if !ok || stamp == nil {
			return false
		}
		maximum := int64(types.DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999))
		return stamp.Timestampval >= temporalInteriorTimestampMargin && stamp.Timestampval <= maximum-temporalInteriorTimestampMargin
	case *plan.Expr_F:
		if value == nil || value.F == nil || value.F.Func == nil || value.F.Func.PubInfo != nil || value.F.Func.Snapshot != nil || len(value.F.AggConfig) != 0 || len(value.F.XXX_unrecognized) != 0 || len(value.F.Func.XXX_unrecognized) != 0 {
			return false
		}
		if len(value.F.Args) == 1 {
			arg := value.F.Args[0]
			if arg == nil || arg.AuxId < 0 || arg.PreparedNumeric != nil {
				return false
			}
			literal, ok := arg.Expr.(*plan.Expr_Lit)
			if !ok || literal == nil || literal.Lit == nil {
				return false
			}
			fsp, ok := literal.Lit.Value.(*plan.Literal_I64Val)
			if !ok || fsp == nil {
				return false
			}
		}
		return indexBoundEvaluationSafe(bound)
	default:
		return false
	}
}

// One topology owns the main interval, zero point and physical tails.
// A nonnil physical key emits serialized prefix leaves directly, avoiding an
// intermediate direct expression tree.
func buildTemporalCandidate(ctx context.Context, column, bound, physical *plan.Expr, op types.ComparisonOp, scale int32) (candidate *plan.Expr, err error) {
	if column == nil || column.GetCol() == nil || bound == nil || scale < 0 || scale > 6 || op > types.ComparisonGreaterEqual ||
		!isDatetimeTimestampTypePair(types.T(column.Typ.Id), types.T(bound.Typ.Id)) || !indexBoundEvaluationSafe(bound) {
		return nil, moerr.NewInternalError(ctx, "unsupported temporal candidate")
	}
	if physical != nil && (physical.GetCol() == nil || types.T(physical.Typ.Id) != types.T_varchar || types.IsUnicodeCollation(uint8(physical.Typ.Charset)) || physical.Typ.ValidateCollation() != nil || column.Typ.ValidateCollation() != nil) {
		return nil, nil
	}
	kind := types.T(column.Typ.Id)
	bind := func(name string, args ...*plan.Expr) (*plan.Expr, error) {
		return BindFuncExprImplByPlanExpr(ctx, name, args)
	}
	name := "__mo_temporal_datetime_bound"
	if kind == types.T_timestamp {
		name = "__mo_temporal_timestamp_bound"
	}
	control := func(value int32) *plan.Expr { return MakePlan2Int32ConstExprWithType(value) }
	endpoint := func(selector int32) (*plan.Expr, error) {
		expr, bindErr := bind(name, DeepCopyExpr(bound), control(int32(op)), control(scale), control(selector))
		if bindErr == nil {
			expr.Typ.Scale = column.Typ.Scale
		}
		return expr, bindErr
	}
	leaf := func(name string, values ...*plan.Expr) (*plan.Expr, error) {
		return lowerIndexComparisonLeaf(ctx, column, physical, name, values...)
	}
	literal := func(v int64) *plan.Expr {
		return &plan.Expr{Typ: column.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Datetimeval{Datetimeval: v}}}}
	}
	interior := kind == types.T_datetime && temporalCandidateInteriorTimestampBound(bound)
	lowerRay := interior && (op == types.ComparisonLess || op == types.ComparisonLessEqual)
	upperRay := interior && (op == types.ComparisonGreater || op == types.ComparisonGreaterEqual)
	var lo, hi *plan.Expr
	if lowerRay {
		lo = literal(math.MinInt64)
	} else if lo, err = endpoint(0); err != nil {
		return nil, err
	}
	if upperRay {
		hi = literal(math.MaxInt64)
	} else if hi, err = endpoint(1); err != nil {
		return nil, err
	}
	candidate, err = leaf("between", lo, hi)
	if err != nil {
		return nil, err
	}
	if kind == types.T_datetime {
		if !interior {
			point, pointErr := endpoint(2)
			if pointErr != nil {
				return nil, pointErr
			}
			pointExpr, pointErr := leaf("=", point)
			if pointErr != nil {
				return nil, pointErr
			}
			candidate, err = bind("or", candidate, pointExpr)
			if err != nil {
				return nil, err
			}
		}
		maxDate := int64(types.DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999))
		tails := [][2]int64{{math.MinInt64, -2}, {maxDate + 1, math.MaxInt64}}
		if lowerRay {
			tails = tails[1:] // the main ray now includes the lower tail and zero
		} else if upperRay {
			tails = tails[:1] // the main ray now includes the upper tail
		}
		for _, tail := range tails {
			tailExpr, tailErr := leaf("between", literal(tail[0]), literal(tail[1]))
			if tailErr != nil {
				return nil, tailErr
			}
			candidate, err = bind("or", candidate, tailExpr)
			if err != nil {
				return nil, err
			}
		}
	}
	return candidate, nil
}

// makeNecessaryIndexHint constructs a necessary physical filter from the atom
// just proved by indexCandidateOperands. The caller owns placement, cost and the
// final relation/transaction proof; this filter never replaces the SQL residual.
func makeNecessaryIndexHint(ctx context.Context, atom indexComparison, index *plan.IndexDef,
	indexTable *plan.TableDef, indexTag int32,
) (*plan.Expr, bool, error) {
	if indexTable == nil || len(indexTable.Cols) == 0 || indexTable.Cols[0] == nil {
		return nil, false, nil
	}
	physical := GetColExpr(indexTable.Cols[0].Typ, indexTag, 0)
	physical.GetCol().Name = indexTable.Cols[0].Name
	var hint *plan.Expr
	var err error
	if indexTableStoresSerializedKey(index) {
		if types.T(physical.Typ.Id) != types.T_varchar {
			return nil, false, nil
		}
		hint, err = atom.necessaryPredicate(ctx, atom.column, physical)
	} else {
		if physical.Typ.Id != atom.column.Typ.Id || physical.Typ.Scale != atom.column.Typ.Scale {
			return nil, false, nil
		}
		hint, err = atom.necessaryPredicate(ctx, physical, nil)
	}
	if err != nil || hint == nil {
		return nil, false, err
	}
	hint.CandidateFilter = true
	return hint, true, nil
}

// indexCandidateOperands rechecks the current path and index at publication.
// Catalog resolution may reenter planning, so an earlier proof is not enough.
func indexCandidateOperands(ctx context.Context, query *plan.Query, scanID int32, index *plan.IndexDef,
	filterPos int,
) (indexComparison, bool, error) {
	if ctx == nil {
		return indexComparison{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return indexComparison{}, false, err
	}
	if !indexCandidatePathSafe(query, scanID) {
		return indexComparison{}, false, nil
	}
	scan := query.Nodes[scanID]
	if len(scan.FilterList) != 1 || filterPos != 0 {
		return indexComparison{}, false, nil
	}
	atom, ok := analyzeIndexComparison(ctx, scan.FilterList[filterPos])
	if !ok || atom.domain != indexComparisonInverse || !indexComparisonMatches(scan, index, atom) {
		return indexComparison{}, false, nil
	}
	return atom, true, nil
}

// indexComparisonMatches checks only index-specific facts. Enumeration already
// proved the current path and atom once; publication repeats the full proof.
func indexComparisonMatches(scan *plan.Node, index *plan.IndexDef, atom indexComparison) bool {
	if scan == nil || scan.TableDef == nil || scan.TableDef.Pkey == nil || len(scan.BindingTags) == 0 ||
		!indexPlanningDefinitionFits(index) || !index.TableExist || !catalog.IsRegularIndexAlgo(index.IndexAlgo) || isSpatialIndexDef(index) ||
		index.KeyFormat != 0 || len(index.Parts) == 0 || regularIndexHasDeclaredPrefix(index) {
		return false
	}
	for _, part := range index.Parts {
		pos, exists := scan.TableDef.Name2ColIndex[catalog.ResolveAlias(part)]
		if !exists || pos < 0 || int(pos) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[pos] == nil {
			return false
		}
	}
	if indexUsesUnicodeCollation(index, scan.TableDef) || functionalIndexColumn(scan.TableDef, index) != nil || !hintedIndexContainsQualifyingRows(index, scan) {
		return false
	}
	position, ok := scan.TableDef.Name2ColIndex[catalog.ResolveAlias(index.Parts[0])]
	if !ok || position < 0 || int(position) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[position] == nil {
		return false
	}
	return atom.column.GetCol().RelPos == scan.BindingTags[0] && atom.column.GetCol().ColPos == position &&
		atom.column.Typ.Id == scan.TableDef.Cols[position].Typ.Id && atom.column.Typ.Scale == scan.TableDef.Cols[position].Typ.Scale
}

func indexResidualColumnsMapped(expr *plan.Expr, mapping map[[2]int32]*plan.Expr) bool {
	if expr == nil {
		return false
	}
	if col := expr.GetCol(); col != nil {
		return mapping[[2]int32{col.RelPos, col.ColPos}] != nil
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if !indexResidualColumnsMapped(arg, mapping) {
				return false
			}
		}
		return true
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if !indexResidualColumnsMapped(item, mapping) {
				return false
			}
		}
		return true
	}
	return expr.GetLit() != nil || expr.GetT() != nil
}

func indexPlanningDefinitionFits(index *plan.IndexDef) bool {
	if index == nil || len(index.Parts) > engine.MaxIndexPlanningParts || len(index.IndexName) > engine.MaxIndexPlanningNameBytes || len(index.IndexTableName) > engine.MaxIndexPlanningNameBytes {
		return false
	}
	for _, part := range index.Parts {
		if len(part) > engine.MaxIndexPlanningNameBytes {
			return false
		}
	}
	return true
}
