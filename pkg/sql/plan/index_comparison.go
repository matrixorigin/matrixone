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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// indexComparisonBound describes the only conversion allowed when a SQL
// comparison operand becomes a serialized key component. A residual cannot
// repair an under-fetch, so an unproved conversion declines the access path.
type indexComparisonBound struct {
	usable    bool
	normalize bool
}

func sameIndexComparisonDomain(key, value types.Type) bool {
	// CHAR IN compares PAD SPACE keys, whereas SERIAL preserves bytes. The
	// raw-vector runtime-filter contract invokes that IN kernel and therefore
	// does not prove that a byte predicate over a persisted CHAR key is safe.
	if key.Oid == types.T_char || value.Oid == types.T_char {
		return false
	}
	return keycodec.SupportsExactRawRuntimeFilterPair(key, value) ||
		key.Oid.IsMySQLString() && value.Oid.IsMySQLString()
}

func analyzeIndexComparisonBound(bound *plan.Expr, keyType plan.Type) indexComparisonBound {
	if bound == nil || !isRuntimeConstExpr(bound) {
		return indexComparisonBound{}
	}
	if lit := bound.GetLit(); lit != nil && lit.Isnull {
		// SERIAL propagates NULL, as SQL comparisons do. Stored SERIAL_FULL
		// keys can still require a decoded residual; the lookup owns that rule.
		return indexComparisonBound{usable: true}
	}
	key, value := makeTypeByPlan2Type(keyType), makeTypeByPlan2Expr(bound)
	if sameIndexComparisonDomain(key, value) {
		return indexComparisonBound{usable: true}
	}
	if key.Oid.IsDecimal() && value.Oid.IsDecimal() && losslessIndexDecimalBound(bound, key) {
		return indexComparisonBound{usable: true, normalize: true}
	}
	// In particular, DATETIME/TIMESTAMP comparison converts the DATETIME in
	// the session zone and truncates it to the TIMESTAMP precision. A cast of
	// the bound is not its inverse (precision buckets and DST folds/gaps).
	return indexComparisonBound{}
}

// losslessIndexDecimalBound proves both exact value preservation and the
// target precision. Assignment casts alone can round or accept -10^width.
func losslessIndexDecimalBound(bound *plan.Expr, key types.Type) bool {
	if bound == nil || key.Width <= 0 || key.Width > 76 || key.Scale < 0 || key.Scale > key.Width {
		return false
	}
	value, ok := decimalLiteralCoefficient(bound)
	if !ok {
		return false
	}
	sourceScale := bound.Typ.Scale
	if sourceScale < 0 || sourceScale > 76 {
		return false
	}
	if sourceScale > key.Scale {
		zero, proved := decimal256TrailingZerosStatus(value, sourceScale-key.Scale)
		if !proved || !zero {
			return false
		}
	}
	scaled, err := value.Scale(key.Scale - sourceScale)
	if err != nil || scaled.Sign() != value.Sign() && !decimal256IsZero(scaled) {
		return false
	}
	return decimal256FitsPrecision(scaled, key.Width)
}

// normalizeSerializedIndexComparison is also used at construction, so a
// caller cannot bypass admission and serialize an incompatible or unnormalized
// operand. SQL filters remain untouched when optional binding fails.
func normalizeSerializedIndexComparison(ctx context.Context, filter *plan.Expr, keyType plan.Type) (*plan.Expr, error) {
	if filter == nil || !serializedIndexComparisonUsable(filter.GetF(), keyType) {
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
	decision := analyzeIndexComparisonBound(bound, keyType)
	if !decision.usable {
		return nil, moerr.NewInternalError(ctx, "unsupported serialized index comparison domain")
	}
	if decision.normalize {
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
func serializedIndexComparisonUsable(fn *plan.Function, keyType plan.Type) bool {
	if fn == nil || fn.Func == nil {
		return false
	}
	if fn.Func.ObjName == "or" {
		if len(fn.Args) == 0 {
			return false
		}
		for _, arm := range fn.Args {
			if arm == nil || !serializedIndexComparisonUsable(arm.GetF(), keyType) {
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
	usable := func(bound *plan.Expr) bool { return analyzeIndexComparisonBound(bound, keyType).usable }
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
		return serializedIndexInUsable(fn.Args[1], keyType)
	default:
		return false
	}
}

func serializedIndexInUsable(rhs *plan.Expr, keyType plan.Type) (usable bool) {
	// The 4.2 vector decoder assumes trusted framing; optional access must
	// decline malformed plan payloads rather than panic during optimization.
	defer func() {
		if recover() != nil {
			usable = false
		}
	}()
	if rhs == nil {
		return false
	}
	if list := rhs.GetList(); list != nil {
		for _, bound := range list.List {
			if !analyzeIndexComparisonBound(bound, keyType).usable {
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
	return analyzeIndexComparisonBound(rhs, keyType).usable
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

func regularIndexEqualityUsable(idxDef *IndexDef, table *plan.TableDef, fn *plan.Function, part int) bool {
	if !indexTableStoresSerializedKey(idxDef) {
		return true // Keep the original typed scalar comparison on a direct key.
	}
	if part < 0 || part >= len(idxDef.Parts) {
		return false
	}
	pos, ok := table.Name2ColIndex[catalog.ResolveAlias(idxDef.Parts[part])]
	return ok && pos >= 0 && int(pos) < len(table.Cols) &&
		serializedIndexComparisonUsable(fn, table.Cols[pos].Typ)
}

func compositeKeyComparisonUsable(fn *plan.Function, table *plan.TableDef) bool {
	if fn != nil && fn.Func != nil && (fn.Func.ObjName == "or" || fn.Func.ObjName == "and") {
		// Each recursively lowered arm/conjunct owns its component proof.
		return true
	}
	column := indexComparisonColumn(fn)
	if column == nil || column.GetCol().ColPos < 0 || int(column.GetCol().ColPos) >= len(table.Cols) {
		return false
	}
	return serializedIndexComparisonUsable(fn, table.Cols[column.GetCol().ColPos].Typ)
}

// normalizeIndexAccessFilters constructs physical access predicates before any
// scan or column mapping is published. Original SQL predicates remain intact.
func normalizeIndexAccessFilters(
	ctx context.Context, idx *IndexDef, table *plan.TableDef,
	filters []*plan.Expr, positions []int32, pairedRange bool,
) ([]*plan.Expr, bool) {
	if idx == nil || table == nil {
		return nil, false
	}
	if !indexTableStoresSerializedKey(idx) {
		return filters, true
	}
	out := make([]*plan.Expr, len(filters))
	copy(out, filters)
	for i, pos := range positions {
		part := i
		if pairedRange {
			part = 0
		}
		if pos < 0 || int(pos) >= len(filters) || part >= len(idx.Parts) {
			return nil, false
		}
		colPos, ok := table.Name2ColIndex[catalog.ResolveAlias(idx.Parts[part])]
		if !ok || colPos < 0 || int(colPos) >= len(table.Cols) || table.Cols[colPos] == nil {
			return nil, false
		}
		normalized, err := normalizeSerializedIndexComparison(ctx, filters[pos], table.Cols[colPos].Typ)
		if err != nil {
			propagateKeySetControlError(ctx, err)
			return nil, false
		}
		out[pos] = normalized
	}
	return out, true
}

// Prefix encodings can compare equal to an escaped-NUL peer. Widen open
// lower bounds and retain the original predicate as a decoded residual.
func indexFunctionComparesByteStringColumn(fn *plan.Function) bool {
	if fn == nil {
		return false
	}
	for _, arg := range fn.Args {
		if arg.GetCol() != nil && types.T(arg.Typ.Id).IsMySQLString() {
			return true
		}
	}
	return false
}

func closePrefixRangeLowerBound(flag *plan.Expr) (*plan.Expr, bool) {
	if flag == nil || flag.GetLit() == nil || flag.GetLit().Isnull {
		return nil, false
	}
	value, ok := flag.GetLit().Value.(*plan.Literal_U8Val)
	if !ok || value.U8Val > 3 {
		return nil, false
	}
	if value.U8Val&1 == 0 {
		return flag, true
	}
	return MakePlan2Uint8ConstExprWithType(uint8(value.U8Val) &^ 1), true
}

func indexLookupUsesPrefixComparison(expr *plan.Expr) bool {
	if expr == nil || expr.GetF() == nil || expr.GetF().Func == nil {
		return false
	}
	fn := expr.GetF()
	switch fn.Func.ObjName {
	case "prefix_eq", "prefix_in", "prefix_between", "prefix_in_range":
		return true
	}
	for _, arg := range fn.Args {
		if indexLookupUsesPrefixComparison(arg) {
			return true
		}
	}
	return false
}

func indexFilterUsesStringColumn(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetCol() != nil && types.T(expr.Typ.Id).IsMySQLString() {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if indexFilterUsesStringColumn(arg) {
				return true
			}
		}
	}
	return false
}
