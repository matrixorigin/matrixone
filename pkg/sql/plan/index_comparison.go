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
		key.Oid.IsMySQLString() && value.Oid.IsMySQLString() &&
			!types.IsUnicodeCollation(key.Charset) && !types.IsUnicodeCollation(value.Charset)
}

func analyzeIndexComparisonBound(ctx context.Context, bound *plan.Expr, keyType plan.Type) indexComparisonBound {
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
	if key.Oid.IsDecimal() && value.Oid.IsDecimal() && losslessIndexDecimalBound(ctx, bound, key) {
		return indexComparisonBound{usable: true, normalize: true}
	}
	// In particular, DATETIME/TIMESTAMP comparison converts the DATETIME in
	// the session zone and truncates it to the TIMESTAMP precision. A cast of
	// the bound is not its inverse (precision buckets and DST folds/gaps).
	return indexComparisonBound{}
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
	usable := func(bound *plan.Expr) bool { return analyzeIndexComparisonBound(ctx, bound, keyType).usable }
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
			if !analyzeIndexComparisonBound(ctx, bound, keyType).usable {
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
	return analyzeIndexComparisonBound(ctx, rhs, keyType).usable
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
