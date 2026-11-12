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
	"bytes"
	"hash/maphash"
	"math"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
)

// This is an optimizer work budget, separate from the existing wire-size
// limit. It bounds conservatively charged key encoding work for one scan, not
// the query's total allocation or peak memory.
const (
	maxIndexKeySetPlanningWork uint64 = 1 << 20
	indexKeySetEncodingPasses  uint64 = 3
	indexKeySetTupleOverhead   uint64 = 128
)

type indexKeySetInputBound struct {
	column int32
	count  int
	bytes  uint64
	ok     bool
}

type indexKeySetWorkBudget struct {
	remaining uint64
	inputs    map[*plan.Expr]indexKeySetInputBound
}

func newIndexKeySetWorkBudget() *indexKeySetWorkBudget {
	return &indexKeySetWorkBudget{remaining: maxIndexKeySetPlanningWork}
}

// inputBound is shared by candidates using the same IN filter. Reject a large
// wire input before decoding or hashing it, and count each logical row even
// when the vector stores a single constant value.
func (budget *indexKeySetWorkBudget) inputBound(expr *plan.Expr) indexKeySetInputBound {
	if budget.inputs != nil {
		if bound, ok := budget.inputs[expr]; ok {
			return bound
		}
	} else {
		budget.inputs = make(map[*plan.Expr]indexKeySetInputBound)
	}
	bound := indexKeySetInputBound{}
	defer func() { budget.inputs[expr] = bound }()
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "in" || len(fn.Args) != 2 ||
		fn.Args[0] == nil || fn.Args[1] == nil || fn.Args[1].GetVec() == nil {
		return bound
	}
	data := fn.Args[1].GetVec().GetData()
	if len(data) == 0 || uint64(len(data)) > maxIndexKeySetPlanningWork/indexKeySetEncodingPasses {
		return bound
	}
	column, count, ok := indexKeySetFilter(expr)
	if !ok {
		return bound
	}
	vec := vector.NewVec(makeTypeByPlan2Type(fn.Args[0].Typ))
	if err := vec.UnmarshalBinary(data); err != nil {
		return bound
	}
	var total uint64
	for row := 0; row < count; row++ {
		size, err := function.SerialEncodedValueSizeBound(vec, row)
		if err != nil || size > maxIndexKeySetPlanningWork/indexKeySetEncodingPasses-total {
			return bound
		}
		total += size
		if vec.IsConst() {
			if total > maxIndexKeySetPlanningWork/indexKeySetEncodingPasses/uint64(count) {
				return bound
			}
			total *= uint64(count)
			break
		}
	}
	bound = indexKeySetInputBound{column: column, count: count, bytes: total, ok: true}
	return bound
}

// literalBound follows the literal variant used by the expression executor,
// which can differ from the expression's declared type after parameter binding.
func indexKeySetLiteralBound(expr *plan.Expr) (uint64, bool) {
	lit := expr.GetLit()
	if lit == nil || lit.Isnull || lit.Src != nil {
		return 0, false
	}
	var typ types.T
	switch value := lit.Value.(type) {
	case *plan.Literal_Sval:
		// The executor materializes Sval as bytes even when Typ is numeric.
		if !compositePointType(expr.Typ) {
			return 0, false
		}
		size := uint64(len(value.Sval))
		if size > (maxIndexKeySetPlanningWork-3)/2 {
			return 0, false
		}
		return 2*size + 3, true
	case *plan.Literal_Bval, *plan.Literal_Defaultval:
		typ = types.T_bool
	case *plan.Literal_I8Val:
		typ = types.T_int8
	case *plan.Literal_I16Val:
		typ = types.T_int16
	case *plan.Literal_I32Val:
		typ = types.T_int32
	case *plan.Literal_I64Val:
		typ = types.T_int64
	case *plan.Literal_U8Val:
		typ = types.T_uint8
	case *plan.Literal_U16Val:
		typ = types.T_uint16
	case *plan.Literal_U32Val:
		typ = types.T_uint32
	case *plan.Literal_U64Val:
		typ = types.T_uint64
	case *plan.Literal_Fval:
		typ = types.T_float32
	case *plan.Literal_Dval:
		typ = types.T_float64
	case *plan.Literal_Dateval:
		typ = types.T_date
	case *plan.Literal_Timeval:
		typ = types.T_time
	case *plan.Literal_Datetimeval:
		typ = types.T_datetime
	case *plan.Literal_Timestampval:
		typ = types.T_timestamp
	case *plan.Literal_Decimal64Val:
		typ = types.T_decimal64
	case *plan.Literal_Decimal128Val:
		typ = types.T_decimal128
	case *plan.Literal_EnumVal:
		typ = types.T_enum
	default:
		return 0, false
	}
	// These executor variants use the declared type to allocate their fixed
	// vector; reject inconsistent metadata instead of guessing its layout.
	switch lit.Value.(type) {
	case *plan.Literal_Timeval, *plan.Literal_Datetimeval, *plan.Literal_Decimal64Val, *plan.Literal_Decimal128Val:
		if types.T(expr.Typ.Id) != typ {
			return 0, false
		}
	}
	return function.SerialEncodedTypeSizeBound(typ.ToType())
}

// estimate rejects unproved or dynamic equality operands instead of freezing
// their current values into a key vector. It charges three passes to cover the
// first tuple, a fast attempt, and a possible full folding fallback.
func (budget *indexKeySetWorkBudget) estimate(filters []*plan.Expr, positions []int32) (uint64, bool) {
	var input indexKeySetInputBound
	var equalityBytes uint64
	for _, pos := range positions {
		if pos < 0 || int(pos) >= len(filters) || filters[pos] == nil {
			return 0, false
		}
		fn := filters[pos].GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) != 2 || fn.Args[1] == nil {
			return 0, false
		}
		switch fn.Func.ObjName {
		case "in":
			if input.ok {
				return 0, false
			}
			input = budget.inputBound(filters[pos])
			if !input.ok {
				return 0, false
			}
		case "=":
			value := fn.Args[1]
			if !compositePointScalar(value) || !rule.IsConstant(value, false) {
				return 0, false
			}
			var size uint64
			var ok bool
			if value.GetLit() != nil {
				size, ok = indexKeySetLiteralBound(value)
			} else {
				size, ok = compositePointValueSize(value)
			}
			if !ok || size > maxIndexKeySetPlanningWork/indexKeySetEncodingPasses-equalityBytes {
				return 0, false
			}
			equalityBytes += size
		default:
			return 0, false
		}
	}
	if !input.ok {
		return 0, false
	}
	singlePassLimit := budget.remaining / indexKeySetEncodingPasses
	if singlePassLimit < indexKeySetTupleOverhead || input.bytes > singlePassLimit ||
		equalityBytes > singlePassLimit-indexKeySetTupleOverhead {
		return 0, false
	}
	perRow := equalityBytes + indexKeySetTupleOverhead
	if perRow > (singlePassLimit-input.bytes)/uint64(input.count) {
		return 0, false
	}
	return (input.bytes + perRow*uint64(input.count)) * indexKeySetEncodingPasses, true
}

func (budget *indexKeySetWorkBudget) charge(work uint64) bool {
	if work > budget.remaining {
		return false
	}
	budget.remaining -= work
	return true
}

// revalidate runs after index resolution and before lowering. The resolver can
// change plan expressions, so a previously cached bound must not be reused to
// materialize a different input. The initial reservation is never refunded.
func (budget *indexKeySetWorkBudget) revalidate(filters []*plan.Expr, positions []int32, reserved uint64, stats *plan.Stats, forced bool) bool {
	for _, pos := range positions {
		if pos < 0 || int(pos) >= len(filters) || filters[pos] == nil {
			return false
		}
		delete(budget.inputs, filters[pos])
	}
	if budget.remaining > maxIndexKeySetPlanningWork || reserved > maxIndexKeySetPlanningWork-budget.remaining {
		return false
	}
	remaining := budget.remaining
	budget.remaining += reserved
	actual, ok := budget.estimate(filters, positions)
	budget.remaining = remaining
	if !ok {
		return false
	}
	for _, pos := range positions {
		fn := filters[pos].GetF()
		if fn != nil && fn.Func != nil && fn.Func.ObjName == "in" {
			bound := budget.inputBound(filters[pos])
			if !bound.ok || !indexKeySetWorthwhile(stats, bound.count, forced) {
				return false
			}
		}
	}
	return actual <= reserved || budget.charge(actual-reserved)
}

// Restore plans use DefaultHugeStats as a placeholder, not as a measured
// scan cost. Treat it as unknown for this optional key-set cost decision.
func (builder *QueryBuilder) indexKeySetCostStats(stats *plan.Stats) *plan.Stats {
	if builder.isRestore {
		return nil
	}
	return stats
}

func indexKeySetWorthwhile(stats *plan.Stats, count int, forced bool) bool {
	if forced || count <= 1 {
		return true
	}
	cost := DefaultStats().Cost
	if hasKnownScanCardinality(stats) && stats.Cost > 0 &&
		!math.IsNaN(stats.Cost) && !math.IsInf(stats.Cost, 0) {
		cost = stats.Cost
	}
	return float64(count) <= cost*0.1
}

// indexKeySetFilter recognizes a constant, non-NULL IN list that can be one
// dimension of a composite-key point set. The other key parts must be equality
// bound. Multiple set dimensions are left to the ordinary range/scan paths
// instead of being silently zipped into incorrect tuples.
func indexKeySetFilter(expr *plan.Expr) (int32, int, bool) {
	fn := expr.GetF()
	if fn == nil || fn.Func.ObjName != "in" || len(fn.Args) != 2 {
		return 0, 0, false
	}
	colExpr := fn.Args[0]
	col := colExpr.GetCol()
	if col == nil || isFloatIndexFilterExpr(colExpr) || !compositePointType(colExpr.Typ) {
		return 0, 0, false
	}
	vecExpr := fn.Args[1]
	if vecExpr.GetVec() == nil {
		return 0, 0, false
	}
	data := vecExpr.GetVec().GetData()
	if len(data) == 0 || len(data) > maxCompositePointBytes {
		return 0, 0, false
	}
	// The plan wrapper for an IN list has an internal vector type. Decode the
	// payload as the compared column's element type instead of rejecting that
	// wrapper as an unsupported scalar key type.
	expectedType := makeTypeByPlan2Type(colExpr.Typ)
	vec := vector.NewVec(expectedType)
	if err := vec.UnmarshalBinary(data); err != nil {
		return 0, 0, false
	}
	actualType := vec.GetType()
	if actualType.Oid != expectedType.Oid || actualType.Scale != expectedType.Scale {
		return 0, 0, false
	}
	if (actualType.Oid == types.T_char || actualType.Oid == types.T_bit) && actualType.Width != expectedType.Width {
		return 0, 0, false
	}
	if vec.Length() < 1 || vec.Length() > maxCompositePoints || vec.HasNull() {
		return 0, 0, false
	}
	return col.ColPos, vec.Length(), true
}

// distinctKeySetComponentCount counts exact serial encodings of the one IN
// component without constructing full composite keys. For a complete unique
// key with all other parts fixed, serial concatenates self-delimiting component
// encodings; therefore two tuples are equal exactly when their IN component
// encodings are equal, independent of the component's position.
//
// The key-set classifier excludes floats and other domains whose SQL equality
// does not match their serialized representation. For string-like values,
// serial uses EncodeStringType, whose zero escaping and terminator make it
// injective on the already-bound bytes. Hash collisions are resolved by a byte
// comparison against the borrowed input, so no full values are copied. Fixed
// types use the same component encoder as serial, including decimals and time.
func distinctKeySetComponentCount(expr *plan.Expr) (int, bool) {
	if _, _, ok := indexKeySetFilter(expr); !ok {
		return 0, false
	}
	fn := expr.GetF()
	vec := vector.NewVec(makeTypeByPlan2Type(fn.Args[0].Typ))
	if err := vec.UnmarshalBinary(fn.Args[1].GetVec().Data); err != nil {
		return 0, false
	}
	if vec.IsConst() {
		return 1, true
	}
	switch vec.GetType().Oid {
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary:
		seed := maphash.MakeSeed()
		seen := make(map[uint64]int, vec.Length())
		next := make([]int, vec.Length())
		distinct := 0
		for row := 0; row < vec.Length(); row++ {
			value := vec.GetBytesAt(row)
			hash := maphash.Bytes(seed, value)
			duplicate := false
			for link := seen[hash]; link != 0; link = next[link-1] {
				if bytes.Equal(value, vec.GetBytesAt(link-1)) {
					duplicate = true
					break
				}
			}
			if !duplicate {
				next[row] = seen[hash]
				seen[hash] = row + 1
				distinct++
			}
		}
		return distinct, true
	default:
		encode, err := function.NewSerialValueEncoder(vec)
		if err != nil {
			return 0, false
		}
		packer := types.NewPacker()
		defer packer.Close()
		seen := make(map[string]struct{}, vec.Length())
		for row := 0; row < vec.Length(); row++ {
			packer.Reset()
			encode(vec, row, packer)
			if packer.Err() != nil {
				return 0, false
			}
			seen[string(packer.GetBuf())] = struct{}{}
		}
		return len(seen), true
	}
}

// keySetIndexSupported rejects truncated index keys. Index writes serialize the
// prefix rather than the original SQL value, so probing with a full value can
// miss rows. Malformed metadata is also unsafe to use for this optimization.
func keySetIndexSupported(idxDef *IndexDef) bool {
	prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
	if err != nil {
		return false
	}
	for _, part := range idxDef.Parts {
		if prefixLengths[catalog.ResolveAlias(part)] > 0 {
			return false
		}
	}
	return true
}

// completeUniqueKeySetFilters finds one IN dimension and equality predicates
// for every other user part. The returned positions still need lowering before
// their cardinality can be used as an exact index-candidate bound.
func completeUniqueKeySetFilters(idxDef *IndexDef, node *plan.Node, budget *indexKeySetWorkBudget) ([]int32, bool) {
	if idxDef == nil || !idxDef.Unique || len(idxDef.Parts) < 2 ||
		node == nil || node.TableDef == nil || len(node.BindingTags) == 0 || !keySetIndexSupported(idxDef) {
		return nil, false
	}
	equalities := make(map[int32]int32)
	sets := make(map[int32]int32)
	for i, expr := range node.FilterList {
		fn := expr.GetF()
		if !hasNonNilFunctionArgs(fn, 2) {
			continue
		}
		kind, col := checkIndexFilter(fn)
		if kind == EqualIndexCondition && col.RelPos == node.BindingTags[0] {
			equalities[col.ColPos] = int32(i)
			continue
		}
		if fn.Func.ObjName == "in" {
			if bound := budget.inputBound(expr); bound.ok && fn.Args[0].GetCol().RelPos == node.BindingTags[0] {
				sets[bound.column] = int32(i)
			}
		}
	}
	filters := make([]int32, 0, len(idxDef.Parts))
	inCount := 0
	for _, part := range idxDef.Parts {
		pos, ok := node.TableDef.Name2ColIndex[catalog.ResolveAlias(part)]
		if !ok {
			return nil, false
		}
		if filter, ok := equalities[pos]; ok {
			filters = append(filters, filter)
		} else if filter, ok := sets[pos]; ok {
			filters = append(filters, filter)
			inCount++
		} else {
			return nil, false
		}
	}
	return filters, inCount == 1
}

// exactUniqueKeySetSize accepts only the completed IN vector produced by
// replaceKeySetCondition. UnmarshalBinary borrows the immutable plan bytes.
func exactUniqueKeySetSize(expr *plan.Expr) (int, bool) {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "in" || len(fn.Args) != 2 || fn.Args[1].GetVec() == nil {
		return 0, false
	}
	literal := fn.Args[1].GetVec()
	if len(literal.Data) == 0 || len(literal.Data) > maxCompositePointBytes {
		return 0, false
	}
	keys := vector.NewVec(types.T_any.ToType())
	if err := keys.UnmarshalBinary(literal.Data); err != nil || keys.GetType().Oid != types.T_varchar ||
		keys.IsConst() || keys.HasNull() || !keys.GetSorted() || keys.Length() < 1 ||
		keys.Length() > maxCompositePoints || int32(keys.Length()) != literal.Len {
		return 0, false
	}
	return keys.Length(), true
}

// marshalKeySetElement builds an independent one-row vector. Window borrows the
// entire variable-length payload, and marshaling it for each row would copy the
// whole IN list repeatedly. scratch owns only the selected row and is reused.
func marshalKeySetElement(src, scratch *vector.Vector, row int, mp *mpool.MPool) ([]byte, error) {
	scratch.ResetWithSameType()
	if err := scratch.UnionOne(src, int64(row), mp); err != nil {
		return nil, err
	}
	return scratch.MarshalBinary()
}

// foldKeySetTuple retains the original per-row bind and constant-fold path.
func (builder *QueryBuilder) foldKeySetTuple(
	idxDef *IndexDef, filterList []*plan.Expr, filterPos []int32,
	inVec, rowVec *vector.Vector, row int, mp *mpool.MPool,
) *plan.Expr {
	data, err := marshalKeySetElement(inVec, rowVec, row, mp)
	if err != nil {
		return nil
	}
	serialArgs := make([]*plan.Expr, len(filterPos))
	for i, pos := range filterPos {
		fn := filterList[pos].GetF()
		if fn.Func.ObjName == "in" {
			serialArgs[i] = &plan.Expr{
				Typ: fn.Args[0].Typ,
				Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{
					Len: int32(len(data)), Data: data,
				}},
			}
		} else {
			serialArgs[i] = DeepCopyExpr(fn.Args[1])
		}
	}
	tuple, err := bindFuncExprAndConstFold(
		builder.GetContext(), builder.compCtx.GetProcess(),
		indexTableLookupSerialFunc(idxDef), serialArgs,
	)
	if err != nil {
		return nil
	}
	return tuple
}

// addKeySetBytes enforces the pre-compaction wire-size budget without integer
// overflow. The budget counts every input tuple, including duplicates.
func addKeySetBytes(total, size int) (int, bool) {
	if total < 0 || size < 0 || total > maxCompositePointBytes || size > maxCompositePointBytes-total {
		return 0, false
	}
	return total + size, true
}

// fastExactKeySet constructs complete unique keys using the same component
// encoders as serial. It first folds the original first tuple and checks both
// its bytes and its serialized vector size against the direct path. Literal
// equality operands are evaluated to their actual runtime vector types, not
// their declared column types. Unproved inputs or errors return false so the
// original per-row folding path remains authoritative.
//
// A true result transfers a non-nil vector to the caller, or a nil vector when
// the original raw per-tuple wire budget is exceeded. The returned vector and
// all temporary vectors are owned by this call until that transfer.
func (builder *QueryBuilder) fastExactKeySet(
	idxDef *IndexDef, filterList []*plan.Expr, filterPos []int32,
	inVec, rowVec *vector.Vector, leadingType types.Type, mp *mpool.MPool,
) (*vector.Vector, bool) {
	for _, pos := range filterPos {
		fn := filterList[pos].GetF()
		if fn.Func.ObjName != "=" {
			continue
		}
		rhs := fn.Args[1]
		lit := rhs.GetLit()
		if lit == nil || lit.Isnull || lit.Src != nil || !compositePointType(rhs.Typ) {
			return nil, false
		}
	}
	first := builder.foldKeySetTuple(idxDef, filterList, filterPos, inVec, rowVec, 0, mp)
	if first == nil || first.GetVec() == nil {
		return nil, false
	}
	firstData := first.GetVec().GetData()
	if _, ok := addKeySetBytes(0, len(firstData)); !ok {
		return nil, true
	}
	firstVec := vector.NewVec(types.T_any.ToType())
	if err := firstVec.UnmarshalBinary(firstData); err != nil ||
		firstVec.GetType().Oid != types.T_varchar || firstVec.Length() != 1 || firstVec.IsNull(0) {
		return nil, false
	}
	firstKey := firstVec.GetBytesAt(0)
	if len(firstKey) == 0 {
		return nil, false
	}

	vecs := make([]*vector.Vector, len(filterPos))
	encoders := make([]function.SerialValueEncoder, len(filterPos))
	freeFns := make([]func(), 0, len(filterPos)-1)
	defer func() {
		for _, free := range freeFns {
			free()
		}
	}()
	for i, pos := range filterPos {
		fn := filterList[pos].GetF()
		if fn.Func.ObjName == "in" {
			vecs[i] = inVec
		} else {
			var err error
			var free func()
			vecs[i], free, err = colexec.GetReadonlyResultFromNoColumnExpression(builder.compCtx.GetProcess(), fn.Args[1])
			if err != nil {
				return nil, false
			}
			freeFns = append(freeFns, free)
			if vecs[i] == nil || vecs[i].Length() != 1 || vecs[i].IsNull(0) {
				return nil, false
			}
		}
		var err error
		encoders[i], err = function.NewSerialValueEncoder(vecs[i])
		if err != nil {
			return nil, false
		}
	}

	keys := vector.NewVec(leadingType)
	transfer := false
	defer func() {
		if !transfer {
			keys.Free(mp)
		}
	}()
	wire := vector.NewVec(*firstVec.GetType())
	defer wire.Free(mp)
	packer := types.NewPacker()
	defer packer.Close()

	// Byte equality is exactly serial equality for these types: serial's
	// escaped, terminated encoding is injective. Hash collisions are checked
	// against the immutable borrowed input. Fixed types use the encoders for
	// every row instead of relying on their in-memory representations.
	var seen map[uint64]int
	var next, sizes []int
	var seed maphash.Seed
	switch inVec.GetType().Oid {
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary:
		seed = maphash.MakeSeed()
		seen = make(map[uint64]int, inVec.Length())
		next = make([]int, inVec.Length())
		sizes = make([]int, inVec.Length())
	}
	serialized := 0
	for row := 0; row < inVec.Length(); row++ {
		var hash uint64
		if seen != nil {
			value := inVec.GetBytesAt(row)
			hash = maphash.Bytes(seed, value)
			duplicate := false
			for link := seen[hash]; link != 0; link = next[link-1] {
				if bytes.Equal(value, inVec.GetBytesAt(link-1)) {
					var ok bool
					serialized, ok = addKeySetBytes(serialized, sizes[link-1])
					if !ok {
						return nil, true
					}
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
		}
		packer.Reset()
		for i, vec := range vecs {
			encodeRow := 0
			if vec == inVec {
				encodeRow = row
			}
			encoders[i](vec, encodeRow, packer)
		}
		if packer.Err() != nil {
			return nil, false
		}
		value := packer.GetBuf()
		if len(value) == 0 || (row == 0 && !bytes.Equal(value, firstKey)) {
			return nil, false
		}
		// No single tuple with a key larger than the remaining budget can fit.
		if len(value) > maxCompositePointBytes-serialized {
			return nil, true
		}
		wire.ResetWithSameType()
		if err := vector.AppendBytes(wire, value, false, mp); err != nil {
			return nil, false
		}
		size, err := wire.MarshalBinarySize()
		if err != nil || (row == 0 && size != len(firstData)) {
			return nil, false
		}
		var ok bool
		serialized, ok = addKeySetBytes(serialized, size)
		if !ok {
			return nil, true
		}
		if err := vector.AppendBytes(keys, value, false, mp); err != nil {
			return nil, false
		}
		if seen != nil {
			sizes[row] = size
			next[row] = seen[hash]
			seen[hash] = row + 1
		}
	}
	transfer = true
	return keys, true
}

// replaceKeySetCondition lowers equality-bound key parts plus one IN dimension
// to the existing serialized PREFIX_IN access primitive. Each tuple is folded
// separately: serializing a scalar equality vector together with an N-element
// IN vector would otherwise produce only one tuple instead of N tuples.
func (builder *QueryBuilder) replaceKeySetCondition(
	idxDef *IndexDef,
	filterList []*plan.Expr,
	filterPos []int32,
	idxTag int32,
	idxTableDef *TableDef,
) *plan.Expr {
	if len(filterPos) < 2 {
		return nil
	}
	compositeFilterSel := 1.0
	var inVec *vector.Vector
	for _, pos := range filterPos {
		filter := filterList[pos]
		fn := filter.GetF()
		if fn == nil || len(fn.Args) != 2 {
			return nil
		}
		switch fn.Func.ObjName {
		case "=":
		case "in":
			if inVec != nil {
				return nil
			}
			if _, _, ok := indexKeySetFilter(filter); !ok {
				return nil
			}
			inVec = vector.NewVec(types.T(fn.Args[0].Typ.Id).ToType())
			if err := inVec.UnmarshalBinary(fn.Args[1].GetVec().GetData()); err != nil {
				return nil
			}
		default:
			return nil
		}
		compositeFilterSel *= filter.Selectivity
	}
	if inVec == nil || builder.compCtx.GetProcess() == nil {
		return nil
	}

	mp := builder.compCtx.GetProcess().Mp()
	rowVec := vector.NewVec(*inVec.GetType())
	defer rowVec.Free(mp)
	leadingColExpr := GetColExpr(idxTableDef.Cols[0].Typ, idxTag, 0)
	// A complete unique-index key is stored as serial(all user parts), with
	// no primary-key suffix. Probe it by exact bytes: prefix matching can also
	// admit a different string whose next byte is an escaped NUL.
	exact := idxDef.Unique && len(filterPos) == len(idxDef.Parts)
	var exactKeys *vector.Vector
	var tuples []*plan.Expr
	prefilled := false
	if exact {
		if types.T(leadingColExpr.Typ.Id) != types.T_varchar {
			return nil
		}
		keyType := makeTypeByPlan2Type(leadingColExpr.Typ)
		if inVec.Length() >= 8 {
			var handled bool
			exactKeys, handled = builder.fastExactKeySet(idxDef, filterList, filterPos, inVec, rowVec, keyType, mp)
			if handled {
				if exactKeys == nil {
					return nil
				}
				prefilled = true
			}
		}
		if exactKeys == nil {
			exactKeys = vector.NewVec(keyType)
		}
		defer exactKeys.Free(mp)
	} else {
		tuples = make([]*plan.Expr, inVec.Length())
	}
	serializedKeyBytes := 0
	for row := 0; !prefilled && row < inVec.Length(); row++ {
		tuple := builder.foldKeySetTuple(idxDef, filterList, filterPos, inVec, rowVec, row, mp)
		if tuple == nil || tuple.GetVec() == nil {
			return nil
		}
		tupleVec := tuple.GetVec()
		var ok bool
		serializedKeyBytes, ok = addKeySetBytes(serializedKeyBytes, len(tupleVec.GetData()))
		if !ok {
			return nil
		}
		if exact {
			// UnmarshalBinary borrows the expression's bytes. AppendBytes copies
			// the one value into our owned vector before either can be released.
			key := vector.NewVec(types.T_any.ToType())
			if err := key.UnmarshalBinary(tupleVec.GetData()); err != nil ||
				key.GetType().Oid != types.T_varchar || key.Length() != 1 || key.IsNull(0) {
				return nil
			}
			value := key.GetBytesAt(0)
			if len(value) == 0 {
				return nil
			}
			if err := vector.AppendBytes(exactKeys, value, false, mp); err != nil {
				return nil
			}
		} else {
			tuples[row] = tuple
		}
	}
	var rightArg *plan.Expr
	funcName := "prefix_in"
	if exact {
		exactKeys.InplaceSortAndCompact()
		data, err := exactKeys.MarshalBinary()
		if err != nil || exactKeys.Length() == 0 || len(data) > maxCompositePointBytes {
			return nil
		}
		rightArg = &plan.Expr{Typ: leadingColExpr.Typ, Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{
			Len: int32(exactKeys.Length()), Data: data,
		}}}
		funcName = "in"
	} else {
		rightArg = &plan.Expr{Typ: leadingColExpr.Typ, Expr: &plan.Expr_List{List: &plan.ExprList{List: tuples}}}
	}
	expr, err := bindFuncExprAndConstFold(
		builder.GetContext(), builder.compCtx.GetProcess(), funcName,
		[]*plan.Expr{leadingColExpr, rightArg},
	)
	if err != nil || expr == nil {
		return nil
	}
	if exact {
		if _, ok := exactUniqueKeySetSize(expr); !ok {
			return nil
		}
	}
	expr.Selectivity = compositeFilterSel
	return expr
}
