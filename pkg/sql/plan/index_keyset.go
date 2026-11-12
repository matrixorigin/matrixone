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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

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
func completeUniqueKeySetFilters(idxDef *IndexDef, node *plan.Node) ([]int32, bool) {
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
		if pos, _, ok := indexKeySetFilter(expr); ok && fn.Args[0].GetCol().RelPos == node.BindingTags[0] {
			sets[pos] = int32(i)
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
	if exact {
		if types.T(leadingColExpr.Typ.Id) != types.T_varchar {
			return nil
		}
		exactKeys = vector.NewVec(makeTypeByPlan2Type(leadingColExpr.Typ))
		defer exactKeys.Free(mp)
	} else {
		tuples = make([]*plan.Expr, inVec.Length())
	}
	serializedKeyBytes := 0
	for row := 0; row < inVec.Length(); row++ {
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
		if err != nil || tuple == nil {
			return nil
		}
		tupleVec := tuple.GetVec()
		if tupleVec == nil {
			return nil
		}
		serializedKeyBytes += len(tupleVec.GetData())
		if serializedKeyBytes > maxCompositePointBytes {
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
