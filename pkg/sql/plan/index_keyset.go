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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
	if inVec == nil {
		return nil
	}

	leadingColExpr := GetColExpr(idxTableDef.Cols[0].Typ, idxTag, 0)
	tuples := make([]*plan.Expr, inVec.Length())
	serializedKeyBytes := 0
	for row := 0; row < inVec.Length(); row++ {
		rowVec, err := inVec.Window(row, row+1)
		if err != nil {
			return nil
		}
		data, err := rowVec.MarshalBinary()
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
		tuples[row], err = bindFuncExprAndConstFold(
			builder.GetContext(), builder.compCtx.GetProcess(),
			indexTableLookupSerialFunc(idxDef), serialArgs,
		)
		if err != nil {
			return nil
		}
		tupleVec := tuples[row].GetVec()
		if tupleVec == nil {
			return nil
		}
		serializedKeyBytes += len(tupleVec.GetData())
		if serializedKeyBytes > maxCompositePointBytes {
			return nil
		}
	}
	rightArg := &plan.Expr{
		Typ:  leadingColExpr.Typ,
		Expr: &plan.Expr_List{List: &plan.ExprList{List: tuples}},
	}
	expr, err := bindFuncExprAndConstFold(
		builder.GetContext(), builder.compCtx.GetProcess(), "prefix_in",
		[]*plan.Expr{leadingColExpr, rightArg},
	)
	if err != nil {
		return nil
	}
	expr.Selectivity = compositeFilterSel
	return expr
}
