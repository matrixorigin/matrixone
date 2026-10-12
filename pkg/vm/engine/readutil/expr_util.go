// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package readutil

import (
	"bytes"
	"context"
	"strings"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

func NewColumnExpr(pos int, typ plan.Type, name string) *plan.Expr {
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				Name:   name,
				ColPos: int32(pos),
			},
		},
	}
}

// ConstructInExpr builds `colName IN (<colVec>)`.
//
// The payload is published ordered and flagged, because zone-map pruning
// binary-searches it (ZM.AnyIn) and drops blocks that hold matching rows when the
// order is wrong -- and because colexec refuses to prune on a payload whose order
// it cannot establish, so an unflagged one silently loses block filtering.
//
// The caller's vector is never modified. Callers pass vectors they use
// positionally elsewhere: disttae's transfer pairs searchPKColumn with
// searchEntryPos and searchBatPos by index, so sorting it in place would
// mis-associate rows. Normalising a private copy costs one clone per scan, not
// per block.
//
// Errors are returned rather than swallowed. Publishing a filter built from a
// payload that failed to encode would prune against garbage, and silently
// skipping normalisation would cost pruning with no signal that anything went
// wrong.
func ConstructInExpr(
	ctx context.Context,
	colName string,
	colVec *vector.Vector,
) (*plan.Expr, error) {
	data, err := colVec.MarshalBinary()
	if err != nil {
		return nil, err
	}
	length := colVec.Length()
	if !colVec.GetSorted() && length > 1 {
		if data, length, err = normalizeInPayload(data); err != nil {
			return nil, err
		}
	}
	colExpr := NewColumnExpr(0, plan2.MakePlan2Type(colVec.GetType()), colName)
	return plan2.MakeInExpr(
		ctx,
		colExpr,
		int32(length),
		data,
		false,
	), nil
}

// normalizeInPayload re-encodes an IN payload in ascending order with the sorted
// flag set, so zone-map pruning can binary-search it.
//
// It decodes into its own vector rather than sorting the caller's: callers pass
// vectors they use positionally elsewhere, and reordering one in place would
// mis-associate rows against its parallel arrays.
//
// A payload carrying NULLs is returned unchanged. InplaceSortAndCompact permutes
// only the value column, so the null bitmap would be left indexing the wrong rows,
// and compaction rebuilds the vector with a nil bitmap, dropping the NULLs
// outright; planner constant folding and normalizePKInVector both sidestep it the
// same way. Publishing that payload unsorted and unflagged costs nothing beyond
// pruning, because zone-map filtering refuses to binary-search an unflagged
// payload and keeps the block. Today's callers all pass PK-derived vectors, which
// cannot be NULL -- the guard is here so a future one cannot silently publish a
// corrupted filter.
func normalizeInPayload(data []byte) ([]byte, int, error) {
	owned := vector.NewVec(types.T_any.ToType())
	if err := owned.UnmarshalBinary(bytes.Clone(data)); err != nil {
		return nil, 0, err
	}
	if owned.GetNulls().Any() {
		return data, owned.Length(), nil
	}
	owned.InplaceSortAndCompact() // also sets the sorted flag
	// Returned directly rather than branched on: there is nothing to unwind here,
	// and the caller discards the length whenever the error is non-nil.
	sorted, err := owned.MarshalBinary()
	return sorted, owned.Length(), err
}

func getColDefByName(expr *plan.Expr, name string, colPos int32, tableDef *plan.TableDef) *plan.ColDef {
	idx := strings.Index(name, ".")
	var pos int32
	if idx >= 0 {
		subName := name[idx+1:]
		pos = tableDef.Name2ColIndex[subName]
	} else {
		pos = tableDef.Name2ColIndex[name]
	}
	common.DoIfDebugEnabled(func() {
		// ColPos is local to the scan (and can be a metadata-only slot),
		// while tableDef is the full relation schema. Validate the name used
		// for resolution instead of indexing this schema with ColPos.
		if int(pos) >= len(tableDef.Cols) || tableDef.Cols[pos].Name != name[strings.LastIndexByte(name, '.')+1:] {
			logutil.Error(
				"Bad-ColExpr",
				zap.String("col-name", name),
				zap.Int32("scan-col-pos", colPos),
				zap.Int32("relation-col-pos", pos),
				zap.String("col-expr", plan2.FormatExpr(expr, plan2.FormatOption{})),
			)
		}
	})
	return tableDef.Cols[pos]
}

func compPkCol(colName string, pkName string) bool {
	dotIdx := strings.Index(colName, ".")
	colName = colName[dotIdx+1:]
	return colName == pkName
}

func evalValue(
	expr *plan.Expr,
	exprImpl *plan.Expr_F,
	tblDef *plan.TableDef,
	isVec bool,
	pkName string,
) (
	ok bool, oid types.T, vals [][]byte,
) {
	var val []byte
	var col *plan.Expr_Col
	var valExprs []*plan.Expr

	if !isVec {
		col, vals, valExprs, ok = mustColConstValueWithTypeFromBinaryFuncExpr(exprImpl)
	} else {
		col, val, ok = mustColVecValueFromBinaryFuncExpr(exprImpl)
	}

	if !ok {
		return false, 0, nil
	}

	colName := col.Col.Name

	common.DoIfDebugEnabled(func() {
		if colName == "" {
			logutil.Error(
				"Bad-ColExpr",
				zap.String("col-name", colName),
				zap.String("pk-name", pkName),
				zap.String("col-expr", plan2.FormatExpr(expr, plan2.FormatOption{})),
			)
		}
	})
	if !compPkCol(colName, pkName) {
		return false, 0, nil
	}

	var (
		colPos int32
		idx    = strings.Index(colName, ".")
	)
	if idx == -1 {
		colPos = tblDef.Name2ColIndex[colName]
	} else {
		colPos = tblDef.Name2ColIndex[colName[idx+1:]]
	}

	common.DoIfDebugEnabled(func() {
		if colPos != col.Col.ColPos {
			logutil.Error(
				"Bad-ColExpr",
				zap.String("col-name", colName),
				zap.Int32("col-actual-pos", col.Col.ColPos),
				zap.Int32("col-expected-pos", colPos),
				zap.String("col-expr", plan2.FormatExpr(expr, plan2.FormatOption{})),
			)
		}
	})

	if isVec {
		return true, types.T(tblDef.Cols[colPos].Typ.Id), [][]byte{val}
	}
	if mixedTemporalColumnAndValues(types.T(tblDef.Cols[colPos].Typ.Id), valExprs) {
		return false, 0, nil
	}
	return true, types.T(tblDef.Cols[colPos].Typ.Id), vals
}

// A BasePKFilter compares persisted primary-key bytes directly. DATETIME and
// TIMESTAMP use different physical domains, so a cross-typed scalar predicate
// cannot be represented without the session time zone. Keep it on the residual
// expression path instead of constructing a filter that can drop matching rows.
func mixedTemporalColumnAndValues(
	columnType types.T,
	values []*plan.Expr,
) bool {
	for _, value := range values {
		valueType := types.T(value.Typ.Id)
		if columnType == types.T_datetime && valueType == types.T_timestamp ||
			columnType == types.T_timestamp && valueType == types.T_datetime {
			return true
		}
	}
	return false
}

func mustColConstValueFromBinaryFuncExpr(
	expr *plan.Expr_F,
) (*plan.Expr_Col, [][]byte, bool) {
	colExpr, vals, _, ok := mustColConstValueWithTypeFromBinaryFuncExpr(expr)
	return colExpr, vals, ok
}

func mustColConstValueWithTypeFromBinaryFuncExpr(
	expr *plan.Expr_F,
) (*plan.Expr_Col, [][]byte, []*plan.Expr, bool) {
	var (
		colExpr  *plan.Expr_Col
		tmpExpr  *plan.Expr_Col
		valExprs []*plan.Expr
		ok       bool
	)

	for idx := range expr.F.Args {
		if tmpExpr, ok = expr.F.Args[idx].Expr.(*plan.Expr_Col); !ok {
			valExprs = append(valExprs, expr.F.Args[idx])
		} else {
			colExpr = tmpExpr
		}
	}

	if len(valExprs) == 0 || colExpr == nil {
		return nil, nil, nil, false
	}

	vals, ok := getConstBytesFromExpr(valExprs)
	if !ok {
		return nil, nil, nil, false
	}
	return colExpr, vals, valExprs, true
}

func getConstBytesFromExpr(exprs []*plan.Expr) ([][]byte, bool) {
	vals := make([][]byte, len(exprs))
	for idx := range exprs {
		if fExpr, ok := exprs[idx].Expr.(*plan.Expr_Fold); ok {
			if fExpr.Fold.Data == nil {
				// cases:
				//   1. array/array.sql
				//   2. array/array_index_1.sql
				//   3. array/array_index.sql
				//   4. dml/select/select.test
				return nil, false
			}

			if len(fExpr.Fold.Data) == 0 {
				// create table t (a varchar primary key);
				// explain analyze select * from t where a = ''; (empty string)
				//
				// other cases:
				// 	1. ddl/alter_table_AddDrop_column.sql
				//  2. cases/ddl/lowercase.test
				//  3. ddl/drop_if_exists.sql
				//  4. ddl/create_table_as_select.sql
				//  5. dml/delete/delete_multiple_table.sql

				vals[idx] = nil
				continue
				//return nil, false
			}

			if !fExpr.Fold.IsConst {
				return nil, false
			}

			vals[idx] = nil
			vals[idx] = append(vals[idx], fExpr.Fold.Data...)
		} else {
			logutil.Warnf("const folded val expr is not a fold expr: %s\n", plan2.FormatExpr(exprs[idx], plan2.FormatOption{}))
			return nil, false
		}
	}

	return vals, true
}

func mustColVecValueFromBinaryFuncExpr(expr *plan.Expr_F) (*plan.Expr_Col, []byte, bool) {
	var (
		colExpr  *plan.Expr_Col
		valExpr  *plan.Expr
		ok       bool
		exprImpl *plan.Expr_Vec
	)
	if colExpr, ok = expr.F.Args[0].Expr.(*plan.Expr_Col); ok {
		valExpr = expr.F.Args[1]
	} else if colExpr, ok = expr.F.Args[1].Expr.(*plan.Expr_Col); ok {
		valExpr = expr.F.Args[0]
	} else {
		return nil, nil, false
	}

	if exprImpl, ok = valExpr.Expr.(*plan.Expr_Vec); !ok {
		if fExpr, ok := valExpr.Expr.(*plan.Expr_Fold); ok {
			if len(fExpr.Fold.Data) == 0 {
				return nil, nil, false
			}
			if fExpr.Fold.IsConst {
				return nil, nil, false
			}
			return colExpr, fExpr.Fold.Data, ok
		}

		logutil.Warnf("const folded val expr is not a vec expr: %s\n", plan2.FormatExpr(valExpr, plan2.FormatOption{}))
		return nil, nil, false
	}

	return colExpr, exprImpl.Vec.Data, ok
}

func MakeColExprForTest(idx int32, typ types.T, colName ...string) *plan.Expr {
	schema := []string{"a", "b", "c", "d"}
	var name = schema[idx]
	if len(colName) > 0 {
		name = colName[0]
	}

	containerType := typ.ToType()
	exprType := plan2.MakePlan2Type(&containerType)

	return &plan.Expr{
		Typ: exprType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: idx,
				Name:   name,
			},
		},
	}
}

func MakeFunctionExprForTest(name string, args []*plan.Expr) *plan.Expr {
	argTypes := make([]types.Type, len(args))
	for i, arg := range args {
		argTypes[i] = plan2.MakeTypeByPlan2Expr(arg)
	}

	finfo, err := function.GetFunctionByName(context.TODO(), name, argTypes)
	if err != nil {
		panic(err)
	}

	retTyp := finfo.GetReturnType()

	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&retTyp),
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     finfo.GetEncodedOverloadID(),
					ObjName: name,
				},
				Args: args,
			},
		},
	}
}

func MakeInExprForTest[T any](
	arg0 *plan.Expr, vals []T, oid types.T, mp *mpool.MPool,
) *plan.Expr {
	vec := vector.NewVec(oid.ToType())
	for _, val := range vals {
		_ = vector.AppendAny(vec, val, false, mp)
	}
	data, _ := vec.MarshalBinary()
	vec.Free(mp)
	return &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: true,
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     function.InFunctionEncodedID,
					ObjName: function.InFunctionName,
				},
				Args: []*plan.Expr{
					arg0,
					{
						Typ: plan2.MakePlan2Type(vec.GetType()),
						Expr: &plan.Expr_Vec{
							Vec: &plan.LiteralVec{
								Len:  int32(len(vals)),
								Data: data,
							},
						},
					},
				},
			},
		},
	}
}
