// Copyright 2022 Matrix Origin
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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (builder *QueryBuilder) buildGenerateSeries(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) == 0 {
		return 0, moerr.NewInvalidArg(builder.GetContext(), "generate_series requires at least one argument", len(exprs))
	}

	boundExprs, retTyp, err := bindGenerateSeriesArgs(builder.GetContext(), exprs)
	if err != nil {
		return 0, err
	}
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name: "generate_series",
			},
			Cols: []*plan.ColDef{{
				Name: "result",
				Typ:  makePlan2Type(&retTyp),
			}},
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: boundExprs,
	}
	return builder.appendNode(node, ctx), nil
}

// bindGenerateSeriesArgs fixes the table function's output schema during
// binding. Numeric arguments keep their existing runtime validation. Temporal
// arguments are normalized to datetime so execution never needs to rewrite
// plan metadata after Prepare.
func bindGenerateSeriesArgs(ctx context.Context, exprs []*plan.Expr) ([]*plan.Expr, types.Type, error) {
	firstType := types.T(exprs[0].Typ.Id)
	if firstType.IsInteger() {
		return exprs, types.T_int64.ToType(), nil
	}
	if !firstType.IsDateRelate() && !firstType.IsMySQLString() {
		return exprs, types.T_varchar.ToType(), nil
	}

	datetimeTyp := types.T_datetime.ToTypeWithScale(generateSeriesDatetimeScale(exprs))
	boundExprs := append([]*plan.Expr(nil), exprs...)
	endpointCount := min(len(boundExprs), 2)
	for i := 0; i < endpointCount; i++ {
		if types.T(boundExprs[i].Typ.Id) == types.T_datetime && boundExprs[i].Typ.Scale == datetimeTyp.Scale {
			continue
		}
		casted, err := appendCastBeforeExpr(ctx, boundExprs[i], makePlan2Type(&datetimeTyp))
		if err != nil {
			return nil, types.Type{}, err
		}
		boundExprs[i] = casted
	}
	if firstType.IsMySQLString() {
		return boundExprs, types.T_varchar.ToType(), nil
	}
	return boundExprs, datetimeTyp, nil
}

func generateSeriesDatetimeScale(exprs []*plan.Expr) int32 {
	var scale int32
	for i := 0; i < min(len(exprs), 2); i++ {
		expr := exprs[i]
		if expr.Typ.Scale > scale {
			scale = expr.Typ.Scale
		}
		if types.T(expr.Typ.Id).IsMySQLString() {
			lit := expr.GetLit()
			if lit == nil {
				return MaxFsp
			}
			if literalScale := datetimeLiteralScale(lit.GetSval()); literalScale > scale {
				scale = literalScale
			}
		}
	}

	if len(exprs) >= 3 {
		step := exprs[2].GetLit()
		if step == nil || strings.Contains(strings.ToLower(step.GetSval()), "microsecond") {
			scale = MaxFsp
		}
	}
	return min(scale, int32(MaxFsp))
}

func datetimeLiteralScale(value string) int32 {
	dot := strings.LastIndexByte(value, '.')
	if dot < 0 {
		return 0
	}

	var scale int32
	for i := dot + 1; i < len(value) && value[i] >= '0' && value[i] <= '9'; i++ {
		scale++
	}
	return min(scale, int32(MaxFsp))
}

func (builder *QueryBuilder) buildGenerateRandomInt64(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) int32 {
	i64Typ := types.T_int64.ToType()
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name: "generate_random_int64",
			},
			Cols: []*plan.ColDef{
				{
					Name: "nth",
					Typ:  makePlan2Type(&i64Typ),
				},
				{
					Name: "i64",
					Typ:  makePlan2Type(&i64Typ),
				},
			},
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx)
}

func (builder *QueryBuilder) buildGenerateRandomFloat64(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) int32 {
	i64Typ := types.T_int64.ToType()
	f64Typ := types.T_float64.ToType()
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name: "generate_random_float64",
			},
			Cols: []*plan.ColDef{
				{
					Name: "nth",
					Typ:  makePlan2Type(&i64Typ),
				},
				{
					Name: "f64",
					Typ:  makePlan2Type(&f64Typ),
				},
			},
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx)
}
