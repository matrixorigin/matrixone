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

// bindGenerateSeriesArgs fixes the table function's output schema when the
// first argument has a known domain. Numeric arguments keep their existing
// runtime validation. Temporal endpoints are normalized to datetime. A direct
// prepared first argument is specialized at EXECUTE using its runtime type.
func bindGenerateSeriesArgs(ctx context.Context, exprs []*plan.Expr) ([]*plan.Expr, types.Type, error) {
	firstType := types.T(exprs[0].Typ.Id)
	if exprs[0].GetP() != nil {
		// The SQL PREPARE transport type is TEXT, not the endpoint's domain.
		// Leave this marker uncoerced so EXECUTE can choose the numeric or
		// temporal path using the actual parameter type. The provisional
		// integer result permits numeric consumers such as SUM and UNION to
		// bind at PREPARE; execution refreshes its domain when the endpoint
		// is temporal.
		return exprs, types.T_int64.ToType(), nil
	}
	if firstType.IsInteger() {
		boundExprs := append([]*plan.Expr(nil), exprs...)
		for i := 1; i < len(boundExprs); i++ {
			if boundExprs[i].GetP() == nil {
				continue
			}
			target := types.T_int64.ToType()
			casted, err := appendCastBeforeExpr(ctx, boundExprs[i], makePlan2Type(&target))
			if err != nil {
				return nil, types.Type{}, err
			}
			boundExprs[i] = casted
		}
		return boundExprs, types.T_int64.ToType(), nil
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
	if len(boundExprs) > 2 && boundExprs[2].GetP() != nil {
		stepType := types.T_varchar.ToType()
		casted, err := appendCastBeforeExpr(ctx, boundExprs[2], makePlan2Type(&stepType))
		if err != nil {
			return nil, types.Type{}, err
		}
		boundExprs[2] = casted
	}
	if firstType.IsMySQLString() {
		return boundExprs, types.T_varchar.ToType(), nil
	}
	return boundExprs, datetimeTyp, nil
}

func generateSeriesDatetimeScale(exprs []*plan.Expr, runtimeValues ...[]any) int32 {
	var scale int32
	for i := 0; i < min(len(exprs), 2); i++ {
		expr := exprs[i]
		if len(runtimeValues) > 0 {
			// PREPARE may have wrapped a marker or a string literal in a
			// provisional DATETIME(6) cast. Infer the EXECUTE scale from the
			// original endpoint, not that provisional cast.
			expr = unwrapPreparedImplicitCast(expr, true)
		}
		if marker := expr.GetP(); marker != nil && len(runtimeValues) > 0 &&
			marker.Pos >= 0 && int(marker.Pos) < len(runtimeValues[0]) {
			if value, ok := runtimeValues[0][marker.Pos].(ParamValue); ok {
				if value.HasRuntimeType && value.RuntimeType.Oid.IsDateRelate() {
					scale = max(scale, value.RuntimeType.Scale)
					continue
				}
				if text, ok := value.Value.(string); ok {
					scale = max(scale, datetimeLiteralScale(text))
					continue
				}
			}
		}
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
		stepExpr := exprs[2]
		if len(runtimeValues) > 0 {
			stepExpr = unwrapPreparedImplicitCast(stepExpr, true)
		}
		step := stepExpr.GetLit()
		if marker := stepExpr.GetP(); marker != nil && len(runtimeValues) > 0 &&
			marker.Pos >= 0 && int(marker.Pos) < len(runtimeValues[0]) {
			if value, ok := runtimeValues[0][marker.Pos].(ParamValue); ok {
				if text, ok := value.Value.(string); ok {
					if strings.Contains(strings.ToLower(text), "microsecond") {
						scale = MaxFsp
					}
					return min(scale, int32(MaxFsp))
				}
			}
		}
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
