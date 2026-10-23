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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
	if isUnresolvedPreparedParam(ctx, exprs[0]) {
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
		for i := range boundExprs {
			// The executor and result column use signed BIGINT. Preserve the
			// source type until this table-function boundary, then convert each
			// unsigned endpoint or marker with the normal range check.
			if !types.T(boundExprs[i].Typ.Id).IsUnsignedInt() &&
				(i == 0 || boundExprs[i].GetP() == nil) {
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

	scaleExprs := append([]*plan.Expr(nil), exprs...)
	for i, expr := range exprs {
		if !types.T(expr.Typ.Id).IsMySQLString() {
			continue
		}
		if value, known := preparedConfigurationValue(ctx, expr); known {
			if text, ok := value.(string); ok {
				// This literal is only a schema witness. The executed endpoint
				// and step retain their original parameter expressions.
				scaleExprs[i] = MakePlan2StringConstExprWithType(text)
			}
		}
	}
	datetimeTyp := types.T_datetime.ToTypeWithScale(generateSeriesDatetimeScale(scaleExprs))
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
		// PREPARE may wrap an endpoint in a provisional cast or in the
		// three-argument temporal CAST0 that carries a paired source-kind
		// output. Inspect the original source only for schema inference; the
		// executable CAST and its BOOL sibling stay intact.
		expr := generateSeriesMetadataSourceExpr(exprs[i])
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
		stepExpr := generateSeriesMetadataSourceExpr(exprs[2])
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

// generateSeriesTemporalProgramCastSource recognizes only the temporal CAST0
// wrapper created by kind lowering. It is an analysis view: callers must keep
// both lexical refs in the executable tree.
func generateSeriesTemporalProgramCastSource(expr *plan.Expr) (source, sourceKind *plan.Expr, ok bool) {
	if expr == nil || expr.GetF() == nil || expr.GetF().GetFunc() == nil {
		return nil, nil, false
	}
	fn := expr.GetF()
	fid, overload := function.DecodeOverloadID(fn.Func.GetObj())
	if fid != function.CAST || overload != 0 || fn.GetSyntaxExplicitCast() || len(fn.Args) != 3 ||
		fn.Args[0] == nil || fn.Args[1] == nil || fn.Args[2] == nil || fn.Args[1].GetT() == nil ||
		types.T(fn.Args[1].Typ.Id) != types.T(expr.Typ.Id) ||
		fn.Args[1].Typ.Width != expr.Typ.Width || fn.Args[1].Typ.Scale != expr.Typ.Scale ||
		!types.T(expr.Typ.Id).IsDateRelate() || types.T(fn.Args[2].Typ.Id) != types.T_bool {
		return nil, nil, false
	}
	source, sourceKind, ok = function.NumericValueProgramPairSourceOwner(fn.Args[0], fn.Args[2])
	if !ok {
		return nil, nil, false
	}
	return source, sourceKind, true
}

// generateSeriesMetadataSourceExpr strips only ordinary implicit casts and
// validated temporal CAST0 owners to inspect an endpoint's parameter/type.
// It never changes the execution expression or treats a standalone REF as a
// transparent source.
func generateSeriesMetadataSourceExpr(expr *plan.Expr) *plan.Expr {
	for expr != nil {
		if source, _, ok := generateSeriesTemporalProgramCastSource(expr); ok {
			expr = source
			continue
		}
		if source, policy, ok := numericValueCastAnalysisSource(expr); ok {
			if policy == function.NumericValueCastPolicyExplicit || expr.GetF().GetSyntaxExplicitCast() {
				return expr
			}
			expr = source
			continue
		}
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || !strings.EqualFold(fn.Func.GetObjName(), "cast") ||
			fn.GetSyntaxExplicitCast() || len(fn.Args) == 0 || fn.Args[0] == nil {
			return expr
		}
		_, overload := function.DecodeOverloadID(fn.Func.GetObj())
		if overload != 0 {
			return expr
		}
		expr = fn.Args[0]
	}
	return nil
}

func generateSeriesSourceParamPosition(expr *plan.Expr) (int, bool) {
	return preparedRuntimeSourceParamPosition(generateSeriesMetadataSourceExpr(expr))
}

func datetimeLiteralScale(value string) int32 {
	value = strings.TrimSpace(value)
	dot := strings.LastIndexByte(value, '.')
	if dot < 0 {
		return 0
	}
	// A dot in the date part separates calendar fields, not seconds.
	// Compact datetimes have no date/time separator; their fraction starts
	// only after the fourteen YYYYMMDDHHMMSS digits.
	if timeStart := strings.IndexAny(value, " T"); timeStart >= 0 {
		if dot < timeStart {
			return 0
		}
	} else {
		if dot != 14 {
			return 0
		}
		for i := 0; i < dot; i++ {
			if value[i] < '0' || value[i] > '9' {
				return 0
			}
		}
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
