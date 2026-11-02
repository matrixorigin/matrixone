// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func temporalIntegerSource(expr *Expr) bool {
	if expr == nil {
		return false
	}
	switch types.T(expr.Typ.Id) {
	case types.T_datetime, types.T_time, types.T_timestamp:
		return true
	default:
		return false
	}
}

// Lower integer evaluation at its value owner, before selection and subquery
// materialization erase the distinction between storage values and expressions.
// A selector forwards one branch's policy; a common-value producer owns its own
// policy. Never taint all branches just because one contains a temporal CAST.
func appendTemporalIntegerCast(ctx context.Context, expr *Expr, target Type, binder *baseBinder) (*Expr, error) {
	if (types.T(target.Id) != types.T_int64 && types.T(target.Id) != types.T_uint64) || !temporalIntegerSource(expr) {
		return appendCastBeforeExprWithOverload(ctx, expr, target, 1)
	}
	if sub := expr.GetSub(); sub != nil && sub.Typ == planpb.SubqueryRef_SCALAR && binder != nil {
		node, subCtx := binder.subqueryNodeAndContext(expr)
		if node != nil && len(node.ProjectList) > 0 && subCtx != nil && len(subCtx.results) > 0 && sub.RowSize == 1 {
			// Correlated projections are lifted across a nullable join. Keep their
			// projects/results/root contract intact until decorrelation has produced
			// the match-guarded value; lower its integer policy at that boundary.
			if len(subCtx.projects) > 0 && hasCorrCol(subCtx.projects[0]) {
				return appendCastBeforeExprWithOverload(ctx, expr, target, 1)
			}
			// A final result projection above DISTINCT/SORT is a different owner
			// from ctx.projects. Only synchronize the logical slot when it is the
			// same producer, otherwise preserve that earlier materialization stage.
			sameProject := len(subCtx.projects) > 0 && exprStructuralEqual(subCtx.projects[0], node.ProjectList[0])
			converted, err := appendTemporalIntegerCast(ctx, node.ProjectList[0], target, binder)
			if err != nil {
				return nil, err
			}
			result := DeepCopyExpr(expr)
			result.Typ = converted.Typ
			result.Typ.NotNullable = false // an empty scalar subquery is NULL
			if sameProject {
				subCtx.projects = append([]*Expr(nil), subCtx.projects...)
				subCtx.projects[0] = converted
			}
			node.ProjectList = append([]*Expr(nil), node.ProjectList...)
			node.ProjectList[0] = converted
			subCtx.results = append([]*Expr(nil), subCtx.results...)
			subCtx.results[0] = converted
			return result, nil
		}
	}
	fn := expr.GetF()
	if fn != nil && fn.Func != nil {
		name := fn.Func.ObjName
		if name == "cast" && len(fn.Args) == 2 && !isExplicitPreparedCast(expr) {
			_, id := function.DecodeOverloadID(fn.Func.Obj)
			source := fn.Args[0]
			// Representation-only widening inserted by the conditional resolver does
			// not own a new integer policy. Do not strip narrowing or zone conversions.
			if id == 0 && source.Typ.Id == expr.Typ.Id && source.Typ.Scale <= expr.Typ.Scale {
				return appendTemporalIntegerCast(ctx, source, target, binder)
			}
		}
		if isExplicitPreparedCast(expr) || name == "coalesce" || name == "least" || name == "greatest" || expr.GetPreparedNumeric().GetIfnullCommonValue() {
			return appendCastBeforeExprWithOverload(ctx, expr, target, function.TemporalCastIntegerOverload)
		}
		if name == "case" || name == "if" || name == "iff" {
			args := append([]*Expr(nil), fn.Args...)
			for _, i := range controlFlowValueIndexes(name, len(args)) {
				converted, err := appendTemporalIntegerCast(ctx, args[i], target, binder)
				if err != nil {
					return nil, err
				}
				args[i] = converted
			}
			// Conditions are retained once, with the existing short-circuit evaluator;
			// only value branches become integers. No duplicate condition/source tree.
			return BindFuncExprImplByPlanExpr(ctx, name, args)
		}
	}
	return appendCastBeforeExprWithOverload(ctx, expr, target, 1)
}
