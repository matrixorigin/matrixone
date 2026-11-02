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

// A freshly bound scalar call owns its graph while its output is lowered.
// Alias expansion must rebind graph references rather than shallow-copy their
// identities (see aliasProjectionNeedsRebind). The program is the value owner,
// not the opaque dependent call returned to the consumer.
func temporalIntegerScalarOutput(expr *Expr, binder *baseBinder) (*planpb.Node, *BindContext, *planpb.CorrelationProgram) {
	if binder == nil || binder.builder == nil {
		return nil, nil, nil
	}
	if sub := expr.GetSub(); sub != nil && sub.Typ == planpb.SubqueryRef_SCALAR && sub.RowSize == 1 {
		node, ctx := binder.subqueryNodeAndContext(expr)
		return node, ctx, nil
	}
	call := expr.GetDependent()
	spec := binder.builder.qry.Correlation
	if call == nil || call.Reducer != planpb.SubqueryRef_SCALAR || len(call.Lhs) != 0 || spec == nil ||
		call.ProgramId == 0 || int(call.ProgramId) > len(spec.Programs) {
		return nil, nil, nil
	}
	program := spec.Programs[call.ProgramId-1]
	if program == nil || len(program.OutputTypes) != 1 || program.RootNode < 0 || int(program.RootNode) >= len(binder.builder.qry.Nodes) {
		return nil, nil, nil
	}
	return binder.builder.qry.Nodes[program.RootNode], binder.builder.ctxByNode[program.RootNode], program
}

func isTemporalIntegerPolicyCast(expr *Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 2 || !temporalIntegerSource(fn.Args[0]) ||
		(types.T(expr.Typ.Id) != types.T_int64 && types.T(expr.Typ.Id) != types.T_uint64) {
		return false
	}
	id, overload := function.DecodeOverloadID(fn.Func.Obj)
	return id == function.CAST && overload == function.TemporalCastIntegerOverload
}

// Lower integer evaluation at its value owner, before selection and subquery
// materialization erase the distinction between storage values and expressions.
// A selector forwards one branch's policy; a common-value producer owns its own
// policy. Never taint all branches just because one contains a temporal CAST.
func appendTemporalIntegerCast(ctx context.Context, expr *Expr, target Type, binder *baseBinder) (*Expr, error) {
	if (types.T(target.Id) != types.T_int64 && types.T(target.Id) != types.T_uint64) || !temporalIntegerSource(expr) {
		return appendCastBeforeExprWithOverload(ctx, expr, target, 1)
	}
	if node, subCtx, program := temporalIntegerScalarOutput(expr, binder); node != nil {
		if len(node.ProjectList) > 0 && subCtx != nil && len(subCtx.results) > 0 {
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
			if program != nil {
				program.OutputTypes[0] = converted.Typ
			} else if !sameProject && len(subCtx.projects) > 0 && hasCorrCol(subCtx.projects[0]) {
				// Keep conversion after the original DISTINCT/SORT/cardinality
				// boundary. A dependent invocation preserves that final projection;
				// lifting only ctx.projects would lose it.
				if binder.builder.dependentRelations == nil {
					binder.builder.dependentRelations = make(map[int32]bool)
				}
				binder.builder.dependentRelations[expr.GetSub().NodeId] = true
			}
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
