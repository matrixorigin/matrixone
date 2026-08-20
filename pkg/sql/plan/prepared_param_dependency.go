// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

type preparedParamCommonTypeDependencyRule struct {
	positions          map[int32]struct{}
	executionPositions map[int32]struct{}
}

func (r *preparedParamCommonTypeDependencyRule) MatchNode(*Node) bool { return false }
func (r *preparedParamCommonTypeDependencyRule) IsApplyExpr() bool    { return true }
func (r *preparedParamCommonTypeDependencyRule) ApplyNode(*Node) error {
	return nil
}

func (r *preparedParamCommonTypeDependencyRule) ApplyExpr(expr *Expr) (*Expr, error) {
	r.visit(expr)
	return expr, nil
}

func (r *preparedParamCommonTypeDependencyRule) visit(expr *Expr) {
	if expr == nil {
		return
	}
	switch impl := expr.Expr.(type) {
	case *planpb.Expr_P:
		if expr.Typ.Enumvalues == "mo_explicit_cast_param_dependency" {
			r.executionPositions[impl.P.Pos] = struct{}{}
		}
	case *planpb.Expr_F:
		functionID, _ := function.DecodeOverloadID(impl.F.Func.Obj)
		if impl.F.Func.GetObjName() == "cast" && len(impl.F.Args) > 0 {
			// An implicit binder cast directly over a parameter fixes the
			// physical input domain just as an explicitly marked CAST does. Do
			// not recursively claim parameters hidden below unrelated functions.
			if param := impl.F.Args[0].GetP(); param != nil {
				r.executionPositions[param.Pos] = struct{}{}
			}
		}
		if functionID == function.COALESCE || functionID == function.GREATEST || functionID == function.LEAST {
			for _, arg := range impl.F.Args {
				r.collectDirectParams(arg)
			}
		}
		for _, arg := range impl.F.Args {
			r.visit(arg)
		}
	case *planpb.Expr_List:
		for _, item := range impl.List.List {
			r.visit(item)
		}
	case *planpb.Expr_W:
		if impl.W == nil {
			return
		}
		r.visit(impl.W.WindowFunc)
		for _, expr := range impl.W.PartitionBy {
			r.visit(expr)
		}
		for _, orderBy := range impl.W.OrderBy {
			if orderBy != nil {
				r.visit(orderBy.Expr)
			}
		}
		if frame := impl.W.Frame; frame != nil {
			if frame.Start != nil {
				r.visit(frame.Start.Val)
			}
			if frame.End != nil {
				r.visit(frame.End.Val)
			}
		}
	}
}

func (r *preparedParamCommonTypeDependencyRule) collectDirectParams(expr *Expr) {
	if expr == nil {
		return
	}
	switch impl := expr.Expr.(type) {
	case *planpb.Expr_P:
		if expr.Typ.Enumvalues == "mo_decimal_common_type_dependency" ||
			strings.HasPrefix(expr.Typ.Enumvalues, "mo_runtime_numeric:") {
			r.positions[impl.P.Pos] = struct{}{}
		}
	case *planpb.Expr_F:
		// Common-type resolution may wrap the marked parameter in an ordinary
		// string cast when the deployment protocol still uses legacy semantics.
		// The marker on the parameter, not a particular CAST encoding, is the
		// dependency contract.
		for _, arg := range impl.F.Args {
			r.collectDirectParams(arg)
		}
	case *planpb.Expr_List:
		for _, item := range impl.List.List {
			r.collectDirectParams(item)
		}
	case *planpb.Expr_W:
		// A common-type function can itself wrap a window expression. Reuse the
		// complete expression traversal so markers anywhere in the WindowSpec are
		// not hidden by the Expr_W envelope.
		r.visit(expr)
	}
}

func (r *preparedParamCommonTypeDependencyRule) visitQuery(query *Query) error {
	if query == nil {
		return nil
	}
	for _, expr := range query.Params {
		r.visit(expr)
	}
	if err := NewVisitPlan(&Plan{Plan: &planpb.Plan_Query{Query: query}}, []VisitPlanRule{r}).Visit(context.Background()); err != nil {
		return err
	}
	for _, background := range query.BackgroundQueries {
		if err := r.visitQuery(background); err != nil {
			return err
		}
	}
	return nil
}

func (r *preparedParamCommonTypeDependencyRule) visitPlan(p *Plan) error {
	if p == nil {
		return nil
	}
	switch planImpl := p.Plan.(type) {
	case *planpb.Plan_Query:
		return r.visitQuery(planImpl.Query)
	case *planpb.Plan_Ddl:
		if planImpl.Ddl != nil {
			return r.visitQuery(planImpl.Ddl.Query)
		}
	case *planpb.Plan_Dcl:
		if planImpl.Dcl == nil {
			return nil
		}
		if setVariables := planImpl.Dcl.GetSetVariables(); setVariables != nil {
			for _, item := range setVariables.Items {
				if item != nil {
					r.visit(item.Value)
					r.visit(item.Reserved)
				}
			}
			if err := r.visitQuery(setVariables.Query); err != nil {
				return err
			}
		}
		if prepare := planImpl.Dcl.GetPrepare(); prepare != nil {
			return r.visitPlan(prepare.Plan)
		}
		if execute := planImpl.Dcl.GetExecute(); execute != nil {
			for _, arg := range execute.Args {
				r.visit(arg)
			}
		}
	}
	return nil
}

// PreparedParamCommonTypeDependencies returns the parameter positions whose
// runtime category participates directly in DECIMAL-aware common-type binding.
func PreparedParamCommonTypeDependencies(p *Plan, paramCount int) []bool {
	if p == nil || paramCount <= 0 {
		return nil
	}
	rule := &preparedParamCommonTypeDependencyRule{
		positions: make(map[int32]struct{}), executionPositions: make(map[int32]struct{}),
	}
	if err := rule.visitPlan(p); err != nil {
		dependencies := make([]bool, paramCount)
		for i := range dependencies {
			dependencies[i] = true
		}
		return dependencies
	}
	if len(rule.positions) == 0 {
		return nil
	}
	dependencies := make([]bool, paramCount)
	for pos := range rule.positions {
		if pos >= 0 && int(pos) < paramCount {
			dependencies[pos] = true
		}
	}
	return dependencies
}

// PreparedParamExecutionDependencies returns parameters whose runtime physical
// type must be rebound even though an explicit CAST fixes the result metadata.
func PreparedParamExecutionDependencies(p *Plan, paramCount int) []bool {
	if p == nil || paramCount <= 0 {
		return nil
	}
	rule := &preparedParamCommonTypeDependencyRule{
		positions: make(map[int32]struct{}), executionPositions: make(map[int32]struct{}),
	}
	if err := rule.visitPlan(p); err != nil {
		dependencies := make([]bool, paramCount)
		for i := range dependencies {
			dependencies[i] = true
		}
		return dependencies
	}
	if len(rule.executionPositions) == 0 {
		return nil
	}
	dependencies := make([]bool, paramCount)
	for pos := range rule.executionPositions {
		if pos >= 0 && int(pos) < paramCount {
			dependencies[pos] = true
		}
	}
	return dependencies
}

// PreparedParamResultMetadataDependencies returns parameter positions whose
// runtime type directly determines a result column's protocol metadata.
func PreparedParamResultMetadataDependencies(p *Plan, paramCount int) []bool {
	columnDependencies := PreparedParamResultMetadataDependencyColumns(p, paramCount)
	var dependencies []bool
	for _, column := range columnDependencies {
		for pos, dependent := range column {
			if dependent {
				if dependencies == nil {
					dependencies = make([]bool, paramCount)
				}
				dependencies[pos] = true
			}
		}
	}
	return dependencies
}

// PreparedParamResultMetadataDependencyColumns returns the parameter positions
// that determine each result column's protocol metadata. The per-column shape
// lets the protocol layer stabilize only the affected DECIMAL columns without
// changing the physical types used by the plan and expression executor.
func PreparedParamResultMetadataDependencyColumns(p *Plan, paramCount int) [][]bool {
	if p == nil || paramCount <= 0 {
		return nil
	}
	query := p.GetQuery()
	if query == nil || len(query.Steps) == 0 {
		return nil
	}
	step := len(query.Steps) - 1
	if query.HasReturning {
		if query.ReturningStep < 0 || int(query.ReturningStep) >= len(query.Steps) {
			return nil
		}
		step = int(query.ReturningStep)
	}
	nodeID := query.Steps[step]
	if nodeID < 0 || int(nodeID) >= len(query.Nodes) || query.Nodes[nodeID] == nil {
		return nil
	}
	root := query.Nodes[nodeID]
	columnDependencies := make([][]bool, len(root.ProjectList))
	for columnPos, rootExpr := range root.ProjectList {
		visited := make(map[[3]int32]bool)
		var dependencies []bool
		var traceOutput func(int32, int32, bool)
		var traceExpr func(int32, *Expr, bool)
		var traceColumn func(int32, *planpb.ColRef, bool)
		markParam := func(param *planpb.ParamRef) {
			if param == nil || param.Pos < 0 || int(param.Pos) >= paramCount {
				return
			}
			if dependencies == nil {
				dependencies = make([]bool, paramCount)
			}
			dependencies[param.Pos] = true
		}
		traceExpr = func(currentNodeID int32, expr *Expr, allowSetCast bool) {
			if expr == nil {
				return
			}
			if param := expr.GetP(); param != nil {
				markParam(param)
				return
			}
			if fn := expr.GetF(); fn != nil && fn.Func != nil {
				switch fn.Func.GetObjName() {
				case "if":
					for i := 1; i < len(fn.Args); i++ {
						traceExpr(currentNodeID, fn.Args[i], true)
					}
					return
				case "coalesce", "greatest", "least":
					allRuntime := len(fn.Args) > 0
					hasNumericPeer := false
					for _, valueArg := range fn.Args {
						if !containsPreparedParam(valueArg) {
							allRuntime = false
							if valueArg != nil && (types.Type{Oid: types.T(valueArg.Typ.Id)}).IsNumeric() {
								hasNumericPeer = true
							}
						}
					}
					for _, valueArg := range fn.Args {
						if allRuntime || hasNumericPeer || valueArg.ExactDecimalParam {
							traceExpr(currentNodeID, valueArg, true)
						}
					}
					return
				case "case":
					for i := 1; i < len(fn.Args); i += 2 {
						traceExpr(currentNodeID, fn.Args[i], true)
					}
					if len(fn.Args)%2 == 1 {
						traceExpr(currentNodeID, fn.Args[len(fn.Args)-1], true)
					}
					return
				}
				// Binder-generated casts are transparent to result provenance.
				// A user-written CAST is the only boundary and carries an explicit
				// marker, so do not infer provenance from an overload identifier.
				if fn.Func.GetObjName() == "cast" && len(fn.Args) == 2 {
					if containsPreparedParamMarker(
						fn.Args[0], "mo_explicit_cast_param_dependency") {
						return
					}
					traceExpr(currentNodeID, fn.Args[0], allowSetCast)
					return
				}
			}
			if fn := expr.GetF(); fn != nil &&
				(allowSetCast || preparedResultTypePropagatingFunction(fn.Func.GetObjName())) {
				if fn.Func == nil {
					return
				}
				for _, arg := range fn.Args {
					traceExpr(currentNodeID, arg, true)
				}
				return
			}
			ref := expr.GetCol()
			if ref != nil {
				if currentNodeID >= 0 && int(currentNodeID) < len(query.Nodes) {
					node := query.Nodes[currentNodeID]
					if node != nil && node.NodeType == planpb.Node_AGG {
						if ref.RelPos == -2 && ref.ColPos >= 0 && int(ref.ColPos) < len(node.AggList) {
							traceExpr(currentNodeID, node.AggList[ref.ColPos], true)
							return
						}
						if ref.RelPos == -1 && ref.ColPos >= 0 && int(ref.ColPos) < len(node.GroupBy) {
							traceExpr(currentNodeID, node.GroupBy[ref.ColPos], true)
							return
						}
					}
					if node != nil && len(node.Children) == 1 {
						traceOutput(node.Children[0], ref.ColPos, allowSetCast)
						return
					}
					if node != nil && len(node.Children) > 1 && ref.RelPos >= 0 &&
						int(ref.RelPos) < len(node.Children) {
						traceOutput(node.Children[ref.RelPos], ref.ColPos, allowSetCast)
						return
					}
				}
				traceColumn(currentNodeID, ref, allowSetCast)
			}
		}
		traceColumn = func(currentNodeID int32, ref *planpb.ColRef, allowSetCast bool) {
			if ref == nil || currentNodeID < 0 || int(currentNodeID) >= len(query.Nodes) {
				return
			}
			node := query.Nodes[currentNodeID]
			if node == nil {
				return
			}
			for tagPos, tag := range node.BindingTags {
				if tag != ref.RelPos {
					continue
				}
				if node.NodeType == planpb.Node_AGG {
					if tagPos == 0 && ref.ColPos >= 0 && int(ref.ColPos) < len(node.GroupBy) {
						traceExpr(currentNodeID, node.GroupBy[ref.ColPos], true)
						return
					}
					if tagPos == 1 && ref.ColPos >= 0 && int(ref.ColPos) < len(node.AggList) {
						traceExpr(currentNodeID, node.AggList[ref.ColPos], true)
						return
					}
				}
				traceOutput(currentNodeID, ref.ColPos, allowSetCast)
				return
			}
			for _, childID := range node.Children {
				if childID < 0 || int(childID) >= len(query.Nodes) || query.Nodes[childID] == nil {
					continue
				}
				child := query.Nodes[childID]
				for childColPos := range child.ProjectList {
					traceOutput(childID, int32(childColPos), allowSetCast)
				}
			}
		}
		traceOutput = func(currentNodeID, colPos int32, allowSetCast bool) {
			allowSetCastKey := int32(0)
			if allowSetCast {
				allowSetCastKey = 1
			}
			key := [3]int32{currentNodeID, colPos, allowSetCastKey}
			if visited[key] || currentNodeID < 0 || int(currentNodeID) >= len(query.Nodes) {
				return
			}
			visited[key] = true
			node := query.Nodes[currentNodeID]
			if node == nil {
				return
			}
			switch node.NodeType {
			case planpb.Node_UNION, planpb.Node_UNION_ALL,
				planpb.Node_INTERSECT, planpb.Node_INTERSECT_ALL,
				planpb.Node_MINUS, planpb.Node_MINUS_ALL:
				for _, childID := range node.Children {
					traceOutput(childID, colPos, true)
				}
				return
			}
			if colPos < 0 || int(colPos) >= len(node.ProjectList) {
				return
			}
			traceExpr(currentNodeID, node.ProjectList[colPos], allowSetCast)
		}
		traceExpr(nodeID, rootExpr, false)
		columnDependencies[columnPos] = dependencies
	}
	return columnDependencies
}

func preparedResultTypePropagatingFunction(name string) bool {
	switch name {
	case "abs", "max", "min", "sum", "round", "ceil", "ceiling", "floor":
		return true
	default:
		return false
	}
}

func containsPreparedParam(expr *Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetP() != nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if containsPreparedParam(arg) {
				return true
			}
		}
	}
	return false
}

func containsPreparedParamMarker(expr *Expr, marker string) bool {
	if expr == nil {
		return false
	}
	if expr.GetP() != nil && expr.Typ.Enumvalues == marker {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if containsPreparedParamMarker(arg, marker) {
				return true
			}
		}
	}
	return false
}
