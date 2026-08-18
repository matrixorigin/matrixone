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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

type preparedParamCommonTypeDependencyRule struct {
	positions map[int32]struct{}
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
	case *planpb.Expr_F:
		functionID, _ := function.DecodeOverloadID(impl.F.Func.Obj)
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
	rule := &preparedParamCommonTypeDependencyRule{positions: make(map[int32]struct{})}
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

// PreparedParamResultMetadataDependencies returns parameter positions whose
// runtime type directly determines a result column's protocol metadata.
func PreparedParamResultMetadataDependencies(p *Plan, paramCount int) []bool {
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
	var dependencies []bool
	for _, expr := range query.Nodes[nodeID].ProjectList {
		param := expr.GetP()
		if param == nil || param.Pos < 0 || int(param.Pos) >= paramCount {
			continue
		}
		if dependencies == nil {
			dependencies = make([]bool, paramCount)
		}
		dependencies[param.Pos] = true
	}
	return dependencies
}
