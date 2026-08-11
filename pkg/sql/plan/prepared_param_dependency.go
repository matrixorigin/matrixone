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
	}
}

func (r *preparedParamCommonTypeDependencyRule) collectDirectParams(expr *Expr) {
	if expr == nil {
		return
	}
	switch impl := expr.Expr.(type) {
	case *planpb.Expr_P:
		if expr.Typ.Enumvalues == "mo_decimal_common_type_dependency" {
			r.positions[impl.P.Pos] = struct{}{}
		}
	case *planpb.Expr_F:
		functionID, _ := function.DecodeOverloadID(impl.F.Func.Obj)
		if functionID == function.CAST && len(impl.F.Args) == 2 && impl.F.Args[1].Typ.Charset == 255 {
			r.collectDirectParams(impl.F.Args[0])
		}
	case *planpb.Expr_List:
		for _, item := range impl.List.List {
			r.collectDirectParams(item)
		}
	}
}

// PreparedParamCommonTypeDependencies returns the parameter positions whose
// runtime category participates directly in DECIMAL-aware common-type binding.
func PreparedParamCommonTypeDependencies(p *Plan, paramCount int) []bool {
	if p == nil || paramCount <= 0 {
		return nil
	}
	rule := &preparedParamCommonTypeDependencyRule{positions: make(map[int32]struct{})}
	if err := NewVisitPlan(p, []VisitPlanRule{rule}).Visit(context.Background()); err != nil {
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
