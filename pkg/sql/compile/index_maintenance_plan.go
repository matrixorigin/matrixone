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

package compile

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// inspectIndexMaintenanceStatement accepts only the fixed SELECT shape emitted
// by an index plugin: one TVF, optionally CROSS APPLY'd to its owned base.
func inspectIndexMaintenanceStatement(stmt tree.Statement, spec *executor.IndexMaintenanceSpec, validator executor.IndexMaintenanceValidator) ([]string, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	if validator == nil {
		return nil, moerr.NewInvalidInputNoCtx("missing index maintenance validator")
	}
	s, ok := stmt.(*tree.Select)
	if !ok || s.With != nil || s.Limit != nil || len(s.OrderBy) != 0 || s.Ep != nil || len(s.IntoVars) != 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid generated index maintenance statement")
	}
	clause, ok := s.Select.(*tree.SelectClause)
	if !ok || clause.From == nil || len(clause.From.Tables) != 1 || clause.Where != nil || clause.GroupBy != nil || clause.Having != nil || clause.Distinct || len(clause.Exprs) != 1 {
		return nil, moerr.NewInvalidInputNoCtx("invalid generated index maintenance SELECT")
	}
	projection, qualifiedStar := clause.Exprs[0].Expr.(*tree.UnresolvedName)
	_, unqualifiedStar := clause.Exprs[0].Expr.(tree.UnqualifiedStar)
	if !unqualifiedStar && (!qualifiedStar || !projection.Star) {
		return nil, moerr.NewInvalidInputNoCtx("invalid index maintenance projection")
	}
	var fn *tree.TableFunction
	var sourceCount int
	var walk func(tree.TableExpr) error
	walk = func(expr tree.TableExpr) error {
		switch node := expr.(type) {
		case *tree.ApplyTableExpr:
			if node.ApplyType != tree.APPLY_TYPE_CROSS {
				return moerr.NewInvalidInputNoCtx("unexpected index maintenance apply")
			}
			if err := walk(node.Left); err != nil {
				return err
			}
			return walk(node.Right)
		case *tree.JoinTableExpr:
			if node.Right != nil || node.Cond != nil || node.Tolerance != nil {
				return moerr.NewInvalidInputNoCtx("unexpected generated maintenance join")
			}
			return walk(node.Left)
		case *tree.AliasedTableExpr:
			return walk(node.Expr)
		case *tree.ParenTableExpr:
			return walk(node.Expr)
		case *tree.TableName:
			sourceCount++
			if sourceCount != 1 || !strings.EqualFold(string(node.ObjectName), spec.BaseName) || (node.SchemaName != "" && !strings.EqualFold(string(node.SchemaName), spec.Database)) || node.AtTsExpr != nil {
				return moerr.NewInvalidInputNoCtx("unexpected index maintenance source")
			}
			return nil
		case *tree.TableFunction:
			if fn != nil || node.Func == nil || node.SelectStmt != nil || node.JSONTable != nil {
				return moerr.NewInvalidInputNoCtx("duplicate or invalid index maintenance function")
			}
			fn = node
			return nil
		default:
			return moerr.NewInvalidInputNoCtx("unexpected index maintenance source shape")
		}
	}
	if err := walk(clause.From.Tables[0]); err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, moerr.NewInvalidInputNoCtx("missing index maintenance function")
	}
	name, ok := fn.Func.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok || !strings.EqualFold(name.ColName(), spec.Function) {
		return nil, moerr.NewInvalidInputNoCtx("unexpected index maintenance function")
	}
	var literals []string
	seenColumn := false
	for _, arg := range fn.Func.Exprs {
		if value, ok := arg.(*tree.NumVal); ok && value.ValType == tree.P_char && !seenColumn {
			literals = append(literals, value.String())
			continue
		}
		if _, ok := arg.(*tree.UnresolvedName); !ok || sourceCount != 1 {
			return nil, moerr.NewInvalidInputNoCtx("nonliteral index maintenance configuration")
		}
		seenColumn = true
	}
	if err := validator.ValidateIndexMaintenance(spec.Function, literals, spec.Clone()); err != nil {
		return nil, err
	}
	return literals, nil
}

func validateIndexMaintenanceViewProbe(stmt *tree.Select) error {
	if stmt == nil || stmt.With != nil || stmt.Limit != nil || len(stmt.OrderBy) != 0 || stmt.Ep != nil || len(stmt.IntoVars) != 0 {
		return moerr.NewInvalidInputNoCtx("invalid index maintenance view probe")
	}
	clause, ok := stmt.Select.(*tree.SelectClause)
	if !ok || clause.From == nil || len(clause.From.Tables) != 1 || len(clause.Exprs) != 1 || clause.Where != nil || clause.GroupBy != nil || clause.Having != nil || clause.Distinct {
		return moerr.NewInvalidInputNoCtx("invalid index maintenance view probe")
	}
	if _, ok := clause.Exprs[0].Expr.(tree.UnqualifiedStar); !ok {
		return moerr.NewInvalidInputNoCtx("invalid index maintenance view probe")
	}
	expr := clause.From.Tables[0]
	for {
		switch node := expr.(type) {
		case *tree.JoinTableExpr:
			if node.Right != nil || node.Cond != nil {
				return moerr.NewInvalidInputNoCtx("invalid index maintenance view probe")
			}
			expr = node.Left
		case *tree.AliasedTableExpr:
			if node.As.Alias != "" || len(node.IndexHints) != 0 {
				return moerr.NewInvalidInputNoCtx("invalid index maintenance view probe")
			}
			expr = node.Expr
		case *tree.TableName:
			if node.SchemaName == "" || node.ObjectName == "" || node.AtTsExpr != nil || node.IcebergRef != nil {
				return moerr.NewInvalidInputNoCtx("invalid index maintenance view probe")
			}
			return nil
		default:
			return moerr.NewInvalidInputNoCtx("invalid index maintenance view probe")
		}
	}
}

func indexMaintenanceLiteral(expr *plan.Expr) (string, bool) {
	if expr == nil {
		return "", false
	}
	if lit := expr.GetLit(); lit != nil && !lit.Isnull {
		if value, ok := lit.Value.(*plan.Literal_Sval); ok {
			return value.Sval, true
		}
	}
	if fn := expr.GetF(); fn != nil && fn.GetFunc() != nil && strings.HasPrefix(strings.ToLower(fn.GetFunc().ObjName), "cast") && len(fn.Args) != 0 {
		return indexMaintenanceLiteral(fn.Args[0])
	}
	return "", false
}

// selectIndexMaintenanceNode reselects on every plan generation and requires
// the plan's literal configuration to match the validated source statement.
func selectIndexMaintenanceNode(query *plan.Query, spec *executor.IndexMaintenanceSpec, literals []string) (int32, error) {
	if query == nil || spec == nil {
		return 0, moerr.NewInvalidInputNoCtx("missing index maintenance query")
	}
	seen := make(map[int32]bool)
	var matches []int32
	var walk func(int32) error
	walk = func(id int32) error {
		if id < 0 || int(id) >= len(query.Nodes) || query.Nodes[id] == nil {
			return moerr.NewInvalidInputNoCtx("invalid index maintenance plan")
		}
		if seen[id] {
			return nil
		}
		seen[id] = true
		node := query.Nodes[id]
		if node.NodeType == plan.Node_FUNCTION_SCAN && node.TableDef != nil && node.TableDef.TblFunc != nil && strings.EqualFold(node.TableDef.TblFunc.Name, spec.Function) {
			bound := make([]string, 0, len(literals))
			for _, arg := range node.TblFuncExprList {
				value, ok := indexMaintenanceLiteral(arg)
				if !ok {
					break
				}
				bound = append(bound, value)
			}
			matchesPrefix := func(values []string) bool {
				if len(values) < len(literals) {
					return false
				}
				for i := range literals {
					if values[i] != literals[i] {
						return false
					}
				}
				return true
			}
			withParam := append([]string{string(node.TableDef.TblFunc.Param)}, bound...)
			if !matchesPrefix(bound) && !matchesPrefix(withParam) {
				return moerr.NewInvalidInputNoCtx("index maintenance plan configuration changed")
			}
			matches = append(matches, node.NodeId)
		}
		for _, child := range node.Children {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	for _, root := range query.Steps {
		if err := walk(root); err != nil {
			return 0, err
		}
	}
	if len(matches) != 1 {
		return 0, moerr.NewInvalidInputNoCtx("expected one index maintenance plan node")
	}
	return matches[0], nil
}

func (c *Compile) attachIndexMaintenance(node *plan.Node, op *table_function.TableFunction) {
	if c.indexMaintenanceSpec != nil && node != nil && node.NodeId == c.indexMaintenanceNodeID && op != nil {
		op.IndexMaintenance = c.indexMaintenanceSpec.Clone()
	}
}
