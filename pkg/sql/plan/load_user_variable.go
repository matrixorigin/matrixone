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

package plan

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type loadUserVariableNullIf struct {
	target   string
	variable string
	expr     tree.Expr
}

type loadUserVariableScanColumn struct {
	name string
	pos  int32
}

// loadNullIfFunction recognizes supported NULLIF assignments without discarding
// wrappers that must still be validated by the expression binder.
func loadNullIfFunction(expr tree.Expr) (*tree.FuncExpr, bool) {
	collated := false
	for {
		switch node := expr.(type) {
		case *tree.CollateExpr:
			collated = true
			expr = node.Expr
		case *tree.ParenExpr:
			expr = node.Expr
		case *tree.FuncExpr:
			name, ok := node.Func.FunctionReference.(*tree.UnresolvedName)
			if ok && strings.EqualFold(name.ColName(), "nullif") && len(node.Exprs) == 2 {
				return node, collated
			}
			return nil, collated
		default:
			return nil, collated
		}
	}
}

// Copy only the validated wrapper path and function arguments. The parsed AST
// belongs to the caller, while binding needs a scan-column reference instead of
// the LOAD row variable.
func copyLoadNullIfWithSource(expr tree.Expr, source string) tree.Expr {
	switch node := expr.(type) {
	case *tree.CollateExpr:
		copied := *node
		copied.Expr = copyLoadNullIfWithSource(node.Expr, source)
		return &copied
	case *tree.ParenExpr:
		copied := *node
		copied.Expr = copyLoadNullIfWithSource(node.Expr, source)
		return &copied
	case *tree.FuncExpr:
		copied := *node
		copied.Exprs = append(tree.Exprs(nil), node.Exprs...)
		copied.Exprs[0] = tree.NewUnresolvedColName(source)
		return &copied
	default:
		return expr
	}
}

func loadUserVariableNullIfAssignments(param *tree.ExternParam, ctx CompilerContext) ([]loadUserVariableNullIf, error) {
	if param == nil || param.Tail == nil {
		return nil, nil
	}

	assignments := make([]loadUserVariableNullIf, 0)
	byTarget := make(map[string]int)
	for _, assignment := range param.Tail.Assignments {
		if assignment == nil {
			continue
		}
		expr, _ := loadNullIfFunction(assignment.Expr)
		if expr == nil {
			continue
		}
		variable, ok := expr.Exprs[0].(*tree.VarExpr)
		if !ok {
			continue
		}
		if variable.System {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "the nullif func first param is not UnresolvedName form")
		}
		if _, ok := expr.Exprs[1].(*tree.NumVal); !ok {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "the nullif func first param is not UnresolvedName form")
		}
		if len(assignment.Names) != 1 || assignment.Names[0] == nil {
			return nil, moerr.NewNotSupported(ctx.GetContext(), "LOAD DATA SET with row variables requires one target column")
		}
		item := loadUserVariableNullIf{
			target:   assignment.Names[0].ColName(),
			variable: variable.Name,
			expr:     assignment.Expr,
		}
		key := strings.ToLower(item.target)
		if index, ok := byTarget[key]; ok {
			assignments[index] = item
		} else {
			byTarget[key] = len(assignments)
			assignments = append(assignments, item)
		}
	}
	return assignments, nil
}

func appendLoadUserVariableScanColumns(
	columnList []tree.LoadColumn,
	assignments []loadUserVariableNullIf,
	targetTable *plan.TableDef,
	scanTable *plan.TableDef,
	tbColToDataCol *map[string]int32,
	projectList *[]*plan.Expr,
) map[string]loadUserVariableScanColumn {
	if scanTable.Name2ColIndex == nil {
		scanTable.Name2ColIndex = make(map[string]int32)
	}
	if *tbColToDataCol == nil {
		*tbColToDataCol = make(map[string]int32)
	}

	referenced := make(map[string]struct{}, len(assignments))
	for _, assignment := range assignments {
		referenced[strings.ToLower(assignment.variable)] = struct{}{}
	}
	lastSource := make(map[string]int, len(referenced))
	for i, column := range columnList {
		variable, ok := column.(*tree.VarExpr)
		if !ok {
			continue
		}
		key := strings.ToLower(variable.Name)
		if _, ok := referenced[key]; ok {
			lastSource[key] = i
		}
	}

	sources := make(map[string]loadUserVariableScanColumn, len(referenced))
	for i, column := range columnList {
		variable, ok := column.(*tree.VarExpr)
		if !ok {
			continue
		}
		key := strings.ToLower(variable.Name)
		if sourceIndex, needed := lastSource[key]; !needed || sourceIndex != i {
			name := uniqueLoadVariableScanName(targetTable, scanTable, *tbColToDataCol, fmt.Sprintf("__mo_load_skip_var_%d", i))
			(*tbColToDataCol)[name] = -1
			continue
		}

		name := uniqueLoadVariableScanName(targetTable, scanTable, *tbColToDataCol, fmt.Sprintf("__mo_load_user_var_%d", i))
		position := int32(len(scanTable.Cols))
		// Row variables carry raw string fields, not target-width VARCHARs.
		// Preserve the full supported string domain through intermediate casts
		// (for example COLLATE); only the final assignment enforces target size.
		columnType := makeGeneratedPlan2Type(types.T_text, types.MaxLongTextLen, 0, false)
		scanTable.Cols = append(scanTable.Cols, &plan.ColDef{Name: name, Typ: columnType})
		scanTable.Name2ColIndex[name] = position
		(*tbColToDataCol)[name] = int32(i)
		if projectList != nil {
			*projectList = append(*projectList, &plan.Expr{
				Typ: columnType,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					ColPos: position,
					Name:   name,
				}},
			})
		}
		sources[key] = loadUserVariableScanColumn{name: name, pos: position}
	}
	return sources
}

func uniqueLoadVariableScanName(targetTable, scanTable *plan.TableDef, mapping map[string]int32, base string) string {
	for name := base; ; name += "_" {
		used := false
		for key := range mapping {
			if strings.EqualFold(key, name) {
				used = true
				break
			}
		}
		if !used {
			for _, table := range []*plan.TableDef{targetTable, scanTable} {
				if table == nil {
					continue
				}
				for _, column := range table.Cols {
					if column != nil && strings.EqualFold(column.Name, name) {
						used = true
						break
					}
				}
				if used {
					break
				}
			}
		}
		if !used {
			return name
		}
	}
}

func (builder *QueryBuilder) bindLoadUserVariableAssignments(
	stmt *tree.Load,
	assignments []loadUserVariableNullIf,
	targetTable *plan.TableDef,
	scanTable *plan.TableDef,
	sources map[string]loadUserVariableScanColumn,
	externalScanTag int32,
) (map[string]*plan.Expr, error) {
	result := make(map[string]*plan.Expr, len(assignments))
	for _, assignment := range assignments {
		var target *plan.ColDef
		for _, column := range targetTable.Cols {
			if column != nil && strings.EqualFold(column.Name, assignment.target) {
				target = column
				break
			}
		}
		if target == nil {
			return nil, moerr.NewBadFieldErrorf(builder.GetContext(), "internal error: column '%s' does not exist", assignment.target)
		}
		if target.Hidden || target.GeneratedCol != nil {
			return nil, moerr.NewInvalidInputf(builder.GetContext(),
				"the value specified for generated column '%s' in table '%s' is not allowed",
				assignment.target, targetTable.Name)
		}
		for column := range stmt.Param.NullMap {
			if strings.EqualFold(column, assignment.target) {
				return nil, moerr.NewInvalidInputf(builder.GetContext(),
					"LOAD DATA SET target '%s' cannot combine column and row-variable NULLIF", assignment.target)
			}
		}
		source, ok := sources[strings.ToLower(assignment.variable)]
		if !ok {
			return nil, moerr.NewInvalidInputf(builder.GetContext(),
				"LOAD DATA SET variable '@%s' is not present in the column list", assignment.variable)
		}

		expression := copyLoadNullIfWithSource(assignment.expr, source.name)
		bound, err := NewDefaultBinderWithColumns(builder.GetContext(), plan.Type{}, scanTable.Cols).BindExpr(expression, 0, false)
		if err != nil {
			return nil, err
		}
		casted, err := builder.forceAssignmentCastExpr(bound, target.Typ, loadAssignmentIgnore(stmt))
		if err != nil {
			return nil, err
		}
		replaceColRefTag(casted, 0, externalScanTag)
		result[target.Name] = casted
	}
	return result, nil
}
