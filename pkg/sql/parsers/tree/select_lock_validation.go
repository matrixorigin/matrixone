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

package tree

import (
	"fmt"
	"reflect"
	"strings"
)

// ValidateSelectLocking follows query-expression contextualization, not lexical
// nesting alone. WITH replaces the expression's local scope; its definitions are
// itemized only when referenced. No catalog lookup or query execution occurs.
func ValidateSelectLocking(stmt Statement) string {
	return validateSelectLockValue(reflect.ValueOf(stmt), nil)
}

// Each real query owns a frame. Parentheses and set operands share that frame,
// so a WITH encountered in the body remains effective for later operands and
// ORDER BY. A true nested query gets a new frame and cannot mutate its parent.
type selectLockScope struct {
	outer *selectLockScope
	with  *selectLockWith
	limit int
}

type selectLockWith struct {
	bindings  map[string]*selectLockCTE
	outer     *selectLockScope
	recursive bool
}

type selectLockCTE struct {
	stmt  Statement
	owner *selectLockWith
	index int
	// A definition is itemized at most once. Mark before descending so a
	// recursive reference cannot repeatedly expand its own definition.
	visited bool
}

func (scope *selectLockScope) setWith(with *With) {
	if with == nil {
		return
	}
	local := &selectLockWith{
		bindings: make(map[string]*selectLockCTE, len(with.CTEs)),
		outer:    scope.outer, recursive: with.IsRecursive,
	}
	for i, cte := range with.CTEs {
		local.bindings[strings.ToLower(string(cte.Name.Alias))] = &selectLockCTE{
			stmt: cte.Stmt, owner: local, index: i,
		}
	}
	scope.with, scope.limit = local, len(with.CTEs)
}

func (scope *selectLockScope) lookup(table *TableName) *selectLockCTE {
	if table.ExplicitSchema || table.ExplicitCatalog {
		return nil
	}
	name := strings.ToLower(string(table.ObjectName))
	if name == "dual" && !table.IsQuoted {
		// The virtual FROM DUAL form cannot reference a CTE named `dual`.
		return nil
	}
	for current := scope; current != nil; current = current.outer {
		if current.with != nil {
			cte := current.with.bindings[name]
			if cte != nil && (cte.index < current.limit || cte.index == current.limit && current.with.recursive) {
				return cte
			}
		}
	}
	return nil
}

func validateSelectLockValue(value reflect.Value, scope *selectLockScope) string {
	if !value.IsValid() {
		return ""
	}
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return ""
		}
		if value.CanInterface() {
			switch node := value.Interface().(type) {
			case *ExplainStmt, *ExplainAnalyze, *ExplainPhyPlan, *With:
				// EXPLAIN skips locking; unused CTE definitions skip itemization.
				return ""
			case *Select:
				return validateSelectLockQuery(node, &selectLockScope{outer: scope}, 0)
			case *Update:
				return validateSelectLockDML(value.Elem(), node.With, scope, false)
			case *Delete:
				return validateSelectLockDML(value.Elem(), node.With, scope, len(node.TableRefs) > 0)
			case *TableName:
				if cte := scope.lookup(node); cte != nil && !cte.visited {
					cte.visited = true
					definition := &selectLockScope{outer: cte.owner.outer, with: cte.owner, limit: cte.index}
					if err := validateSelectLockValue(reflect.ValueOf(cte.stmt), definition); err != "" {
						return err
					}
				}
				// Keep visiting timestamp/snapshot expressions on the table even
				// when its CTE body was already itemized.
			case *SampleExpr:
				return validateSelectLockValue(reflect.ValueOf(node.columns), scope)
			}
		}
		return validateSelectLockValue(value.Elem(), scope)
	}
	switch value.Kind() {
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if err := validateSelectLockValue(value.Field(i), scope); err != "" {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := validateSelectLockValue(value.Index(i), scope); err != "" {
				return err
			}
		}
	}
	return ""
}

func validateSelectLockDML(value reflect.Value, with *With, outer *selectLockScope, deleteAliases bool) string {
	scope := &selectLockScope{outer: outer}
	scope.setWith(with)
	for i := 0; i < value.NumField(); i++ {
		// Multi-table DELETE's target names refer to FROM aliases, not CTE
		// definitions. Only its TableRefs introduce actual table references.
		if deleteAliases && value.Type().Field(i).Name == "Tables" {
			continue
		}
		if err := validateSelectLockValue(value.Field(i), scope); err != "" {
			return err
		}
	}
	return ""
}

func validateSelectLockQuery(stmt SelectStatement, scope *selectLockScope, locks int) string {
	// Iterate the right/parenthesis spine; defer each layer's trailing clauses
	// until its body has established the effective WITH. Each field is visited
	// once, without cloning the accumulated CTE environment.
	var layers []*Select
	for {
		switch node := stmt.(type) {
		case *Select:
			scope.setWith(node.With)
			if node.SelectLockInfo != nil {
				locks++
			}
			layers = append(layers, node)
			stmt = node.Select
		case *ParenSelect:
			stmt = node.Select
		case *UnionClause:
			if err := validateSelectLockQuery(node.Left, scope, 0); err != "" {
				return err
			}
			stmt = node.Right
		default:
			if clause, ok := stmt.(*SelectClause); ok && locks > 1 && clause.From != nil {
				for _, table := range clause.From.Tables {
					if name := directSelectLockTable(table, scope, true); name != "" {
						return fmt.Sprintf("Table %s appears in multiple locking clauses.", name)
					}
				}
			}
			if err := validateSelectLockValue(reflect.ValueOf(stmt), scope); err != "" {
				return err
			}
			for i := len(layers) - 1; i >= 0; i-- {
				value := reflect.ValueOf(layers[i]).Elem()
				typ := value.Type()
				for j := 0; j < value.NumField(); j++ {
					if name := typ.Field(j).Name; name == "Select" || name == "With" {
						continue
					}
					if err := validateSelectLockValue(value.Field(j), scope); err != "" {
						return err
					}
				}
			}
			return ""
		}
	}
}

func directSelectLockTable(table TableExpr, scope *selectLockScope, allowDual bool) string {
	switch node := table.(type) {
	case *TableName:
		name := string(node.ObjectName)
		if !node.ExplicitSchema && !node.ExplicitCatalog {
			if allowDual && !node.IsQuoted && strings.EqualFold(name, "dual") {
				return ""
			}
			if scope.lookup(node) != nil {
				return ""
			}
		}
		return name
	case *AliasedTableExpr:
		return directSelectLockTable(node.Expr, scope, allowDual && node.As.Alias == "")
	case *ParenTableExpr:
		return directSelectLockTable(node.Expr, scope, false)
	case *JoinTableExpr:
		if name := directSelectLockTable(node.Left, scope, allowDual && node.Right == nil); name != "" {
			return name
		}
		return directSelectLockTable(node.Right, scope, false)
	case *ApplyTableExpr:
		if name := directSelectLockTable(node.Left, scope, false); name != "" {
			return name
		}
		return directSelectLockTable(node.Right, scope, false)
	}
	return ""
}
