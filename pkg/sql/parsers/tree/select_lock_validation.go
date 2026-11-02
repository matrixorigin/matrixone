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
	"unicode/utf8"
)

// ValidateSelectContext shares query-expression contextualization between lock
// checks and digest-only table-name checks. WITH definitions are itemized only
// on reference. Neither check resolves a catalog or executes a query.
func ValidateSelectContext(stmt Statement, nameKey func(string) string, locks, tableNames bool) string {
	return validateSelectLockValue(reflect.ValueOf(stmt), &selectLockScope{
		nameKey: nameKey, checkLocks: locks, checkTableNames: tableNames,
	})
}

// ValidMySQLTableName models the system-charset name check, not the digest's
// independent 64-byte display limit. Supplementary characters convert to one
// replacement character before itemization, so they still count as one here.
func ValidMySQLTableName(name string) bool {
	return name != "" && !strings.ContainsRune(" \t\r\n\v\f", rune(name[len(name)-1])) &&
		(len(name) <= 64 || utf8.RuneCountInString(name) <= 64)
}

func validateDigestTableName(name *TableName, alias bool) string {
	if !alias && !ValidMySQLTableName(string(name.ObjectName)) ||
		name.ExplicitSchema && !ValidMySQLTableName(string(name.SchemaName)) {
		return "Invalid table or database name."
	}
	return ""
}

func validateDigestShowTableName(table *UnresolvedObjectName, database string) string {
	if table == nil {
		return ""
	}
	name := table.ToTableName()
	if database != "" {
		// SHOW's trailing FROM database replaces the table's own qualifier.
		name.SchemaName, name.ExplicitSchema = Identifier(database), true
	}
	return validateDigestTableName(&name, false)
}

// Each real query owns a frame. Parentheses and set operands share that frame,
// so a WITH encountered in the body remains effective for later operands and
// ORDER BY. A true nested query gets a new frame and cannot mutate its parent.
type selectLockScope struct {
	outer            *selectLockScope
	with             *selectLockWith
	limit            int
	nameKey          func(string) string
	checkLocks       bool
	checkTableNames  bool
	checkViewColumns bool
}

func (scope *selectLockScope) child() *selectLockScope {
	return &selectLockScope{outer: scope, nameKey: scope.nameKey,
		checkLocks: scope.checkLocks, checkTableNames: scope.checkTableNames,
		checkViewColumns: scope.checkViewColumns}
}

func validateExplainTableNames(stmt Statement, scope *selectLockScope) string {
	if !scope.checkTableNames {
		return ""
	}
	inner := *scope
	inner.checkLocks = false // EXPLAIN still contextualizes names, but not locks.
	return validateSelectLockValue(reflect.ValueOf(stmt), &inner)
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
		local.bindings[scope.nameKey(string(cte.Name.Alias))] = &selectLockCTE{
			stmt: cte.Stmt, owner: local, index: i,
		}
	}
	scope.with, scope.limit = local, len(with.CTEs)
}

func (scope *selectLockScope) lookup(table *TableName) *selectLockCTE {
	if table.ExplicitSchema || table.ExplicitCatalog {
		return nil
	}
	name := scope.nameKey(string(table.ObjectName))
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
			case *CreateView:
				return validateDigestViewNames(value.Elem(), node.ColNames, scope)
			case *AlterView:
				return validateDigestViewNames(value.Elem(), node.ColNames, scope)
			case *ExplainStmt:
				return validateExplainTableNames(node.Statement, scope)
			case *ExplainAnalyze:
				return validateExplainTableNames(node.Statement, scope)
			case *ExplainPhyPlan:
				return validateExplainTableNames(node.Statement, scope)
			case *With:
				// Unused CTE definitions are not itemized.
				return ""
			case *AttributeReference:
				// Inline REFERENCES does not check its table name. Preserve
				// the ordinary lock walk over any other expression children.
				inner := *scope
				inner.checkTableNames = false
				return validateSelectLockValue(value.Elem(), &inner)
			case *ForeignKey:
				if scope.checkTableNames && node.Refer != nil && node.Refer.TableName != nil {
					if err := validateDigestTableName(node.Refer.TableName, false); err != "" {
						return err
					}
				}
			case *AlterOptionTableName:
				if scope.checkTableNames && node.Name != nil {
					name := node.Name.ToTableName()
					if err := validateDigestTableName(&name, false); err != "" {
						return err
					}
				}
			case *ShowColumns:
				if scope.checkTableNames {
					if err := validateDigestShowTableName(node.Table, node.DBName); err != "" {
						return err
					}
				}
			case *ShowIndex:
				if scope.checkTableNames {
					if err := validateDigestShowTableName(node.TableName, node.DbName); err != "" {
						return err
					}
				}
			case *Select:
				return validateSelectLockQuery(node, scope.child(), 0)
			case *Update:
				return validateSelectLockDML(value.Elem(), node.With, scope, false)
			case *Delete:
				if scope.checkTableNames && len(node.TableRefs) > 0 {
					for _, target := range node.Tables {
						if name, ok := target.(*TableName); ok {
							if err := validateDigestTableName(name, true); err != "" {
								return err
							}
						}
					}
				}
				return validateSelectLockDML(value.Elem(), node.With, scope, len(node.TableRefs) > 0)
			case *SampleExpr:
				return validateSelectLockValue(reflect.ValueOf(node.columns), scope)
			}
		}
		return validateSelectLockValue(value.Elem(), scope)
	}
	switch value.Kind() {
	case reflect.Struct:
		if value.CanInterface() {
			node := value.Interface()
			if column, ok := node.(SelectExpr); ok && scope.checkViewColumns &&
				column.As != nil && (column.As.Origin() != "" || column.As.Quoted) && !validDigestViewColumnName(column.As.Origin()) {
				return "Invalid view column name."
			}
			// Empty unquoted nodes represent implicit no-FROM inputs and
			// unset optional DDL names. A real empty quoted name is distinct.
			if name, ok := node.(TableName); ok &&
				(name.ObjectName != "" || name.IsQuoted || name.ExplicitSchema || name.ExplicitCatalog) {
				if scope.checkTableNames {
					if err := validateDigestTableName(&name, false); err != "" {
						return err
					}
				}
				if cte := scope.lookup(&name); cte != nil && !cte.visited {
					cte.visited = true
					definition := *scope
					definition.outer, definition.with, definition.limit = cte.owner.outer, cte.owner, cte.index
					if err := validateSelectLockValue(reflect.ValueOf(cte.stmt), &definition); err != "" {
						return err
					}
				}
				// Snapshot expressions remain visible after CTE itemization.
			}
		}
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

func validDigestViewColumnName(name string) bool {
	// A view's STRING alias can contain NUL even though SQL quoted
	// identifiers cannot. MySQL checks only its C-string name prefix.
	name, _, _ = strings.Cut(name, "\x00")
	return ValidMySQLTableName(name)
}

func validateDigestViewNames(value reflect.Value, columns IdentifierList, scope *selectLockScope) string {
	inner := *scope
	if scope.checkTableNames {
		for _, column := range columns {
			if !validDigestViewColumnName(string(column)) {
				return "Invalid view column name."
			}
		}
		// Like table references, SELECT aliases inside CTE definitions are
		// checked only when itemized. The explicit view list is checked above.
		inner.checkViewColumns = true
	}
	return validateSelectLockValue(value, &inner)
}

func validateSelectLockDML(value reflect.Value, with *With, outer *selectLockScope, deleteAliases bool) string {
	scope := outer.child()
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
			if clause, ok := stmt.(*SelectClause); ok && scope.checkLocks && locks > 1 && clause.From != nil {
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
