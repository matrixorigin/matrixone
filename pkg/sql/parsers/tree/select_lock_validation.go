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

// ValidateSelectLocking checks complete query blocks after their WITH scope is
// known. Parenthesis wrappers share a block; derived/CTE/scalar query bodies do
// not. Only direct base tables receive these clauses, not DUAL or derived/CTE
// references. No catalog lookup or query execution is involved.
func ValidateSelectLocking(stmt Statement) string {
	return validateSelectLockValue(reflect.ValueOf(stmt), make(map[string]int))
}

func validateSelectLockValue(value reflect.Value, ctes map[string]int) string {
	if !value.IsValid() {
		return ""
	}
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return ""
		}
		if value.CanInterface() {
			switch node := value.Interface().(type) {
			case *ExplainStmt, *ExplainAnalyze, *ExplainPhyPlan:
				// MySQL does not contextualize locking clauses in EXPLAIN mode,
				// including ANALYZE. Other syntax validation still applies.
				return ""
			case *Select:
				return validateSelectLockQuery(node, ctes, 0)
			case *SampleExpr:
				// SampleExpr owns private expression fields. Do not lose their queries
				// by reflecting through fields which cannot be interfaced.
				return validateSelectLockValue(reflect.ValueOf(node.columns), ctes)
			}
		}
		return validateSelectLockValue(value.Elem(), ctes)
	}
	switch value.Kind() {
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if err := validateSelectLockValue(value.Field(i), ctes); err != "" {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := validateSelectLockValue(value.Index(i), ctes); err != "" {
				return err
			}
		}
	}
	return ""
}

func validateSelectLockQuery(query *Select, ctes map[string]int, locks int) string {
	var introduced []string
	// Share one invocation-local scope map rather than cloning the accumulated
	// CTE set at every depth. Restore bindings on every return/error path.
	restore := func() {
		for _, name := range introduced {
			ctes[name]--
			if ctes[name] == 0 {
				delete(ctes, name)
			}
		}
		introduced = introduced[:0]
	}
	defer restore()
	for {
		if query.With != nil {
			// Parentheses and set operands still own the same query expression.
			// A later WITH replaces its local bindings, not the enclosing query's.
			restore()
			for _, cte := range query.With.CTEs {
				name := strings.ToLower(string(cte.Name.Alias))
				if query.With.IsRecursive {
					ctes[name]++
					introduced = append(introduced, name)
				}
				if err := validateSelectLockValue(reflect.ValueOf(cte.Stmt), ctes); err != "" {
					return err
				}
				if !query.With.IsRecursive {
					ctes[name]++
					introduced = append(introduced, name)
				}
			}
		}
		if query.SelectLockInfo != nil {
			locks++
		}
		// Visit sibling expressions in this layer's CTE scope, but handle the
		// parenthesis chain iteratively so deeply grouped queries stay linear.
		value := reflect.ValueOf(query).Elem()
		typ := value.Type()
		for i := 0; i < value.NumField(); i++ {
			if name := typ.Field(i).Name; name == "Select" || name == "With" {
				continue
			}
			if err := validateSelectLockValue(value.Field(i), ctes); err != "" {
				return err
			}
		}
		body := query.Select
		for {
			switch node := body.(type) {
			case *ParenSelect:
				query = node.Select
			case *Select:
				query = node
			case *UnionClause:
				// Contextualization leaves the final operand as the active block.
				// Earlier operands and true nested queries do not inherit this lock.
				if err := validateSelectLockValue(reflect.ValueOf(node.Left), ctes); err != "" {
					return err
				}
				body = node.Right
				continue
			default:
				return validateSelectLockBody(body, ctes, locks)
			}
			break
		}
	}
}

func validateSelectLockBody(body SelectStatement, ctes map[string]int, locks int) string {
	if clause, ok := body.(*SelectClause); ok && locks > 1 && clause.From != nil {
		for _, table := range clause.From.Tables {
			if name := directSelectLockTable(table, ctes, true); name != "" {
				return fmt.Sprintf("Table %s appears in multiple locking clauses.", name)
			}
		}
	}
	return validateSelectLockValue(reflect.ValueOf(body), ctes)
}

func directSelectLockTable(table TableExpr, ctes map[string]int, allowDual bool) string {
	switch node := table.(type) {
	case *TableName:
		name := string(node.ObjectName)
		if !node.ExplicitSchema && !node.ExplicitCatalog {
			if allowDual && !node.IsQuoted && strings.EqualFold(name, "dual") {
				return ""
			}
			if ctes[strings.ToLower(name)] > 0 {
				return ""
			}
		}
		return name
	case *AliasedTableExpr:
		return directSelectLockTable(node.Expr, ctes, allowDual && node.As.Alias == "")
	case *ParenTableExpr:
		return directSelectLockTable(node.Expr, ctes, false)
	case *JoinTableExpr:
		if name := directSelectLockTable(node.Left, ctes, allowDual && node.Right == nil); name != "" {
			return name
		}
		return directSelectLockTable(node.Right, ctes, false)
	case *ApplyTableExpr:
		if name := directSelectLockTable(node.Left, ctes, false); name != "" {
			return name
		}
		return directSelectLockTable(node.Right, ctes, false)
	}
	return ""
}
