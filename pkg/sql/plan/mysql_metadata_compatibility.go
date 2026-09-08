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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// rewriteMySQLMetadataCompatibility completes the natural key between
// information_schema.KEY_COLUMN_USAGE and TABLE_CONSTRAINTS. Constraint and
// table names are scoped by schema, so joining these views without
// CONSTRAINT_SCHEMA can multiply rows when schemas reuse the same names.
func rewriteMySQLMetadataCompatibility(stmt *tree.Select) {
	clause, ok := stmt.Select.(*tree.SelectClause)
	if !ok || clause.From == nil {
		return
	}
	for _, table := range clause.From.Tables {
		rewriteInformationSchemaConstraintJoin(table)
	}
}

func rewriteInformationSchemaConstraintJoin(table tree.TableExpr) {
	join, ok := table.(*tree.JoinTableExpr)
	if !ok {
		return
	}

	rewriteInformationSchemaConstraintJoin(join.Left)
	rewriteInformationSchemaConstraintJoin(join.Right)

	using, ok := join.Cond.(*tree.UsingJoinCond)
	if !ok || len(using.Cols) != 2 ||
		!identifierListContains(using.Cols, "constraint_name") ||
		!identifierListContains(using.Cols, "table_name") {
		return
	}

	leftSchema, leftTable, leftOK := tableIdentity(join.Left)
	rightSchema, rightTable, rightOK := tableIdentity(join.Right)
	if !leftOK || !rightOK ||
		!strings.EqualFold(leftSchema, "information_schema") ||
		!strings.EqualFold(leftTable, "key_column_usage") ||
		!strings.EqualFold(rightSchema, "information_schema") ||
		!strings.EqualFold(rightTable, "table_constraints") {
		return
	}

	using.Cols = append(tree.IdentifierList{tree.Identifier("constraint_schema")}, using.Cols...)
}

func tableIdentity(table tree.TableExpr) (schema, name string, ok bool) {
	if aliased, aliasedOK := table.(*tree.AliasedTableExpr); aliasedOK {
		table = aliased.Expr
	}
	tableName, ok := table.(*tree.TableName)
	if !ok || !tableName.ExplicitSchema {
		return "", "", false
	}
	return string(tableName.Schema()), string(tableName.Name()), true
}

func identifierListContains(columns tree.IdentifierList, name string) bool {
	for _, column := range columns {
		if strings.EqualFold(string(column), name) {
			return true
		}
	}
	return false
}
