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

package table_function

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// parseLegacyCheckConstraintRows recovers metadata for catalog rows written
// before SchemaExtra.Checks was introduced.  Those rows retain the original
// CREATE TABLE statement in rel_createsql.  Parsing the AST (instead of using
// string slicing) also covers column-level CHECK definitions.
func parseLegacyCheckConstraintRows(
	ctx context.Context,
	schema string,
	table string,
	createSQL string,
) ([]checkConstraintRow, error) {
	if strings.TrimSpace(createSQL) == "" ||
		!strings.Contains(strings.ToUpper(createSQL), "CHECK") {
		return nil, nil
	}
	stmt, err := parsers.ParseOneWithSQLMode(ctx, dialect.MYSQL, createSQL, 1, "")
	if err != nil {
		return nil, err
	}
	defer stmt.Free()
	createStmt, ok := stmt.(*tree.CreateTable)
	if !ok {
		return nil, nil
	}

	rows := make([]checkConstraintRow, 0, len(createStmt.Defs))
	for _, def := range createStmt.Defs {
		switch typedDef := def.(type) {
		case *tree.CheckIndex:
			rows = append(rows, legacyCheckConstraintRow(
				schema, table, typedDef.ConstraintSymbol, typedDef.Expr, typedDef.Enforced, len(rows)+1))
		case *tree.ColumnTableDef:
			for _, attr := range typedDef.Attributes {
				check, ok := attr.(*tree.AttributeCheckConstraint)
				if !ok {
					continue
				}
				rows = append(rows, legacyCheckConstraintRow(
					schema, table, check.Name, check.Expr, check.Enforced, len(rows)+1))
			}
		}
	}
	return rows, nil
}

func legacyCheckConstraintRow(
	schema string,
	table string,
	name string,
	expr tree.Expr,
	enforced bool,
	ordinal int,
) checkConstraintRow {
	if name == "" {
		name = fmt.Sprintf("__mo_chk_%d", ordinal)
	}
	state := "NO"
	if enforced {
		state = "YES"
	}
	return checkConstraintRow{
		schema:         schema,
		table:          table,
		name:           name,
		clause:         formatLegacyCheckConstraintExpr(expr),
		constraintType: "CHECK",
		enforced:       state,
	}
}

func formatLegacyCheckConstraintExpr(expr tree.Expr) string {
	if expr == nil {
		return ""
	}
	ctx := tree.NewFmtCtx(
		dialect.MYSQL,
		tree.WithSingleQuoteString(),
		tree.WithQuoteIdentifier(),
		tree.WithModeIndependentStringLiterals(),
	)
	expr.Format(ctx)
	return ctx.String()
}
