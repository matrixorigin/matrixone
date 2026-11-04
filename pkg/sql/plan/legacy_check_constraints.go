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
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// LegacyCheckConstraint is recovered from the original SQL of a pre-structured catalog row.
type LegacyCheckConstraint struct {
	Name     string
	Clause   string
	Enforced string
}

// RecoverLegacyCheckConstraints recovers metadata for catalog rows written
// before SchemaExtra.Checks was introduced.  Those rows retain the original
// CREATE TABLE statement in rel_createsql.  Parsing the AST (instead of using
// string slicing) also covers column-level CHECK definitions.
func RecoverLegacyCheckConstraints(
	ctx context.Context,
	createSQL string,
) ([]LegacyCheckConstraint, error) {
	rows, _, err := recoverLegacyCheckConstraints(ctx, createSQL)
	return rows, err
}

// Return one verified parser mode as well, so token-preserving edits can use
// the same interpretation as the existing metadata authority.
func recoverLegacyCheckConstraints(ctx context.Context, createSQL string) ([]LegacyCheckConstraint, string, error) {
	if strings.TrimSpace(createSQL) == "" ||
		!strings.Contains(strings.ToUpper(createSQL), "CHECK") {
		return nil, "", nil
	}
	var canonical []LegacyCheckConstraint
	var firstErr error
	parsed := 0
	var canonicalMode string
	for _, sqlMode := range mysql.ParserSQLModeCombinations() {
		stmt, err := parsers.ParseOneWithSQLMode(ctx, dialect.MYSQL, createSQL, 1, sqlMode)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		createStmt, ok := stmt.(*tree.CreateTable)
		if !ok {
			stmt.Free()
			continue
		}
		rows := legacyCheckRowsFromCreateTable(createStmt)
		stmt.Free()
		if parsed == 0 {
			canonical = rows
			canonicalMode = sqlMode
		} else if !equalLegacyCheckRows(canonical, rows) {
			return nil, "", moerr.NewInvalidInput(ctx,
				"cannot recover legacy CHECK constraints with ambiguous SQL mode")
		}
		parsed++
	}
	if parsed == 0 {
		return nil, "", firstErr
	}
	return canonical, canonicalMode, nil
}

func legacyCheckRowsFromCreateTable(
	createStmt *tree.CreateTable,
) []LegacyCheckConstraint {
	rows := make([]LegacyCheckConstraint, 0, len(createStmt.Defs))
	for _, def := range createStmt.Defs {
		switch typedDef := def.(type) {
		case *tree.CheckIndex:
			rows = append(rows, legacyCheckConstraintRow(
				typedDef.ConstraintSymbol, typedDef.Expr, typedDef.Enforced, len(rows)+1))
		case *tree.ColumnTableDef:
			for _, attr := range typedDef.Attributes {
				check, ok := attr.(*tree.AttributeCheckConstraint)
				if !ok {
					continue
				}
				rows = append(rows, legacyCheckConstraintRow(
					check.Name, check.Expr, check.Enforced, len(rows)+1))
			}
		}
	}
	return rows
}

func equalLegacyCheckRows(left, right []LegacyCheckConstraint) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Name != right[i].Name ||
			left[i].Clause != right[i].Clause ||
			left[i].Enforced != right[i].Enforced {
			return false
		}
	}
	return true
}

func legacyCheckConstraintRow(
	name string,
	expr tree.Expr,
	enforced bool,
	ordinal int,
) LegacyCheckConstraint {
	if name == "" {
		name = fmt.Sprintf("__mo_chk_%d", ordinal)
	}
	state := "NO"
	if enforced {
		state = "YES"
	}
	return LegacyCheckConstraint{
		Name:     name,
		Clause:   formatLegacyCheckConstraintExpr(expr),
		Enforced: state,
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
