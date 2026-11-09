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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type showIndexGeneratedSourcesKey struct{}

type showIndexGeneratedSources map[*tree.TableName]*ObjectRef

func showIndexGeneratedSource(ctx context.Context, table *tree.TableName) *ObjectRef {
	sources, _ := ctx.Value(showIndexGeneratedSourcesKey{}).(showIndexGeneratedSources)
	return sources[table]
}

// collectShowIndexGeneratedSources runs before the user's WHERE is attached.
// The generated SELECT consists of UNION branches, derived SELECTs and scalar
// COUNT subqueries. Reject unexpected relations, and require every catalog
// index name to occur, so a changed template cannot widen this capability.
func collectShowIndexGeneratedSources(ctx context.Context, stmt *tree.Select, db string, names map[string]struct{}, parent *ObjectRef) (showIndexGeneratedSources, error) {
	sources := make(showIndexGeneratedSources)
	seen := make(map[string]bool, len(names))
	if len(names) != 0 && parent == nil {
		return nil, moerr.NewInternalError(ctx, "missing base table for SHOW INDEX metadata")
	}
	var selectStmt func(tree.SelectStatement) error
	var tableExpr func(tree.TableExpr) error
	var expression func(tree.Expr) error
	selectStmt = func(s tree.SelectStatement) error {
		switch n := s.(type) {
		case *tree.Select:
			return selectStmt(n.Select)
		case *tree.ParenSelect:
			return selectStmt(n.Select)
		case *tree.UnionClause:
			if err := selectStmt(n.Left); err != nil {
				return err
			}
			return selectStmt(n.Right)
		case *tree.SelectClause:
			if n.From != nil {
				for _, table := range n.From.Tables {
					if err := tableExpr(table); err != nil {
						return err
					}
				}
			}
			for _, column := range n.Exprs {
				if err := expression(column.Expr); err != nil {
					return err
				}
			}
			return nil
		default:
			return moerr.NewInternalError(ctx, "unexpected SHOW INDEX metadata query")
		}
	}
	tableExpr = func(t tree.TableExpr) error {
		switch n := t.(type) {
		case *tree.JoinTableExpr:
			// The MySQL parser wraps even a single FROM source in a
			// one-sided join. Generated metadata SQL has no real joins.
			if n.Right != nil {
				return moerr.NewInternalError(ctx, "unexpected SHOW INDEX metadata join")
			}
			return tableExpr(n.Left)
		case *tree.AliasedTableExpr:
			return tableExpr(n.Expr)
		case *tree.ParenTableExpr:
			return tableExpr(n.Expr)
		case *tree.Select:
			return selectStmt(n)
		case *tree.TableName:
			name := string(n.ObjectName)
			if name == "" { // parser's source for a SELECT without FROM
				return nil
			}
			if _, ok := names[name]; !ok || string(n.SchemaName) != db {
				return moerr.NewInternalError(ctx, "unexpected SHOW INDEX metadata relation")
			}
			sources[n] = parent
			seen[name] = true
			return nil
		default:
			return moerr.NewInternalError(ctx, "unexpected SHOW INDEX metadata source")
		}
	}
	expression = func(e tree.Expr) error {
		switch n := e.(type) {
		case *tree.Subquery:
			return selectStmt(n.Select)
		case *tree.ParenExpr:
			return expression(n.Expr)
		}
		return nil
	}
	if err := selectStmt(stmt); err != nil {
		return nil, err
	}
	for name := range names {
		if !seen[name] {
			return nil, moerr.NewInternalError(ctx, "missing SHOW INDEX metadata relation")
		}
	}
	return sources, nil
}

// buildShowIndexMetadataPlan scopes generated-source identities to this build.
// The predicate is attached only after the generated nodes have been recorded.
func buildShowIndexMetadataPlan(ctx CompilerContext, stmt *tree.Select, where *tree.Where, db string, names map[string]struct{}, parent *ObjectRef) (*Plan, error) {
	sources, err := collectShowIndexGeneratedSources(ctx.GetContext(), stmt, db, names, parent)
	if err != nil {
		return nil, err
	}
	if where != nil {
		stmt.Select.(*tree.SelectClause).Where = where
	}
	previous := ctx.GetContext()
	ctx.SetContext(context.WithValue(previous, showIndexGeneratedSourcesKey{}, sources))
	defer ctx.SetContext(previous)
	return getReturnDdlBySelectStmt(ctx, stmt, plan.DataDefinition_SHOW_INDEX)
}
