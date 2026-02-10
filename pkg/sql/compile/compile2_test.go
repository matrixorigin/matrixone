// Copyright 2024 Matrix Origin
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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Tests for rewriteAutoModeToPre
// ============================================================================

func TestRewriteAutoModeToPre_Select(t *testing.T) {
	tests := []struct {
		name     string
		stmt     tree.Statement
		expected bool
		validate func(t *testing.T, stmt tree.Statement)
	}{
		{
			name: "rewrite select with mode=auto",
			stmt: &tree.Select{
				RankOption: &tree.RankOption{
					Option: map[string]string{"mode": "auto"},
				},
			},
			expected: true,
			validate: func(t *testing.T, stmt tree.Statement) {
				sel := stmt.(*tree.Select)
				assert.Equal(t, "pre", sel.RankOption.Option["mode"])
			},
		},
		{
			name: "rewrite select with mode=AUTO (case insensitive)",
			stmt: &tree.Select{
				RankOption: &tree.RankOption{
					Option: map[string]string{"mode": "AUTO"},
				},
			},
			expected: true,
			validate: func(t *testing.T, stmt tree.Statement) {
				sel := stmt.(*tree.Select)
				assert.Equal(t, "pre", sel.RankOption.Option["mode"])
			},
		},
		{
			name: "no rewrite for mode=post",
			stmt: &tree.Select{
				RankOption: &tree.RankOption{
					Option: map[string]string{"mode": "post"},
				},
			},
			expected: false,
			validate: func(t *testing.T, stmt tree.Statement) {
				sel := stmt.(*tree.Select)
				assert.Equal(t, "post", sel.RankOption.Option["mode"])
			},
		},
		{
			name: "no rewrite for mode=pre",
			stmt: &tree.Select{
				RankOption: &tree.RankOption{
					Option: map[string]string{"mode": "pre"},
				},
			},
			expected: false,
			validate: func(t *testing.T, stmt tree.Statement) {
				sel := stmt.(*tree.Select)
				assert.Equal(t, "pre", sel.RankOption.Option["mode"])
			},
		},
		{
			name: "no rewrite for mode=force",
			stmt: &tree.Select{
				RankOption: &tree.RankOption{
					Option: map[string]string{"mode": "force"},
				},
			},
			expected: false,
			validate: func(t *testing.T, stmt tree.Statement) {
				sel := stmt.(*tree.Select)
				assert.Equal(t, "force", sel.RankOption.Option["mode"])
			},
		},
		{
			name:     "no rewrite for nil RankOption",
			stmt:     &tree.Select{},
			expected: false,
			validate: func(t *testing.T, stmt tree.Statement) {
				sel := stmt.(*tree.Select)
				assert.Nil(t, sel.RankOption)
			},
		},
		{
			name: "no rewrite for empty Option map",
			stmt: &tree.Select{
				RankOption: &tree.RankOption{
					Option: map[string]string{},
				},
			},
			expected: false,
			validate: func(t *testing.T, stmt tree.Statement) {
				sel := stmt.(*tree.Select)
				_, exists := sel.RankOption.Option["mode"]
				assert.False(t, exists)
			},
		},
		{
			name: "no rewrite for nil Option map",
			stmt: &tree.Select{
				RankOption: &tree.RankOption{},
			},
			expected: false,
			validate: func(t *testing.T, stmt tree.Statement) {
				sel := stmt.(*tree.Select)
				assert.Nil(t, sel.RankOption.Option)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rewriteAutoModeToPre(tt.stmt)
			assert.Equal(t, tt.expected, result)
			if tt.validate != nil {
				tt.validate(t, tt.stmt)
			}
		})
	}
}

func TestRewriteAutoModeToPre_ExplainStmts(t *testing.T) {
	// Test that rewrite works through ExplainStmt wrapper
	innerSelect := &tree.Select{
		RankOption: &tree.RankOption{
			Option: map[string]string{"mode": "auto"},
		},
	}
	explainStmt := tree.NewExplainStmt(innerSelect, "text")

	result := rewriteAutoModeToPre(explainStmt)
	assert.True(t, result)
	assert.Equal(t, "pre", innerSelect.RankOption.Option["mode"])
}

func TestRewriteAutoModeToPre_ExplainAnalyze(t *testing.T) {
	innerSelect := &tree.Select{
		RankOption: &tree.RankOption{
			Option: map[string]string{"mode": "auto"},
		},
	}
	explainAnalyze := tree.NewExplainAnalyze(innerSelect, "text")

	result := rewriteAutoModeToPre(explainAnalyze)
	assert.True(t, result)
	assert.Equal(t, "pre", innerSelect.RankOption.Option["mode"])
}

func TestRewriteAutoModeToPre_Insert(t *testing.T) {
	// Test INSERT ... SELECT with mode=auto
	innerSelect := &tree.Select{
		RankOption: &tree.RankOption{
			Option: map[string]string{"mode": "auto"},
		},
	}
	insertStmt := &tree.Insert{
		Rows: innerSelect,
	}

	result := rewriteAutoModeToPre(insertStmt)
	assert.True(t, result)
	assert.Equal(t, "pre", innerSelect.RankOption.Option["mode"])
}

func TestRewriteAutoModeToPre_Replace(t *testing.T) {
	// Test REPLACE ... SELECT with mode=auto
	innerSelect := &tree.Select{
		RankOption: &tree.RankOption{
			Option: map[string]string{"mode": "auto"},
		},
	}
	replaceStmt := &tree.Replace{
		Rows: innerSelect,
	}

	result := rewriteAutoModeToPre(replaceStmt)
	assert.True(t, result)
	assert.Equal(t, "pre", innerSelect.RankOption.Option["mode"])
}

func TestRewriteAutoModeToPre_NilStatement(t *testing.T) {
	result := rewriteAutoModeToPre(nil)
	assert.False(t, result)
}

func TestRewriteAutoModeToPre_UnsupportedStatement(t *testing.T) {
	// CreateTable should not be affected
	createTable := &tree.CreateTable{}
	result := rewriteAutoModeToPre(createTable)
	assert.False(t, result)
}

// ============================================================================
// Tests for forceModePre
// ============================================================================

func TestForceModePre_Select(t *testing.T) {
	tests := []struct {
		name     string
		stmt     tree.Statement
		expected bool
		validate func(t *testing.T, stmt tree.Statement)
	}{
		{
			name:     "force mode on select with nil RankOption",
			stmt:     &tree.Select{},
			expected: true,
			validate: func(t *testing.T, stmt tree.Statement) {
				sel := stmt.(*tree.Select)
				require.NotNil(t, sel.RankOption)
				assert.Equal(t, "pre", sel.RankOption.Option["mode"])
			},
		},
		{
			name: "force mode on select with empty Option",
			stmt: &tree.Select{
				RankOption: &tree.RankOption{},
			},
			expected: true,
			validate: func(t *testing.T, stmt tree.Statement) {
				sel := stmt.(*tree.Select)
				assert.Equal(t, "pre", sel.RankOption.Option["mode"])
			},
		},
		{
			name: "force mode overwrites existing mode=post",
			stmt: &tree.Select{
				RankOption: &tree.RankOption{
					Option: map[string]string{"mode": "post"},
				},
			},
			expected: true,
			validate: func(t *testing.T, stmt tree.Statement) {
				sel := stmt.(*tree.Select)
				assert.Equal(t, "pre", sel.RankOption.Option["mode"])
			},
		},
		{
			name: "force mode preserves other options",
			stmt: &tree.Select{
				RankOption: &tree.RankOption{
					Option: map[string]string{"mode": "post", "other": "value"},
				},
			},
			expected: true,
			validate: func(t *testing.T, stmt tree.Statement) {
				sel := stmt.(*tree.Select)
				assert.Equal(t, "pre", sel.RankOption.Option["mode"])
				assert.Equal(t, "value", sel.RankOption.Option["other"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := forceModePre(tt.stmt)
			assert.Equal(t, tt.expected, result)
			if tt.validate != nil {
				tt.validate(t, tt.stmt)
			}
		})
	}
}

func TestForceModePre_ExplainStmts(t *testing.T) {
	innerSelect := &tree.Select{}
	explainStmt := tree.NewExplainStmt(innerSelect, "text")

	result := forceModePre(explainStmt)
	assert.True(t, result)
	require.NotNil(t, innerSelect.RankOption)
	assert.Equal(t, "pre", innerSelect.RankOption.Option["mode"])
}

func TestForceModePre_Insert(t *testing.T) {
	innerSelect := &tree.Select{}
	insertStmt := &tree.Insert{
		Rows: innerSelect,
	}

	result := forceModePre(insertStmt)
	assert.True(t, result)
	require.NotNil(t, innerSelect.RankOption)
	assert.Equal(t, "pre", innerSelect.RankOption.Option["mode"])
}

func TestForceModePre_NilStatement(t *testing.T) {
	result := forceModePre(nil)
	assert.False(t, result)
}

func TestForceModePre_NilRows(t *testing.T) {
	insertStmt := &tree.Insert{
		Rows: nil,
	}
	result := forceModePre(insertStmt)
	assert.False(t, result)
}

// ============================================================================
// Tests for rewriteAutoModeInSelect
// ============================================================================

func TestRewriteAutoModeInSelect_NilSelect(t *testing.T) {
	result := rewriteAutoModeInSelect(nil)
	assert.False(t, result)
}

// ============================================================================
// Tests for isAdaptiveVectorSearch
// ============================================================================

func TestIsAdaptiveVectorSearch(t *testing.T) {
	c := &Compile{}

	tests := []struct {
		name     string
		qry      *plan.Query
		expected bool
	}{
		{
			name:     "nil query",
			qry:      nil,
			expected: false,
		},
		{
			name: "no nodes",
			qry: &plan.Query{
				Nodes: []*plan.Node{},
			},
			expected: false,
		},
		{
			name: "nodes without RankOption",
			qry: &plan.Query{
				Nodes: []*plan.Node{
					{NodeType: plan.Node_TABLE_SCAN},
					{NodeType: plan.Node_SORT},
				},
			},
			expected: false,
		},
		{
			name: "node with RankOption but mode is not auto",
			qry: &plan.Query{
				Nodes: []*plan.Node{
					{
						NodeType:   plan.Node_SORT,
						RankOption: &plan.RankOption{Mode: "post"},
					},
				},
			},
			expected: false,
		},
		{
			name: "node with RankOption mode=pre",
			qry: &plan.Query{
				Nodes: []*plan.Node{
					{
						NodeType:   plan.Node_SORT,
						RankOption: &plan.RankOption{Mode: "pre"},
					},
				},
			},
			expected: false,
		},
		{
			name: "node with RankOption mode=auto",
			qry: &plan.Query{
				Nodes: []*plan.Node{
					{
						NodeType:   plan.Node_SORT,
						RankOption: &plan.RankOption{Mode: "auto"},
					},
				},
			},
			expected: true,
		},
		{
			name: "multiple nodes, one with mode=auto",
			qry: &plan.Query{
				Nodes: []*plan.Node{
					{NodeType: plan.Node_TABLE_SCAN},
					{
						NodeType:   plan.Node_SORT,
						RankOption: &plan.RankOption{Mode: "auto"},
					},
					{NodeType: plan.Node_PROJECT},
				},
			},
			expected: true,
		},
		{
			name: "node with empty RankOption",
			qry: &plan.Query{
				Nodes: []*plan.Node{
					{
						NodeType:   plan.Node_SORT,
						RankOption: &plan.RankOption{},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := c.isAdaptiveVectorSearch(tt.qry)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ============================================================================
// Tests for recursive rewrite functions
// ============================================================================

func TestRewriteAutoModeToPre_Complex(t *testing.T) {
	// 1. Test UnionClause
	unionSelect := &tree.Select{
		Select: &tree.UnionClause{
			Left: &tree.Select{
				RankOption: &tree.RankOption{Option: map[string]string{"mode": "auto"}},
			},
			Right: &tree.Select{
				RankOption: &tree.RankOption{Option: map[string]string{"mode": "auto"}},
			},
			Type: tree.UNION,
		},
	}
	assert.True(t, rewriteAutoModeToPre(unionSelect))
	left := unionSelect.Select.(*tree.UnionClause).Left.(*tree.Select)
	right := unionSelect.Select.(*tree.UnionClause).Right.(*tree.Select)
	assert.Equal(t, "pre", left.RankOption.Option["mode"])
	assert.Equal(t, "pre", right.RankOption.Option["mode"])

	// 2. Test Subquery in FROM clause (AliasedTableExpr -> Subquery)
	subquerySelect := &tree.Select{
		Select: &tree.SelectClause{
			From: &tree.From{
				Tables: []tree.TableExpr{
					&tree.AliasedTableExpr{
						Expr: &tree.Subquery{
							Select: &tree.Select{
								RankOption: &tree.RankOption{Option: map[string]string{"mode": "auto"}},
							},
						},
					},
				},
			},
		},
	}
	assert.True(t, rewriteAutoModeToPre(subquerySelect))
	tbl := subquerySelect.Select.(*tree.SelectClause).From.Tables[0].(*tree.AliasedTableExpr)
	inner := tbl.Expr.(*tree.Subquery).Select.(*tree.Select)
	assert.Equal(t, "pre", inner.RankOption.Option["mode"])

	// 3. Test JoinTableExpr
	joinSelect := &tree.Select{
		Select: &tree.SelectClause{
			From: &tree.From{
				Tables: []tree.TableExpr{
					&tree.JoinTableExpr{
						Left: &tree.AliasedTableExpr{
							Expr: &tree.Subquery{
								Select: &tree.Select{
									RankOption: &tree.RankOption{Option: map[string]string{"mode": "auto"}},
								},
							},
						},
						Right: &tree.AliasedTableExpr{
							Expr: &tree.Subquery{
								Select: &tree.Select{
									RankOption: &tree.RankOption{Option: map[string]string{"mode": "auto"}},
								},
							},
						},
						JoinType: tree.JOIN_TYPE_INNER,
					},
				},
			},
		},
	}
	assert.True(t, rewriteAutoModeToPre(joinSelect))
	jt := joinSelect.Select.(*tree.SelectClause).From.Tables[0].(*tree.JoinTableExpr)
	leftSub := jt.Left.(*tree.AliasedTableExpr).Expr.(*tree.Subquery).Select.(*tree.Select)
	rightSub := jt.Right.(*tree.AliasedTableExpr).Expr.(*tree.Subquery).Select.(*tree.Select)
	assert.Equal(t, "pre", leftSub.RankOption.Option["mode"])
	assert.Equal(t, "pre", rightSub.RankOption.Option["mode"])

	// 4. Test ParenSelect and ParenTableExpr
	parenSelect := &tree.Select{
		Select: &tree.ParenSelect{
			Select: &tree.Select{
				Select: &tree.SelectClause{
					From: &tree.From{
						Tables: []tree.TableExpr{
							&tree.ParenTableExpr{
								Expr: &tree.AliasedTableExpr{
									Expr: &tree.Subquery{
										Select: &tree.Select{
											RankOption: &tree.RankOption{Option: map[string]string{"mode": "auto"}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	assert.True(t, rewriteAutoModeToPre(parenSelect))
	innerParen := parenSelect.Select.(*tree.ParenSelect).Select.Select.(*tree.SelectClause)
	innerTable := innerParen.From.Tables[0].(*tree.ParenTableExpr).Expr.(*tree.AliasedTableExpr)
	finalInner := innerTable.Expr.(*tree.Subquery).Select.(*tree.Select)
	assert.Equal(t, "pre", finalInner.RankOption.Option["mode"])
}

func TestForceModePre_EdgeCases(t *testing.T) {
	// explain wrappers
	assert.True(t, forceModePre(tree.NewExplainPhyPlan(&tree.Select{}, "text")))

	// ExplainFor usually has nil statement in NewExplainFor, so it returns false
	assert.False(t, forceModePre(tree.NewExplainFor("text", 1)))

	// unsupported
	assert.False(t, forceModePre(&tree.ShowDatabases{}))

	// ExplainStmt through unsupported
	assert.False(t, forceModePre(tree.NewExplainStmt(&tree.ShowDatabases{}, "text")))
}

func TestRewriteAutoModeInTableExpr_StatementSource(t *testing.T) {
	stmtSource := &tree.StatementSource{
		Statement: &tree.Select{
			RankOption: &tree.RankOption{Option: map[string]string{"mode": "auto"}},
		},
	}
	assert.True(t, rewriteAutoModeInTableExpr(stmtSource))
	assert.Equal(t, "pre", stmtSource.Statement.(*tree.Select).RankOption.Option["mode"])
}

func TestRewriteAutoModeInTableExpr_ApplyTableExpr(t *testing.T) {
	applyExpr := &tree.ApplyTableExpr{
		Left: &tree.AliasedTableExpr{
			Expr: &tree.Subquery{
				Select: &tree.Select{
					RankOption: &tree.RankOption{Option: map[string]string{"mode": "auto"}},
				},
			},
		},
		Right: &tree.AliasedTableExpr{
			Expr: &tree.Subquery{
				Select: &tree.Select{
					RankOption: &tree.RankOption{Option: map[string]string{"mode": "auto"}},
				},
			},
		},
	}
	assert.True(t, rewriteAutoModeInTableExpr(applyExpr))
	left := applyExpr.Left.(*tree.AliasedTableExpr).Expr.(*tree.Subquery).Select.(*tree.Select)
	right := applyExpr.Right.(*tree.AliasedTableExpr).Expr.(*tree.Subquery).Select.(*tree.Select)
	assert.Equal(t, "pre", left.RankOption.Option["mode"])
	assert.Equal(t, "pre", right.RankOption.Option["mode"])
}
