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

package tree

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

type passthroughExprVisitor struct{}

func (passthroughExprVisitor) Enter(expr Expr) (Expr, bool) {
	return expr, false
}

func (passthroughExprVisitor) Exit(expr Expr) (Expr, bool) {
	return expr, true
}

func TestCaseExprAcceptHandlesOptionalOperands(t *testing.T) {
	num := func(value int64) *NumVal {
		return NewNumVal(value, strconv.FormatInt(value, 10), false, P_int64)
	}

	for _, tc := range []struct {
		name string
		expr *CaseExpr
	}{
		{
			name: "searched case has no input expression",
			expr: NewCaseExpr(
				nil,
				[]*When{NewWhen(num(1), num(1))},
				num(0),
			),
		},
		{
			name: "case without else",
			expr: NewCaseExpr(
				num(1),
				[]*When{NewWhen(num(1), num(1))},
				nil,
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			visited, ok := tc.expr.Accept(passthroughExprVisitor{})
			require.True(t, ok)
			require.Same(t, tc.expr, visited)
		})
	}
}

type renameExprVisitor struct {
	oldName          string
	newName          string
	visitedNumValues *int
}

func (v renameExprVisitor) Enter(expr Expr) (Expr, bool) {
	if _, ok := expr.(*NumVal); ok && v.visitedNumValues != nil {
		*v.visitedNumValues++
	}
	name, ok := expr.(*UnresolvedName)
	if ok && name.ColName() == v.oldName {
		return NewUnresolvedColName(v.newName), true
	}
	return expr, false
}

func (renameExprVisitor) Exit(expr Expr) (Expr, bool) {
	return expr, true
}

func TestExprAcceptVisitsSpecialOperands(t *testing.T) {
	num := func(value int64) *NumVal {
		return NewNumVal(value, strconv.FormatInt(value, 10), false, P_int64)
	}
	visitor := renameExprVisitor{oldName: "old_col", newName: "new_col"}

	t.Run("like escape expression", func(t *testing.T) {
		expr := NewComparisonExprWithEscape(
			LIKE,
			num(1),
			num(1),
			NewUnresolvedColName("old_col"),
		)
		visited, ok := expr.Accept(visitor)
		require.True(t, ok)
		require.Equal(t, "new_col", visited.(*ComparisonExpr).Escape.(*UnresolvedName).ColName())
	})

	t.Run("fulltext key parts and pattern", func(t *testing.T) {
		visitedNumValues := 0
		expr := &FullTextMatchExpr{
			KeyParts: []*KeyPart{
				{ColName: NewUnresolvedColName("old_col")},
				{Expr: NewUnresolvedColName("old_col")},
			},
			Pattern: num(1),
		}
		visited, ok := expr.Accept(renameExprVisitor{
			oldName:          "old_col",
			newName:          "new_col",
			visitedNumValues: &visitedNumValues,
		})
		require.True(t, ok)
		match := visited.(*FullTextMatchExpr)
		require.Equal(t, "new_col", match.KeyParts[0].ColName.ColName())
		require.Equal(t, "new_col", match.KeyParts[1].Expr.(*UnresolvedName).ColName())
		require.Equal(t, 1, visitedNumValues)
	})

	t.Run("fulltext prepared pattern", func(t *testing.T) {
		pattern := NewParamExpr(0)
		expr := &FullTextMatchExpr{
			KeyParts: []*KeyPart{{ColName: NewUnresolvedColName("old_col")}},
			Pattern:  pattern,
		}
		visited, ok := expr.Accept(passthroughExprVisitor{})
		require.True(t, ok)
		require.Same(t, pattern, visited.(*FullTextMatchExpr).Pattern)
	})

	t.Run("sample columns", func(t *testing.T) {
		expr, err := NewSampleRowsFuncExpression(
			1,
			false,
			Exprs{NewUnresolvedColName("old_col")},
			"row",
		)
		require.NoError(t, err)
		visited, ok := expr.Accept(visitor)
		require.True(t, ok)
		sample, ok := visited.(*SampleExpr)
		require.True(t, ok)
		columns, isStar := sample.GetColumns()
		require.False(t, isStar)
		require.Len(t, columns, 1)
		require.Equal(t, "new_col", columns[0].(*UnresolvedName).ColName())
	})
}
