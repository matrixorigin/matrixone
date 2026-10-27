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

package mysql

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestSoundsLikeSyntaxAndPrecedence(t *testing.T) {
	for _, test := range []struct {
		input string
		want  string
	}{
		{`select 'Hello' SoUnDs LiKe 'Hallo' as matched`, `select soundex('Hello') = soundex('Hallo') as matched`},
		{"select 'Hello' SoUnDs\nLiKe 'Hallo'", `select soundex('Hello') = soundex('Hallo')`},
		{`/*! select 'Hello' sounds */ like 'Hallo'`, `select soundex('Hello') = soundex('Hallo')`},
		{`select sounds like 'H%' from t`, `select sounds like 'H%' from t`},
		{`select a + 1 sounds like b + 2 from t`, `select soundex(a + 1) = soundex(b + 2) from t`},
		{`select a sounds like b = 1 from t`, `select soundex(a) = soundex(b) = 1 from t`},
		{`select (a = b) sounds like (c or d) from t`, `select soundex((a = b)) = soundex((c or d)) from t`},
		{`select null sounds like ''`, `select soundex(null) = soundex('')`},
		{`select 'Hello' sounds /* predicate */ like 'Hallo'`, `select soundex('Hello') = soundex('Hallo')`},
		{`select 'Hello' sounds /*! like */ 'Hallo'`, `select soundex('Hello') = soundex('Hallo')`},
		{`select 'Hello' sounds /*!50000 like */ 'Hallo'`, `select soundex('Hello') = soundex('Hallo')`},
		{`select 'Hello' /*! sounds */ like 'Hallo'`, `select soundex('Hello') = soundex('Hallo')`},
		{`select 'Hello' /*!50000 sounds */ like 'Hallo'`, `select soundex('Hello') = soundex('Hallo')`},
		{`select /*! 'Hello' sounds like 'Hallo' */`, `select soundex('Hello') = soundex('Hallo')`},
		{`select 1 + 2 sounds like 3 * 4`, `select soundex(1 + 2) = soundex(3 * 4)`},
		{`select a sounds like b is null`, `select soundex(a) = soundex(b) is null`},
		{`select a = b sounds like c`, `select a = (soundex(b) = soundex(c))`},
		{`select a sounds like b = c sounds like d`, `select soundex(a) = soundex(b) = (soundex(c) = soundex(d))`},
		{`select not a sounds like b and c = 1 or false`, `select not soundex(a) = soundex(b) and c = 1 or false`},
		{`select (a sounds like b) sounds like c`, `select soundex((soundex(a) = soundex(b))) = soundex(c)`},
		{`select concat(a, 'x') sounds like (select b from t)`, `select soundex(concat(a, 'x')) = soundex((select b from t))`},
		{`select case when a sounds like b then 1 else 0 end from t where a sounds like ?`, `select case when soundex(a) = soundex(b) then 1 else 0 end from t where soundex(a) = soundex(?)`},
	} {
		t.Run(test.input, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), test.input, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			formatted := tree.StringWithOpts(stmt, dialect.MYSQL, tree.WithSingleQuoteString())
			require.Equal(t, test.want, formatted)

			roundTrip, err := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err)
			t.Cleanup(roundTrip.Free)
			require.Equal(t, formatted, tree.StringWithOpts(roundTrip, dialect.MYSQL, tree.WithSingleQuoteString()))
		})
	}
}

func TestSoundsLikeComparisonNesting(t *testing.T) {
	stmt, err := ParseOne(context.Background(), "select a = b sounds like c", 1)
	require.NoError(t, err)
	defer stmt.Free()
	outer := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.ComparisonExpr)
	require.Equal(t, tree.EQUAL, outer.Op)
	require.IsType(t, &tree.UnresolvedName{}, outer.Left)
	inner := outer.Right.(*tree.ComparisonExpr)
	require.Equal(t, tree.EQUAL, inner.Op)
	for _, operand := range []tree.Expr{inner.Left, inner.Right} {
		fn := operand.(*tree.FuncExpr)
		require.Equal(t, "soundex", fn.FuncName.Origin())
		require.Len(t, fn.Exprs, 1)
		require.IsType(t, &tree.UnresolvedName{}, fn.Exprs[0])
	}
}

func TestSoundsLikeIdentifierAndErrors(t *testing.T) {
	for _, input := range []string{
		"select sounds from sounds",
		"select 1 sounds, 2",
		"select sounds(1)",
		"select sounds sounds like sounds from sounds",
		"select sounds as sounds from sounds",
		"select 'hello' sounds from sounds",
		"select 'hello' sounds /* alias */ from sounds",
		"select 'hello' sounds",
		"select sounds(like_name) from sounds",
		"select sounds.sounds from sounds.sounds",
		"create table sounds (sounds varchar(32))",
		"select sounds like 'hello'",
	} {
		stmt, err := ParseOne(context.Background(), input, 1)
		require.NoError(t, err, input)
		stmt.Free()
	}
	for _, input := range []string{
		"select 'hello' sounds like",
		"select 'hello' sounds 'hallo'",
		"select 'hello' not sounds like 'halo'",
		"select 'hello' sounds like 'halo' escape 'x'",
		"select 'hello' sounds like 'halo' sounds like 'hallo'",
	} {
		_, err := ParseOne(context.Background(), input, 1)
		require.Error(t, err, input)
	}
}

func TestSoundsLikeParameterOrder(t *testing.T) {
	stmt, err := ParseOne(context.Background(), "select ? sounds like concat(?, ?), ?", 1)
	require.NoError(t, err)
	defer stmt.Free()
	exprs := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs
	cmp := exprs[0].Expr.(*tree.ComparisonExpr)
	left := cmp.Left.(*tree.FuncExpr)
	require.Len(t, left.Exprs, 1)
	require.Len(t, cmp.Right.(*tree.FuncExpr).Exprs, 1)
	require.Equal(t, 4, tree.ParameterCount(stmt))
	right := cmp.Right.(*tree.FuncExpr).Exprs[0].(*tree.FuncExpr)
	for i, expr := range []tree.Expr{left.Exprs[0], right.Exprs[0], right.Exprs[1], exprs[1].Expr} {
		require.Equal(t, i+1, expr.(*tree.ParamExpr).Offset)
	}
}

func TestSoundsLikeHighNotPrecedence(t *testing.T) {
	for _, mode := range []string{"", "HIGH_NOT_PRECEDENCE"} {
		t.Run(mode, func(t *testing.T) {
			stmt, err := ParseOneWithSQLMode(context.Background(), "select not a sounds like b", 1, mode)
			require.NoError(t, err)
			defer stmt.Free()
			expr := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			if mode == "" {
				cmp := expr.(*tree.NotExpr).Expr.(*tree.ComparisonExpr)
				require.IsType(t, &tree.UnresolvedName{}, cmp.Left.(*tree.FuncExpr).Exprs[0])
			} else {
				cmp := expr.(*tree.ComparisonExpr)
				require.IsType(t, &tree.NotExpr{}, cmp.Left.(*tree.FuncExpr).Exprs[0])
			}
			formatted := tree.StringWithOpts(stmt, dialect.MYSQL, tree.WithSingleQuoteString())
			roundTrip, err := ParseOneWithSQLMode(context.Background(), formatted, 1, mode)
			require.NoError(t, err)
			defer roundTrip.Free()
			require.Equal(t, formatted, tree.StringWithOpts(roundTrip, dialect.MYSQL, tree.WithSingleQuoteString()))
		})
	}
}

func TestSoundsLikeLookaheadDoesNotRecurse(t *testing.T) {
	scanner := NewScanner(dialect.MYSQL, strings.Repeat("sounds ", 4096)+"like 'halo'")
	defer PutScanner(scanner)
	token, value := scanner.Scan()
	require.Equal(t, ID, token)
	require.Equal(t, "sounds", value)
	// The probe above must not advance the original scanner beyond this token.
	token, value = scanner.Scan()
	require.Equal(t, ID, token)
	require.Equal(t, "sounds", value)
}
