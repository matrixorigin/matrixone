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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestSoundsLikeSyntaxAndPrecedence(t *testing.T) {
	for _, tt := range []struct {
		name, input, formatted string
	}{
		{"literal", "select 'Hello' sounds like 'Hallo'", "select soundex('Hello') = soundex('Hallo')"},
		{"mixed case", "select 'Hello' SoUnDs\nLiKe 'Hallo'", "select soundex('Hello') = soundex('Hallo')"},
		{"comment", "select 'Hello' sounds /* comment */ like 'Hallo'", "select soundex('Hello') = soundex('Hallo')"},
		{"executable comment", "select 'Hello' sounds /*! like */ 'Hallo'", "select soundex('Hello') = soundex('Hallo')"},
		{"comment boundary", "/*! select 'Hello' sounds */ like 'Hallo'", "select soundex('Hello') = soundex('Hallo')"},
		{"ordinary like", "select sounds like 'H%' from t", "select sounds like 'H%' from t"},
		{"arithmetic", "select a + 1 sounds like b + 2 from t", "select soundex(a + 1) = soundex(b + 2) from t"},
		{"comparison", "select a sounds like b = 1 from t", "select soundex(a) = soundex(b) = 1 from t"},
		{"negation", "select not a sounds like b from t", "select not soundex(a) = soundex(b) from t"},
		{"conjunction", "select a sounds like b and c from t", "select soundex(a) = soundex(b) and c from t"},
		{"parentheses", "select (a = b) sounds like (c or d) from t", "select soundex((a = b)) = soundex((c or d)) from t"},
		{"null", "select null sounds like ''", "select soundex(null) = soundex('')"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), tt.input, 1)
			require.NoError(t, err)
			defer stmt.Free()
			formatted := tree.StringWithOpts(stmt, dialect.MYSQL, tree.WithSingleQuoteString())
			require.Equal(t, tt.formatted, formatted)

			roundTrip, err := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err)
			defer roundTrip.Free()
			require.Equal(t, formatted, tree.StringWithOpts(roundTrip, dialect.MYSQL, tree.WithSingleQuoteString()))
		})
	}
}

func TestSoundsLikePreservesOperandsAndParameters(t *testing.T) {
	stmt, err := ParseOne(context.Background(), "select ? sounds like ?", 1)
	require.NoError(t, err)
	defer stmt.Free()

	selectClause := stmt.(*tree.Select).Select.(*tree.SelectClause)
	comparison, ok := selectClause.Exprs[0].Expr.(*tree.ComparisonExpr)
	require.True(t, ok)
	require.Equal(t, tree.EQUAL, comparison.Op)
	for i, operand := range []tree.Expr{comparison.Left, comparison.Right} {
		call, ok := operand.(*tree.FuncExpr)
		require.True(t, ok)
		require.Equal(t, "soundex", call.FuncName.Compare())
		require.Len(t, call.Exprs, 1)
		param, ok := call.Exprs[0].(*tree.ParamExpr)
		require.True(t, ok)
		require.Equal(t, i+1, param.Offset)
	}
	require.Equal(t, 2, tree.ParameterCount(stmt))
}

func TestSoundsRemainsNonReservedIdentifier(t *testing.T) {
	for _, input := range []string{
		"select sounds from sounds",
		"select sounds as sounds from sounds",
		"select 1 sounds",
		"select 1 sounds from sounds",
		"select 1 sounds, 2",
		"select sounds(1)",
		"select sounds.sounds from sounds.sounds",
		"select sounds sounds like sounds from sounds",
		"create table sounds (sounds varchar(20))",
	} {
		t.Run(input, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), input, 1)
			require.NoError(t, err)
			defer stmt.Free()
		})
	}
}

func TestSoundsLikeSyntaxErrors(t *testing.T) {
	for _, input := range []string{
		"select 'Hello' sounds 'Hallo'",
		"select 'Hello' sounds like",
		"select 'Hello' not sounds like 'Hallo'",
		"select 'Hello' sounds like 'Hallo' escape '!'",
	} {
		t.Run(input, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), input, 1)
			if stmt != nil {
				defer stmt.Free()
			}
			require.Error(t, err)
		})
	}
}
