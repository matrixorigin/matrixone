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

package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// Local sliding-window ranks (issue #29947): mrank / cumrank / tmrank are
// non-reserved window tokens following the LAG/LEAD pattern, with the
// mandatory window_spec the grammar rule function_call_window enforces.

// TestLocalRankWindowArityRejectedAtParse pins the productions' fixed
// argument shapes (mrank exactly two, cumrank exactly one, tmrank's width an
// interval_expr): wrong spellings are parse rejections, exactly like the
// OVER-less probe, and never reach the binder or the catalog gates.
func TestLocalRankWindowArityRejectedAtParse(t *testing.T) {
	for _, sql := range []string{
		"select mrank(v) over (order by id) from t",
		"select mrank(v, 3, 1) over (order by id) from t",
		"select cumrank() over (order by id) from t",
		"select cumrank(v, v) over (order by id) from t",
		"select tmrank(v, 3) over (order by id) from t",
		"select tmrank(v, id) over (order by id) from t",
		"select tmrank(v, interval 3 day, 1) over (order by id) from t",
		"select tmrank(v) over (order by id) from t",
	} {
		_, err := ParseOne(context.Background(), sql, 1)
		require.Error(t, err, sql)
		require.Contains(t, err.Error(), "syntax error", sql)
	}
}

func TestLocalRankWindowProductions(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		argc int
	}{
		{
			name: "MRANK with a physical width",
			sql:  "select mrank(v, 3) over (order by id) from t",
			argc: 2,
		},
		{
			name: "CUMRANK bare",
			sql:  "select cumrank(v) over (order by id) from t",
			argc: 1,
		},
		{
			name: "TMRANK with an INTERVAL width",
			sql:  "select tmrank(v, interval 3 day) over (order by tick) from t",
			argc: 2,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), testCase.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			fn, ok := firstSelectExpr(t, stmt).(*tree.FuncExpr)
			require.True(t, ok)
			switch testCase.name {
			case "MRANK with a physical width":
				require.Equal(t, "mrank", fn.Func.FunctionReference.(*tree.UnresolvedName).ColName())
			case "CUMRANK bare":
				require.Equal(t, "cumrank", fn.Func.FunctionReference.(*tree.UnresolvedName).ColName())
			case "TMRANK with an INTERVAL width":
				require.Equal(t, "tmrank", fn.Func.FunctionReference.(*tree.UnresolvedName).ColName())
				require.Len(t, fn.Exprs, 2)
				width, ok := fn.Exprs[1].(*tree.FuncExpr)
				require.True(t, ok)
				require.Equal(t, "interval", width.Func.FunctionReference.(*tree.UnresolvedName).ColName())
				require.Len(t, width.Exprs, 2)
				_, isUnit := width.Exprs[1].(*tree.TimeUnitExpr)
				require.True(t, isUnit)
			}
			require.NotNil(t, fn.WindowSpec)
			require.False(t, fn.WindowSpec.HasFrame)
			require.Len(t, fn.Exprs, testCase.argc)
		})
	}
}

func TestLocalRankIdentifiersStayUnquoted(t *testing.T) {
	for _, sql := range []string{
		"select mrank from t",
		"select cumrank, tmrank from t",
		"select t.mrank from t",
		"create table mrank (cumrank int)",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
		})
	}
}

func TestLocalRankOverlessProbeRejectedLikeRank(t *testing.T) {
	for _, sql := range []string{
		"select mrank(v, 3) from t",
		"select cumrank(v) from t",
		"select tmrank(v, interval 3 day) from t",
	} {
		t.Run("reject "+sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), sql, 1)
			require.Error(t, err)
			// rank() without OVER produces the same 1064 syntax-error class
			// today (the lexer grammar failure); the three new tokens share
			// its mandatory-window_spec rule.
			require.Error(t, err)
			require.Contains(t, err.Error(), "syntax error", err.Error())
		})
	}
}
