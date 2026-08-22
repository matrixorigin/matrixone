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

package datastream

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// testCols mimics a scan's TableDef.Cols; deparsing resolves columns by
// ColPos, so col() records the position and sets a qualified display name
// (as scan-node ColRefs carry in practice) to prove Name is never parsed.
var testCols = []*plan.ColDef{
	{Name: "a"},
	{Name: "b"},
	{Name: "s"},
	{Name: "col1"},
	{Name: "col2"},
	{Name: "d"},
	{Name: "ts"},
	{Name: "n"},
	{Name: "a.b"},    // MO permits dotted column names
	{Name: "wei`rd"}, // and backticks
	{Name: "ghost", Hidden: true},
}

func col(name string) *plan.Expr {
	for i, def := range testCols {
		if def.Name == name {
			return &plan.Expr{Expr: &plan.Expr_Col{
				Col: &plan.ColRef{ColPos: int32(i), Name: "t1." + name},
			}}
		}
	}
	// unknown name: out-of-range position, must not be pushed
	return &plan.Expr{Expr: &plan.Expr_Col{
		Col: &plan.ColRef{ColPos: int32(len(testCols) + 7), Name: name},
	}}
}

func i64(v int64) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: v}}},
	}
}

func str(v string) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: v}}},
	}
}

func fn(name string, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: name},
			Args: args,
		}},
	}
}

func list(items ...*plan.Expr) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: items}}}
}

func TestDeparseFilters(t *testing.T) {
	cases := []struct {
		name string
		expr *plan.Expr
		want string // "" means not pushable
	}{
		{"eq", fn("=", col("a"), i64(1)), "(`a` = 1)"},
		{"gt-string", fn(">", col("col2"), str("2020-11-11 00:00:00")), "(`col2` > '2020-11-11 00:00:00')"},
		{"neq-normalized", fn("!=", col("a"), i64(2)), "(`a` <> 2)"},
		{"and", fn("and", fn(">", col("a"), i64(1)), fn("<", col("a"), i64(9))), "((`a` > 1) AND (`a` < 9))"},
		{"or", fn("or", fn("=", col("a"), i64(1)), fn("=", col("a"), i64(2))), "((`a` = 1) OR (`a` = 2))"},
		{"not", fn("not", fn("=", col("a"), i64(1))), "(NOT (`a` = 1))"},
		{"in", fn("in", col("a"), list(i64(1), i64(2), i64(3))), "(`a` IN (1, 2, 3))"},
		{"between", fn("between", col("a"), i64(1), i64(5)), "(`a` BETWEEN 1 AND 5)"},
		{"isnull", fn("isnull", col("a")), "(`a` IS NULL)"},
		{"isnotnull", fn("isnotnull", col("a")), "(`a` IS NOT NULL)"},
		{"like", fn("like", col("s"), str("ab%")), "(`s` LIKE 'ab%')"},
		{"escape-quote", fn("=", col("s"), str("o'brien\\x")), "(`s` = 'o\\'brien\\\\x')"},
		// names resolved by position, never parsed from the display name:
		// a column literally named "a.b" pushes with its real identifier
		{"dotted-col-name", fn("=", col("a.b"), i64(1)), "(`a.b` = 1)"},
		{"backtick-col-name", fn("=", col("wei`rd"), i64(1)), "(`wei``rd` = 1)"},

		// not pushable
		{"func-call", fn("abs", col("a")), ""},
		{"cast-arg", fn("=", fn("cast", col("a")), i64(1)), ""},
		{"param", fn("=", col("a"), &plan.Expr{Expr: &plan.Expr_P{}}), ""},
		{"in-nonlist", fn("in", col("a"), i64(1)), ""},
		{"colpos-out-of-range", fn("=", col("no_such_col"), i64(1)), ""},
		{"hidden-col", fn("=", col("ghost"), i64(1)), ""},
		{"nonprintable-str", fn("=", col("s"), str("a\x01b")), ""},
		{"nil", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text, pushed := DeparseFilters([]*plan.Expr{tc.expr}, testCols, time.UTC)
			if tc.want == "" {
				require.False(t, pushed[0])
				require.Equal(t, "", text)
			} else {
				require.True(t, pushed[0])
				require.Equal(t, tc.want, text)
			}
		})
	}
}

func TestDeparseFiltersMixedConjuncts(t *testing.T) {
	exprs := []*plan.Expr{
		fn(">", col("a"), i64(1)),
		fn("abs", col("a")), // not pushable
		fn("<", col("b"), i64(9)),
	}
	text, pushed := DeparseFilters(exprs, testCols, time.UTC)
	require.Equal(t, "(`a` > 1) AND (`b` < 9)", text)
	require.Equal(t, []bool{true, false, true}, pushed)
}

func TestDeparseLiteralKinds(t *testing.T) {
	loc, err := time.LoadLocation("UTC")
	require.NoError(t, err)

	dt, err := types.ParseDatetime("2021-01-02 03:04:05", 0)
	require.NoError(t, err)
	date, err := types.ParseDateCast("2021-01-02")
	require.NoError(t, err)

	dtExpr := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_datetime)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Datetimeval{Datetimeval: int64(dt)}}},
	}
	dateExpr := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_date)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dateval{Dateval: int32(date)}}},
	}
	boolExpr := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}},
	}
	nullExpr := &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
	tsExpr := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_timestamp)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Timestampval{Timestampval: int64(dt.ToTimestamp(loc))}}},
	}

	text, pushed := DeparseFilters([]*plan.Expr{fn("=", col("d"), dtExpr)}, testCols, loc)
	require.True(t, pushed[0])
	require.Equal(t, "(`d` = '2021-01-02 03:04:05')", text)

	text, pushed = DeparseFilters([]*plan.Expr{fn("=", col("d"), dateExpr)}, testCols, loc)
	require.True(t, pushed[0])
	require.Equal(t, "(`d` = '2021-01-02')", text)

	text, pushed = DeparseFilters([]*plan.Expr{fn("=", col("b"), boolExpr)}, testCols, loc)
	require.True(t, pushed[0])
	require.Equal(t, "(`b` = TRUE)", text)

	text, pushed = DeparseFilters([]*plan.Expr{fn("=", col("ts"), tsExpr)}, testCols, loc)
	require.True(t, pushed[0])
	require.Equal(t, "(`ts` = '2021-01-02 03:04:05')", text)

	// timestamp without a session location is not pushable
	_, pushed = DeparseFilters([]*plan.Expr{fn("=", col("ts"), tsExpr)}, testCols, nil)
	require.False(t, pushed[0])

	// IS NULL via literal null comparison stays pushable as text
	text, pushed = DeparseFilters([]*plan.Expr{fn("=", col("n"), nullExpr)}, testCols, loc)
	require.True(t, pushed[0])
	require.Equal(t, "(`n` = NULL)", text)
}
