// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func whereAliasContext(enabled bool) *MockCompilerContext {
	ctx := NewMockCompilerContext(false)
	fallback := NewMockCompilerContext(false)
	ctx.ResolveVariableFunc = func(name string, system, global bool) (interface{}, error) {
		if name == "enable_where_alias" {
			if enabled {
				return int8(1), nil
			}
			return int8(0), nil
		}
		return fallback.ResolveVariable(name, system, global)
	}
	return ctx
}

func TestWhereSelectAliases(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		err  string
	}{
		{"column", "select n_nationkey as k from nation where k > 1", ""},
		{"expression", "select abs(n_nationkey) + 1 as k from nation where k > 1 and k < 4", ""},
		{"constant", "select 2 as k where k > 1", ""},
		{"case insensitive", "select n_nationkey as K from nation where k > 1", ""},
		{"null", "select n_comment as c from nation where c is null", ""},
		{"predicate forms", "select n_nationkey as k from nation where case when k between 1 and 3 then cast(k as signed) in (1, 2) else not (k = 0) end", ""},
		{"alias input ambiguity", "select n_nationkey as k from nation a, nation b where k > 1", "ambiguous"},
		{"subquery expression", "select (select max(r_regionkey) from region) as k from nation where k > 1", ""},
		{"child own alias", "select n_name from nation where exists (select r_regionkey as k from region where k > 1)", ""},
		{"source wins", "select n_name as n_nationkey from nation where n_nationkey > 1", ""},
		{"unused duplicates", "select n_name as k, n_nationkey as k from nation where n_nationkey > 1", ""},
		{"source beats duplicates", "select n_name as n_nationkey, n_comment as n_nationkey from nation where n_nationkey > 1", ""},
		{"volatile expression", "select rand() as k from nation where k > 0.5", ""},
		{"qualified", "select n_nationkey as k from nation where nation.k > 1", "does not exist"},
		{"duplicate", "select n_name as k, n_nationkey as k from nation where k > 1", "ambiguous"},
		{"source ambiguity", "select a.n_name as n_nationkey from nation a, nation b where n_nationkey > 1", "ambiguous"},
		{"aggregate", "select sum(n_nationkey) as k from nation where k > 1", "not allowed in WHERE"},
		{"window", "select row_number() over () as k from nation where k > 1", "not allowed in WHERE"},
		{"self reference", "select k as k from nation where k > 1", "does not exist"},
		{"chain", "select n_nationkey as k, k + 1 as j from nation where j > 1", "does not exist"},
		{"cycle", "select j as k, k as j from nation where k > 1", "does not exist"},
		{"no outer alias", "select n_nationkey as k from nation where exists (select r_name from region where k > 1)", "does not exist"},
		{"no join alias", "select a.n_nationkey as k from nation a join region b on k = b.r_regionkey", "does not exist"},
	}
	for _, mode := range bindModes {
		for _, tc := range cases {
			t.Run(mode.name+"/"+tc.name, func(t *testing.T) {
				_, err := buildOneQuery(t, whereAliasContext(true), tc.sql, mode.prepare)
				if tc.err == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, tc.err)
				}
			})
		}
		t.Run(mode.name+"/disabled", func(t *testing.T) {
			_, err := buildOneQuery(t, whereAliasContext(false), "select n_nationkey as k from nation where k > 1", mode.prepare)
			require.ErrorContains(t, err, "does not exist")
		})
	}
}

func TestWhereSelectAliasPreservesTimeBoundary(t *testing.T) {
	for _, name := range []string{TimeWindowStart, TimeWindowEnd} {
		builder := NewQueryBuilder(pbplan.Query_SELECT, whereAliasContext(true), false, true)
		ctx := NewBindContext(builder, nil)
		ctx.timeTag = 10
		binder := NewWhereBinder(builder, ctx)
		binder.aliases = map[string]tree.Expr{name: tree.NewNumVal(int64(1), "1", false, tree.P_int64)}
		expr, err := binder.BindColRef(tree.NewUnresolvedName(tree.NewCStr(name, 1)), 0, false)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_timestamp), expr.Typ.Id)
		require.Equal(t, ctx.timeTag, expr.GetCol().RelPos)
	}
}

// Compare typed filters with explicit source expressions, independently of
// alias expansion. This catches success with the wrong column/expression.
func TestWhereSelectAliasFiltersMatchExplicitExpressions(t *testing.T) {
	for _, tc := range []struct{ aliased, explicit string }{
		{"select n_nationkey + 1 as k from nation where k > 2", "select n_nationkey + 1 as k from nation where n_nationkey + 1 > 2"},
		{"select n_nationkey as k from nation a full join nation b using(n_nationkey) where k > 1", "select n_nationkey as k from nation a full join nation b using(n_nationkey) where n_nationkey > 1"},
		{"select n_name as n_nationkey from nation where n_nationkey > 1", "select n_name as n_nationkey from nation where nation.n_nationkey > 1"},
		{"select n_nationkey from nation where exists (select r_regionkey as n_nationkey from region where n_nationkey > 1)", "select n_nationkey from nation where exists (select r_regionkey as n_nationkey from region where nation.n_nationkey > 1)"},
	} {
		a, err := buildOneQuery(t, whereAliasContext(true), tc.aliased, false)
		require.NoError(t, err)
		b, err := buildOneQuery(t, whereAliasContext(true), tc.explicit, false)
		require.NoError(t, err)
		filters := func(p *Plan) [][]*Expr {
			var result [][]*Expr
			for _, node := range p.GetQuery().Nodes {
				if len(node.FilterList) > 0 {
					result = append(result, node.FilterList)
				}
			}
			return result
		}
		require.NotEmpty(t, filters(a))
		require.Equal(t, filters(b), filters(a))
	}
}
