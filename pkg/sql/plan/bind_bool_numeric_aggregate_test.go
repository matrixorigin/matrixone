// Copyright 2021 - 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

// boolSumAvgMockContext answers sql_mode with the mock's default modes, plus
// ENABLE_BOOL_SUMAVG when enabled, so a test states only the mode under test
// and still proves the token composes with the modes a session already has.
func boolSumAvgMockContext(enabled bool) *MockCompilerContext {
	ctx := NewMockCompilerContext(false)
	if enabled {
		ctx.SetSqlModeOverride("ONLY_FULL_GROUP_BY," + mysql.SQLModeEnableBoolSumAvg)
	}
	return ctx
}

// bindModes are the two entry points a statement can take into the binder.
// The mode is resolved once on the QueryBuilder, so both must agree.
var bindModes = []struct {
	name    string
	prepare bool
}{
	{"direct", false},
	{"prepare", true},
}

func buildOneQuery(t *testing.T, ctx CompilerContext, sql string, prepare bool) (*Plan, error) {
	t.Helper()
	stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	defer stmts[0].Free()
	return BuildPlan(ctx, stmts[0], prepare)
}

// firstAggregate returns the single aggregate of the built plan. It reads the
// bound plan rather than the SQL text, so it proves what the executor runs.
func firstAggregate(t *testing.T, p *Plan) *Expr {
	t.Helper()
	query := p.GetQuery()
	require.NotNil(t, query)
	for _, node := range query.Nodes {
		if len(node.AggList) == 0 {
			continue
		}
		require.Len(t, node.AggList, 1)
		return node.AggList[0]
	}
	t.Fatal("plan contains no aggregate")
	return nil
}

func aggregateArgTypes(t *testing.T, p *Plan) []types.T {
	t.Helper()
	fn := firstAggregate(t, p).GetF()
	require.NotNil(t, fn)
	argTypes := make([]types.T, len(fn.Args))
	for i, arg := range fn.Args {
		argTypes[i] = types.T(arg.Typ.Id)
	}
	return argTypes
}

// A BOOL argument is rejected when the compatibility token is explicitly
// absent, from both entry points. This preserves a strict-typing opt-out.
func TestBoolNumericAggregateRejectedWhenModeDisabled(t *testing.T) {
	for _, mode := range bindModes {
		for _, sql := range []string{
			"select sum(n_nationkey <> 0) from nation",
			"select avg(n_nationkey <> 0) from nation",
			"select sum(n_nationkey <> 0) over () from nation",
			"select n_name from nation group by n_name having sum(n_nationkey <> 0) > 0",
		} {
			_, err := buildOneQuery(t, boolSumAvgMockContext(false), sql, mode.prepare)
			require.Error(t, err, "%s %s", mode.name, sql)
			require.Contains(t, err.Error(), "invalid argument aggregate function", "%s %s", mode.name, sql)
		}
	}
}

// An unreadable sql_mode must behave as the strict default, never as an error:
// this mode only ever relaxes a restriction.
func TestBoolNumericAggregateUnresolvableSQLModeStaysStrict(t *testing.T) {
	for name, resolve := range map[string]func(string, bool, bool) (interface{}, error){
		"resolve fails": func(string, bool, bool) (interface{}, error) {
			return nil, moerr.NewInternalErrorNoCtx("variable store unavailable")
		},
		"sql_mode unset": func(string, bool, bool) (interface{}, error) {
			return nil, nil
		},
		"sql_mode is not a string": func(string, bool, bool) (interface{}, error) {
			return int64(0), nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := NewMockCompilerContext(false)
			ctx.ResolveVariableFunc = resolve
			_, err := buildOneQuery(t, ctx, "select sum(n_nationkey <> 0) from nation", false)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid argument aggregate function")
		})
	}
}

// Under the mode, SUM/AVG over BOOL bind as the TINYINT aggregate from both
// entry points. Asserting the bound argument type (not the SQL text) proves
// the executor runs the existing integer aggregate rather than a BOOL path.
func TestBoolNumericAggregateBindsAsTinyint(t *testing.T) {
	cases := []struct {
		sql        string
		wantReturn types.T
	}{
		// SUM(tinyint) -> bigint; AVG(tinyint) -> Decimal128 follows the exact
		// numeric AVG contract for the type the BOOL argument is read as.
		{"select sum(n_nationkey <> 0) from nation", types.T_int64},
		{"select avg(n_nationkey <> 0) from nation", types.T_decimal128},
		{"select sum(json_unquote(json_extract(cast('{\"code\":\"v1\"}' as json), '$.code')) = 'v1')", types.T_int64},
		{"select sum(distinct n_nationkey <> 0) from nation", types.T_int64},
		{"select sum(n_nationkey <> 0) from nation group by n_name", types.T_int64},
		{"select n_name from nation group by n_name having sum(n_nationkey <> 0) > 0", types.T_int64},
	}
	for _, mode := range bindModes {
		for _, c := range cases {
			p, err := buildOneQuery(t, boolSumAvgMockContext(true), c.sql, mode.prepare)
			require.NoError(t, err, "%s %s", mode.name, c.sql)
			require.Equal(t, []types.T{types.T_int8}, aggregateArgTypes(t, p), "%s %s", mode.name, c.sql)
			require.Equal(t, c.wantReturn, types.T(firstAggregate(t, p).Typ.Id), "%s %s", mode.name, c.sql)
		}
		// A window aggregate takes the window binder's route to the same
		// coercion.
		_, err := buildOneQuery(t, boolSumAvgMockContext(true),
			"select sum(n_nationkey <> 0) over () from nation", mode.prepare)
		require.NoError(t, err, mode.name)
	}
}

// The coercion is confined to the BOOL argument of SUM/AVG. Every neighbouring
// case must bind exactly as it did before the mode existed, whether it already
// worked (min/max/count over BOOL, sum over a number) or is still rejected
// (aggregates outside the mode's scope, non-numeric arguments).
func TestBoolNumericAggregateScopeIsUnchanged(t *testing.T) {
	ctx := boolSumAvgMockContext(true)

	t.Run("bool argument of other aggregates keeps its own binding", func(t *testing.T) {
		for _, sql := range []string{
			"select min(n_nationkey <> 0) from nation",
			"select max(n_nationkey <> 0) from nation",
		} {
			p, err := buildOneQuery(t, ctx, sql, false)
			require.NoError(t, err, sql)
			require.Equal(t, []types.T{types.T_bool}, aggregateArgTypes(t, p), sql)
		}
	})

	t.Run("aggregates outside the scope still reject bool", func(t *testing.T) {
		for _, sql := range []string{
			"select bit_and(n_nationkey <> 0) from nation",
			"select bit_or(n_nationkey <> 0) from nation",
			"select bit_xor(n_nationkey <> 0) from nation",
		} {
			_, err := buildOneQuery(t, ctx, sql, false)
			require.Error(t, err, sql)
		}
	})

	t.Run("non-bool arguments are untouched", func(t *testing.T) {
		for _, mode := range bindModes {
			p, err := buildOneQuery(t, ctx, "select sum(n_nationkey) from nation", mode.prepare)
			require.NoError(t, err, mode.name)
			require.Equal(t, []types.T{types.T_int32}, aggregateArgTypes(t, p), mode.name)

			_, err = buildOneQuery(t, ctx, "select sum(n_name) from nation", mode.prepare)
			require.Error(t, err, "%s: a VARCHAR argument must still be rejected", mode.name)
		}
	})

	t.Run("non-aggregate calls bind identically with the mode on and off", func(t *testing.T) {
		// Whatever an ordinary function does with the same predicate, it must
		// do the same thing either way -- this control does not assume which
		// outcome that is.
		for _, sql := range []string{
			"select abs(n_nationkey <> 0) from nation",
			"select n_nationkey <> 0 from nation",
			"select count(n_nationkey <> 0) from nation",
		} {
			onPlan, onErr := buildOneQuery(t, boolSumAvgMockContext(true), sql, false)
			offPlan, offErr := buildOneQuery(t, boolSumAvgMockContext(false), sql, false)
			if offErr != nil {
				require.Error(t, onErr, sql)
				require.Equal(t, offErr.Error(), onErr.Error(), sql)
				continue
			}
			require.NoError(t, onErr, sql)
			require.Equal(t, offPlan.String(), onPlan.String(), sql)
		}
	})
}
