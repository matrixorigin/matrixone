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

// mysqlCompatibleMockContext answers mysql_compatible with the given value and
// leaves every other variable to the shared mock, so a test states only the
// setting under test.
func mysqlCompatibleMockContext(enabled bool) *MockCompilerContext {
	ctx := NewMockCompilerContext(false)
	ctx.ResolveVariableFunc = func(name string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		if name == MysqlCompatibleVarName {
			if enabled {
				return int8(1), nil
			}
			return int8(0), nil
		}
		return nil, nil
	}
	return ctx
}

func buildOneQuery(t *testing.T, ctx CompilerContext, sql string) (*Plan, error) {
	t.Helper()
	stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	defer stmts[0].Free()
	return BuildPlan(ctx, stmts[0], false)
}

// aggregateArgTypes returns the argument types of the single aggregate in the
// built plan. It reads the bound plan rather than the SQL text, so it proves
// what the executor will actually run.
func aggregateArgTypes(t *testing.T, p *Plan) []types.T {
	t.Helper()
	query := p.GetQuery()
	require.NotNil(t, query)
	for _, node := range query.Nodes {
		if len(node.AggList) == 0 {
			continue
		}
		require.Len(t, node.AggList, 1)
		fn := node.AggList[0].GetF()
		require.NotNil(t, fn)
		argTypes := make([]types.T, len(fn.Args))
		for i, arg := range fn.Args {
			argTypes[i] = types.T(arg.Typ.Id)
		}
		return argTypes
	}
	t.Fatal("plan contains no aggregate")
	return nil
}

func aggregateReturnType(t *testing.T, p *Plan) types.T {
	t.Helper()
	query := p.GetQuery()
	require.NotNil(t, query)
	for _, node := range query.Nodes {
		if len(node.AggList) == 0 {
			continue
		}
		return types.T(node.AggList[0].Typ.Id)
	}
	t.Fatal("plan contains no aggregate")
	return types.T_any
}

// A BOOL argument is rejected by default. MO's stricter typing is the correct
// behavior; the flag only relaxes it on request, so the default must keep
// producing the established diagnostic.
func TestBoolNumericAggregateRejectedByDefault(t *testing.T) {
	for _, sql := range []string{
		"select sum(n_nationkey <> 0) from nation",
		"select avg(n_nationkey <> 0) from nation",
	} {
		_, err := buildOneQuery(t, mysqlCompatibleMockContext(false), sql)
		require.Error(t, err, sql)
		require.Contains(t, err.Error(), "invalid argument aggregate function", sql)
	}
}

// An unreadable variable must behave as the strict default, never as an error:
// this setting only ever relaxes a restriction.
func TestBoolNumericAggregateUnresolvableVariableStaysStrict(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.ResolveVariableFunc = func(string, bool, bool) (interface{}, error) {
		return nil, moerr.NewInternalErrorNoCtx("variable store unavailable")
	}
	_, err := buildOneQuery(t, ctx, "select sum(n_nationkey <> 0) from nation")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid argument aggregate function")
}

// Under the flag, SUM/AVG over BOOL bind as the TINYINT aggregate. Asserting
// the bound argument type (not the SQL text) proves the executor runs the
// existing integer aggregate rather than some new BOOL path.
func TestBoolNumericAggregateBindsAsTinyint(t *testing.T) {
	cases := []struct {
		sql        string
		wantReturn types.T
	}{
		// sum(tinyint) -> bigint and avg(tinyint) -> double are MO's own
		// conventions for the type the BOOL argument is read as.
		{"select sum(n_nationkey <> 0) from nation", types.T_int64},
		{"select avg(n_nationkey <> 0) from nation", types.T_float64},
		{"select sum(distinct n_nationkey <> 0) from nation", types.T_int64},
		{"select sum(n_nationkey <> 0) from nation group by n_name", types.T_int64},
	}
	for _, c := range cases {
		p, err := buildOneQuery(t, mysqlCompatibleMockContext(true), c.sql)
		require.NoError(t, err, c.sql)
		require.Equal(t, []types.T{types.T_int8}, aggregateArgTypes(t, p), c.sql)
		require.Equal(t, c.wantReturn, aggregateReturnType(t, p), c.sql)
	}
}

// The coercion is confined to the BOOL argument of SUM/AVG. Every neighbouring
// case must bind exactly as it did before the flag existed, whether it already
// worked (min/max/count over BOOL, sum over a number) or is still rejected
// (aggregates outside the flag's scope, non-numeric arguments).
func TestBoolNumericAggregateScopeIsUnchanged(t *testing.T) {
	ctx := mysqlCompatibleMockContext(true)

	t.Run("bool argument of other aggregates keeps its own binding", func(t *testing.T) {
		for _, sql := range []string{
			"select min(n_nationkey <> 0) from nation",
			"select max(n_nationkey <> 0) from nation",
		} {
			p, err := buildOneQuery(t, ctx, sql)
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
			_, err := buildOneQuery(t, ctx, sql)
			require.Error(t, err, sql)
		}
	})

	t.Run("non-bool arguments are untouched", func(t *testing.T) {
		p, err := buildOneQuery(t, ctx, "select sum(n_nationkey) from nation")
		require.NoError(t, err)
		require.Equal(t, []types.T{types.T_int32}, aggregateArgTypes(t, p))

		_, err = buildOneQuery(t, ctx, "select sum(n_name) from nation")
		require.Error(t, err, "a VARCHAR argument must still be rejected")
	})

	t.Run("non-aggregate calls bind identically with the flag on and off", func(t *testing.T) {
		// The flag resolves a variable only for a single-argument SUM/AVG whose
		// argument bound to BOOL. Whatever an ordinary function does with the
		// same predicate, it must do the same thing either way -- this control
		// does not assume which outcome that is.
		for _, sql := range []string{
			"select abs(n_nationkey <> 0) from nation",
			"select n_nationkey <> 0 from nation",
			"select count(n_nationkey <> 0) from nation",
		} {
			onPlan, onErr := buildOneQuery(t, mysqlCompatibleMockContext(true), sql)
			offPlan, offErr := buildOneQuery(t, mysqlCompatibleMockContext(false), sql)
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
