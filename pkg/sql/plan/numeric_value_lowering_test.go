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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestNumericValueKindDistinctLoweringPreservesEqualityTuple(t *testing.T) {
	const source = `SELECT CASE WHEN id=1 THEN X'31' ELSE '1' END AS v
		FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS source`

	t.Run("unaffected distinct keeps ordinary plan", func(t *testing.T) {
		p := buildNumericValuePlan(t, "SELECT DISTINCT v FROM ("+source+") AS d")
		for _, node := range p.GetQuery().Nodes {
			if node == nil {
				continue
			}
			require.Empty(t, node.PhysicalEqualityKeyList)
		}
	})

	t.Run("demanded passenger is excluded from dedup keys", func(t *testing.T) {
		p := buildNumericValuePlan(t, "SELECT ABS(v), SQRT(v) FROM (SELECT DISTINCT v FROM ("+source+") AS d) AS result")
		found := false
		for _, node := range p.GetQuery().Nodes {
			if node == nil || node.NodeType != planpb.Node_AGG || len(node.GroupByHashKey) == 0 {
				continue
			}
			// The source value and its BOOL passenger are both retained as
			// grouping payload. Only the original visible value is hashed.
			if len(node.GroupBy) == 2 && len(node.GroupByHashKey) == 1 {
				require.Equal(t, int32(0), node.GroupByHashKey[0])
				found = true
			}
		}
		require.True(t, found, "the rewritten DISTINCT aggregate must keep its original key subset")
	})
}

func TestNumericValueKindLoweringUsesOwnerSpecificCompanions(t *testing.T) {
	const source = `CASE WHEN id=1 THEN X'31' ELSE '1' END`
	for _, tc := range []struct {
		name  string
		query string
		check func(*testing.T, *planpb.Plan)
	}{
		{
			name:  "private integer argument",
			query: `SELECT REPEAT('a', ` + source + `) FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS s`,
			check: func(t *testing.T, p *planpb.Plan) {
				found := findNumericValueFunction(p, function.CAST, function.IntegerArgumentCastOverload)
				require.NotNil(t, found)
				require.Len(t, found.Args, 3)
				require.Equal(t, int32(types.T_bool), found.Args[2].Typ.Id)
			},
		},
		{
			name:  "OCT",
			query: `SELECT OCT(` + source + `) FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS s`,
			check: func(t *testing.T, p *planpb.Plan) {
				found := findNumericValueFunction(p, function.OCT, -1)
				require.NotNil(t, found)
				require.Len(t, found.Args, 2)
				require.Equal(t, int32(types.T_bool), found.Args[1].Typ.Id)
			},
		},
		{
			name:  "MAKETIME seconds",
			query: `SELECT MAKETIME(1, 0, ` + source + `) FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS s`,
			check: func(t *testing.T, p *planpb.Plan) {
				found := findNumericValueFunction(p, function.MAKETIME, -1)
				require.NotNil(t, found)
				require.Len(t, found.Args, 4)
				require.Equal(t, int32(types.T_bool), found.Args[3].Typ.Id)
			},
		},
		{
			name:  "explicit numeric cast",
			query: `SELECT CAST(` + source + ` AS SIGNED) FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS s`,
			check: func(t *testing.T, p *planpb.Plan) {
				found := findNumericValueFunction(p, function.CAST, function.NumericValueCastOverload)
				require.NotNil(t, found)
				require.Len(t, found.Args, 4)
				require.Equal(t, int32(types.T_bool), found.Args[2].Typ.Id)
			},
		},
		{
			name:  "IF condition",
			query: `SELECT IF(` + source + `, 1, 0) FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS s`,
			check: func(t *testing.T, p *planpb.Plan) {
				found := findNumericValueFunction(p, function.IFF, -1)
				require.NotNil(t, found)
				require.Len(t, found.Args, 4)
				require.Equal(t, int32(types.T_bool), found.Args[3].Typ.Id)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := buildNumericValuePlan(t, tc.query)
			tc.check(t, p)
		})
	}
}

func TestNumericValueLoweringKeepsStaticOrdinaryCastIdentities(t *testing.T) {
	for _, sql := range []string{
		`SELECT CAST(NULL AS int unsigned)`,
		`SELECT CAST('1' AS SIGNED)`,
		`SELECT CAST(split_part(upper('1'), ' ', 1) AS SIGNED)`,
		`SELECT ABS(split_part(upper('1'), ' ', 1))`,
	} {
		t.Run(sql, func(t *testing.T) {
			p := buildNumericValuePlan(t, sql)
			features, err := planpb.RequiredRemoteExpressionFeatures(p.GetQuery())
			require.NoError(t, err)
			require.False(t, features.NumericValuePrograms,
				"a source with a statically false kind must keep the original cast contract")
			require.Nil(t, findNumericValueFunction(p, function.CAST, function.NumericValueCastOverload))
		})
	}
}

func TestNumericValueLoweringUsesOnlyFrozenPreparedTextKind(t *testing.T) {
	for _, tc := range []struct {
		name                string
		bindings            []PreparedSourceBinding
		values              []any
		allowFrozenTextKind bool
		ordinary            bool
	}{
		{
			name:                "frozen text kind at comparison",
			bindings:            []PreparedSourceBinding{{Position: 0, Type: types.T_text.ToType()}},
			values:              []any{ParamValue{Value: "16001", IsBinaryProtocol: true}},
			allowFrozenTextKind: true,
			ordinary:            true,
		},
		{
			name:                "ordinary text outside comparison",
			bindings:            []PreparedSourceBinding{{Position: 0, Type: types.T_text.ToType()}},
			values:              []any{ParamValue{Value: "16001", IsBinaryProtocol: true}},
			allowFrozenTextKind: false,
		},
		{
			name:                "binary value kind",
			bindings:            []PreparedSourceBinding{{Position: 0, Type: types.T_text.ToType()}},
			values:              []any{ParamValue{Value: "16001", IsBin: true, IsBinaryProtocol: true}},
			allowFrozenTextKind: true,
		},
		{
			name:                "unknown execution value",
			bindings:            []PreparedSourceBinding{{Position: 0, Type: types.T_text.ToType()}},
			allowFrozenTextKind: true,
		},
		{
			name:                "unknown source binding",
			values:              []any{ParamValue{Value: "16001", IsBinaryProtocol: true}},
			allowFrozenTextKind: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.SetContext(withPreparedSourceBindings(context.Background(), tc.bindings, tc.values))
			state := preparedBindingState(mock.ctxt.GetContext())
			lowerer := &numericValueKindLowerer{builder: &QueryBuilder{compCtx: &mock.ctxt}}
			source := &Expr{
				Typ:  makeSimplePlan2Type(types.T_text),
				Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
			}
			raw, kind, err := lowerer.numericArgumentProgramOutputs(source, tc.allowFrozenTextKind)
			require.NoError(t, err)
			require.NotNil(t, raw)
			require.NotNil(t, kind)
			if tc.ordinary {
				require.True(t, isFalseBoolLiteral(kind))
				require.Equal(t, int32(0), raw.GetP().Pos)
				require.True(t, state.valueDependent,
					"the false-kind specialization must remain execution-specific")
				return
			}
			_, slot, ok := numericProgramRefIdentity(kind)
			require.True(t, ok, "unknown or binary kind must retain the paired program")
			require.Equal(t, int64(1), slot)
			_, ok = numericProgramRawValue(raw)
			require.True(t, ok, "the value ref must retain its validated lexical owner")
		})
	}
}

func findNumericValueFunction(p *planpb.Plan, functionID int32, overloadID int32) *planpb.Function {
	if p == nil || p.GetQuery() == nil {
		return nil
	}
	var visit func(*planpb.Expr) *planpb.Function
	visit = func(expr *planpb.Expr) *planpb.Function {
		if expr == nil {
			return nil
		}
		if f := expr.GetF(); f != nil {
			fid, oid := function.DecodeOverloadID(f.GetFunc().GetObj())
			if fid == functionID && (overloadID < 0 || oid == overloadID) {
				return f
			}
			for _, arg := range f.Args {
				if found := visit(arg); found != nil {
					return found
				}
			}
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				if found := visit(item); found != nil {
					return found
				}
			}
		}
		if sub := expr.GetSub(); sub != nil {
			return visit(sub.Child)
		}
		return nil
	}
	for _, node := range p.GetQuery().Nodes {
		if node == nil {
			continue
		}
		for _, group := range [][]*planpb.Expr{node.ProjectList, node.GroupBy, node.AggList, node.FilterList, node.OnList} {
			for _, expr := range group {
				if found := visit(expr); found != nil {
					return found
				}
			}
		}
	}
	return nil
}

func buildNumericValuePlan(t *testing.T, sql string) *planpb.Plan {
	t.Helper()
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	statements, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, statement := range statements {
			statement.Free()
		}
	})
	p, err := BuildPlan(ctx, statements[0], false)
	require.NoError(t, err)
	return p
}
