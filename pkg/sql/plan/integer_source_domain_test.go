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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPreparedIntegerSourceDomainBoundaries(t *testing.T) {
	text := ParamValue{Value: "1.5", SourceType: types.T_text.ToType(), HasSourceType: true}
	real := ParamValue{Value: float64(1.5), SourceType: types.T_float64.ToType(), HasSourceType: true}
	null := ParamValue{Value: nil, SourceType: types.T_text.ToType(), HasSourceType: true}
	for _, tc := range []struct {
		name, source, want string
		params             []any
	}{
		{"text coalesce", "coalesce(?,0e0)", "a", []any{text}},
		{"text ifnull", "ifnull(?,0e0)", "a", []any{text}},
		{"mixed text first", "coalesce(?,?)", "a", []any{text, real}},
		{"null first", "coalesce(?,?)", "a.b", []any{null, real}},
		{"numeric peer", "coalesce(?,0e0)", "a.b", []any{real}},
		{"null peer", "coalesce(?,null)", "a.b", []any{real}},
		{"null peer ifnull", "ifnull(?,null)", "a.b", []any{real}},
		{"null peer nested case", "coalesce(case when true then ? else null end,null)", "a.b", []any{real}},
		{"null peer nested nullif", "coalesce(nullif(?,0),null)", "a.b", []any{real}},
		{"null peer text", "coalesce(?,null)", "a", []any{text}},
		{"explicit text null peer", "coalesce(?,cast(null as char))", "a", []any{real}},
		{"explicit float peer", "coalesce(?,cast(0 as double))", "a.b", []any{real}},
	} {
		for _, insert := range []bool{false, true} {
			name := tc.name + "/select"
			if insert {
				name = tc.name + "/insert"
			}
			t.Run(name, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				params := vector.NewVec(types.T_text.ToType())
				defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
				for _, value := range tc.params {
					p := value.(ParamValue)
					require.NoError(t, vector.AppendBytes(params, []byte(fmt.Sprint(p.Value)), p.Value == nil, proc.Mp()))
				}
				proc.SetPrepareParams(params)
				expr := `substring_index("a.b.c.d",".",` + tc.source + `)`
				sql := "select " + expr
				if insert {
					sql = "insert into constraint_test.emp(empno,ename) values (1," + expr + ")"
				}
				prepared, err := runOneStmt(NewMockOptimizer(false), t, "prepare source_domain from '"+sql+"'")
				require.NoError(t, err)
				original := prepared.GetDcl().GetPrepare().Plan
				var bound *Plan
				if insert {
					bound, _, err = FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites(proc.Ctx, original, tc.params)
				} else {
					bound, _, err = FillValuesOfParamsInPlanWithSpecialization(proc.Ctx, original, tc.params)
				}
				require.NoError(t, err)
				var resultExpr *Expr
				require.NoError(t, planpb.VisitExpressionsInOwner(bound, func(root *Expr) error {
					return planpb.VisitExprTree(root, func(e *Expr) error {
						if fn := e.GetF(); fn != nil && fn.Func.GetObjName() == "substring_index" {
							resultExpr = e
						}
						return nil
					})
				}))
				require.NotNil(t, resultExpr)
				result, free, err := colexec.GetReadonlyResultFromExpression(proc, resultExpr, []*batch.Batch{batch.EmptyForConstFoldBatch})
				require.NoError(t, err)
				defer free()
				require.Equal(t, tc.want, result.GetStringAt(0))
			})
		}
	}
}

func TestPreparedIntegerSourceProjectionDomains(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		positions []int32
	}{
		{"scalar project", `select substring_index("a.b.c.d",".",(select coalesce(?,0e0) where true))`, []int32{0}},
		{"column peer", `select substring_index("a.b.c.d",".",coalesce(?,x)) from (select 0e0 as x) d`, []int32{0}},
		{"right union marker", `select substring_index("a.b.c.d",".",(select 0e0 where false union all select ?))`, []int32{0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			prepared, err := runOneStmt(NewMockOptimizer(false), t, "prepare source_projection from '"+tc.sql+"'")
			require.NoError(t, err)
			original := prepared.GetDcl().GetPrepare().Plan
			require.Equal(t, tc.positions, PreparedPlanNumericFallbackParamPositions(original))
			bound, _, err := FillValuesOfParamsInPlanWithSpecialization(proc.Ctx, original, []any{
				ParamValue{Value: float64(1.5), RuntimeType: types.T_float64.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
			})
			require.NoError(t, err)
			// Check the actual producer as well as the consumer. Supplying an invented
			// DOUBLE batch to the outer projection would hide a stale inner TEXT type.
			var producers int
			require.NoError(t, planpb.VisitExpressionsInOwner(bound, func(root *Expr) error {
				return planpb.VisitExprTree(root, func(e *Expr) error {
					if fn := e.GetF(); fn != nil && fn.Func.GetObjName() == "coalesce" {
						producers++
						require.Equal(t, int32(types.T_float64), e.Typ.Id, "inner COALESCE domain")
					}
					return nil
				})
			}))
			if tc.name != "right union marker" {
				require.Positive(t, producers)
			}
			consumer := findPlanFunctionExpr(bound, "substring_index")
			require.NotNil(t, consumer)
			require.Equal(t, int32(types.T_float64), consumer.GetF().Args[2].GetF().Args[0].Typ.Id)
		})
	}
}

func TestPreparedIntegerNullPhysicalDomains(t *testing.T) {
	for _, source := range []string{
		"(select ? group by 1)",
		"(select ? union all select null limit 1)",
		"(select coalesce(?,null) group by 1)",
	} {
		t.Run(source, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			prepared, err := runOneStmt(NewMockOptimizer(false), t,
				`prepare null_domain from 'select substring_index("a.b.c.d",".",`+source+`) '`)
			require.NoError(t, err)
			original := prepared.GetDcl().GetPrepare().Plan
			for _, binary := range []bool{false, true} {
				bound, specialized, err := FillValuesOfParamsInPlanWithSpecialization(proc.Ctx, original, []any{
					ParamValue{Value: nil, SourceType: types.T_text.ToType(), HasSourceType: true, IsBinaryProtocol: binary},
				})
				require.NoError(t, err)
				require.True(t, specialized)
				for _, node := range bound.GetQuery().Nodes {
					for _, expressions := range [][]*Expr{node.ProjectList, node.GroupBy, node.AggList} {
						for _, expr := range expressions {
							require.NotEqual(t, int32(types.T_any), expr.Typ.Id, "physical output in node %d", node.NodeId)
						}
					}
				}
			}
		})
	}
}

func TestPreparedIntegerNestedBitAggregateSource(t *testing.T) {
	for _, name := range []string{"bit_or", "bit_and", "bit_xor"} {
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			prepared, err := runOneStmt(NewMockOptimizer(false), t,
				`prepare bit_source from 'select substring_index("a.b.c.d",".",(select `+name+`(?) from nation))'`)
			require.NoError(t, err)
			bound, _, err := FillValuesOfParamsInPlanWithSpecialization(proc.Ctx, prepared.GetDcl().GetPrepare().Plan, []any{
				ParamValue{Value: "1.5", IsBinaryProtocol: true},
			})
			require.NoError(t, err)
			aggregate := findPlanFunctionExpr(bound, name)
			require.NotNil(t, aggregate)
			input := aggregate.GetF().Args[0]
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, input, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, uint64(1), vector.MustFixedColWithTypeCheck[uint64](result)[0])
		})
	}
}

func TestPreparedResultPeerUsesCurrentOccurrence(t *testing.T) {
	proc := testutil.NewProcess(t)
	floatType := types.T_float64.ToType()
	textType := types.T_text.ToType()
	currentColumn := &Expr{Typ: makePlan2Type(&floatType), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	peer, err := appendCastBeforeExpr(proc.Ctx, currentColumn, makePlan2Type(&textType))
	require.NoError(t, err)
	peer.PreparedNumeric = &planpb.PreparedNumericMetadata{
		ProvisionalResultPeer: true, ProvisionalResultPeerTypeId: int32(types.T_float64),
		// Deliberately invalid legacy snapshot: it must never enter execution.
		StringDomainSource: &Expr{Typ: makePlan2Type(&floatType), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 7, ColPos: 999}}},
	}
	restored, err := restorePreparedResultPeer(proc.Ctx, peer)
	require.NoError(t, err)
	require.Same(t, currentColumn, restored)

	folded := makePlan2StringConstExprWithType("1.5")
	folded.PreparedNumeric = peer.PreparedNumeric
	restored, err = restorePreparedResultPeer(proc.Ctx, folded)
	require.NoError(t, err)
	result, free, err := colexec.GetReadonlyResultFromExpression(proc, restored, []*batch.Batch{batch.EmptyForConstFoldBatch})
	require.NoError(t, err)
	defer free()
	require.Equal(t, 1.5, vector.MustFixedColWithTypeCheck[float64](result)[0])
	require.Equal(t, int32(types.T_varchar), folded.Typ.Id, "cached source must stay unchanged")

	nullPeer := &Expr{Typ: makePlan2Type(&textType), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
	nullPeer.PreparedNumeric = &planpb.PreparedNumericMetadata{ProvisionalResultPeer: true}
	restored, err = restorePreparedResultPeer(proc.Ctx, nullPeer)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_any), restored.Typ.Id)
	require.True(t, restored.GetLit().GetIsnull())
	require.Nil(t, restored.PreparedNumeric)
	require.Equal(t, int32(types.T_text), nullPeer.Typ.Id, "cached NULL peer must stay unchanged")

	for _, value := range []*Expr{currentColumn, makePlan2StringConstExprWithType("1.5")} {
		restored, err = restorePreparedResultPeer(proc.Ctx, value)
		require.NoError(t, err)
		require.Same(t, value, restored)
	}
}

func TestPreparedArithmeticExplicitDoublePeer(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t, `prepare cast_boundary from 'select (? / 2) + cast(1 as double)'`)
	require.NoError(t, err)
	proc := testutil.NewProcess(t)
	bound, _, err := FillValuesOfParamsInPlanWithSpecialization(proc.Ctx, prepared.GetDcl().GetPrepare().Plan, []any{
		ParamValue{Value: "9007199254740993.5", SourceType: types.New(types.T_decimal128, 17, 1), HasSourceType: true},
	})
	require.NoError(t, err)
	query := bound.GetQuery()
	require.Equal(t, int32(types.T_float64), query.Nodes[query.Steps[0]].ProjectList[0].Typ.Id)
}
