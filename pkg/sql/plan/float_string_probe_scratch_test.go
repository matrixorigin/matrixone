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
	"fmt"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// Keep the previous clone/fold/consume implementation independent of scratch.
func legacyFloatCloneMatches(p *legacyFloatSessionProbe, ctx CompilerContext, budget *tableDumpBindingBudget) (bool, error) {
	proc := legacyFloatSessionProbeProcess(ctx, p.mode)
	session := legacyFloatProbeSession{proc.GetSessionInfo().TimeZone, function.TemporalLocaleNameForProcess(proc)}
	for _, island := range p.islands {
		if err := ctx.GetContext().Err(); err != nil {
			return false, err
		}
		var folded *planpb.Expr
		memo := island.memo
		if memo != nil && memo.owner == budget {
			// Charge a lookup, then the actual decoded comparison input on hits.
			if err := chargeFloatStringRebind(&planpb.Expr{}, budget); err != nil {
				return false, err
			}
			if result, exists := memo.results[session]; exists {
				if result.value != island.wantValue {
					return false, nil
				}
				folded = &planpb.Expr{}
				if err := proto.Unmarshal(result.data, folded); err != nil {
					return false, err
				}
				if err := chargeFloatStringRebind(folded, budget); err != nil {
					return false, err
				}
			}
		}
		if folded == nil {
			if err := chargeFloatStringRebind(island.source, budget); err != nil {
				return false, err
			}
			var err error
			folded, err = ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(island.source), proc, false, true)
			if err != nil {
				return false, ctx.GetContext().Err()
			}
			m := &budget.probeMemo
			if memo != nil && memo.owner == budget && folded.GetLit() != nil {
				if err := chargeFloatStringRebind(folded, budget); err != nil {
					return false, err
				}
				value, err := legacyFloatLiteralFingerprint(folded)
				if err != nil {
					return false, err
				}
				if m.admits(proto.Size(folded)) {
					data, err := proto.Marshal(folded)
					if err != nil {
						return false, err
					}
					if memo.results == nil {
						memo.results = make(map[legacyFloatProbeSession]legacyFloatProbeResult)
					}
					memo.results[session] = legacyFloatProbeResult{data, value}
					m.entries++
					m.bytes += len(data) + legacyFloatProbeMemoOverhead
				}
				if value != island.wantValue {
					return false, nil
				}
			}
		}
		if err := chargeFloatStringRebind(island.want, budget); err != nil {
			return false, err
		}
		if !sameLegacyFloatOrigin(island.want, folded) {
			return false, ctx.GetContext().Err()
		}
	}
	return true, nil
}

func TestLegacyFloatScratchFoldReset(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	ctx := compiler.GetContext()
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		e, err := BindFuncExprImplByPlanExpr(ctx, name, args)
		require.NoError(t, err)
		return e
	}
	cast := func(input *planpb.Expr, typ types.Type) *planpb.Expr {
		e, err := makePlan2CastExpr(ctx, input, makePlan2Type(&typ))
		require.NoError(t, err)
		return e
	}
	temporal := bind("date_format", bind("from_unixtime", makePlan2Int64ConstExprWithType(0)), makePlan2StringConstExprWithType("%M %H"))
	sum := bind("+", makePlan2Int64ConstExprWithType(1), makePlan2Int64ConstExprWithType(2))
	prepared := cast(DeepCopyExpr(sum), types.T_float64.ToType())
	prepared.PreparedNumeric = &planpb.PreparedNumericMetadata{ProvisionalResultPeer: true}
	integerDiv := bind("div", makePlan2Int64ConstExprWithType(7), makePlan2Int64ConstExprWithType(2))
	integerDiv.Typ.Width = 10
	src := makePlan2StringConstExprWithType("frozen")
	src.GetLit().Src = DeepCopyExpr(temporal)
	serialized := makePlan2StringConstExprWithType("serialized")
	serialized.GetLit().IsSerialized = true
	list := func(items ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_List{List: &planpb.ExprList{List: items}}}
	}
	explicitDecimal, err := appendExplicitCastBeforeExpr(ctx, makePlan2StringConstExprWithType("1234567890123456789012345678901234567890123456789012345678901234.5"), types.New(types.T_decimal256, 65, 1).PlanType())
	require.NoError(t, err)
	cases := []struct {
		name             string
		source           *planpb.Expr
		wantErr          bool
		wantVec, wantSrc bool
	}{
		{name: "temporal", source: temporal},
		{name: "list", source: list(makePlan2Int64ConstExprWithType(3), makePlan2Int64ConstExprWithType(4)), wantVec: true},
		{name: "decimal256", source: explicitDecimal, wantVec: true},
		{name: "nested_list", source: list(DeepCopyExpr(sum), makePlan2Int64ConstExprWithType(4))},
		{name: "warning", source: list(DeepCopyExpr(sum), bind("/", makePlan2Int64ConstExprWithType(1), makePlan2Int64ConstExprWithType(0)))},
		{name: "prepared_src", source: prepared, wantSrc: true},
		{name: "integer_div_src", source: integerDiv, wantSrc: true},
		{name: "literal_src", source: src, wantSrc: true},
		{name: "null", source: makePlan2NullConstExprWithType()},
		{name: "serialized", source: serialized},
		{name: "partial_error", source: list(DeepCopyExpr(sum), cast(makePlan2StringConstExprWithType("bad"), types.New(types.T_decimal256, 65, 1))), wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sourceBytes, err := proto.Marshal(tc.source)
			require.NoError(t, err)
			initial := DeepCopyExpr(tc.source)
			initialBytes, err := proto.Marshal(initial)
			require.NoError(t, err)
			island := legacyFloatSessionIsland{source: tc.source}
			root := island.foldInput()
			require.NotNil(t, island.scratch)
			// Positive execution numbering resets, negative memo ids and sparse
			// prepared provenance follow DeepCopy, not proto.Clone.
			require.True(t, proto.Equal(initial, root))
			for _, session := range []struct {
				zone   *time.Location
				locale string
			}{
				{time.UTC, "en_US"}, {time.FixedZone("B", 8*3600), "de_DE"}, {time.UTC, "en_US"},
			} {
				compiler.GetProcess().GetSessionInfo().TimeZone = session.zone
				compiler.GetProcess().GetSessionInfo().LCTimeNames = session.locale
				proc := legacyFloatSessionProbeProcess(compiler, 0)
				expected, oldErr := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(tc.source), proc, false, true)
				func() {
					defer island.resetScratch()
					actual, newErr := ConstantFold(batch.EmptyForConstFoldBatch, island.foldInput(), proc, false, true)
					if tc.wantErr {
						require.Error(t, oldErr)
						require.EqualError(t, newErr, oldErr.Error())
						return
					}
					require.NoError(t, oldErr)
					require.NoError(t, newErr)
					require.True(t, proto.Equal(expected, actual), "complete fold output")
					oldWire, err := proto.Marshal(expected)
					require.NoError(t, err)
					newWire, err := proto.Marshal(actual)
					require.NoError(t, err)
					require.Equal(t, oldWire, newWire)
					if tc.wantVec {
						require.NotNil(t, actual.GetVec())
					}
					if tc.wantSrc {
						require.NotNil(t, actual.GetLit().Src)
					}
				}()
				require.Same(t, root, island.foldInput())
				resetBytes, err := proto.Marshal(root)
				require.NoError(t, err)
				require.Equal(t, initialBytes, resetBytes)
				require.True(t, proto.Equal(initial, root))
				after, err := proto.Marshal(tc.source)
				require.NoError(t, err)
				require.Equal(t, sourceBytes, after)
			}
		})
	}
}

func TestLegacyFloatScratchMatchesOracle(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	source, err := BindFuncExprImplByPlanExpr(compiler.GetContext(), "from_unixtime", []*planpb.Expr{makePlan2Int64ConstExprWithType(0)})
	require.NoError(t, err)
	source.AuxId = 42
	source.GetF().Args[0].AuxId = -3
	source.GetF().Args[0].PreparedNumeric = &planpb.PreparedNumericMetadata{ProvisionalResultPeer: true}
	before, err := proto.Marshal(source)
	require.NoError(t, err)
	want, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(source), legacyFloatSessionProbeProcess(compiler, 0), false, true)
	require.NoError(t, err)
	p := &legacyFloatSessionProbe{islands: []legacyFloatSessionIsland{{source: source, want: want}}}
	old := &legacyFloatSessionProbe{islands: []legacyFloatSessionIsland{{source: source, want: want}}}
	budget, oldBudget := &tableDumpBindingBudget{}, &tableDumpBindingBudget{}
	require.NoError(t, budget.prepareSessionIsland(&p.islands[0], 0))
	require.NoError(t, oldBudget.prepareSessionIsland(&old.islands[0], 0))
	var saved []byte
	for i, zone := range []*time.Location{time.UTC, time.FixedZone("B", 8*3600), time.UTC} {
		compiler.GetProcess().GetSessionInfo().TimeZone = zone
		a, err := p.matches(compiler, budget)
		b, oldErr := legacyFloatCloneMatches(old, compiler, oldBudget)
		require.Equal(t, oldErr, err)
		require.Equal(t, b, a)
		require.Equal(t, oldBudget.visits, budget.visits)
		require.Equal(t, oldBudget.probeMemo.bytes, budget.probeMemo.bytes)
		require.Equal(t, old.islands[0].memo.results, p.islands[0].memo.results)
		if i == 0 {
			for _, result := range p.islands[0].memo.results {
				saved = append([]byte(nil), result.data...)
			}
		}
		if i == 2 {
			require.Equal(t, saved, p.islands[0].memo.results[legacyFloatProbeSession{zone, function.TemporalLocaleNameForProcess(compiler.GetProcess())}].data)
		}
		require.True(t, proto.Equal(DeepCopyExpr(source), p.islands[0].scratch.root))
	}
	// An error after a successful source charge must restore the work tree.
	_, err = p.matches(compiler, &tableDumpBindingBudget{visits: tableDumpMaxCandidateVisits - 2})
	require.Error(t, err)
	require.True(t, proto.Equal(DeepCopyExpr(source), p.islands[0].scratch.root))
	canceled, cancel := context.WithCancel(compiler.GetContext())
	cancel()
	compiler.SetContext(canceled)
	_, err = p.matches(compiler, budget)
	require.ErrorIs(t, err, context.Canceled)
	compiler.SetContext(context.Background())
	after, err := proto.Marshal(source)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestLegacyFloatScratchAdmission(t *testing.T) {
	for _, source := range []*planpb.Expr{
		nil, {}, {Expr: &planpb.Expr_F{}}, {Expr: &planpb.Expr_F{F: &planpb.Function{}}},
		{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{nil}}}},
		{Expr: &planpb.Expr_Lit{}}, {Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: (*planpb.Literal_I64Val)(nil)}}},
		{Expr: &planpb.Expr_T{}}, {Expr: &planpb.Expr_P{P: &planpb.ParamRef{}}},
		{Expr: &planpb.Expr_V{V: &planpb.VarRef{}}}, {Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}},
		{Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{}}},
	} {
		_, _, ok := legacyFloatScratchShape(source)
		require.False(t, ok)
	}
	// Valid fallback inputs preserve the old DeepCopy output and do not
	// accidentally publish a reusable owner.
	for _, source := range []*planpb.Expr{
		{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 3}}},
		{Expr: &planpb.Expr_V{V: &planpb.VarRef{Name: "v"}}},
		{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
		{Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Data: []byte{1, 2}}}},
	} {
		island := legacyFloatSessionIsland{source: source}
		require.True(t, proto.Equal(DeepCopyExpr(source), island.foldInput()))
		require.Nil(t, island.scratch)
	}
}

func TestLegacyFloatScratchTerminalCleanup(t *testing.T) {
	compiler := floatCatalogCompiler(t)
	want := makePlan2StringConstExprWithType("hint")
	probe := &legacyFloatSessionProbe{islands: []legacyFloatSessionIsland{{source: want, want: want}}, searches: []tableDumpSessionCandidate{{increment: 0}}}
	zones := func(yield func(*time.Location) bool) { yield(time.UTC) }
	for _, terminal := range []string{"success", "error", "panic", "cancel", "budget"} {
		t.Run(terminal, func(t *testing.T) {
			probe.islands[0].foldInput()
			require.NotNil(t, probe.islands[0].scratch)
			budget := &tableDumpBindingBudget{}
			if terminal == "budget" {
				budget.visits = tableDumpMaxCandidateVisits
			}
			if terminal == "cancel" {
				ctx, cancel := context.WithCancel(compiler.GetContext())
				cancel()
				compiler.SetContext(ctx)
				defer compiler.SetContext(context.Background())
			}
			run := func() {
				_, err := matchLegacyFloatSessionProbes(compiler, time.UTC, "en_US", zones, []*legacyFloatSessionProbe{probe}, budget, tableDumpMaxCandidateVisits,
					func(tableDumpSessionCandidate, *time.Location, string) (*planpb.Expr, error) {
						require.Nil(t, probe.islands[0].scratch, "clear before full bind")
						if terminal == "panic" {
							panic("bind")
						}
						if terminal == "error" {
							return nil, context.Canceled
						}
						return want, nil
					}, func(tableDumpSessionCandidate, *planpb.Expr) (bool, error) { return true, nil })
				if terminal == "error" || terminal == "cancel" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.NoError(t, err)
				}
			}
			if terminal == "panic" {
				require.PanicsWithValue(t, "bind", run)
			} else {
				run()
			}
			require.Nil(t, probe.islands[0].scratch)
		})
	}
}

func TestLegacyFloatScratchIndependentReaders(t *testing.T) {
	compilers := []*MockCompilerContext{floatCatalogCompiler(t), floatCatalogCompiler(t)}
	source, err := BindFuncExprImplByPlanExpr(compilers[0].GetContext(), "from_unixtime", []*planpb.Expr{makePlan2Int64ConstExprWithType(0)})
	require.NoError(t, err)
	before, err := proto.Marshal(source)
	require.NoError(t, err)
	type result struct {
		root  *planpb.Expr
		err   error
		bytes int64
	}
	results := make(chan result, len(compilers))
	start := make(chan struct{})
	for _, compiler := range compilers {
		go func() {
			<-start
			p := &legacyFloatSessionProbe{islands: []legacyFloatSessionIsland{{source: source}}}
			run := func() error {
				for _, zone := range []*time.Location{time.UTC, time.FixedZone("B", 8*3600), time.UTC} {
					compiler.GetProcess().GetSessionInfo().TimeZone = zone
					expected, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(source), legacyFloatSessionProbeProcess(compiler, 0), false, true)
					if err != nil {
						return err
					}
					p.islands[0].want = expected
					matched, err := p.matches(compiler, &tableDumpBindingBudget{})
					if err != nil {
						return err
					}
					if !matched {
						return fmt.Errorf("reader did not match own session")
					}
					if !proto.Equal(DeepCopyExpr(source), p.islands[0].scratch.root) {
						return fmt.Errorf("reader retained folded graph")
					}
				}
				return nil
			}
			err := run()
			var root *planpb.Expr
			if p.islands[0].scratch != nil {
				root = p.islands[0].scratch.root
			}
			results <- result{root, err, compiler.GetProcess().Mp().CurrNB()}
		}()
	}
	close(start)
	first := <-results
	second := <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.NotSame(t, first.root, second.root)
	require.Zero(t, first.bytes)
	require.Zero(t, second.bytes)
	after, err := proto.Marshal(source)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
