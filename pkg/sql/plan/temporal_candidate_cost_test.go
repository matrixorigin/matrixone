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
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestTemporalIndexSelectionRequiresOriginalAndExecutionProof(t *testing.T) {
	for _, scenario := range []string{"eligible", "unmarked lifetime", "missing scope", "missing original owner", "unproved original owner", "parameter provenance", "prepared statement", "sirius", "clock drift", "session row limit", "unknown session row limit"} {
		t.Run(scenario, func(t *testing.T) {
			builder, _, _ := temporalCBOTraversalFixture(t)
			switch scenario {
			case "unmarked lifetime":
				builder.qry.TemporalIndexNeedsReplan = false
			case "missing scope":
				builder.temporalResolverAllowed = false
			case "missing original owner":
				builder.temporalOriginalScan = temporalOriginalScan{}
			case "unproved original owner":
				scan := builder.qry.Nodes[0]
				scan.FilterList = append(scan.FilterList, DeepCopyExpr(scan.FilterList[0]))
				steps := builder.qry.Steps
				builder.qry.Steps = nil // The original owner is captured before step publication.
				builder.collectOriginalTemporalOwner(0)
				builder.qry.Steps = steps
				require.False(t, builder.temporalOriginalScan.safe)
				scan.FilterList = scan.FilterList[:1] // Model a later equivalent rewrite.
			case "parameter provenance":
				builder.temporalInput.intent.ParameterFree = false
			case "prepared statement":
				builder.isPrepareStatement = true
			case "sirius":
				builder.temporalInput.intent.Placement.Sirius = true
			case "clock drift":
				builder.temporalInput.intent.UnixTime++
			case "session row limit":
				builder.sessionSelectLimitMayStopEarly = true
			case "unknown session row limit":
				builder.compCtx.GetProcess().Base.SessionInfo.ApplySQLSelectLimit = true
				builder.compCtx.GetProcess().SetResolveVariableFunc(nil)
			}
			scan := builder.qry.Nodes[0]
			mapping := make(map[[2]int32]*plan.Expr)
			id, err := builder.applyIndicesWithOrder(0, map[[2]int32]int{{scan.BindingTags[0], 0}: 1}, mapping, false)
			require.NoError(t, err)
			if scenario == "eligible" {
				require.NotZero(t, id, "the common fixture must actually reach and select the late path")
				require.NotEmpty(t, mapping)
				require.True(t, builder.qry.Nodes[id].BlockFilterList[0].TemporalCandidateFilter)
			} else {
				require.Zero(t, id)
				require.Empty(t, mapping)
				require.Len(t, builder.qry.Nodes, 1)
			}
		})
	}
}

func TestTemporalMissingBorrowPreservesCancellation(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "disabled"}[disabled], func(t *testing.T) {
			builder, _, _ := temporalCBOTraversalFixture(t)
			builder.clearTemporalBorrows()
			builder.temporalBorrowDisabled = disabled
			stop := errors.New("planning stopped")
			ctx, cancel := context.WithCancelCause(builder.GetContext())
			cancel(stop)
			builder.compCtx.SetContext(ctx)
			scan := builder.qry.Nodes[0]
			mapping := make(map[[2]int32]*plan.Expr)
			_, err := builder.applyIndices(0, map[[2]int32]int{{scan.BindingTags[0], 0}: 1}, mapping)
			require.ErrorIs(t, err, stop)
			require.Empty(t, mapping)
			require.Len(t, builder.qry.Nodes, 1)
		})
	}
}

type trackingResolveContext struct {
	CompilerContext
	resolves int
}

func (c *trackingResolveContext) Resolve(database, name string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	c.resolves++
	return c.CompilerContext.Resolve(database, name, snapshot)
}

type changingPlanningResolver struct {
	compiler CompilerContext
	answers  []bool
	checks   int
	base     int
	hidden   int
}

func (r *changingPlanningResolver) IsSameCompiler(CompilerContext) bool {
	index := min(r.checks, len(r.answers)-1)
	r.checks++
	return r.answers[index]
}

func (r *changingPlanningResolver) ResolveBaseForPlanning(database, name string, snapshot *Snapshot) (*ObjectRef, *TableDef, *BorrowedTable, error) {
	r.base++
	ref, def, err := r.compiler.Resolve(database, name, snapshot)
	return ref, def, nil, err
}

func (r *changingPlanningResolver) ResolveIndexForPlanning(base *ObjectRef, name string, snapshot *Snapshot) (*ObjectRef, *TableDef, *BorrowedTable, error) {
	r.hidden++
	ref, def, err := r.compiler.ResolveIndexTableByRef(base, name, snapshot)
	return ref, def, nil, err
}

func (r *changingPlanningResolver) BorrowStillCurrent(CompilerContext, *process.Process, *BorrowedTable) bool {
	return false
}

func TestParameterizedPreparedPlanningUsesOrdinaryResolver(t *testing.T) {
	fixture, _, _ := temporalCBOTraversalFixture(t)
	scan := fixture.qry.Nodes[0]
	underlying := fixture.compCtx.(*temporalCBOContext).encodedIndexCostTestContext
	underlying.tables["base"] = scan.TableDef
	underlying.objects["base"] = scan.ObjRef
	compiler := &trackingResolveContext{CompilerContext: underlying}
	resolver := &changingPlanningResolver{compiler: compiler, answers: []bool{true}}
	stmt, err := mysql.ParseOne(compiler.GetContext(), "select d from base where id = ?", 1)
	require.NoError(t, err)
	defer stmt.Free()

	proc := compiler.GetProcess()
	params := vector.NewVec(types.T_varchar.ToType())
	defer func() {
		proc.SetPrepareParams(nil)
		params.Free(proc.Mp())
	}()
	require.NoError(t, vector.AppendBytes(params, []byte("7"), false, proc.Mp()))
	proc.SetPrepareParams(params)
	source := types.T_int64.ToType()
	intent := fixture.temporalInput.intent
	intent.ParameterFree = false
	bound, err := BuildPreparedExecutionPlanWithTemporalIntent(compiler, stmt,
		[]PreparedSourceBinding{{Position: 0, Type: source}},
		[]any{ParamValue{Value: int64(7), SourceType: source, HasSourceType: true}}, &intent, resolver)
	require.NoError(t, err)
	require.NotNil(t, bound.Plan.GetQuery())
	require.Equal(t, 1, compiler.resolves, "the table must still be resolved once")
	require.Zero(t, resolver.base, "a parameterized attempt cannot use a temporal borrow")
	require.Zero(t, resolver.hidden)
	require.False(t, bound.Plan.GetQuery().GetTemporalIndexNeedsReplan())

	missing, err := mysql.ParseOne(compiler.GetContext(), "select * from missing_table where id = ?", 1)
	require.NoError(t, err)
	defer missing.Free()
	_, err = BuildPreparedExecutionPlanWithTemporalIntent(compiler, missing,
		[]PreparedSourceBinding{{Position: 0, Type: source}},
		[]any{ParamValue{Value: int64(7), SourceType: source, HasSourceType: true}}, &intent, resolver)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable), err)
	require.Equal(t, 2, compiler.resolves, "the failed lookup must also use the ordinary resolver once")
	require.Zero(t, resolver.base)
}

func TestTemporalPlanningResolverIdentityChanges(t *testing.T) {
	for _, tc := range []struct {
		name            string
		answers         []bool
		processMismatch bool
	}{
		{name: "initial rejection stays rejected", answers: []bool{false, true}},
		{name: "later rejection falls back", answers: []bool{true, false, true}},
		{name: "process mismatch", answers: []bool{true}, processMismatch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture, _, _ := temporalCBOTraversalFixture(t)
			scan := fixture.qry.Nodes[0]
			underlying := fixture.compCtx.(*temporalCBOContext).encodedIndexCostTestContext
			underlying.tables["base"] = scan.TableDef
			underlying.objects["base"] = scan.ObjRef
			compiler := &trackingResolveContext{CompilerContext: underlying}
			resolver := &changingPlanningResolver{compiler: compiler, answers: tc.answers}
			stmt, err := mysql.ParseOne(compiler.GetContext(), "select d from base where d > now()", 1)
			require.NoError(t, err)
			defer stmt.Free()
			root := stmt.(*tree.Select)
			intent := fixture.temporalInput.intent
			if tc.processMismatch {
				intent.UnixTime++
			}
			input := NewTemporalPlanningInput(root, intent, resolver)
			p, err := BuildPlanWithTemporalInput(compiler, root, false, input)
			require.NoError(t, err)
			require.False(t, input.claim(root), "the attempted root must consume its input")
			require.Equal(t, intent, input.intent)
			require.Same(t, resolver, input.resolver)
			require.Equal(t, 1, compiler.resolves, "fallback must use the ordinary resolver once")
			require.Zero(t, resolver.base)
			require.Zero(t, resolver.hidden)
			require.True(t, p.GetQuery().GetTemporalIndexNeedsReplan(), "resolver rejection must not drop the cache-lifetime fact")
			if tc.processMismatch {
				require.Zero(t, resolver.checks)
			} else if !tc.answers[0] {
				require.Equal(t, 1, resolver.checks, "initial rejection must remain latched")
				require.True(t, resolver.IsSameCompiler(compiler), "the test resolver would later accept")
			} else {
				require.GreaterOrEqual(t, resolver.checks, 2)
			}
		})
	}
}

func TestTemporalPlanningResolverCanRecoverAfterLaterMismatch(t *testing.T) {
	fixture, _, _ := temporalCBOTraversalFixture(t)
	scan := fixture.qry.Nodes[0]
	underlying := fixture.compCtx.(*temporalCBOContext).encodedIndexCostTestContext
	underlying.tables["base"] = scan.TableDef
	underlying.objects["base"] = scan.ObjRef
	compiler := &trackingResolveContext{CompilerContext: underlying}
	resolver := &changingPlanningResolver{compiler: compiler, answers: []bool{true, false, true}}
	stmt, err := mysql.ParseOne(compiler.GetContext(), "select a.d from base a join base b on a.d = b.d", 1)
	require.NoError(t, err)
	defer stmt.Free()
	root := stmt.(*tree.Select)
	_, err = BuildPlanWithTemporalInput(compiler, root, false, NewTemporalPlanningInput(root, fixture.temporalInput.intent, resolver))
	require.NoError(t, err)
	require.Equal(t, 2, compiler.resolves)
	require.Equal(t, 1, resolver.base, "the later matching table lookup can use its resolver")
	require.Zero(t, resolver.hidden)
	require.GreaterOrEqual(t, resolver.checks, 3)
}

func TestTemporalCandidateDirectEntryWithoutInput(t *testing.T) {
	builder, _, index := temporalCBOTraversalFixture(t)
	builder.temporalInput = nil
	_, err := builder.enumerateTemporalIndexCandidates(0, []*plan.IndexDef{index}, nil, func(regularIndexAccessCandidate) {
		t.Fatal("a missing input cannot yield a temporal candidate")
	})
	require.NoError(t, err)
	id, applied, err := builder.applyTemporalIndexCandidate(0, index, nil, nil, temporalIndexAccess{budget: 1})
	require.NoError(t, err)
	require.Zero(t, id)
	require.False(t, applied)
}

func TestTemporalIndexSelectionFromPublicPlanning(t *testing.T) {
	builder, scanID, index, _ := newEncodedIndexCostTestCase(t,
		[]string{"event_time", catalog.CreateAlias(catalog.CPrimaryKeyColName)}, nil,
		&plan.Stats{TableCnt: 800000, Outcnt: 400000, Selectivity: 0.5, Cost: 800000, BlockNum: 49}, map[int32]int{3: 1}, false)
	scan := builder.qry.Nodes[scanID]
	scan.TableDef.Indexes = []*plan.IndexDef{index}
	fixture := builder.compCtx.(*encodedIndexCostTestContext)
	ctx := &encodedIndexPlanTestContext{MockCompilerContext: fixture.MockCompilerContext, statsCache: fixture.statsCache}
	ctx.tables["events"] = scan.TableDef
	ctx.objects["events"] = scan.ObjRef
	proc := ctx.GetProcess()
	proc.GetSessionInfo().TimeZone = time.UTC
	proc.Base.UnixTime = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	stats := builder.getStatsInfoByTableID(scan.TableDef.TblId).GetStats()
	stats.MinValMap["event_time"] = float64(types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0))
	stats.MaxValMap["event_time"] = float64(types.DatetimeFromClock(2026, 6, 2, 0, 0, 0, 0))
	stats.DataTypeMap["event_time"] = uint64(types.T_datetime)
	stats.BlockNumber = 49
	ctx.statsByID = map[uint64]*statspb.StatsInfo{scan.TableDef.TblId: stats}
	stmts, err := mysql.Parse(ctx.GetContext(), "select event_time from events where event_time > now()", 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	defer stmts[0].Free()
	stmt := stmts[0].(*tree.Select)
	intent := TemporalPlanningIntent{ParameterFree: true, TimeZone: time.UTC, UnixTime: proc.Base.UnixTime,
		Placement: schedule.TemporalAccessIntent{CurrentCN: schedule.Worker{ID: "cn", Addr: "local:6001", Route: schedule.WorkerRouteLocal}}}
	wrapper := &trackingResolveContext{CompilerContext: ctx}
	otherCompiler := &temporalCBOContext{encodedIndexCostTestContext: fixture}
	p, err := BuildPlanWithTemporalInput(wrapper, stmt, false, NewTemporalPlanningInput(stmt, intent, otherCompiler))
	require.NoError(t, err)
	require.Positive(t, wrapper.resolves, "a mismatched resolver must not bypass the compiler wrapper")
	q := p.GetQuery()
	require.NotNil(t, q)
	require.True(t, q.GetTemporalIndexNeedsReplan())
	found := false
	for _, node := range q.Nodes {
		for _, filter := range node.FilterList {
			found = found || filter.GetTemporalCandidateFilter()
		}
	}
	require.False(t, found, "public planning without a native physical observation must retain the safe scan")
}

func TestTemporalCandidateCostEvaluationErrors(t *testing.T) {
	ctx := context.Background()
	require.Nil(t, temporalCandidateCostEvaluationError(moerr.NewInvalidInputNoCtx("SQL diagnostic")))
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, moerr.NewOOM(ctx), moerr.NewInternalError(ctx, "internal")} {
		require.ErrorIs(t, temporalCandidateCostEvaluationError(err), err)
	}
}
