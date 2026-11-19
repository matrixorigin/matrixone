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
	"github.com/stretchr/testify/require"
)

func TestTemporalIndexSelectionRequiresOriginalAndExecutionProof(t *testing.T) {
	for _, scenario := range []string{"eligible", "unmarked lifetime", "missing scope", "missing original owner", "other original scan", "unproved original owner", "parameter provenance", "prepared statement", "sirius", "clock drift", "named zone", "session row limit", "unknown session row limit"} {
		t.Run(scenario, func(t *testing.T) {
			builder, _, _ := newIndexCostFixture(t)
			switch scenario {
			case "unmarked lifetime":
				builder.qry.ExecutionDependentAccess = false
			case "missing scope":
				builder.planningInput.owner = nil
			case "missing original owner":
				builder.originalIndexScan = originalIndexScan{}
			case "other original scan":
				builder.originalIndexScan.scanID++
			case "unproved original owner":
				scan := builder.qry.Nodes[0]
				scan.FilterList = append(scan.FilterList, DeepCopyExpr(scan.FilterList[0]))
				steps := builder.qry.Steps
				builder.qry.Steps = nil // The original owner is captured before step publication.
				builder.collectOriginalIndexOwner(0)
				builder.qry.Steps = steps
				require.False(t, builder.originalIndexScan.safe)
				scan.FilterList = scan.FilterList[:1] // Model a later equivalent rewrite.
			case "parameter provenance":
				compiler := builder.compCtx.(*indexCostCompilerFixture).encodedIndexCostTestContext.MockCompilerContext
				compiler.SetContext(withPreparedSourceBindings(builder.GetContext(), nil, nil))
			case "prepared statement":
				builder.isPrepareStatement = true
			case "sirius":
				builder.planningInput.intent.Placement.Sirius = true
			case "clock drift":
				builder.planningInput.intent.UnixTime++
			case "named zone":
				zone, err := time.LoadLocation("America/New_York")
				require.NoError(t, err)
				builder.compCtx.GetProcess().GetSessionInfo().TimeZone = zone
				builder.planningInput.intent.TimeZone = zone
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
				require.True(t, builder.qry.Nodes[id].BlockFilterList[0].CandidateFilter)
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
			builder, _, _ := newIndexCostFixture(t)
			builder.clearScanBorrows()
			builder.borrowResolutionDisabled = disabled
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
	resolves, borrowRequests int
	onResolve                func()
}

func (c *trackingResolveContext) Resolve(database, name string, snapshot *Snapshot, output *TableResolution) (*ObjectRef, *TableDef, error) {
	output.Reset()
	c.resolves++
	if output != nil {
		c.borrowRequests++
	}
	if c.onResolve != nil {
		c.onResolve()
	}
	return c.CompilerContext.Resolve(database, name, snapshot, nil)
}

func TestParameterizedPreparedPlanningUsesOrdinaryResolver(t *testing.T) {
	fixture, _, _ := newIndexCostFixture(t)
	scan := fixture.qry.Nodes[0]
	underlying := fixture.compCtx.(*indexCostCompilerFixture).encodedIndexCostTestContext
	underlying.tables["base"] = scan.TableDef
	underlying.objects["base"] = scan.ObjRef
	compiler := &trackingResolveContext{CompilerContext: underlying}
	stmt, err := mysql.ParseOne(compiler.GetContext(), "select d from base where d > now() and id = ?", 1)
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
	bound, err := BuildPreparedExecutionPlan(compiler, stmt,
		[]PreparedSourceBinding{{Position: 0, Type: source}},
		[]any{ParamValue{Value: int64(7), SourceType: source, HasSourceType: true}})
	require.NoError(t, err)
	require.NotNil(t, bound.Plan.GetQuery())
	require.Equal(t, 1, compiler.resolves, "the table must still be resolved once")
	require.Zero(t, compiler.borrowRequests, "a parameterized attempt cannot borrow a native relation")
	require.False(t, bound.Plan.GetQuery().GetExecutionDependentAccess())
	require.False(t, HasCapturedExecutionAccess(bound.Plan))
	require.False(t, bound.ValueDependent, "an unsupported temporal access must preserve prepared reuse")

	missing, err := mysql.ParseOne(compiler.GetContext(), "select * from missing_table where id = ?", 1)
	require.NoError(t, err)
	defer missing.Free()
	_, err = BuildPreparedExecutionPlan(compiler, missing,
		[]PreparedSourceBinding{{Position: 0, Type: source}},
		[]any{ParamValue{Value: int64(7), SourceType: source, HasSourceType: true}})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable), err)
	require.Equal(t, 2, compiler.resolves, "the failed lookup must also use the ordinary resolver once")
	require.Zero(t, compiler.borrowRequests)
}

func TestPlanningInputCompilerIdentityChanges(t *testing.T) {
	for _, scenario := range []string{"initial rejection stays rejected", "owner changes during lookup", "clock mismatch", "row limit mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			fixture, _, _ := newIndexCostFixture(t)
			scan := fixture.qry.Nodes[0]
			underlying := fixture.compCtx.(*indexCostCompilerFixture).encodedIndexCostTestContext
			underlying.tables["base"], underlying.objects["base"] = scan.TableDef, scan.ObjRef
			compiler := &trackingResolveContext{CompilerContext: underlying}
			stmt, err := mysql.ParseOne(compiler.GetContext(), "select d from base where d > now()", 1)
			require.NoError(t, err)
			defer stmt.Free()
			root := stmt.(*tree.Select)
			intent := fixture.planningInput.intent
			input := NewPlanningInput(root, intent, compiler)
			switch scenario {
			case "initial rejection stays rejected":
				input.owner = underlying
				compiler.onResolve = func() { input.owner = compiler }
			case "owner changes during lookup":
				compiler.onResolve = func() { input.owner = underlying }
			case "clock mismatch":
				input.intent.UnixTime++
			case "row limit mismatch":
				compiler.GetProcess().Base.SessionInfo.ApplySQLSelectLimit = true
				compiler.GetProcess().SetResolveVariableFunc(nil)
			}
			result, err := BuildPlan(compiler, root, false, input)
			require.NoError(t, err)
			require.False(t, input.claim(root))
			require.Equal(t, 1, compiler.resolves)
			expected := 0
			if scenario == "owner changes during lookup" || scenario == "clock mismatch" {
				expected = 1
			}
			require.Equal(t, expected, compiler.borrowRequests)
			require.True(t, result.GetQuery().GetExecutionDependentAccess(), "optional borrow admission must not erase the reuse dependency")
		})
	}
}

func TestPlanningInputCanRecoverAfterLaterCompilerMismatch(t *testing.T) {
	fixture, _, _ := newIndexCostFixture(t)
	scan := fixture.qry.Nodes[0]
	underlying := fixture.compCtx.(*indexCostCompilerFixture).encodedIndexCostTestContext
	underlying.tables["base"], underlying.objects["base"] = scan.TableDef, scan.ObjRef
	compiler := &trackingResolveContext{CompilerContext: underlying}
	stmt, err := mysql.ParseOne(compiler.GetContext(), "select a.d from base a join base b on a.d=b.d join base c on a.d=c.d", 1)
	require.NoError(t, err)
	defer stmt.Free()
	root := stmt.(*tree.Select)
	input := NewPlanningInput(root, fixture.planningInput.intent, compiler)
	compiler.onResolve = func() {
		if compiler.resolves == 1 {
			input.owner = underlying
		} else {
			input.owner = compiler
		}
	}
	_, err = BuildPlan(compiler, root, false, input)
	require.NoError(t, err)
	require.Equal(t, 3, compiler.resolves)
	require.Equal(t, 2, compiler.borrowRequests, "a rejected lookup cannot poison a later matching ordinary lookup")
}

func TestTemporalCandidateDirectEntryWithoutInput(t *testing.T) {
	builder, _, index := newIndexCostFixture(t)
	builder.planningInput = nil
	access, err := builder.observeIndexCost(0, nil)
	require.NoError(t, err)
	require.Nil(t, access)
	scan, err := builder.buildRegularIndexOnlyScan(index, builder.qry.Nodes[0], nil, nil, &regularIndexOnlyMatch{observation: &indexCostObservation{budget: 1}})
	require.NoError(t, err)
	require.Nil(t, scan)

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
	intent := ExecutionIntent{TimeZone: time.UTC, UnixTime: proc.Base.UnixTime,
		Placement: schedule.LocalAccessIntent{CurrentCN: schedule.Worker{ID: "cn", Addr: "local:6001", Route: schedule.WorkerRouteLocal}}}
	wrapper := &trackingResolveContext{CompilerContext: ctx}
	otherCompiler := &indexCostCompilerFixture{encodedIndexCostTestContext: fixture}
	p, err := BuildPlan(wrapper, stmt, false, NewPlanningInput(stmt, intent, otherCompiler))
	require.NoError(t, err)
	require.Positive(t, wrapper.resolves, "a mismatched resolver must not bypass the compiler wrapper")
	q := p.GetQuery()
	require.NotNil(t, q)
	require.True(t, q.GetExecutionDependentAccess())
	found := false
	for _, node := range q.Nodes {
		for _, filter := range node.FilterList {
			found = found || filter.GetCandidateFilter()
		}
	}
	require.False(t, found, "public planning without a native physical observation must retain the safe scan")
}

func TestTemporalCandidateCostEvaluationErrors(t *testing.T) {
	ctx := context.Background()
	require.Nil(t, indexCostEvaluationError(moerr.NewInvalidInputNoCtx("SQL diagnostic")))
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, moerr.NewOOM(ctx), moerr.NewInternalError(ctx, "internal")} {
		require.ErrorIs(t, indexCostEvaluationError(err), err)
	}
}
