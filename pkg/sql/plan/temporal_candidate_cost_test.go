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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/stretchr/testify/require"
)

func TestTemporalIndexSelectionRequiresOriginalAndExecutionProof(t *testing.T) {
	for _, scenario := range []string{"eligible", "missing scope", "missing original owner", "parameter provenance", "sirius", "clock drift", "session row limit", "unknown session row limit"} {
		t.Run(scenario, func(t *testing.T) {
			builder, _, _ := temporalCBOTraversalFixture(t)
			switch scenario {
			case "missing scope":
				builder.temporalPlanningScope = nil
			case "missing original owner":
				builder.temporalOriginalSafeScans = nil
			case "parameter provenance":
				builder.temporalPlanningIntent.ParameterFree = false
			case "sirius":
				builder.temporalPlanningIntent.Placement.Sirius = true
			case "clock drift":
				builder.temporalPlanningIntent.UnixTime++
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
	p, err := BuildPlanWithTemporalScope(ctx, stmt, false, NewTemporalPlanningScope(stmt, intent))
	require.NoError(t, err)
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
