// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type temporalCBOTestTxn struct{ client.TxnOperator }
type temporalCBOTestWorkspace struct{ client.Workspace }
type temporalCBORelationFixture struct {
	engine.Relation
	observation     TemporalRangeStatistics
	unknown, verify bool
	invalidRead     bool
	onValidate      func()
	onRead          func()
	onVerify        func(*int)
	verified        int
}

func (s *temporalCBORelationFixture) ValidateTemporalRead(context.Context, engine.TemporalRead) bool {
	if s.onValidate != nil {
		s.onValidate()
	}
	return !s.invalidRead
}

func (s *temporalCBORelationFixture) ReadTemporalCostStats(context.Context, engine.TemporalRead, *statsinfo.StatsInfo, string) (engine.TemporalStats, bool, error) {
	if s.onRead != nil {
		s.onRead()
	}
	return s.observation, !s.unknown, nil
}
func (s *temporalCBORelationFixture) VerifyTemporalIndexPair(_ context.Context, _ engine.TemporalRead, _ engine.TemporalPairCheck, budget *int) (bool, error) {
	s.verified++
	if s.onVerify != nil {
		s.onVerify(budget)
	}
	return s.verify, nil
}

// Like the frontend resolver, capture the exact definition returned by this
// resolution, after its ordinary metadata clone.
type temporalCBOContext struct {
	*encodedIndexCostTestContext
	scope             *TemporalPlanningScope
	hidden            engine.Relation
	skipHiddenCapture bool
	read              engine.TemporalRead
}

func (c *temporalCBOContext) ResolveIndexTableByRef(ref *ObjectRef, name string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	ref, full, err := c.MockCompilerContext.ResolveIndexTableByRef(ref, name, snapshot)
	if err == nil && !c.skipHiddenCapture {
		c.scope.AddNativeRelation(ref, full, c.hidden, c.read)
	}
	return ref, full, err
}

func temporalCBOTraversalFixture(t *testing.T) (*QueryBuilder, *temporalCBORelationFixture, *pb.IndexDef) {
	t.Helper()
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	proc.Base.TxnOperator = &temporalCBOTestTxn{}
	proc.GetSessionInfo().TimeZone = time.UTC
	proc.Base.UnixTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	mock := NewMockCompilerContext(true, proc)
	mock.SetContext(proc.Ctx)
	builder := NewQueryBuilder(pb.Query_SELECT, mock, false, true)
	tag := builder.genNewBindTag()
	typ := pb.Type{Id: int32(types.T_datetime), Scale: 6, NotNullable: true}
	integer := pb.Type{Id: int32(types.T_int64), NotNullable: true}
	base := &pb.TableDef{TblId: 42, Name: "base", Cols: []*pb.ColDef{{Name: "d", Seqnum: 1, Typ: typ}, {Name: "id", Seqnum: 2, Typ: integer, Primary: true}}, Name2ColIndex: map[string]int32{"d": 0, "id": 1}, Pkey: &pb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}}
	column := GetColExpr(typ, tag, 0)
	column.GetCol().Name = "d"
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	filter, err := BindFuncExprImplByPlanExpr(proc.Ctx, ">", []*pb.Expr{column, now})
	require.NoError(t, err)
	ref := &pb.ObjectRef{Obj: 42, SchemaName: "test", ObjName: "base"}
	scan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, TableDef: base, ObjRef: ref, BindingTags: []int32{tag}, FilterList: []*pb.Expr{filter}, Stats: &pb.Stats{TableCnt: 512 * objectio.BlockMaxRows, Outcnt: 100, Cost: 512 * objectio.BlockMaxRows}}
	builder.qry.Nodes = []*pb.Node{scan}
	builder.qry.Steps = []int32{0}
	builder.ctxByNode = []*BindContext{nil}
	builder.addNameByColRef(tag, base)
	index := &pb.IndexDef{IndexName: "kd", IndexTableName: "hidden", IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(), TableExist: true, Parts: []string{"d", catalog.CreateAlias("id")}}
	base.Indexes = []*pb.IndexDef{index}
	hidden := &pb.TableDef{TblId: 43, Name: "hidden", TableType: catalog.SystemIndexRel, Cols: []*pb.ColDef{{Name: catalog.IndexTableIndexColName, Typ: pb.Type{Id: int32(types.T_varchar)}, Primary: true}, {Name: catalog.IndexTablePrimaryColName, Typ: integer}}, Name2ColIndex: map[string]int32{catalog.IndexTableIndexColName: 0, catalog.IndexTablePrimaryColName: 1}, Pkey: &pb.PrimaryKeyDef{PkeyColName: catalog.IndexTableIndexColName, Names: []string{catalog.IndexTableIndexColName}}}
	hiddenRef := &pb.ObjectRef{Obj: 43, SchemaName: "test", ObjName: "hidden"}
	mock.tables["hidden"], mock.objects["hidden"] = hidden, hiddenRef
	stats := NewStatsInfo()
	stats.TableCnt = scan.Stats.TableCnt
	stats.SizeMap = map[string]uint64{"d": uint64(stats.TableCnt) * 8, "id": uint64(stats.TableCnt) * 8}
	cache := NewStatsCache()
	cache.Set(base.TblId, stats)
	builder.compCtx = &encodedIndexCostTestContext{MockCompilerContext: mock, statsCache: cache}
	intent := TemporalPlanningIntent{ParameterFree: true, TimeZone: time.UTC, UnixTime: proc.Base.UnixTime, Placement: schedule.TemporalAccessIntent{CurrentCN: schedule.Worker{ID: "cn", Addr: "cn:6001", Route: schedule.WorkerRouteLocal}}}
	scope := NewTemporalPlanningScope(&tree.Select{}, intent)
	t.Cleanup(scope.Close)
	builder.temporalPlanningScope = scope
	builder.temporalPlanningIntent = intent
	builder.hasTemporalPlanningIntent = true
	builder.temporalOriginalSafeScans = map[int32]bool{0: true}
	source := &temporalCBORelationFixture{verify: true, observation: TemporalRangeStatistics{Rows: stats.TableCnt, Min: float64(types.DatetimeFromClock(2024, 1, 1, 0, 0, 0, 0)), Max: float64(types.DatetimeFromClock(2026, 1, 1, 0, 1, 40, 0)), NDV: stats.TableCnt, Kind: types.T_datetime}}
	read := engine.TemporalRead{Op: proc.GetTxnOperator(), Stamp: client.CatalogReadStamp{TransactionID: "fixture", Snapshot: timestamp.Timestamp{PhysicalTime: 1}, Workspace: &temporalCBOTestWorkspace{}}}
	require.True(t, scope.AddNativeRelation(ref, base, source, read))
	builder.compCtx = &temporalCBOContext{encodedIndexCostTestContext: builder.compCtx.(*encodedIndexCostTestContext), scope: scope, hidden: &temporalCBORelationFixture{}, read: read}
	return builder, source, index
}

func TestTemporalCBOThroughIndexTraversal(t *testing.T) {
	for _, scenario := range []string{"selected", "unknown stats", "unknown estimate", "budget exhausted", "cancel binding", "cancel verification", "winner rejected", "different hidden definition", "early producer"} {
		t.Run(scenario, func(t *testing.T) {
			builder, source, _ := temporalCBOTraversalFixture(t)
			stop := errors.New("statement owner stopped")
			ctx, cancel := context.WithCancelCause(builder.GetContext())
			defer cancel(nil)
			builder.compCtx.SetContext(ctx)
			switch scenario {
			case "unknown stats":
				source.unknown = true
			case "unknown estimate":
				source.observation.NDV = 0
			case "budget exhausted":
				source.onVerify = func(budget *int) { *budget = 0 }
			case "cancel binding":
				source.onRead = func() { cancel(stop) }
			case "cancel verification":
				source.onVerify = func(*int) { cancel(stop) }
			case "winner rejected":
				source.verify = false
			case "different hidden definition":
				context := builder.compCtx.(*temporalCBOContext)
				mock := context.MockCompilerContext
				previous := mock.tables["hidden"]
				context.skipHiddenCapture = true
				require.True(t, context.scope.AddNativeRelation(mock.objects["hidden"], previous, context.hidden, context.read))
				changed := DeepCopyTableDef(previous, true)
				changed.Cols[0].Typ.Scale++
				mock.tables["hidden"] = changed
			}
			scan := builder.qry.Nodes[0]
			mapping := make(map[[2]int32]*pb.Expr)
			var id int32
			var err error
			refs := map[[2]int32]int{{scan.BindingTags[0], 0}: 1}
			if scenario == "early producer" {
				id, err = builder.applyIndicesForFilters(0, scan, refs, mapping)
			} else {
				id, err = builder.applyIndicesWithOrder(0, refs, mapping, false)
			}
			if scenario == "cancel binding" || scenario == "cancel verification" {
				require.ErrorIs(t, err, stop)
			} else {
				require.NoError(t, err)
			}
			if scenario == "different hidden definition" {
				require.Zero(t, source.verified, "an uncaptured definition must decline before native verification")
			}
			if scenario != "selected" {
				require.Equal(t, int32(0), id)
				require.Empty(t, mapping)
				require.Len(t, builder.qry.Nodes, 1)
				return
			}
			require.NotEqual(t, int32(0), id)
			node := builder.qry.Nodes[id]
			require.True(t, node.IndexScanInfo.IsIndexScan)
			require.True(t, node.Stats.ForceOneCN)
			require.Len(t, node.FilterList, 1, "verified storage pruning preserves the complete SQL residual")
			require.False(t, node.FilterList[0].TemporalCandidateFilter)
			require.Len(t, node.BlockFilterList, 1)
			require.NotEmpty(t, mapping)
			require.Equal(t, 1, source.verified)
		})
	}
}

func TestTemporalCBOConstructionBudgetBeforeCopy(t *testing.T) {
	builder, _, _ := temporalCBOTraversalFixture(t)
	atom, ok := temporalCandidateAtomForExpr(builder.qry.Nodes[0].FilterList[0])
	require.True(t, ok)
	budget := 1
	require.False(t, prepayTemporalCBOConstruction(builder.GetContext(), atom.bound, &budget))
	require.LessOrEqual(t, budget, 0)
}

func TestTemporalCBOBasePricesOnlyFinalReaderPruning(t *testing.T) {
	for _, scenario := range []string{"empty", "mirror", "unknown overlap", "invalid overlap", "unknown blocks", "different predicate", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			builder, source, _ := temporalCBOTraversalFixture(t)
			scan := builder.qry.Nodes[0]
			originalSelectivity := scan.FilterList[0].Selectivity
			refs := map[[2]int32]int{{scan.BindingTags[0], 0}: 1}
			cost := builder.newEncodedRegularIndexCostContext(scan, refs)
			oldWork := cost.baseWork
			observation := source.observation
			observation.Blocks = 512
			observation.HasOverlap = true
			observation.Overlap = 0
			if scenario != "empty" {
				scan.BlockFilterList = []*pb.Expr{DeepCopyExpr(scan.FilterList[0])}
			}
			switch scenario {
			case "unknown overlap":
				observation.HasOverlap = false
			case "invalid overlap":
				observation.Overlap = 2
			case "unknown blocks":
				observation.Blocks = 0
			case "different predicate":
				scan.BlockFilterList[0].GetF().Args[0].GetCol().ColPos = 1
			case "cancelled":
				ctx, cancel := context.WithCancel(builder.GetContext())
				cancel()
				builder.compCtx.SetContext(ctx)
			}
			budget := engine.MaxTemporalBindingWork
			work, ok := builder.temporalCBOBaseWork(scan, observation, .000001, cost, &budget)
			switch scenario {
			case "empty":
				require.True(t, ok)
				require.Equal(t, observation.Rows*(cost.baseWidth+regularIndexPredicateRowWork), work)
			case "mirror":
				require.True(t, ok)
				require.Less(t, work, oldWork)
			default:
				require.False(t, ok)
			}
			require.Equal(t, oldWork, cost.baseWork)
			require.Equal(t, originalSelectivity, scan.FilterList[0].Selectivity)
		})
	}
}

func TestTemporalCBOFinalReaderChangesActualSelection(t *testing.T) {
	for _, scenario := range []string{"mirror low overlap", "mirror high overlap", "empty reader"} {
		t.Run(scenario, func(t *testing.T) {
			builder, source, _ := temporalCBOTraversalFixture(t)
			scan := builder.qry.Nodes[0]
			originalStats := *scan.Stats
			source.observation.NDV = 100
			source.observation.Blocks = 512
			source.observation.HasOverlap = true
			if scenario != "empty reader" {
				scan.BlockFilterList = []*pb.Expr{DeepCopyExpr(scan.FilterList[0])}
			}
			if scenario == "mirror high overlap" {
				source.observation.Overlap = 1
			}
			mapping := make(map[[2]int32]*pb.Expr)
			id, err := builder.applyIndicesWithOrder(0, map[[2]int32]int{{scan.BindingTags[0], 0}: 1}, mapping, false)
			require.NoError(t, err)
			require.Equal(t, originalStats, *scan.Stats)
			if scenario == "mirror low overlap" {
				require.Zero(t, id)
				require.Empty(t, mapping)
				require.Len(t, builder.qry.Nodes, 1)
			} else {
				require.NotZero(t, id)
				require.NotEmpty(t, mapping)
				require.True(t, builder.qry.Nodes[id].Stats.ForceOneCN)
				require.Len(t, builder.qry.Nodes[id].FilterList, 1)
			}
		})
	}
}
