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
	"maps"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type temporalCBOTestTxn struct{ client.TxnOperator }
type temporalCBOTestWorkspace struct{ client.Workspace }
type temporalCBORelationFixture struct {
	engine.Relation
	observation     engine.IndexCostStats
	unknown, verify bool
	onRead          func()
	onVerify        func(*int)
	verified        int
	input           *engine.IndexBlockInput
	observed        int
}

func (s *temporalCBORelationFixture) ReadIndexCostStats(context.Context, client.CatalogReadStamp, *statsinfo.StatsInfo, string) (engine.IndexCostStats, bool, error) {
	if s.onRead != nil {
		s.onRead()
	}
	return s.observation, !s.unknown, nil
}
func (s *temporalCBORelationFixture) VerifyIndexPair(_ context.Context, _ client.CatalogReadStamp, _ engine.IndexPairCheck, budget *int) (bool, error) {
	s.verified++
	if s.onVerify != nil {
		s.onVerify(budget)
	}
	return s.verify, nil
}

func (s *temporalCBORelationFixture) ObserveTemporalBlockInput(_ context.Context, _ client.CatalogReadStamp, _ engine.TemporalBlockInputRequest, _ *int) (engine.IndexBlockInput, bool, error) {
	s.observed++
	if s.input == nil || s.unknown {
		return engine.IndexBlockInput{}, false, nil
	}
	return *s.input, true, nil
}

func TestTemporalCBOObservationRequiresUsableIndex(t *testing.T) {
	for _, scenario := range []string{"not covering", "wrong leading part", "two matching indexes"} {
		t.Run(scenario, func(t *testing.T) {
			builder, source, index := temporalCBOTraversalFixture(t)
			scan := builder.qry.Nodes[0]
			scan.BlockFilterList = []*pb.Expr{DeepCopyExpr(scan.FilterList[0])}
			source.observation.Blocks = 512
			source.input = &engine.IndexBlockInput{Rows: uint64(scan.Stats.TableCnt), Blocks: 512}
			switch scenario {
			case "not covering":
				index.Parts = []string{"id"}
			case "wrong leading part":
				index.Parts = []string{"id", "d"}
			case "two matching indexes":
				other := *index
				other.IndexName = "second"
				scan.TableDef.Indexes = append(scan.TableDef.Indexes, &other)
			}
			_, err := builder.applyIndicesWithOrder(0, map[[2]int32]int{{scan.BindingTags[0], 0}: 1}, make(map[[2]int32]*pb.Expr), false)
			require.NoError(t, err)
			if scenario == "two matching indexes" {
				require.Equal(t, 1, source.observed, "all candidates share one observation")
			} else {
				require.Zero(t, source.observed, "unusable indexes cannot pay a block walk")
			}
		})
	}
}

// The fixture returns the hidden borrow with the same resolution result.
type temporalCBOContext struct {
	*encodedIndexCostTestContext
	hidden         engine.Relation
	onResolve      func()
	mismatchHidden bool
	read           client.CatalogReadStamp
}

func (c *temporalCBOContext) BorrowedTableStillCurrent(proc *process.Process, borrow *BorrowedTable) bool {
	return proc != nil && borrow != nil && borrow.Relation != nil && proc.GetTxnOperator() == borrow.Read.Op
}
func (c *temporalCBOContext) ResolveIndexTableByRef(ref *ObjectRef, name string, snapshot *Snapshot, outputs ...*TableResolution) (*ObjectRef, *TableDef, error) {
	output := PrepareTableResolution(outputs...)
	if c.onResolve != nil {
		c.onResolve()
	}
	ref, full, err := c.MockCompilerContext.ResolveIndexTableByRef(ref, name, snapshot)
	if err != nil || ref == nil || full == nil {
		return ref, full, err
	}
	if c.mismatchHidden {
		ref = DeepCopyObjectRef(ref)
		ref.Obj++
	}
	if output != nil {
		output.Borrow = &BorrowedTable{Relation: c.hidden, Read: c.read}
	}
	return ref, full, nil
}

func temporalCBOTraversalFixture(t *testing.T) (*QueryBuilder, *temporalCBORelationFixture, *pb.IndexDef) {
	t.Helper()
	proc := testutil.NewProcess(t)
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
	intent := ExecutionIntent{TimeZone: time.UTC, UnixTime: proc.Base.UnixTime, Placement: schedule.LocalAccessIntent{CurrentCN: schedule.Worker{ID: "cn", Addr: "cn:6001", Route: schedule.WorkerRouteLocal}}}
	source := &temporalCBORelationFixture{verify: true, observation: engine.IndexCostStats{Rows: stats.TableCnt, Min: float64(types.DatetimeFromClock(2024, 1, 1, 0, 0, 0, 0)), Max: float64(types.DatetimeFromClock(2026, 1, 1, 0, 1, 40, 0)), NDV: stats.TableCnt, Kind: types.T_datetime}}
	read := (client.CatalogReadStamp{TransactionID: "fixture", Snapshot: timestamp.Timestamp{PhysicalTime: 1}, Workspace: &temporalCBOTestWorkspace{}}).BindRead(proc.GetTxnOperator(), 0)
	compiler := &temporalCBOContext{encodedIndexCostTestContext: builder.compCtx.(*encodedIndexCostTestContext), hidden: &temporalCBORelationFixture{}, read: read}
	builder.compCtx = compiler
	builder.planningInput = &PlanningInput{intent: intent, owner: compiler}
	builder.borrowResolutionAllowed = true
	builder.rememberScanBorrow(0, ref, base, &BorrowedTable{Relation: source, Read: read})
	steps := builder.qry.Steps
	builder.qry.Steps = nil
	builder.collectOriginalTemporalOwner(0)
	builder.qry.Steps = steps
	require.True(t, builder.temporalOriginalScan.safe)
	require.True(t, builder.qry.GetExecutionDependentAccess())
	return builder, source, index
}

func TestTemporalCBOThroughIndexTraversal(t *testing.T) {
	for _, scenario := range []string{"selected", "empty access", "cancel empty access", "unknown stats", "unknown estimate", "budget exhausted", "cancel binding", "cancel verification", "cancel verification with exhausted budget", "winner rejected", "different hidden definition", "resolver changes predicate", "resolver changes path", "invalid key collation", "bind tag overflow", "incomplete enumeration", "early producer"} {
		t.Run(scenario, func(t *testing.T) {
			builder, source, index := temporalCBOTraversalFixture(t)
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
			case "cancel empty access":
				cancel(stop)
			case "cancel verification":
				source.onVerify = func(*int) { cancel(stop) }
			case "cancel verification with exhausted budget":
				source.onVerify = func(budget *int) {
					*budget = 0
					cancel(stop)
				}
			case "winner rejected":
				source.verify = false
			case "invalid key collation":
				builder.compCtx.(*temporalCBOContext).tables["hidden"].Cols[0].Typ.Charset = 255
			case "bind tag overflow":
				builder.nextBindTag = math.MaxInt32
			case "incomplete enumeration":
				bad := DeepCopyTableDef(builder.qry.Nodes[0].TableDef, true).Indexes[0]
				bad.IndexName = strings.Repeat("x", engine.MaxIndexPlanningNameBytes+1)
				builder.qry.Nodes[0].TableDef.Indexes = append(builder.qry.Nodes[0].TableDef.Indexes, bad)
			case "different hidden definition":
				builder.compCtx.(*temporalCBOContext).mismatchHidden = true
			case "resolver changes predicate":
				builder.compCtx.(*temporalCBOContext).onResolve = func() {
					builder.qry.Nodes[0].FilterList[0] = MakePlan2BoolConstExprWithType(true)
				}
			case "resolver changes path":
				builder.compCtx.(*temporalCBOContext).onResolve = func() {
					builder.qry.Nodes[0].Limit = MakePlan2Int64ConstExprWithType(1)
				}
			}
			scan := builder.qry.Nodes[0]
			beforeTag, beforeNames, beforeTags := builder.nextBindTag, maps.Clone(builder.nameByColRef), maps.Clone(builder.tag2NodeID)
			mapping := make(map[[2]int32]*pb.Expr)
			var id int32
			var err error
			refs := map[[2]int32]int{{scan.BindingTags[0], 0}: 1}
			if scenario == "empty access" || scenario == "cancel empty access" {
				var applied bool
				id, applied, err = builder.applyTemporalIndexCandidate(0, index, mapping, nil, temporalIndexAccess{})
				require.False(t, applied)
			} else if scenario == "early producer" {
				id, err = builder.applyIndicesForFilters(0, scan, refs, mapping)
			} else {
				id, err = builder.applyIndicesWithOrder(0, refs, mapping, false)
			}
			if scenario == "cancel binding" || scenario == "cancel verification" || scenario == "cancel verification with exhausted budget" || scenario == "cancel empty access" {
				require.ErrorIs(t, err, stop)
			} else {
				require.NoError(t, err)
			}
			if scenario == "different hidden definition" || scenario == "resolver changes predicate" || scenario == "resolver changes path" || scenario == "incomplete enumeration" || scenario == "empty access" || scenario == "cancel empty access" {
				require.Zero(t, source.verified, "invalid access must decline before native verification")
			}
			if scenario != "selected" {
				require.Equal(t, int32(0), id)
				require.Empty(t, mapping)
				require.Len(t, builder.qry.Nodes, 1)
				require.Equal(t, beforeTag, builder.nextBindTag, "declined candidates allocate no binding tag")
				require.Equal(t, beforeNames, builder.nameByColRef)
				require.Equal(t, beforeTags, builder.tag2NodeID)
				return
			}
			require.NotEqual(t, int32(0), id)
			node := builder.qry.Nodes[id]
			require.True(t, node.IndexScanInfo.IsIndexScan)
			require.True(t, node.Stats.ForceOneCN)
			require.Len(t, node.FilterList, 1, "verified storage pruning preserves the complete SQL residual")
			require.False(t, node.FilterList[0].CandidateFilter)
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

func TestTemporalCBORejectsUnmatchedReaderBeforeStats(t *testing.T) {
	for _, scenario := range []string{"column", "bound", "operator", "scale", "marker", "provenance", "clean literal"} {
		t.Run(scenario, func(t *testing.T) {
			builder, source, _ := temporalCBOTraversalFixture(t)
			scan := builder.qry.Nodes[0]
			if scenario == "provenance" || scenario == "clean literal" {
				bound := scan.FilterList[0].GetF().Args[1]
				scan.FilterList[0].GetF().Args[1] = &pb.Expr{Typ: bound.Typ, Expr: &pb.Expr_Lit{Lit: &pb.Literal{
					Value: &pb.Literal_Timestampval{Timestampval: int64(types.UnixNanoToTimestamp(builder.compCtx.GetProcess().Base.UnixTime))},
				}}}
			}
			scan.BlockFilterList = []*pb.Expr{DeepCopyExpr(scan.FilterList[0])}
			filter := scan.BlockFilterList[0]
			switch scenario {
			case "column":
				filter.GetF().Args[0].GetCol().ColPos = 1
			case "bound":
				filter.GetF().Args[1].GetF().Func.ObjName = "different_clock"
			case "operator":
				other, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "<", filter.GetF().Args)
				require.NoError(t, err)
				scan.BlockFilterList[0] = other
			case "scale":
				filter.GetF().Args[0].Typ.Scale++
			case "marker":
				filter.CandidateFilter = true
			case "provenance":
				filter.GetF().Args[1].GetLit().Src = &pb.Expr{Expr: &pb.Expr_P{P: &pb.ParamRef{}}}
			}
			readStats := false
			source.onRead = func() { readStats = true }
			beforeTag := builder.nextBindTag
			beforeNames, beforeTags := maps.Clone(builder.nameByColRef), maps.Clone(builder.tag2NodeID)
			mapping := make(map[[2]int32]*pb.Expr)
			refs := map[[2]int32]int{{scan.BindingTags[0], 0}: 1}
			id, err := builder.applyIndicesWithOrder(0, refs, mapping, false)
			require.NoError(t, err)
			require.Zero(t, id)
			if scenario == "clean literal" {
				require.True(t, readStats)
			} else {
				require.False(t, readStats)
			}
			require.Zero(t, source.observed)
			require.Empty(t, mapping)
			require.Len(t, builder.qry.Nodes, 1)
			require.Equal(t, beforeTag, builder.nextBindTag)
			require.Equal(t, beforeNames, builder.nameByColRef)
			require.Equal(t, beforeTags, builder.tag2NodeID)
		})
	}
}

func TestTemporalCBOReaderProofRequiresBudget(t *testing.T) {
	zero, one := 0, 1
	for _, tc := range []struct {
		name   string
		budget *int
	}{{"missing", nil}, {"exhausted", &zero}, {"insufficient", &one}} {
		t.Run(tc.name, func(t *testing.T) {
			builder, _, _ := temporalCBOTraversalFixture(t)
			require.False(t, temporalBaseReaderShapeSafe(builder.GetContext(), builder.qry, 0, tc.budget))
		})
	}
}

func TestTemporalCBOBaseRequiresFinalReaderPruning(t *testing.T) {
	for _, scenario := range []string{"empty", "mirror", "unknown blocks", "different predicate", "multiple blocks", "multiple filters", "cancelled", "invalid selectivity"} {
		t.Run(scenario, func(t *testing.T) {
			builder, source, _ := temporalCBOTraversalFixture(t)
			scan := builder.qry.Nodes[0]
			originalSelectivity := scan.FilterList[0].Selectivity
			refs := map[[2]int32]int{{scan.BindingTags[0], 0}: 1}
			source.observation.Blocks = 512
			source.input = &engine.IndexBlockInput{Rows: uint64(scan.Stats.TableCnt), Blocks: 512}
			if scenario != "empty" {
				scan.BlockFilterList = []*pb.Expr{DeepCopyExpr(scan.FilterList[0])}
				scan.BlockFilterList[0].Selectivity = 0.5
				scan.BlockFilterList[0].Ndv = 123
			}
			switch scenario {
			case "unknown blocks":
				source.observation.Blocks = 0
			case "different predicate":
				scan.BlockFilterList[0].GetF().Args[0].GetCol().ColPos = 1
			case "multiple blocks":
				scan.BlockFilterList = append(scan.BlockFilterList, DeepCopyExpr(scan.BlockFilterList[0]))
			case "multiple filters":
				scan.FilterList = append(scan.FilterList, DeepCopyExpr(scan.FilterList[0]))
			case "cancelled":
				ctx, cancel := context.WithCancel(builder.GetContext())
				cancel()
				builder.compCtx.SetContext(ctx)
			case "invalid selectivity":
				source.observation.NDV = math.NaN()
			}
			mapping := make(map[[2]int32]*pb.Expr)
			beforeTag := builder.nextBindTag
			beforeNames, beforeTags := maps.Clone(builder.nameByColRef), maps.Clone(builder.tag2NodeID)
			id, err := builder.applyIndicesWithOrder(0, refs, mapping, false)
			if scenario == "cancelled" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			switch scenario {
			case "empty", "mirror":
				require.NotZero(t, id)
			default:
				require.Zero(t, id)
				require.Empty(t, mapping)
				require.Zero(t, source.observed)
				require.Len(t, builder.qry.Nodes, 1)
				require.Equal(t, beforeTag, builder.nextBindTag)
				require.Equal(t, beforeNames, builder.nameByColRef)
				require.Equal(t, beforeTags, builder.tag2NodeID)
			}
			require.Equal(t, originalSelectivity, scan.FilterList[0].Selectivity)
		})
	}
}

func TestTemporalCBOBaseCostRespectsLocalReaders(t *testing.T) {
	observe := func(maxDOP int64, blocks uint64, tp bool) float64 {
		t.Helper()
		builder, source, index := temporalCBOTraversalFixture(t)
		scan := builder.qry.Nodes[0]
		scan.Stats.BlockNum = 64
		if tp {
			scan.Stats.BlockNum, scan.Stats.Cost = 16, 1000
		}
		scan.BlockFilterList = []*pb.Expr{DeepCopyExpr(scan.FilterList[0])}
		builder.qry.MaxDop = maxDOP
		source.observation.Blocks = 512
		source.input = &engine.IndexBlockInput{Rows: objectio.BlockMaxRows, Blocks: blocks}
		cost := builder.newEncodedRegularIndexCostContext(scan, map[[2]int32]int{{scan.BindingTags[0], 0}: 1})
		var work float64
		_, err := builder.enumerateTemporalIndexCandidates(0, []*pb.IndexDef{index}, cost, func(candidate regularIndexAccessCandidate) {
			work = candidate.baseWork
		})
		require.NoError(t, err)
		require.Positive(t, work)
		// Compare costing against the ordinary query-plan DOP, capped by the
		// observed number of matching blocks.
		cpus := min(int64(system.GoMaxProcs()), maxDOP)
		queryPlan := &pb.Plan{Plan: &pb.Plan_Query{Query: builder.qry}}
		CalcQueryDOP(queryPlan, int32(cpus), 1, GetExecType(builder.qry, false, false))
		readers := min(float64(scan.Stats.Dop), float64(blocks))
		serialWork := float64(source.input.Rows) * (cost.baseWidth + float64(len(scan.FilterList))*regularIndexPredicateRowWork)
		require.InDelta(t, serialWork/readers, work, serialWork*1e-8)
		return work
	}
	serial := observe(1, 4, false)
	parallel := observe(4, 4, false)
	if system.GoMaxProcs() > 1 {
		require.Less(t, parallel, serial, "a parallel base scan must receive its reader advantage")
	} else {
		require.Equal(t, serial, parallel)
	}
	require.Equal(t, serial, observe(4, 1, false), "a single matching block cannot use extra readers")
	require.Equal(t, observe(1, 4, true), observe(4, 4, true), "TP execution uses one reader")
}

func TestTemporalCBOFinalReaderChangesActualSelection(t *testing.T) {
	for _, scenario := range []string{"ordered", "scattered", "unknown layout", "empty reader"} {
		t.Run(scenario, func(t *testing.T) {
			builder, source, _ := temporalCBOTraversalFixture(t)
			scan := builder.qry.Nodes[0]
			originalStats := *scan.Stats
			source.observation.NDV = 100
			source.observation.Blocks = 512
			source.input = &engine.IndexBlockInput{Rows: objectio.BlockMaxRows, Blocks: 1}
			if scenario != "empty reader" {
				scan.BlockFilterList = []*pb.Expr{DeepCopyExpr(scan.FilterList[0])}
			}
			if scenario == "scattered" {
				source.input = &engine.IndexBlockInput{Rows: uint64(scan.Stats.TableCnt), Blocks: 512}
			}
			if scenario == "unknown layout" {
				source.input = nil
			}
			mapping := make(map[[2]int32]*pb.Expr)
			id, err := builder.applyIndicesWithOrder(0, map[[2]int32]int{{scan.BindingTags[0], 0}: 1}, mapping, false)
			require.NoError(t, err)
			require.Equal(t, originalStats, *scan.Stats)
			if scenario == "ordered" || scenario == "unknown layout" {
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
