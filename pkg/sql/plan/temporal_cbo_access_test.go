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
	"fmt"
	"maps"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
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
type indexCostRelationFixture struct {
	engine.Relation
	observation     engine.IndexCostStats
	observeErr      error
	onObserve       func()
	unknown, verify bool
	onRead          func()
	onVerify        func(*int)
	verified        int
	input           *engine.IndexBlockInput
	observed        int
}

func (s *indexCostRelationFixture) ReadIndexCostStats(context.Context, client.CatalogReadStamp, *statsinfo.StatsInfo, string) (engine.IndexCostStats, bool, error) {
	if s.onRead != nil {
		s.onRead()
	}
	return s.observation, !s.unknown, nil
}
func (s *indexCostRelationFixture) VerifyIndexPair(_ context.Context, _ client.CatalogReadStamp, _ engine.IndexPairCheck, budget *int) (bool, error) {
	s.verified++
	if s.onVerify != nil {
		s.onVerify(budget)
	}
	return s.verify, nil
}

func (s *indexCostRelationFixture) ObserveIndexBlockInput(_ context.Context, _ client.CatalogReadStamp, _ engine.IndexBlockInputRequest, _ *int) (engine.IndexBlockInput, bool, error) {
	s.observed++
	if s.onObserve != nil {
		s.onObserve()
	}
	if s.observeErr != nil {
		return engine.IndexBlockInput{}, false, s.observeErr
	}
	if s.input == nil || s.unknown {
		return engine.IndexBlockInput{}, false, nil
	}
	return *s.input, true, nil
}

func TestTemporalCBOObservationRequiresUsableIndex(t *testing.T) {
	for _, scenario := range []string{"not covering", "wrong leading part", "two matching indexes"} {
		t.Run(scenario, func(t *testing.T) {
			builder, source, index := newIndexCostFixture(t)
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
type indexCostCompilerFixture struct {
	*encodedIndexCostTestContext
	hidden         engine.Relation
	onResolve      func()
	mismatchHidden bool
	read           client.CatalogReadStamp
	resolveErr     error
}

func (c *indexCostCompilerFixture) BorrowedTableStillCurrent(proc *process.Process, borrow *BorrowedTable) bool {
	return proc != nil && borrow != nil && borrow.Relation != nil && proc.GetTxnOperator() == borrow.Read.Op
}
func (c *indexCostCompilerFixture) ResolveIndexTableByRef(ref *ObjectRef, name string, snapshot *Snapshot, output *TableResolution) (*ObjectRef, *TableDef, error) {
	output.Reset()
	if c.onResolve != nil {
		c.onResolve()
	}
	if c.resolveErr != nil {
		return nil, nil, c.resolveErr
	}
	ref, full, err := c.MockCompilerContext.ResolveIndexTableByRef(ref, name, snapshot, nil)
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

func newIndexCostFixture(t *testing.T) (*QueryBuilder, *indexCostRelationFixture, *pb.IndexDef) {
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
	source := &indexCostRelationFixture{verify: true, observation: engine.IndexCostStats{Rows: stats.TableCnt, Min: float64(types.DatetimeFromClock(2024, 1, 1, 0, 0, 0, 0)), Max: float64(types.DatetimeFromClock(2026, 1, 1, 0, 1, 40, 0)), NDV: stats.TableCnt, Kind: types.T_datetime}}
	read := (client.CatalogReadStamp{TransactionID: "fixture", Snapshot: timestamp.Timestamp{PhysicalTime: 1}, Workspace: &temporalCBOTestWorkspace{}}).BindRead(proc.GetTxnOperator(), 0)
	compiler := &indexCostCompilerFixture{encodedIndexCostTestContext: builder.compCtx.(*encodedIndexCostTestContext), hidden: &indexCostRelationFixture{}, read: read}
	builder.compCtx = compiler
	builder.planningInput = &PlanningInput{intent: intent, owner: compiler}
	builder.rememberScanBorrow(0, ref, base, &BorrowedTable{Relation: source, Read: read})
	steps := builder.qry.Steps
	builder.qry.Steps = nil
	builder.collectOriginalIndexOwner(0)
	builder.qry.Steps = steps
	require.True(t, builder.originalIndexScan.safe)
	require.True(t, builder.qry.GetExecutionDependentAccess())
	return builder, source, index
}

func TestTemporalCBOThroughIndexTraversal(t *testing.T) {
	for _, scenario := range []string{"selected", "empty access", "cancel empty access", "unknown stats", "unknown estimate", "budget exhausted", "cancel binding", "cancel verification", "cancel verification with exhausted budget", "clock drift during observation", "zone drift during observation", "clock drift during resolve", "zone drift during verification", "transaction drift during verification", "winner rejected", "different hidden definition", "resolver changes predicate", "resolver changes path", "invalid key collation", "bind tag overflow", "incomplete enumeration", "early producer"} {
		t.Run(scenario, func(t *testing.T) {
			builder, source, index := newIndexCostFixture(t)
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
			case "clock drift during observation":
				source.onRead = func() { builder.compCtx.GetProcess().Base.UnixTime++ }
			case "zone drift during observation":
				source.onRead = func() { builder.compCtx.GetProcess().GetSessionInfo().TimeZone = time.FixedZone("drift", 3600) }
			case "clock drift during resolve":
				builder.compCtx.(*indexCostCompilerFixture).onResolve = func() { builder.compCtx.GetProcess().Base.UnixTime++ }
			case "zone drift during verification":
				source.onVerify = func(*int) { builder.compCtx.GetProcess().GetSessionInfo().TimeZone = time.FixedZone("drift", 3600) }
			case "transaction drift during verification":
				proc := builder.compCtx.GetProcess()
				original := proc.Base.TxnOperator
				defer func() { proc.Base.TxnOperator = original }()
				source.onVerify = func(*int) { proc.Base.TxnOperator = &temporalCBOTestTxn{} }
			case "winner rejected":
				source.verify = false
			case "invalid key collation":
				builder.compCtx.(*indexCostCompilerFixture).tables["hidden"].Cols[0].Typ.Charset = 255
			case "bind tag overflow":
				builder.nextBindTag = math.MaxInt32
			case "incomplete enumeration":
				bad := DeepCopyTableDef(builder.qry.Nodes[0].TableDef, true).Indexes[0]
				bad.IndexName = strings.Repeat("x", engine.MaxIndexPlanningNameBytes+1)
				builder.qry.Nodes[0].TableDef.Indexes = append(builder.qry.Nodes[0].TableDef.Indexes, bad)
			case "different hidden definition":
				builder.compCtx.(*indexCostCompilerFixture).mismatchHidden = true
			case "resolver changes predicate":
				builder.compCtx.(*indexCostCompilerFixture).onResolve = func() {
					builder.qry.Nodes[0].FilterList[0] = MakePlan2BoolConstExprWithType(true)
				}
			case "resolver changes path":
				builder.compCtx.(*indexCostCompilerFixture).onResolve = func() {
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
				var built *pb.Node
				built, err = builder.buildRegularIndexOnlyScan(index, scan, mapping, nil, &regularIndexOnlyMatch{observation: &indexCostObservation{}})
				require.Nil(t, built)
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
			require.True(t, node.IndexScanInfo.RequiresCapturedExecution)
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
	builder, _, _ := newIndexCostFixture(t)
	atom, ok := analyzeIndexComparison(builder.GetContext(), builder.qry.Nodes[0].FilterList[0])
	require.True(t, ok)
	budget := 1
	require.False(t, atom.prepayNecessaryPredicate(builder.GetContext(), &budget))
	require.LessOrEqual(t, budget, 0)
}

func TestTemporalCBORejectsUnmatchedReaderBeforeStats(t *testing.T) {
	for _, scenario := range []string{"column", "bound", "operator", "scale", "marker", "provenance", "clean literal"} {
		t.Run(scenario, func(t *testing.T) {
			builder, source, _ := newIndexCostFixture(t)
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
			builder, _, _ := newIndexCostFixture(t)
			require.False(t, indexBaseReaderShapeSafe(builder.GetContext(), builder.qry, 0, tc.budget))
		})
	}
}

func TestTemporalCBOBaseRequiresFinalReaderPruning(t *testing.T) {
	for _, scenario := range []string{"empty", "mirror", "unknown blocks", "different predicate", "multiple blocks", "multiple filters", "cancelled", "invalid selectivity"} {
		t.Run(scenario, func(t *testing.T) {
			builder, source, _ := newIndexCostFixture(t)
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
		builder, source, index := newIndexCostFixture(t)
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
		require.True(t, cost.indexCoversRequiredColumns(index))
		access, err := builder.observeIndexCost(0, cost)
		require.NoError(t, err)
		require.NotNil(t, access)
		work := access.baseWork
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
			builder, source, _ := newIndexCostFixture(t)
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

// Exercise the real ordinary selector and constructor, including the mandatory
// resolver checkpoint. Optional observations must not manufacture a candidate.
func TestIntegerIndexCostObservation(t *testing.T) {
	for _, scenario := range []string{"ordered base", "shuffled index", "missing metadata", "observation error", "resolver error", "initial clock drift", "resolver clock drift", "resolver zone drift", "observation clock drift", "observation zone drift", "observation transaction drift", "observation row limit", "cancel observation"} {
		t.Run(scenario, func(t *testing.T) {
			builder, source, _ := newIndexCostFixture(t)
			scan := builder.qry.Nodes[0]
			proc := builder.compCtx.GetProcess()
			scan.TableDef.Cols[0].Typ = pb.Type{Id: int32(types.T_int64), NotNullable: true}
			column := GetColExpr(scan.TableDef.Cols[0].Typ, scan.BindingTags[0], 0)
			bound := MakePlan2Int64ConstExprWithType(int64(scan.Stats.TableCnt) - 100)
			filter, err := BindFuncExprImplByPlanExpr(proc.Ctx, ">=", []*pb.Expr{column, bound})
			require.NoError(t, err)
			filter.Selectivity = 100 / scan.Stats.TableCnt
			scan.FilterList = []*pb.Expr{filter}
			scan.BlockFilterList = []*pb.Expr{DeepCopyExpr(filter)}
			scan.Stats.Selectivity = filter.Selectivity
			source.observation.Kind = types.T_int64
			source.observation.Min, source.observation.Max = 0, scan.Stats.TableCnt
			source.observation.Blocks = 512
			source.input = &engine.IndexBlockInput{Rows: objectio.BlockMaxRows, Blocks: 1}
			steps := builder.qry.Steps
			builder.qry.Steps = nil
			builder.qry.ExecutionDependentAccess = false
			builder.collectOriginalIndexOwner(0)
			builder.qry.Steps = steps
			require.True(t, builder.originalIndexScan.safe)
			require.False(t, builder.qry.ExecutionDependentAccess, "a literal integer does not acquire a clock dependency")
			compiler := builder.compCtx.(*indexCostCompilerFixture)
			resolved := 0
			compiler.onResolve = func() { resolved++ }
			source.onRead = func() { require.Equal(t, 1, resolved, "ordinary resolution must precede optional observation") }
			stop := errors.New("statement stopped while observing blocks")
			ctx, cancel := context.WithCancelCause(builder.GetContext())
			defer cancel(nil)
			builder.compCtx.SetContext(ctx)
			switch scenario {
			case "shuffled index":
				source.input = &engine.IndexBlockInput{Rows: uint64(scan.Stats.TableCnt), Blocks: 512}
			case "missing metadata":
				source.input = nil
			case "observation error":
				source.observeErr = errors.New("optional metadata unavailable")
			case "resolver error":
				compiler.resolveErr = errors.New("mandatory index resolution failed")
			case "initial clock drift":
				proc.Base.UnixTime++
			case "resolver clock drift":
				compiler.onResolve = func() { resolved++; proc.Base.UnixTime++ }
			case "resolver zone drift":
				compiler.onResolve = func() { resolved++; proc.GetSessionInfo().TimeZone = time.FixedZone("drift", 3600) }
			case "observation clock drift":
				source.onObserve = func() { proc.Base.UnixTime++ }
			case "observation zone drift":
				source.onObserve = func() { proc.GetSessionInfo().TimeZone = time.FixedZone("drift", 3600) }
			case "observation transaction drift":
				original := proc.Base.TxnOperator
				defer func() { proc.Base.TxnOperator = original }()
				source.onObserve = func() { proc.Base.TxnOperator = &temporalCBOTestTxn{} }
			case "observation row limit":
				source.onObserve = func() {
					proc.Base.SessionInfo.ApplySQLSelectLimit = true
					proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { return uint64(1), nil })
				}
			case "cancel observation":
				source.onObserve = func() { cancel(stop) }
			}
			run := func() (int32, error) {
				return builder.applyIndicesWithOrder(0, map[[2]int32]int{{scan.BindingTags[0], 0}: 1}, make(map[[2]int32]*pb.Expr), false)
			}
			if scenario == "resolver error" {
				_, err := run()
				require.ErrorIs(t, err, compiler.resolveErr)
				require.Zero(t, source.observed)
				return
			}
			id, err := run()
			if scenario == "cancel observation" {
				require.ErrorIs(t, err, stop)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, resolved)
			switch scenario {
			case "ordered base", "initial clock drift", "resolver clock drift", "resolver zone drift", "observation clock drift", "observation zone drift":
				require.Equal(t, int32(0), id)
			default:
				require.NotEqual(t, int32(0), id)
				require.True(t, builder.qry.Nodes[id].IndexScanInfo.IsIndexScan)
			}
			require.Equal(t, 1, source.observed)
			for _, node := range builder.qry.Nodes {
				for _, expr := range append(append([]*pb.Expr(nil), node.FilterList...), node.BlockFilterList...) {
					require.False(t, expr.CandidateFilter)
				}
			}
		})
	}
}

// The previous clone-and-clear proof is the reference for this allocation-only
// change. Metadata and unknown protobuf fields remain part of identity.
func TestIndexCostFilterEqualityPreservesNonStatsAndSources(t *testing.T) {
	builder, _, _ := newIndexCostFixture(t)
	original := builder.qry.Nodes[0].FilterList[0]
	for _, scenario := range []string{"same", "nested stats", "column", "scale", "function", "marker", "aux", "expr unknown", "function unknown", "type unknown", "same unknown"} {
		t.Run(scenario, func(t *testing.T) {
			a, b := proto.Clone(original).(*pb.Expr), proto.Clone(original).(*pb.Expr)
			switch scenario {
			case "nested stats":
				b.Ndv, b.Selectivity = 17, .7
				b.GetF().Args[0].Ndv = 3
				b.GetF().Args[1].Selectivity = .4
			case "column":
				b.GetF().Args[0].GetCol().ColPos++
			case "scale":
				b.GetF().Args[0].Typ.Scale++
			case "function":
				b.GetF().Args[1].GetF().Func.ObjName = "different"
			case "marker":
				b.GetF().Args[1].CandidateFilter = true
			case "aux":
				b.GetF().Args[0].AuxId = 7
			case "expr unknown":
				b.XXX_unrecognized = []byte{0xa0, 0x06, 1}
			case "function unknown":
				b.GetF().XXX_unrecognized = []byte{0xa0, 0x06, 1}
			case "type unknown":
				b.GetF().Args[0].Typ.XXX_unrecognized = []byte{0xa0, 0x06, 1}
			case "same unknown":
				a.XXX_unrecognized = []byte{0xa0, 0x06, 1}
				b.XXX_unrecognized = []byte{0xa0, 0x06, 1}
			}
			beforeA, beforeB := proto.Clone(a), proto.Clone(b)
			x, y := proto.Clone(a).(*pb.Expr), proto.Clone(b).(*pb.Expr)
			var clearStats func(*pb.Expr)
			clearStats = func(e *pb.Expr) {
				e.Ndv, e.Selectivity = 0, 0
				if f := e.GetF(); f != nil {
					for _, arg := range f.Args {
						clearStats(arg)
					}
				}
			}
			clearStats(x)
			clearStats(y)
			budget := engine.MaxIndexPlanningWork
			want := scenario == "same" || scenario == "nested stats" || scenario == "same unknown"
			require.Equal(t, want, proto.Equal(x, y))
			require.Equal(t, want, indexCostFiltersEqual(builder.GetContext(), a, b, &budget))
			require.True(t, proto.Equal(beforeA, a))
			require.True(t, proto.Equal(beforeB, b))
		})
	}
}

// Both indexes cover the same mixed predicate. A one-byte difference in the
// whole-table payload makes their observed costs close without inventing a
// floating-point score or depending on the order in which they are considered.
func TestObservedIndexCandidateRanking(t *testing.T) {
	for _, delta := range []int64{-1, 0, 1} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("bytes=%d/reverse=%v", delta, reverse), func(t *testing.T) {
				builder, source, first := newIndexCostFixture(t)
				scan := builder.qry.Nodes[0]
				compiler := builder.compCtx.(*indexCostCompilerFixture)
				cached := compiler.statsCache.Get(scan.TableDef.TblId)
				stats := cached.GetStats()
				rows := uint64(scan.Stats.TableCnt)
				for _, name := range []string{"a", "b"} {
					pos := len(scan.TableDef.Cols)
					scan.TableDef.Name2ColIndex[name] = int32(pos)
					scan.TableDef.Cols = append(scan.TableDef.Cols, &pb.ColDef{Name: name, Seqnum: uint32(pos + 1), Typ: pb.Type{Id: int32(types.T_varchar), Width: 32}})
					stats.SizeMap[name] = 8 * rows
				}
				stats.SizeMap["b"] = uint64(int64(stats.SizeMap["b"]) + delta)
				builder.addNameByColRef(scan.BindingTags[0], scan.TableDef)
				first.Parts = []string{"d", "a", catalog.CreateAlias("id")}
				second := addIndexCostAlternative(builder, first)
				second.Parts = []string{"d", "b", catalog.CreateAlias("id")}
				if reverse {
					scan.TableDef.Indexes[0], scan.TableDef.Indexes[1] = second, first
				}
				want := first.IndexName
				if delta < 0 {
					want = second.IndexName
				} else if delta == 0 {
					want = scan.TableDef.Indexes[0].IndexName
				}
				scan.BlockFilterList = []*pb.Expr{DeepCopyExpr(scan.FilterList[0])}
				source.observation.Blocks = 512
				source.input = &engine.IndexBlockInput{Rows: rows, Blocks: 512}
				refs := map[[2]int32]int{{scan.BindingTags[0], 0}: 1}
				cost := builder.newEncodedRegularIndexCostContext(scan, refs)
				require.Less(t, math.Abs(cost.serialWidths[2]-cost.serialWidths[3]), 1e-6)
				if delta != 0 {
					require.NotEqual(t, cost.serialWidths[2], cost.serialWidths[3])
				}
				mapping := make(map[[2]int32]*pb.Expr)
				id, err := builder.applyIndicesWithOrder(0, refs, mapping, false)
				require.NoError(t, err)
				require.NotZero(t, id)
				require.Equal(t, want, builder.qry.Nodes[id].IndexScanInfo.IndexName)
				require.Equal(t, 1, source.observed)
				require.NotEmpty(t, mapping)
			})
		}
	}
}

func addIndexCostAlternative(builder *QueryBuilder, first *pb.IndexDef) *pb.IndexDef {
	second := *first
	second.IndexName, second.IndexTableName = "second", "hidden_second"
	second.Parts = append([]string(nil), first.Parts...)
	builder.qry.Nodes[0].TableDef.Indexes = append(builder.qry.Nodes[0].TableDef.Indexes, &second)
	compiler := builder.compCtx.(*indexCostCompilerFixture)
	hidden := DeepCopyTableDef(compiler.tables[first.IndexTableName], true)
	hidden.TblId, hidden.Name = 44, second.IndexTableName
	ref := DeepCopyObjectRef(compiler.objects[first.IndexTableName])
	ref.Obj, ref.ObjName = 44, second.IndexTableName
	compiler.tables[second.IndexTableName], compiler.objects[second.IndexTableName] = hidden, ref
	return &second
}

// This test-local provider consumes the observation budget at the real engine
// handoff, leaving the shared selector to own subsequent admission and cleanup.
type indexCostBudgetRelation struct {
	*indexCostRelationFixture
	remaining int
}

func (r *indexCostBudgetRelation) ObserveIndexBlockInput(ctx context.Context, read client.CatalogReadStamp, req engine.IndexBlockInputRequest, budget *int) (engine.IndexBlockInput, bool, error) {
	*budget = min(*budget, r.remaining)
	return r.indexCostRelationFixture.ObserveIndexBlockInput(ctx, read, req, budget)
}

func TestObservedIndexCandidateIncompletePriceDiscardsEarlierWinner(t *testing.T) {
	for _, secondCandidate := range []bool{false, true} {
		t.Run(fmt.Sprintf("second=%v", secondCandidate), func(t *testing.T) {
			builder, source, first := newIndexCostFixture(t)
			scan := builder.qry.Nodes[0]
			if secondCandidate {
				addIndexCostAlternative(builder, first)
			}
			atom, ok := analyzeIndexComparison(builder.GetContext(), scan.FilterList[0])
			require.True(t, ok)
			probe := indexCostObservation{comparison: atom, full: scan.TableDef, budget: engine.MaxIndexPlanningWork}
			_, complete, err := necessaryIndexPredicateWork(builder.GetContext(), first, &probe)
			require.NoError(t, err)
			require.True(t, complete)
			priceUnits := engine.MaxIndexPlanningWork - probe.budget
			require.Greater(t, priceUnits, 64)
			// Permit one real price plus the small fixture's metadata traversal,
			// but not a second price. The one-index control must still publish.
			_, borrow, ok := builder.scanBorrow(0)
			require.True(t, ok)
			borrow.Relation = &indexCostBudgetRelation{indexCostRelationFixture: source, remaining: priceUnits + 64}
			scan.BlockFilterList = []*pb.Expr{DeepCopyExpr(scan.FilterList[0])}
			source.observation.Blocks = 512
			source.input = &engine.IndexBlockInput{Rows: uint64(scan.Stats.TableCnt), Blocks: 512}
			resolved := 0
			builder.compCtx.(*indexCostCompilerFixture).onResolve = func() { resolved++ }
			beforeTag, beforeNames, beforeTags := builder.nextBindTag, maps.Clone(builder.nameByColRef), maps.Clone(builder.tag2NodeID)
			mapping := make(map[[2]int32]*pb.Expr)
			id, err := builder.applyIndicesWithOrder(0, map[[2]int32]int{{scan.BindingTags[0], 0}: 1}, mapping, false)
			require.NoError(t, err)
			require.Equal(t, 1, source.observed)
			if !secondCandidate {
				require.NotZero(t, id)
				require.Equal(t, first.IndexName, builder.qry.Nodes[id].IndexScanInfo.IndexName)
				require.Equal(t, 1, resolved)
				return
			}
			require.Zero(t, id)
			require.Zero(t, resolved, "incomplete enumeration cannot publish its earlier winner")
			require.Empty(t, mapping)
			require.Len(t, builder.qry.Nodes, 1)
			require.Equal(t, beforeTag, builder.nextBindTag)
			require.Equal(t, beforeNames, builder.nameByColRef)
			require.Equal(t, beforeTags, builder.tag2NodeID)
		})
	}
}
