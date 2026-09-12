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

package compile

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type testLoadLogtailBarrierEngine struct {
	engine.Engine
	calls int
}

func (e *testLoadLogtailBarrierEngine) AcquireLogtailReadBarrier(
	context.Context,
) (timestamp.Timestamp, error) {
	e.calls++
	return timestamp.Timestamp{PhysicalTime: 1}, nil
}

type advancingLoadTimestampWaiter struct{}

const testLoadUniqueIndexMaxRowLocks = 20_000

func (advancingLoadTimestampWaiter) GetTimestamp(
	_ context.Context,
	ts timestamp.Timestamp,
) (timestamp.Timestamp, error) {
	return ts.Next(), nil
}

func (advancingLoadTimestampWaiter) NotifyLatestCommitTS(timestamp.Timestamp) {}
func (advancingLoadTimestampWaiter) Close()                                   {}
func (advancingLoadTimestampWaiter) LatestTS() timestamp.Timestamp            { return timestamp.Timestamp{} }

func newLoadUniqueIndexPromotionPlan() *plan.Plan {
	basePKType := plan.Type{Id: int32(types.T_uint32), Width: 32}
	hiddenPKType := plan.Type{
		Id:      int32(types.T_varchar),
		Width:   types.MaxVarcharLen,
		Charset: uint32(types.CharsetBinary),
	}
	baseRef := &plan.ObjectRef{Db: 1, Obj: 10, SchemaName: "db", ObjName: "t"}
	indexName := catalog.UniqueIndexTableNamePrefix + "u"
	indexRef := &plan.ObjectRef{Db: 1, Obj: 20, SchemaName: "db", ObjName: indexName}
	base := &plan.TableDef{
		TblId:     10,
		Name:      "t",
		TableType: catalog.SystemOrdinaryRel,
		Cols:      []*plan.ColDef{{Name: "id", Typ: basePKType}},
		Name2ColIndex: map[string]int32{
			"id": 0,
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Indexes: []*plan.IndexDef{{
			IndexName:      "u",
			Parts:          []string{"u"},
			Unique:         true,
			IndexTableName: indexName,
			TableExist:     true,
			IndexAlgo:      catalog.MoIndexDefaultAlgo.ToString(),
		}},
	}
	hidden := &plan.TableDef{
		TblId:     20,
		Name:      indexName,
		TableType: catalog.SystemIndexRel,
		Cols: []*plan.ColDef{{
			Name: catalog.IndexTableIndexColName,
			Typ:  hiddenPKType,
		}},
		Name2ColIndex: map[string]int32{catalog.IndexTableIndexColName: 0},
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{catalog.IndexTableIndexColName},
			PkeyColName: catalog.IndexTableIndexColName,
		},
	}
	load := &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		Stats:    &plan.Stats{Cost: 1 << 20, Outcnt: testLoadUniqueIndexMaxRowLocks + 1, Rowsize: 1 << 10},
		TableDef: &plan.TableDef{Createsql: `{
            "ScanType":1,
            "FileSize":1073741824,
            "Filepath":"load/large.csv",
            "CompressType":"auto",
            "Format":"csv",
            "Local":false
        }`},
		ExternScan: &plan.ExternScan{
			Type:     int32(plan.ExternType_LOAD),
			LoadType: 1,
			Format:   "csv",
		},
	}
	lock := &plan.Node{
		NodeType: plan.Node_LOCK_OP,
		Children: []int32{0},
		TableDef: base,
		LockTargets: []*plan.LockTarget{
			{TableId: 10, ObjRef: baseRef, PrimaryColTyp: basePKType, LockTable: true, Mode: lockpb.LockMode_Exclusive},
			{TableId: 20, ObjRef: indexRef, PrimaryColTyp: hiddenPKType, Mode: lockpb.LockMode_Exclusive},
		},
	}
	update := &plan.Node{
		NodeType: plan.Node_MULTI_UPDATE,
		Children: []int32{1},
		UpdateCtxList: []*plan.UpdateCtx{
			{ObjRef: baseRef, TableDef: base},
			{ObjRef: indexRef, TableDef: hidden},
		},
	}
	return &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		StmtType: plan.Query_INSERT,
		Steps:    []int32{2},
		Nodes:    []*plan.Node{load, lock, update},
		LoadTag:  true,
	}}}
}

func addSecondLoadUniqueIndexPromotionTarget(pn *plan.Plan) {
	qry := pn.GetQuery()
	lockNode := qry.Nodes[1]
	updateNode := qry.Nodes[2]
	base := lockNode.TableDef
	firstIndex := base.Indexes[0]
	firstHidden := updateNode.UpdateCtxList[1].TableDef

	indexName := catalog.UniqueIndexTableNamePrefix + "u2"
	secondIndex := plan2.DeepCopyIndexDef(firstIndex)
	secondIndex.IndexName = "u2"
	secondIndex.IndexTableName = indexName
	base.Indexes = append(base.Indexes, secondIndex)

	secondHidden := plan2.DeepCopyTableDef(firstHidden, true)
	secondHidden.TblId = 21
	secondHidden.Name = indexName
	secondRef := &plan.ObjectRef{Db: 1, Obj: 21, SchemaName: "db", ObjName: indexName}
	secondTarget := plan2.DeepCopyLockTarget(lockNode.LockTargets[1])
	secondTarget.TableId = 21
	secondTarget.ObjRef = secondRef
	lockNode.LockTargets = append(lockNode.LockTargets, secondTarget)
	updateNode.UpdateCtxList = append(updateNode.UpdateCtxList, &plan.UpdateCtx{
		ObjRef:   secondRef,
		TableDef: secondHidden,
	})
}

func loadHiddenLockRows(t *testing.T) ([][]byte, []byte) {
	t.Helper()
	pkType := types.T_varchar.ToType()
	packer := types.NewPacker()
	defer packer.Close()
	has, fullRange, granularity := lockop.GetFetchRowsFunc(pkType)(
		nil, packer, pkType, 0, true, nil, nil)
	require.True(t, has)
	require.Equal(t, lockpb.Granularity_Range, granularity)

	packer.Reset()
	packer.EncodeStringType([]byte("middle-key"))
	return fullRange, packer.Bytes()
}

func loadPromotionWaitDeadline(
	t *testing.T,
	txnOp client.TxnOperator,
	tableID uint64,
) int64 {
	t.Helper()
	for _, waitLock := range txnOp.GetOverview().WaitLocks {
		if waitLock.TableID == tableID {
			require.Positive(t, waitLock.Options.LockWaitDeadline)
			return waitLock.Options.LockWaitDeadline
		}
	}
	t.Fatalf("transaction is not waiting for table %d", tableID)
	return 0
}

func TestLoadUniqueIndexPromotionTxnEligibility(t *testing.T) {
	validMeta := txn.TxnMeta{
		ID:        []byte("txn"),
		Status:    txn.TxnStatus_Active,
		Mode:      txn.TxnMode_Pessimistic,
		Isolation: txn.TxnIsolation_RC,
	}
	validOptions := txn.TxnOptions{Autocommit: true}
	tests := []struct {
		name    string
		meta    txn.TxnMeta
		options txn.TxnOptions
		want    bool
	}{
		{name: "implicit autocommit RC", meta: validMeta, options: validOptions, want: true},
		{name: "BEGIN", meta: validMeta, options: txn.TxnOptions{Autocommit: true, ByBegin: true}},
		{name: "autocommit off", meta: validMeta, options: txn.TxnOptions{}},
		{name: "snapshot isolation", meta: func() txn.TxnMeta { v := validMeta; v.Isolation = txn.TxnIsolation_SI; return v }(), options: validOptions},
		{name: "optimistic", meta: func() txn.TxnMeta { v := validMeta; v.Mode = txn.TxnMode_Optimistic; return v }(), options: validOptions},
		{name: "mirror", meta: func() txn.TxnMeta { v := validMeta; v.Mirror = true; return v }(), options: validOptions},
		{name: "aborted", meta: func() txn.TxnMeta { v := validMeta; v.Status = txn.TxnStatus_Aborted; return v }(), options: validOptions},
		{name: "missing transaction id", meta: func() txn.TxnMeta { v := validMeta; v.ID = nil; return v }(), options: validOptions},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			txnOp := mock_frontend.NewMockTxnOperator(ctrl)
			txnOp.EXPECT().Txn().Return(test.meta)
			txnOp.EXPECT().TxnOptions().Return(test.options)
			require.Equal(t, test.want, loadUniqueIndexPromotionTxnEligible(txnOp))
		})
	}
}

func TestAnalyzeLoadUniqueIndexPromotionPlan(t *testing.T) {
	pn := newLoadUniqueIndexPromotionPlan()
	targets, ok := analyzeLoadUniqueIndexPromotionPlan(pn, testLoadUniqueIndexMaxRowLocks)
	require.True(t, ok)
	require.Len(t, targets, 1)
	require.Equal(t, uint64(20), targets[0].rowTarget.TableId)
	require.NotSame(t, pn.GetQuery().Nodes[1].LockTargets[1], targets[0].rowTarget)
	pn.GetQuery().LoadWriteS3 = true
	_, ok = analyzeLoadUniqueIndexPromotionPlan(pn, testLoadUniqueIndexMaxRowLocks)
	require.True(t, ok,
		"an execution-path marker must not change lock-safety admission")

	for _, floatType := range []types.T{types.T_float32, types.T_float64} {
		t.Run(floatType.String()+" base primary key", func(t *testing.T) {
			candidate := newLoadUniqueIndexPromotionPlan()
			floatPK := plan.Type{Id: int32(floatType)}
			candidate.GetQuery().Nodes[1].TableDef.Cols[0].Typ = floatPK
			candidate.GetQuery().Nodes[1].LockTargets[0].PrimaryColTyp = floatPK
			_, admitted := analyzeLoadUniqueIndexPromotionPlan(candidate, testLoadUniqueIndexMaxRowLocks)
			require.True(t, admitted)
		})
		t.Run(floatType.String()+" unique hidden primary key", func(t *testing.T) {
			candidate := newLoadUniqueIndexPromotionPlan()
			floatPK := plan.Type{Id: int32(floatType)}
			candidate.GetQuery().Nodes[2].UpdateCtxList[1].TableDef.Cols[0].Typ = floatPK
			candidate.GetQuery().Nodes[1].LockTargets[1].PrimaryColTyp = floatPK
			_, admitted := analyzeLoadUniqueIndexPromotionPlan(candidate, testLoadUniqueIndexMaxRowLocks)
			require.True(t, admitted)
		})
	}

	tests := []struct {
		name   string
		mutate func(*plan.Plan)
	}{
		{"estimate at row-lock budget", func(p *plan.Plan) { p.GetQuery().Nodes[0].Stats.Outcnt = testLoadUniqueIndexMaxRowLocks }},
		{"small byte estimate", func(p *plan.Plan) { p.GetQuery().Nodes[0].Stats.Cost = 1 }},
		{"nan row estimate", func(p *plan.Plan) { p.GetQuery().Nodes[0].Stats.Outcnt = math.NaN() }},
		{"infinite row estimate", func(p *plan.Plan) { p.GetQuery().Nodes[0].Stats.Outcnt = math.Inf(1) }},
		{"nan byte estimate", func(p *plan.Plan) { p.GetQuery().Nodes[0].Stats.Cost = math.NaN() }},
		{"infinite row size", func(p *plan.Plan) { p.GetQuery().Nodes[0].Stats.Rowsize = math.Inf(1) }},
		{"missing source metadata", func(p *plan.Plan) { p.GetQuery().Nodes[0].TableDef.Createsql = "" }},
		{"compressed source", func(p *plan.Plan) {
			p.GetQuery().Nodes[0].TableDef.Createsql = `{"ScanType":1,"FileSize":1073741824,"Filepath":"large.csv.gz","CompressType":"auto","Format":"csv"}`
		}},
		{"local source", func(p *plan.Plan) {
			p.GetQuery().Nodes[0].TableDef.Createsql = `{"ScanType":1,"FileSize":1073741824,"Filepath":"large.csv","CompressType":"none","Format":"csv","Local":true}`
		}},
		{"foreign key", func(p *plan.Plan) { p.GetQuery().Nodes[1].TableDef.Fkeys = []*plan.ForeignKeyDef{{}} }},
		{"incoming foreign key", func(p *plan.Plan) { p.GetQuery().Nodes[1].TableDef.RefChildTbls = []uint64{99} }},
		{"temporary", func(p *plan.Plan) { p.GetQuery().Nodes[1].TableDef.IsTemporary = true }},
		{"partitioned", func(p *plan.Plan) { p.GetQuery().Nodes[1].TableDef.Partition = &plan.Partition{} }},
		{"system relation", func(p *plan.Plan) { p.GetQuery().Nodes[1].TableDef.TableType = catalog.SystemClusterRel }},
		{"system schema", func(p *plan.Plan) {
			p.GetQuery().Nodes[1].LockTargets[0].ObjRef.SchemaName = catalog.MO_CATALOG
		}},
		{"contradictory schema", func(p *plan.Plan) {
			p.GetQuery().Nodes[1].LockTargets[0].ObjRef.DbName = "other"
		}},
		{"fake primary key", func(p *plan.Plan) { p.GetQuery().Nodes[1].TableDef.Pkey.PkeyColName = catalog.FakePrimaryKeyColName }},
		{"unsupported index", func(p *plan.Plan) {
			p.GetQuery().Nodes[1].TableDef.Indexes[0].IndexAlgo = catalog.MoIndexRTreeAlgo.ToString()
		}},
		{"async index", func(p *plan.Plan) { p.GetQuery().Nodes[1].TableDef.Indexes[0].IndexAlgoParams = `{"async":"true"}` }},
		{"unsupported hidden pk", func(p *plan.Plan) {
			unsupported := plan.Type{Id: int32(types.T_blob)}
			p.GetQuery().Nodes[2].UpdateCtxList[1].TableDef.Cols[0].Typ = unsupported
			p.GetQuery().Nodes[1].LockTargets[1].PrimaryColTyp = unsupported
		}},
		{"unsupported base pk", func(p *plan.Plan) {
			unsupported := plan.Type{Id: int32(types.T_blob)}
			p.GetQuery().Nodes[1].TableDef.Cols[0].Typ = unsupported
			p.GetQuery().Nodes[1].LockTargets[0].PrimaryColTyp = unsupported
		}},
		{"partial target vector", func(p *plan.Plan) { p.GetQuery().Nodes[1].LockTargets = p.GetQuery().Nodes[1].LockTargets[:1] }},
		{"duplicate lock shape", func(p *plan.Plan) {
			p.GetQuery().Nodes = append(p.GetQuery().Nodes, &plan.Node{NodeType: plan.Node_LOCK_OP})
			p.GetQuery().Steps = append(p.GetQuery().Steps, int32(len(p.GetQuery().Nodes)-1))
		}},
		{"cyclic plan", func(p *plan.Plan) { p.GetQuery().Nodes[0].Children = []int32{2} }},
		{"multiple roots", func(p *plan.Plan) { p.GetQuery().Steps = append(p.GetQuery().Steps, 0) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidate := newLoadUniqueIndexPromotionPlan()
			tc.mutate(candidate)
			_, admitted := analyzeLoadUniqueIndexPromotionPlan(candidate, testLoadUniqueIndexMaxRowLocks)
			require.False(t, admitted)
		})
	}
}

func TestLoadEstimateEligible(t *testing.T) {
	tests := []struct {
		name     string
		stats    *plan.Stats
		budget   float64
		eligible bool
	}{
		{
			name: "above row budget at byte threshold",
			stats: &plan.Stats{
				Cost:    1 << 20,
				Outcnt:  20_001,
				Rowsize: 1 << 10,
			},
			budget:   20_000,
			eligible: true,
		},
		{
			name: "equal to row budget",
			stats: &plan.Stats{
				Cost:    1 << 20,
				Outcnt:  20_000,
				Rowsize: 1 << 10,
			},
			budget: 20_000,
		},
		{
			name: "above row budget below byte threshold",
			stats: &plan.Stats{
				Cost:    (1 << 20) - 1,
				Outcnt:  20_001,
				Rowsize: 1 << 10,
			},
			budget: 20_000,
		},
		{name: "missing stats", budget: 20_000},
		{name: "invalid row estimate", stats: &plan.Stats{Cost: 1 << 20, Outcnt: math.NaN(), Rowsize: 1 << 10}, budget: 20_000},
		{name: "invalid byte estimate", stats: &plan.Stats{Cost: math.Inf(1), Outcnt: 20_001, Rowsize: 1 << 10}, budget: 20_000},
		{name: "invalid budget", stats: &plan.Stats{Cost: 1 << 20, Outcnt: 20_001, Rowsize: 1 << 10}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.eligible, loadEstimateEligible(tc.stats, tc.budget))
		})
	}
}

func BenchmarkAnalyzeLoadUniqueIndexPromotionPlan(b *testing.B) {
	pn := newLoadUniqueIndexPromotionPlan()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, ok := analyzeLoadUniqueIndexPromotionPlan(pn, testLoadUniqueIndexMaxRowLocks); !ok {
			b.Fatal("eligible plan rejected")
		}
	}
}

func BenchmarkPrepareLoadUniqueIndexPromotionOrdinaryPlan(b *testing.B) {
	c := &Compile{}
	pn := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_SELECT}}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c.prepareLoadUniqueIndexPromotion(pn)
	}
}

func TestAnalyzeLoadUniqueIndexPromotionAllowsSynchronousNonUniqueSibling(t *testing.T) {
	pn := newLoadUniqueIndexPromotionPlan()
	qry := pn.GetQuery()
	base := qry.Nodes[1].TableDef
	update := qry.Nodes[2]
	const nonUniqueName = "__mo_index_secondary_n"
	base.Indexes = append(base.Indexes, &plan.IndexDef{
		IndexName:      "n",
		Parts:          []string{"n"},
		IndexTableName: nonUniqueName,
		TableExist:     true,
		IndexAlgo:      catalog.MoIndexDefaultAlgo.ToString(),
	})
	nonUniqueRef := &plan.ObjectRef{
		Db: 1, Obj: 30, SchemaName: "db", ObjName: nonUniqueName,
	}
	update.UpdateCtxList = append(update.UpdateCtxList, &plan.UpdateCtx{
		ObjRef: nonUniqueRef,
		TableDef: &plan.TableDef{
			TblId:     30,
			Name:      nonUniqueName,
			TableType: catalog.SystemIndexRel,
		},
	})

	targets, ok := analyzeLoadUniqueIndexPromotionPlan(pn, testLoadUniqueIndexMaxRowLocks)
	require.True(t, ok)
	require.Len(t, targets, 1,
		"only UNIQUE row-lock targets are replaced; synchronous non-unique maintenance stays exact main")
}

func TestAnalyzeLoadUniqueIndexPromotionMatchesPlannerShape(t *testing.T) {
	stmt, err := mysql.ParseOne(t.Context(),
		"load data inline format='csv', data='1,a,b' into table constraint_test.dept_composite_uk fields terminated by ','",
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()

	qry, err := plan2.NewMockOptimizer(true).Optimize(stmt)
	require.NoError(t, err)
	pn := &plan.Plan{Plan: &plan.Plan_Query{Query: qry}}
	require.False(t, qry.LoadWriteS3,
		"the modern LOAD binder does not use the legacy write-path marker")
	var external, lockNode, updateNode *plan.Node
	for _, node := range qry.Nodes {
		switch node.NodeType {
		case plan.Node_EXTERNAL_SCAN:
			external = node
		case plan.Node_LOCK_OP:
			lockNode = node
		case plan.Node_MULTI_UPDATE:
			updateNode = node
		}
	}
	require.NotNil(t, external)
	require.NotNil(t, lockNode)
	require.NotNil(t, updateNode)
	external.Stats = &plan.Stats{Cost: 1 << 20, Outcnt: testLoadUniqueIndexMaxRowLocks + 1, Rowsize: 1 << 10}
	external.ExternScan.LoadType = 1
	external.ExternScan.Format = "csv"
	external.TableDef.Createsql = `{"ScanType":1,"FileSize":1073741824,"Filepath":"load/large.csv","CompressType":"auto","Format":"csv"}`

	base := lockNode.TableDef
	require.NotNil(t, base)
	require.NotNil(t, base.Pkey)
	for _, target := range lockNode.LockTargets {
		if target.TableId == base.TblId {
			continue
		}
		for _, updateCtx := range updateNode.UpdateCtxList {
			if updateCtx.TableDef.TblId != target.TableId {
				continue
			}
			// The generic planner mock does not preserve the relkind=i catalog
			// property on hidden tables; production catalog resolution does.
			updateCtx.TableDef.TableType = catalog.SystemIndexRel
			if updateCtx.TableDef.Name2ColIndex == nil {
				updateCtx.TableDef.Name2ColIndex = map[string]int32{
					updateCtx.TableDef.Pkey.PkeyColName: 0,
				}
			}
			// The mock's legacy hidden schema says varchar(255), while the
			// production composite-key builder and LockTarget use binary
			// varchar(MaxVarcharLen). Repair only that mock-catalog inconsistency;
			// retain the planner's actual uint32 base and varchar hidden types.
			pkIndex := updateCtx.TableDef.Name2ColIndex[updateCtx.TableDef.Pkey.PkeyColName]
			updateCtx.TableDef.Cols[pkIndex].Typ = target.PrimaryColTyp
		}
	}

	targets, ok := analyzeLoadUniqueIndexPromotionPlan(pn, testLoadUniqueIndexMaxRowLocks)
	require.True(t, ok, "the classifier must match the planner's real LOAD/LOCK_OP/MULTI_UPDATE topology")
	require.NotEmpty(t, targets)
}

func TestLoadLogtailReadBarrierCapabilityUnwrapsEntireEngine(t *testing.T) {
	direct := &testLoadLogtailBarrierEngine{}
	got, ok := loadLogtailReadBarrier(direct)
	require.True(t, ok)
	require.Same(t, direct, got)

	wrapped := &engine.EntireEngine{Engine: &engine.EntireEngine{Engine: direct}}
	got, ok = loadLogtailReadBarrier(wrapped)
	require.True(t, ok)
	require.Same(t, direct, got)

	_, ok = loadLogtailReadBarrier(&engine.EntireEngine{Engine: newStubEngine()})
	require.False(t, ok,
		"the wrapper method must not make an unsupported underlying engine eligible")
}

func TestPrepareLoadUniqueIndexPromotionRequiresRuntimeAdmission(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	meta := txn.TxnMeta{
		ID:        []byte("txn-admission"),
		Status:    txn.TxnStatus_Active,
		Mode:      txn.TxnMode_Pessimistic,
		Isolation: txn.TxnIsolation_RC,
	}
	opts := txn.TxnOptions{
		Autocommit:      true,
		LockWaitTimeout: int64(time.Second),
	}
	txnOp.EXPECT().Txn().Return(meta).AnyTimes()
	txnOp.EXPECT().TxnOptions().Return(opts).AnyTimes()
	proc := testutil.NewProcess(t)
	proc.Base.TxnOperator = txnOp
	lockService := mock_lock.NewMockLockService(ctrl)
	lockService.EXPECT().GetConfig().Return(lockservice.Config{
		MaxLockRowCount: testLoadUniqueIndexMaxRowLocks,
	}).AnyTimes()
	proc.Base.LockService = lockService
	rt := moruntime.ServiceRuntime(proc.GetService())
	require.NotNil(t, rt)
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion39)

	pn := newLoadUniqueIndexPromotionPlan()
	c := &Compile{proc: proc, e: &testLoadLogtailBarrierEngine{}}
	c.prepareLoadUniqueIndexPromotion(pn)
	require.NotNil(t, c.loadUniqueIndexPromotion)
	require.True(t, c.loadUniqueIndexPromotionOwner)
	require.Equal(t, loadUniqueIndexPromotionEligible, c.loadUniqueIndexPromotion.phase)

	c.clearLoadUniqueIndexPromotion()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion38)
	c.prepareLoadUniqueIndexPromotion(pn)
	require.Nil(t, c.loadUniqueIndexPromotion,
		"rolling upgrades below the read-barrier protocol must keep exact-main behavior")
}

func TestPrepareLoadUniqueIndexPromotionSkipsOrdinaryPlanBeforeRuntime(t *testing.T) {
	c := &Compile{}
	require.NotPanics(t, func() {
		c.prepareLoadUniqueIndexPromotion(&plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
			StmtType: plan.Query_SELECT,
		}}})
	})
	require.Nil(t, c.loadUniqueIndexPromotion)
}

func TestMaybePromoteLoadUniqueIndexesWithRealLockService(t *testing.T) {
	moruntime.RunTest("", func(rt moruntime.Runtime) {
		moruntime.SetupServiceBasedRuntime("s1", rt)
		lockservice.RunLockServicesForTest(
			zapcore.DebugLevel,
			[]string{"s1"},
			time.Second,
			func(_ lockservice.LockTableAllocator, services []lockservice.LockService) {
				rt.SetGlobalVariables(moruntime.LockService, services[0])
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion39))

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				sender, err := rpc.NewSender(rpc.Config{}, rt)
				require.NoError(t, err)

				txnClient := client.NewTxnClient(
					"s1",
					sender,
					client.WithLockService(services[0]),
					client.WithTimestampWaiter(advancingLoadTimestampWaiter{}),
				)
				txnClient.Resume()
				defer func() { require.NoError(t, txnClient.Close()) }()

				txnOp, err := txnClient.New(
					ctx,
					timestamp.Timestamp{},
					client.WithTxnMode(txn.TxnMode_Pessimistic),
					client.WithTxnIsolation(txn.TxnIsolation_RC),
					client.WithBeginAutoCommit(false, true),
					client.WithTxnLockWaitTimeout(5*time.Second),
				)
				require.NoError(t, err)
				rolledBack := false
				defer func() {
					if !rolledBack {
						require.NoError(t, txnOp.Rollback(ctx))
					}
				}()

				proc := process.NewTopProcess(
					ctx,
					mpool.MustNewZero(),
					txnClient,
					txnOp,
					nil,
					services[0],
					nil,
					nil,
					nil,
					nil,
					nil,
				)
				pn := newLoadUniqueIndexPromotionPlan()
				eng := &testLoadLogtailBarrierEngine{}
				c := &Compile{
					proc:                         proc,
					e:                            eng,
					pn:                           pn,
					resourceAttemptOwnerEligible: true,
					lockTables: map[uint64]*plan.LockTarget{
						10: pn.GetQuery().Nodes[1].LockTargets[0],
					},
				}
				c.prepareLoadUniqueIndexPromotion(pn)
				require.NotNil(t, c.loadUniqueIndexPromotion)

				// This is the real consumer boundary: the ordinary base lock is
				// acquired first, then promotion adds the hidden full-domain lock,
				// runs the engine barrier, and installs its returned frontier.
				require.NoError(t, c.lockTable())
				require.True(t, txnOp.HasLockTable(10))
				err = c.maybePromoteLoadUniqueIndexes()
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
				require.True(t, txnOp.HasLockTable(20))
				require.Equal(t, loadUniqueIndexPromotionFenced, c.loadUniqueIndexPromotion.phase)
				require.True(t, txnOp.Txn().SnapshotTS.Greater(c.loadUniqueIndexPromotion.frontier))
				require.Equal(t, 1, eng.calls)
				require.NoError(t, c.maybePromoteLoadUniqueIndexes())
				require.Equal(t, 1, eng.calls,
					"a fenced physical re-entry must not issue a second barrier")

				// A real point writer must wait behind the promoted full-domain
				// hidden-table range until whole-transaction rollback releases it.
				fullRange, pointKey := loadHiddenLockRows(t)
				writerTxn := []byte("hidden-writer")
				writerDone := make(chan error, 1)
				go func() {
					_, lockErr := services[0].Lock(ctx, 20, [][]byte{pointKey}, writerTxn, lockpb.LockOptions{
						Granularity:     lockpb.Granularity_Row,
						Mode:            lockpb.LockMode_Exclusive,
						Policy:          lockpb.WaitPolicy_Wait,
						LockWaitTimeout: 5,
					})
					writerDone <- lockErr
				}()
				require.NoError(t, lockservice.WaitWaiters(services[0], 0, 20, fullRange[0], 1))

				require.NoError(t, txnOp.Rollback(ctx))
				rolledBack = true
				select {
				case lockErr := <-writerDone:
					require.NoError(t, lockErr)
				case <-ctx.Done():
					t.Fatal("hidden writer did not proceed after promotion rollback")
				}
				require.NoError(t, services[0].Unlock(ctx, writerTxn, timestamp.Timestamp{}))
			},
			nil,
		)
	})
}

func TestLoadUniqueIndexPromotionCancellationReleasesAcquiredPrefix(t *testing.T) {
	moruntime.RunTest("", func(rt moruntime.Runtime) {
		moruntime.SetupServiceBasedRuntime("s1", rt)
		lockservice.RunLockServicesForTest(
			zapcore.DebugLevel,
			[]string{"s1"},
			time.Second,
			func(_ lockservice.LockTableAllocator, services []lockservice.LockService) {
				rt.SetGlobalVariables(moruntime.LockService, services[0])
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion39))

				cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancelCleanup()
				promotionParent, cancelPromotion := context.WithCancel(cleanupCtx)
				defer cancelPromotion()

				sender, err := rpc.NewSender(rpc.Config{}, rt)
				require.NoError(t, err)
				txnClient := client.NewTxnClient(
					"s1",
					sender,
					client.WithLockService(services[0]),
					client.WithTimestampWaiter(advancingLoadTimestampWaiter{}),
				)
				txnClient.Resume()
				defer func() { require.NoError(t, txnClient.Close()) }()

				txnOp, err := txnClient.New(
					promotionParent,
					timestamp.Timestamp{},
					client.WithTxnMode(txn.TxnMode_Pessimistic),
					client.WithTxnIsolation(txn.TxnIsolation_RC),
					client.WithBeginAutoCommit(false, true),
					client.WithTxnLockWaitTimeout(5*time.Second),
				)
				require.NoError(t, err)
				rolledBack := false
				defer func() {
					if !rolledBack {
						require.NoError(t, txnOp.Rollback(cleanupCtx))
					}
				}()

				fullRange, pointKey := loadHiddenLockRows(t)
				firstHolderTxn := []byte("first-target-holder")
				_, err = services[0].Lock(cleanupCtx, 20, fullRange, firstHolderTxn, lockpb.LockOptions{
					Granularity:     lockpb.Granularity_Range,
					Mode:            lockpb.LockMode_Exclusive,
					Policy:          lockpb.WaitPolicy_Wait,
					LockWaitTimeout: 5,
				})
				require.NoError(t, err)
				firstHolderReleased := false
				defer func() {
					if !firstHolderReleased {
						require.NoError(t, services[0].Unlock(
							cleanupCtx, firstHolderTxn, timestamp.Timestamp{}))
					}
				}()

				secondHolderTxn := []byte("second-target-holder")
				_, err = services[0].Lock(cleanupCtx, 21, fullRange, secondHolderTxn, lockpb.LockOptions{
					Granularity:     lockpb.Granularity_Range,
					Mode:            lockpb.LockMode_Exclusive,
					Policy:          lockpb.WaitPolicy_Wait,
					LockWaitTimeout: 5,
				})
				require.NoError(t, err)
				holderReleased := false
				defer func() {
					if !holderReleased {
						require.NoError(t, services[0].Unlock(
							cleanupCtx, secondHolderTxn, timestamp.Timestamp{}))
					}
				}()

				proc := process.NewTopProcess(
					promotionParent,
					mpool.MustNewZero(),
					txnClient,
					txnOp,
					nil,
					services[0],
					nil,
					nil,
					nil,
					nil,
					nil,
				)
				pn := newLoadUniqueIndexPromotionPlan()
				addSecondLoadUniqueIndexPromotionTarget(pn)
				eng := &testLoadLogtailBarrierEngine{}
				c := &Compile{
					proc:                         proc,
					e:                            eng,
					pn:                           pn,
					resourceAttemptOwnerEligible: true,
					lockTables: map[uint64]*plan.LockTarget{
						10: pn.GetQuery().Nodes[1].LockTargets[0],
					},
				}
				c.prepareLoadUniqueIndexPromotion(pn)
				require.NotNil(t, c.loadUniqueIndexPromotion)
				require.Len(t, c.loadUniqueIndexPromotion.targets, 2)
				require.NoError(t, c.lockTable())

				promotionDone := make(chan error, 1)
				go func() { promotionDone <- c.maybePromoteLoadUniqueIndexes() }()
				require.NoError(t, lockservice.WaitWaiters(services[0], 0, 20, fullRange[0], 1))
				firstDeadline := loadPromotionWaitDeadline(t, txnOp, 20)
				require.NoError(t, services[0].Unlock(
					cleanupCtx, firstHolderTxn, timestamp.Timestamp{}))
				firstHolderReleased = true

				require.NoError(t, lockservice.WaitWaiters(services[0], 0, 21, fullRange[0], 1))
				secondDeadline := loadPromotionWaitDeadline(t, txnOp, 21)
				require.Equal(t, firstDeadline, secondDeadline,
					"every hidden target must carry the same aggregate absolute deadline")
				require.True(t, txnOp.HasLockTable(20),
					"the first sorted hidden target must be owned before target two waits")

				cancelPromotion()
				select {
				case promotionErr := <-promotionDone:
					require.ErrorIs(t, promotionErr, context.Canceled)
				case <-cleanupCtx.Done():
					t.Fatal("canceled promotion lock wait did not return")
				}
				require.Zero(t, eng.calls, "barrier must not run after partial lock acquisition")
				require.Equal(t, loadUniqueIndexPromotionDisabled, c.loadUniqueIndexPromotion.phase)

				require.NoError(t, txnOp.Rollback(cleanupCtx))
				rolledBack = true
				contenderTxn := []byte("prefix-release-contender")
				_, err = services[0].Lock(cleanupCtx, 20, [][]byte{pointKey}, contenderTxn, lockpb.LockOptions{
					Granularity: lockpb.Granularity_Row,
					Mode:        lockpb.LockMode_Exclusive,
					Policy:      lockpb.WaitPolicy_FastFail,
				})
				require.NoError(t, err,
					"whole-transaction rollback must release the acquired target prefix")
				require.NoError(t, services[0].Unlock(
					cleanupCtx, contenderTxn, timestamp.Timestamp{}))
				require.NoError(t, services[0].Unlock(
					cleanupCtx, secondHolderTxn, timestamp.Timestamp{}))
				holderReleased = true
			},
			nil,
		)
	})
}

func TestExecuteLoadUniqueIndexPromotionOrderingAndMetric(t *testing.T) {
	targets := []loadUniqueIndexPromotionTarget{
		{rowTarget: &plan.LockTarget{TableId: 11}},
		{rowTarget: &plan.LockTarget{TableId: 12}},
	}
	frontier := timestamp.Timestamp{PhysicalTime: 100}
	installed := frontier.Next()
	var events []string
	metricCalls := 0
	gotFrontier, gotSnapshot, err := executeLoadUniqueIndexPromotion(
		context.Background(),
		context.Background(),
		targets,
		func(_ context.Context, target loadUniqueIndexPromotionTarget) error {
			events = append(events, "lock-"+strconv.FormatUint(target.rowTarget.TableId, 10))
			return nil
		},
		func(context.Context) (timestamp.Timestamp, error) {
			events = append(events, "barrier")
			return frontier, nil
		},
		func(_ context.Context, ts timestamp.Timestamp) error {
			require.Equal(t, frontier, ts)
			events = append(events, "snapshot")
			return nil
		},
		func() timestamp.Timestamp { return installed },
		func(context.Context, context.Context, time.Duration, error) { metricCalls++ },
	)
	require.NoError(t, err)
	require.Equal(t, frontier, gotFrontier)
	require.Equal(t, installed, gotSnapshot)
	require.Equal(t, []string{"lock-11", "lock-12", "barrier", "snapshot"}, events)
	require.Equal(t, 1, metricCalls)
}

func TestExecuteLoadUniqueIndexPromotionFailures(t *testing.T) {
	targets := []loadUniqueIndexPromotionTarget{
		{rowTarget: &plan.LockTarget{TableId: 11}},
		{rowTarget: &plan.LockTarget{TableId: 12}},
	}
	lockErr := errors.New("lock failed")
	barrierCalls, metricCalls := 0, 0
	_, _, err := executeLoadUniqueIndexPromotion(
		context.Background(), context.Background(), targets,
		func(_ context.Context, target loadUniqueIndexPromotionTarget) error {
			if target.rowTarget.TableId == 11 {
				return lockErr
			}
			return nil
		},
		func(context.Context) (timestamp.Timestamp, error) { barrierCalls++; return timestamp.Timestamp{}, nil },
		func(context.Context, timestamp.Timestamp) error { return nil },
		func() timestamp.Timestamp { return timestamp.Timestamp{} },
		func(context.Context, context.Context, time.Duration, error) { metricCalls++ },
	)
	require.ErrorIs(t, err, lockErr)
	require.Zero(t, barrierCalls)
	require.Zero(t, metricCalls)

	barrierErr := errors.New("barrier failed")
	_, _, err = executeLoadUniqueIndexPromotion(
		context.Background(), context.Background(), nil,
		func(context.Context, loadUniqueIndexPromotionTarget) error { return nil },
		func(context.Context) (timestamp.Timestamp, error) { return timestamp.Timestamp{}, barrierErr },
		func(context.Context, timestamp.Timestamp) error {
			t.Fatal("snapshot update after barrier failure")
			return nil
		},
		func() timestamp.Timestamp { return timestamp.Timestamp{} },
		func(_ context.Context, _ context.Context, _ time.Duration, got error) {
			metricCalls++
			require.ErrorIs(t, got, barrierErr)
		},
	)
	require.ErrorIs(t, err, barrierErr)
	require.Equal(t, 1, metricCalls)

	updateErr := errors.New("snapshot update failed")
	metricCalls = 0
	frontier := timestamp.Timestamp{PhysicalTime: 10}
	_, _, err = executeLoadUniqueIndexPromotion(
		context.Background(), context.Background(), nil,
		func(context.Context, loadUniqueIndexPromotionTarget) error { return nil },
		func(context.Context) (timestamp.Timestamp, error) { return frontier, nil },
		func(context.Context, timestamp.Timestamp) error { return updateErr },
		func() timestamp.Timestamp { return timestamp.Timestamp{} },
		func(context.Context, context.Context, time.Duration, error) { metricCalls++ },
	)
	require.ErrorIs(t, err, updateErr)
	require.Equal(t, 1, metricCalls,
		"the metric owns only the attempted barrier and must not duplicate on later failure")

	_, _, err = executeLoadUniqueIndexPromotion(
		context.Background(), context.Background(), nil,
		func(context.Context, loadUniqueIndexPromotionTarget) error { return nil },
		func(context.Context) (timestamp.Timestamp, error) { return frontier, nil },
		func(context.Context, timestamp.Timestamp) error { return nil },
		func() timestamp.Timestamp { return frontier },
		func(context.Context, context.Context, time.Duration, error) {},
	)
	require.ErrorContains(t, err, "did not advance")
}

func TestLoadUniqueIndexPromotionUsesOneAggregateDeadline(t *testing.T) {
	parent, cancelParent := context.WithTimeout(context.Background(), time.Second)
	defer cancelParent()
	promotion, cancelPromotion := context.WithTimeoutCause(parent, 10*time.Millisecond, lockservice.ErrLockTimeout)
	defer cancelPromotion()
	secondLockCalled := false
	_, _, err := executeLoadUniqueIndexPromotion(
		parent, promotion,
		[]loadUniqueIndexPromotionTarget{
			{rowTarget: &plan.LockTarget{TableId: 11}},
			{rowTarget: &plan.LockTarget{TableId: 12}},
		},
		func(ctx context.Context, target loadUniqueIndexPromotionTarget) error {
			if target.rowTarget.TableId == 12 {
				secondLockCalled = true
			}
			<-ctx.Done()
			return context.Cause(ctx)
		},
		func(context.Context) (timestamp.Timestamp, error) {
			t.Fatal("barrier after timeout")
			return timestamp.Timestamp{}, nil
		},
		func(context.Context, timestamp.Timestamp) error { return nil },
		func() timestamp.Timestamp { return timestamp.Timestamp{} },
		func(context.Context, context.Context, time.Duration, error) {},
	)
	require.ErrorIs(t, normalizeLoadUniqueIndexPromotionError(parent, promotion, err), lockservice.ErrLockTimeout)
	require.False(t, secondLockCalled)
	require.NoError(t, parent.Err(), "the internal aggregate timeout must not cancel the caller")
}

func TestNormalizeLoadUniqueIndexPromotionPreservesCallerCancellation(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	promotion, cancelPromotion := context.WithTimeoutCause(parent, time.Second, lockservice.ErrLockTimeout)
	cancelParent()
	defer cancelPromotion()
	<-promotion.Done()
	require.ErrorIs(t,
		normalizeLoadUniqueIndexPromotionError(parent, promotion, context.Cause(promotion)),
		context.Canceled)
}

func TestNormalizeLoadUniqueIndexPromotionPreservesCallerDeadline(t *testing.T) {
	parent, cancelParent := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelParent()
	promotion, cancelPromotion := context.WithTimeoutCause(parent, time.Second, lockservice.ErrLockTimeout)
	defer cancelPromotion()
	<-promotion.Done()
	require.ErrorIs(t,
		normalizeLoadUniqueIndexPromotionError(parent, promotion, context.Cause(promotion)),
		context.DeadlineExceeded)
}

func TestLoadUniqueIndexPromotionProofCoversExactTarget(t *testing.T) {
	pn := newLoadUniqueIndexPromotionPlan()
	target := pn.GetQuery().Nodes[1].LockTargets[1]
	state := &loadUniqueIndexPromotionState{
		phase: loadUniqueIndexPromotionFenced,
		targets: []loadUniqueIndexPromotionTarget{{
			rowTarget: plan2.DeepCopyLockTarget(target),
		}},
	}
	require.True(t, state.coversRowTarget(target))
	copyWithDifferentPosition := plan2.DeepCopyLockTarget(target)
	copyWithDifferentPosition.PrimaryColIdxInBat++
	require.False(t, state.coversRowTarget(copyWithDifferentPosition))
}

func TestLoadUniqueIndexPromotionLifecycleOwnership(t *testing.T) {
	pn := newLoadUniqueIndexPromotionPlan()
	state := &loadUniqueIndexPromotionState{
		phase:       loadUniqueIndexPromotionEligible,
		logicalPlan: pn,
		targets: []loadUniqueIndexPromotionTarget{{
			rowTarget: &plan.LockTarget{TableId: 20},
		}},
	}
	root := &Compile{
		loadUniqueIndexPromotion:      state,
		loadUniqueIndexPromotionOwner: true,
	}
	borrower := &Compile{}
	borrower.inheritLoadUniqueIndexPromotion(root)
	borrower.clearLoadUniqueIndexPromotion()
	require.Nil(t, borrower.loadUniqueIndexPromotion)
	require.Equal(t, loadUniqueIndexPromotionEligible, state.phase)
	require.NotNil(t, state.logicalPlan)

	root.clearLoadUniqueIndexPromotion()
	require.Nil(t, root.loadUniqueIndexPromotion)
	require.Equal(t, loadUniqueIndexPromotionDisabled, state.phase)
	require.Nil(t, state.logicalPlan)
	require.Nil(t, state.targets)
}

func TestLoadUniqueIndexPromotionRetryInvalidation(t *testing.T) {
	state := &loadUniqueIndexPromotionState{phase: loadUniqueIndexPromotionEligible}
	c := &Compile{loadUniqueIndexPromotion: state}
	c.onLoadUniqueIndexPromotionRetry(false)
	require.Equal(t, loadUniqueIndexPromotionDisabled, state.phase)

	state.phase = loadUniqueIndexPromotionFenced
	c.onLoadUniqueIndexPromotionRetry(false)
	require.Equal(t, loadUniqueIndexPromotionFenced, state.phase,
		"only an ordinary physical retry may retain a completed proof")

	c.onLoadUniqueIndexPromotionRetry(true)
	require.Equal(t, loadUniqueIndexPromotionDisabled, state.phase,
		"a logical rebuild must invalidate the physical proof")
}

func TestBindLoadUniqueIndexPromotionSnapshotIsProofScoped(t *testing.T) {
	oldSnapshot := timestamp.Timestamp{PhysicalTime: 10}
	frontier := timestamp.Timestamp{PhysicalTime: 20}
	installed := frontier.Next()
	pn := newLoadUniqueIndexPromotionPlan()
	state := &loadUniqueIndexPromotionState{
		phase:             loadUniqueIndexPromotionFenced,
		logicalPlan:       pn,
		frontier:          frontier,
		installedSnapshot: installed,
	}
	root := &Compile{pn: pn, loadUniqueIndexPromotion: state, loadUniqueIndexPromotionOwner: true}
	retry := &Compile{
		proc:                     testutil.NewProcess(t),
		loadUniqueIndexPromotion: state,
		planSnapshotTS:           oldSnapshot,
		hasPlanSnapshotTS:        true,
	}
	root.bindLoadUniqueIndexPromotionSnapshot(retry, false)
	require.Equal(t, installed, retry.planSnapshotTS)
	require.False(t, retry.planGenerationReused)

	rebuilt := &Compile{
		proc:                     testutil.NewProcess(t),
		loadUniqueIndexPromotion: state,
		planSnapshotTS:           oldSnapshot,
		hasPlanSnapshotTS:        true,
	}
	root.bindLoadUniqueIndexPromotionSnapshot(rebuilt, true)
	require.Equal(t, oldSnapshot, rebuilt.planSnapshotTS)
}

func TestCompileLockFiltersOnlyValidatedPromotionProof(t *testing.T) {
	pn := newLoadUniqueIndexPromotionPlan()
	targets, ok := analyzeLoadUniqueIndexPromotionPlan(pn, testLoadUniqueIndexMaxRowLocks)
	require.True(t, ok)
	frontier := timestamp.Timestamp{PhysicalTime: 20}
	installed := frontier.Next()
	txnID := []byte("txn-1")
	state := &loadUniqueIndexPromotionState{
		phase:                   loadUniqueIndexPromotionFenced,
		logicalPlan:             pn,
		targets:                 targets,
		txnID:                   txnID,
		firstPhysicalGeneration: 0,
		frontier:                frontier,
		installedSnapshot:       installed,
	}

	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{
		ID:         txnID,
		Status:     txn.TxnStatus_Active,
		Mode:       txn.TxnMode_Pessimistic,
		Isolation:  txn.TxnIsolation_RC,
		SnapshotTS: installed,
	}).AnyTimes()
	txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{
		Autocommit: true,
	}).AnyTimes()
	proc := testutil.NewProcess(t)
	proc.Base.TxnOperator = txnOp
	c := &Compile{
		proc:                     proc,
		pn:                       pn,
		executionGeneration:      1,
		loadUniqueIndexPromotion: state,
		lockTables:               make(map[uint64]*plan.LockTarget),
		planSnapshotTS:           installed,
		hasPlanSnapshotTS:        true,
	}
	canonical := pn.GetQuery().Nodes[1]
	before := plan2.DeepCopyNode(canonical)
	got, err := c.compileLock(canonical, []*Scope{{}})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, c.lockTables, 1)
	require.Contains(t, c.lockTables, uint64(10))
	require.Equal(t, before.LockTargets, canonical.LockTargets,
		"proof filtering must stay compiler-local")

	c.planSnapshotTS = frontier
	c.lockTables = make(map[uint64]*plan.LockTarget)
	_, err = c.compileLock(canonical, []*Scope{{}})
	require.ErrorContains(t, err, "invalid LOAD unique-index promotion retry proof")
	require.Empty(t, c.lockTables)
	c.planSnapshotTS = installed

	state.txnID = []byte("different")
	c.lockTables = make(map[uint64]*plan.LockTarget)
	_, err = c.compileLock(canonical, []*Scope{{}})
	require.ErrorContains(t, err, "does not match transaction generation")
	require.Empty(t, c.lockTables,
		"proof validation must fail before publishing any local lock target")
}
