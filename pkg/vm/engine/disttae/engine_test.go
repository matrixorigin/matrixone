// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

/*
type testTxnOperator struct {
	meta txn.TxnMeta
}

func TestCache(t *testing.T) {
	db := new(DB)
	ctx := context.Background()
	ts := newTimestamp(rand.Int63())
	_ = db.Update(ctx, nil, 0, 0, ts)
	_ = db.BlockList(ctx, nil, 0, 0, ts, nil)
	_, _ = db.NewReader(ctx, 0, nil, nil, 0, 0, ts, nil)
}

func TestEngine(t *testing.T) {
	ctx := context.Background()
	getClusterDetails := func() (details logservice.ClusterDetails, err error) {
		return
	}
	txnOp := newTestTxnOperator()
	m := mheap.New(guest.New(1<<20, host.New(1<<20)))
	e := New(m, ctx, nil, getClusterDetails)
	err := e.New(ctx, txnOp)
	require.NoError(t, err)
	err = e.Create(ctx, "test", txnOp)
	require.NoError(t, err)
	err = e.Delete(ctx, "test", txnOp)
	require.NoError(t, err)
	err = e.Commit(ctx, txnOp)
	require.NoError(t, err)
	err = e.Rollback(ctx, txnOp)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
	_, err = e.Nodes()
	require.NoError(t, err)
	hints := e.Hints()
	require.Equal(t, time.Minute*5, hints.CommitOrRollbackTimeout)
}

func TestTransaction(t *testing.T) {
	txn := &Transaction{
		readOnly: false,
		meta:     newTxnMeta(rand.Int63()),
		fileMap:  make(map[string]uint64),
	}
	txn.writes = append(txn.writes, make([]Entry, 0, 1))
	ro := txn.ReadOnly()
	require.Equal(t, false, ro)
	err := txn.WriteBatch(INSERT, 0, 0, "test", "test", batch.NewOffHeapEmpty())
	require.NoError(t, err)
	txn.IncStatementId()
	txn.RegisterFile("test")
	err = txn.WriteFile(DELETE, 0, 0, "test", "test", "test")
	require.NoError(t, err)
	ctx := context.TODO()

	bm := makeBlockMetaForTest()
	_, err = blockWrite(ctx, bm, testutil.NewBatch([]types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
	}, true, 20, testutil.NewMheap()), testutil.NewFS())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// fmt.Printf("%v", blks)

	_, _ = txn.getRow(ctx, 0, 0, nil, nil, MakeFunctionExprForTest(">", []*plan.Expr{
		MakeColExprForTest(0, types.T_int64),
		plan2.MakePlan2Int64ConstExprWithType(20),
	}), nil)

	_, _ = txn.getRows(ctx, 0, 0, nil, nil, MakeFunctionExprForTest(">", []*plan.Expr{
		MakeColExprForTest(0, types.T_int64),
		plan2.MakePlan2Int64ConstExprWithType(20),
	}), nil)

}

func TestTable(t *testing.T) {
	tbl := new(table)
	ctx := context.TODO()
	_, _ = tbl.Rows(ctx)
	_, _ = tbl.Size(ctx, "test")
	_, _ = tbl.Ranges(ctx)
	_, _ = tbl.TableDefs(ctx)
	_, _ = tbl.GetPrimaryKeys(ctx)
	_, _ = tbl.GetHideKeys(ctx)
	_ = tbl.Write(ctx, nil)
	_ = tbl.Update(ctx, nil)
	_ = tbl.Delete(ctx, nil, "test")
	_, _ = tbl.Truncate(ctx)
	_ = tbl.AddTableDef(ctx, nil)
	_ = tbl.DelTableDef(ctx, nil)
	_ = tbl.GetTableID(ctx)
	_, _ = tbl.NewReader(ctx, 0, nil, nil)
}

func TestTools(t *testing.T) {
	_ = genCreateTableTuple("test")
	_ = genCreateColumnTuple(nil)
	_ = genDropTableTuple("test")
	_ = genDropColumnsTuple("test")
	_ = genDatabaseIdExpr("test")
	_ = genTableIdExpr(0, "test")
}

func newTestTxnOperator() *testTxnOperator {
	return &testTxnOperator{
		meta: newTxnMeta(rand.Int63()),
	}
}

func (op *testTxnOperator) Txn() txn.TxnMeta {
	return op.meta
}

func (op *testTxnOperator) Snapshot() ([]byte, error) {
	return nil, nil
}

func (op *testTxnOperator) ApplySnapshot(data []byte) error {
	return nil
}

func (op *testTxnOperator) Read(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (op *testTxnOperator) Write(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (op *testTxnOperator) WriteAndCommit(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (op *testTxnOperator) Commit(ctx context.Context) error {
	return nil
}

func (op *testTxnOperator) Rollback(ctx context.Context) error {
	return nil
}

func newTimestamp(v int64) timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: v}
}

func newTxnMeta(snapshotTS int64) txn.TxnMeta {
	id := uuid.New()
	return txn.TxnMeta{
		ID:         id[:],
		Status:     txn.TxnStatus_Active,
		SnapshotTS: newTimestamp(snapshotTS),
	}
}
*/

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/version"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type engineNodesClusterClient struct {
	details logpb.ClusterDetails
}

func (c *engineNodesClusterClient) GetClusterDetails(context.Context) (logpb.ClusterDetails, error) {
	return c.details, nil
}

func TestEngineNodesExposesRuntimeStateToScheduler(t *testing.T) {
	t.Run("default discovery", func(t *testing.T) {
		e := newEngineWithClusterDetails(t, logpb.ClusterDetails{
			CNStores: []logpb.CNStore{
				newEngineNodesCNStore("working-cn", "working-pipeline", nil, metadata.WorkState_Working, version.CommitID),
				newEngineNodesCNStore("draining-cn", "draining-pipeline", nil, metadata.WorkState_Draining, version.CommitID),
				newEngineNodesCNStore("drained-cn", "drained-pipeline", nil, metadata.WorkState_Drained, version.CommitID),
				newEngineNodesCNStore("unknown-cn", "unknown-pipeline", nil, metadata.WorkState_Unknown, version.CommitID),
				newEngineNodesCNStore("old-binary-cn", "old-binary-pipeline", nil, metadata.WorkState_Working, "different-commit"),
			},
		})

		nodes, err := e.Nodes(false, "", "", nil)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{
			"working-pipeline",
			"draining-pipeline",
			"drained-pipeline",
			"unknown-pipeline",
		}, nodeAddresses(nodes))
		require.Equal(t, map[string]metadata.WorkState{
			"working-pipeline":  metadata.WorkState_Working,
			"draining-pipeline": metadata.WorkState_Draining,
			"drained-pipeline":  metadata.WorkState_Drained,
			"unknown-pipeline":  metadata.WorkState_Unknown,
		}, nodeWorkStates(nodes))
	})

	t.Run("common tenant label route", func(t *testing.T) {
		accountLabel := map[string]metadata.LabelList{
			"account": {Labels: []string{"app"}},
		}
		e := newEngineWithClusterDetails(t, logpb.ClusterDetails{
			CNStores: []logpb.CNStore{
				newEngineNodesCNStore("working-cn", "working-pipeline", accountLabel, metadata.WorkState_Working, version.CommitID),
				newEngineNodesCNStore("draining-cn", "draining-pipeline", accountLabel, metadata.WorkState_Draining, version.CommitID),
				newEngineNodesCNStore("drained-cn", "drained-pipeline", accountLabel, metadata.WorkState_Drained, version.CommitID),
				newEngineNodesCNStore("unknown-cn", "unknown-pipeline", accountLabel, metadata.WorkState_Unknown, version.CommitID),
				newEngineNodesCNStore("old-binary-cn", "old-binary-pipeline", accountLabel, metadata.WorkState_Working, "different-commit"),
			},
		})

		nodes, err := e.Nodes(false, "app", "user", map[string]string{"account": "app"})
		require.NoError(t, err)
		require.ElementsMatch(t, []string{
			"working-pipeline",
			"draining-pipeline",
			"drained-pipeline",
			"unknown-pipeline",
		}, nodeAddresses(nodes))
	})

	t.Run("runtime-ineligible labeled CN does not replace working fallback", func(t *testing.T) {
		accountLabel := map[string]metadata.LabelList{
			"account": {Labels: []string{"app"}},
		}
		e := newEngineWithClusterDetails(t, logpb.ClusterDetails{
			CNStores: []logpb.CNStore{
				newEngineNodesCNStore("fallback-cn", "fallback-pipeline", nil, metadata.WorkState_Working, version.CommitID),
				newEngineNodesCNStore("draining-cn", "draining-pipeline", accountLabel, metadata.WorkState_Draining, version.CommitID),
			},
		})

		nodes, err := e.Nodes(false, "app", "user", map[string]string{"account": "app"})
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"fallback-pipeline", "draining-pipeline"}, nodeAddresses(nodes))
	})

	t.Run("super tenant label route", func(t *testing.T) {
		accountLabel := map[string]metadata.LabelList{
			"account": {Labels: []string{"sys"}},
		}
		cases := []struct {
			name       string
			isInternal bool
			tenant     string
		}{
			{name: "internal query", isInternal: true, tenant: "app"},
			{name: "sys tenant", tenant: "sys"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				e := newEngineWithClusterDetails(t, logpb.ClusterDetails{
					CNStores: []logpb.CNStore{
						newEngineNodesCNStore("working-cn", "working-pipeline", accountLabel, metadata.WorkState_Working, version.CommitID),
						newEngineNodesCNStore("draining-cn", "draining-pipeline", accountLabel, metadata.WorkState_Draining, version.CommitID),
						newEngineNodesCNStore("drained-cn", "drained-pipeline", accountLabel, metadata.WorkState_Drained, version.CommitID),
						newEngineNodesCNStore("unknown-cn", "unknown-pipeline", accountLabel, metadata.WorkState_Unknown, version.CommitID),
						newEngineNodesCNStore("old-binary-cn", "old-binary-pipeline", accountLabel, metadata.WorkState_Working, "different-commit"),
					},
				})

				nodes, err := e.Nodes(tc.isInternal, tc.tenant, "root", map[string]string{"account": "sys"})
				require.NoError(t, err)
				require.ElementsMatch(t, []string{
					"working-pipeline",
					"draining-pipeline",
					"drained-pipeline",
					"unknown-pipeline",
				}, nodeAddresses(nodes))
			})
		}
	})
}

func TestEngineQueryCandidateProvidersSeparateInventoryAndPool(t *testing.T) {
	appLabel := map[string]metadata.LabelList{
		"account": {Labels: []string{"app"}},
	}
	sysLabel := map[string]metadata.LabelList{
		"account": {Labels: []string{"sys"}},
	}
	e := newEngineWithClusterDetails(t, logpb.ClusterDetails{
		CNStores: []logpb.CNStore{
			newEngineNodesCNStore("app-working", "app-working:6001", appLabel, metadata.WorkState_Working, version.CommitID),
			newEngineNodesCNStore("app-draining", "app-draining:6001", appLabel, metadata.WorkState_Draining, version.CommitID),
			newEngineNodesCNStore("sys-working", "sys-working:6001", sysLabel, metadata.WorkState_Working, version.CommitID),
			newEngineNodesCNStore("old-app", "old-app:6001", appLabel, metadata.WorkState_Working, "different-commit"),
		},
	})

	candidates, err := e.DiscoverQueryCandidates(context.Background())
	require.NoError(t, err)
	require.Len(t, candidates, 3)
	require.ElementsMatch(t,
		[]string{"app-working", "app-draining", "sys-working"},
		queryCandidateServiceIDs(candidates))

	labels := map[string]string{"account": "app"}
	nodes, err := e.ResolveQueryCandidatePool(
		context.Background(),
		candidates,
		engine.QueryCandidatePoolRequest{
			Tenant:  "app",
			CNLabel: labels,
		},
	)
	require.NoError(t, err)
	require.ElementsMatch(t,
		[]string{"app-working:6001", "app-draining:6001"},
		nodeAddresses(nodes))
	require.Equal(t, map[string]string{"account": "app"}, labels)
}

func TestEngineCandidateDiscoveryExcludesIncompatibleCNBeforePoolFallback(t *testing.T) {
	appLabel := map[string]metadata.LabelList{
		"account": {Labels: []string{"app"}},
	}
	e := newEngineWithClusterDetails(t, logpb.ClusterDetails{
		CNStores: []logpb.CNStore{
			newEngineNodesCNStore("old-app", "old-app:6001", appLabel, metadata.WorkState_Working, "different-commit"),
			newEngineNodesCNStore("compatible-fallback", "fallback:6001", nil, metadata.WorkState_Working, version.CommitID),
		},
	})

	nodes, err := e.Nodes(false, "app", "user", map[string]string{"account": "app"})
	require.NoError(t, err)
	require.Equal(t, []string{"fallback:6001"}, nodeAddresses(nodes))
}

func TestEngineQueryCandidateProvidersHonorCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := &Engine{service: "missing-runtime"}

	_, err := e.DiscoverQueryCandidates(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, err = e.ResolveQueryCandidatePool(ctx, nil, engine.QueryCandidatePoolRequest{})
	require.ErrorIs(t, err, context.Canceled)
}

type cancelAfterChecksContext struct {
	context.Context
	remaining int
	done      chan struct{}
}

func newCancelAfterChecksContext(checks int) *cancelAfterChecksContext {
	return &cancelAfterChecksContext{
		Context:   context.Background(),
		remaining: checks,
		done:      make(chan struct{}),
	}
}

func (c *cancelAfterChecksContext) Done() <-chan struct{} {
	return c.done
}

func (c *cancelAfterChecksContext) Err() error {
	if c.remaining > 0 {
		c.remaining--
		if c.remaining == 0 {
			close(c.done)
		}
	}
	if c.remaining == 0 {
		return context.DeadlineExceeded
	}
	return nil
}

func TestEngineQueryCandidatePoolHonorsCancellationDuringIteration(t *testing.T) {
	candidates := engine.QueryCandidates{
		{Service: metadata.CNService{ServiceID: "cn-1", WorkState: metadata.WorkState_Working}},
		{Service: metadata.CNService{ServiceID: "cn-2", WorkState: metadata.WorkState_Working}},
		{Service: metadata.CNService{ServiceID: "cn-3", WorkState: metadata.WorkState_Working}},
	}
	e := new(Engine)

	t.Run("without labels", func(t *testing.T) {
		ctx := newCancelAfterChecksContext(3)
		_, err := e.ResolveQueryCandidatePool(ctx, candidates, engine.QueryCandidatePoolRequest{})
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("while building labeled route input", func(t *testing.T) {
		ctx := newCancelAfterChecksContext(5)
		_, err := e.ResolveQueryCandidatePool(ctx, candidates, engine.QueryCandidatePoolRequest{
			Tenant:  "app",
			CNLabel: map[string]string{"account": "app"},
		})
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func newEngineWithClusterDetails(t *testing.T, details logpb.ClusterDetails) *Engine {
	t.Helper()

	sid := t.Name()
	rt := moruntime.DefaultRuntimeWithLevel(zap.InfoLevel)
	moruntime.SetupServiceBasedRuntime(sid, rt)

	cluster := clusterservice.NewMOCluster(sid, &engineNodesClusterClient{details: details}, time.Hour)
	cluster.ForceRefresh(true)
	t.Cleanup(cluster.Close)
	rt.SetGlobalVariables(moruntime.ClusterService, cluster)

	return &Engine{service: sid}
}

func newEngineNodesCNStore(
	uuid string,
	serviceAddress string,
	labels map[string]metadata.LabelList,
	workState metadata.WorkState,
	commitID string,
) logpb.CNStore {
	return logpb.CNStore{
		UUID:           uuid,
		ServiceAddress: serviceAddress,
		Labels:         labels,
		WorkState:      workState,
		CommitID:       commitID,
	}
}

func nodeAddresses(nodes []engine.Node) []string {
	addrs := make([]string, 0, len(nodes))
	for _, node := range nodes {
		addrs = append(addrs, node.Addr)
	}
	return addrs
}

func nodeWorkStates(nodes []engine.Node) map[string]metadata.WorkState {
	states := make(map[string]metadata.WorkState, len(nodes))
	for _, node := range nodes {
		states[node.Addr] = node.WorkState
	}
	return states
}

func queryCandidateServiceIDs(candidates engine.QueryCandidates) []string {
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.Service.ServiceID)
	}
	return ids
}

func TestFilterDeleteDatabaseRelationsSkipsAlreadyDeletedRelation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txn := &Transaction{tableOps: newTableOps()}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().Return(txn).AnyTimes()
	txnOp.EXPECT().Txn().Return(txnpb.TxnMeta{}).AnyTimes()

	const (
		accountID    = uint32(7)
		databaseID   = uint64(11)
		dbName       = "acc_test02"
		deletedTable = "aff01"
		activeTable  = "pri01"
		tableID      = uint64(13)
	)
	db := &txnDatabase{
		accountId:    accountID,
		databaseId:   databaseID,
		databaseName: dbName,
		op:           txnOp,
	}
	txn.tableOps.addDeleteTable(genTableKey(accountID, deletedTable, databaseID, dbName), 0, tableID)

	rels := filterDeleteDatabaseRelations(db, []string{deletedTable, activeTable}, dbName, txnOp)
	require.Equal(t, []string{activeTable}, rels)
}

func TestIsDeleteDatabaseRelationDeletedInTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txn := &Transaction{tableOps: newTableOps()}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().Return(txn).AnyTimes()

	const (
		accountID  = uint32(7)
		databaseID = uint64(11)
		dbName     = "acc_test02"
		tableName  = "aff01"
		tableID    = uint64(13)
	)
	db := &txnDatabase{
		databaseId:   databaseID,
		databaseName: dbName,
		op:           txnOp,
	}

	require.False(t, isDeleteDatabaseRelationDeletedInTxn(db, accountID, tableName))

	txn.tableOps.addDeleteTable(genTableKey(accountID, tableName, databaseID, dbName), 0, tableID)
	require.True(t, isDeleteDatabaseRelationDeletedInTxn(db, accountID, tableName))

	require.False(t, isDeleteDatabaseRelationDeletedInTxn(db, accountID+1, tableName))
	require.False(t, isDeleteDatabaseRelationDeletedInTxn(db, accountID, "other_table"))
}

func TestIsDeleteDatabaseRelationDeletedInTxnUsesSystemAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txn := &Transaction{tableOps: newTableOps()}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().Return(txn).AnyTimes()

	const tableName = catalog.MO_TABLES
	db := &txnDatabase{
		databaseId:   catalog.MO_CATALOG_ID,
		databaseName: catalog.MO_CATALOG,
		op:           txnOp,
	}

	txn.tableOps.addDeleteTable(genTableKey(catalog.System_Account, tableName, catalog.MO_CATALOG_ID, catalog.MO_CATALOG), 0, catalog.MO_TABLES_ID)
	require.True(t, isDeleteDatabaseRelationDeletedInTxn(db, uint32(99), tableName))
}
