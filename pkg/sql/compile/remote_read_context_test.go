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
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

type m0StampedTxn struct {
	client.TxnOperator
	revision uint64
}

func (op *m0StampedTxn) CatalogReadStamp() (client.CatalogReadStamp, error) {
	return client.CatalogReadStamp{TransactionID: "txn", Snapshot: op.SnapshotTS(), Revision: op.revision}, nil
}

func TestM0DistinctSnapshotScanTxnBinding(t *testing.T) {
	enableSharedRemoteGuard(t)
	ctrl := gomock.NewController(t)
	c := newMergeTopFallbackTestCompile(t)
	t.Cleanup(c.proc.Free)
	now := timestamp.Timestamp{PhysicalTime: 300}
	first := timestamp.Timestamp{PhysicalTime: 100}
	second := timestamp.Timestamp{PhysicalTime: 200}
	parent := mock_frontend.NewMockTxnOperator(ctrl)
	parent.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: now}).AnyTimes()
	parent.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	parent.EXPECT().GetWorkspace().Return(&disttae.Transaction{}).AnyTimes()
	child := func(ts timestamp.Timestamp) *mock_frontend.MockTxnOperator {
		op := mock_frontend.NewMockTxnOperator(ctrl)
		op.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: ts}).AnyTimes()
		op.EXPECT().SnapshotTS().Return(ts).AnyTimes()
		op.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		op.EXPECT().GetWorkspace().Return(&disttae.Transaction{}).AnyTimes()
		return op
	}
	one, two := child(first), child(second)
	parent.EXPECT().CloneSnapshotOp(first).Return(one).AnyTimes()
	parent.EXPECT().CloneSnapshotOp(second).Return(two).AnyTimes()
	c.proc.Base.TxnOperator = parent
	scan := func(ts *timestamp.Timestamp) *Scope {
		return &Scope{DataSource: &Source{node: &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "t"}, TableDef: &plan.TableDef{Name: "t"}, ScanSnapshot: &plan.Snapshot{TS: ts}}}}
	}
	op, _, err := c.getCompileTableScanDataSourceTxn(scan(&first))
	require.NoError(t, err)
	require.Equal(t, first, op.SnapshotTS())
	c.proc.SetCloneTxnOperator(one)
	op, _, err = c.getCompileTableScanDataSourceTxn(scan(&second))
	require.NoError(t, err)
	require.Equal(t, second, op.SnapshotTS(), "each scan must bind its requested snapshot")
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", two).Return(db, nil)
	db.EXPECT().Relation(gomock.Any(), "t", gomock.Any()).Return(rel, nil)
	c.e = eng
	_, _, _, err = c.handleDbRelContextWithBoundTxn(scan(&second).DataSource.node, true, c.proc.Ctx, one)
	require.Error(t, err, "a bound transaction from another snapshot must be rejected before opening the relation")
	opened, _, _, err := c.handleDbRelContext(scan(&second).DataSource.node, false)
	require.NoError(t, err)
	require.Same(t, rel, opened)
}

func TestM0RemoteRangesUsePipelineCancellationAndBoundAccount(t *testing.T) {
	enableSharedRemoteGuard(t)
	ctrl := gomock.NewController(t)
	c := newMergeTopFallbackTestCompile(t)
	root := defines.AttachAccountId(c.proc.GetTopContext(), 42)
	c.proc.ReplaceTopCtx(root)
	c.proc.Ctx = root
	childCtx, cancelChild := context.WithCancel(defines.AttachAccountId(root, 99))
	defer cancelChild()
	child := c.proc.NewNoContextChildProc(0)
	child.Ctx = childCtx
	ts := timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4}
	inner := mock_frontend.NewMockTxnOperator(ctrl)
	inner.EXPECT().GetWorkspace().Return(&disttae.Transaction{}).AnyTimes()
	inner.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	inner.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: ts}).AnyTimes()
	inner.EXPECT().SnapshotTS().Return(ts).AnyTimes()
	op := &m0StampedTxn{TxnOperator: inner}
	c.proc.Base.TxnOperator = op
	def := &plan.TableDef{TblId: 42, Version: 1, Name: "customer", Cols: []*plan.ColDef{
		{ColId: 11, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
	}}
	node := &plan.Node{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer", Obj: 42}, TableDef: def, Stats: &plan.Stats{}}
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	c.e = eng
	eng.EXPECT().Database(gomock.Any(), "db", op).Return(db, nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), "customer", gomock.Any()).Return(rel, nil).AnyTimes()
	rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
	entered := make(chan uint32, 1)
	release := make(chan struct{})
	finished := make(chan struct{})
	result := make(chan error, 1)
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ engine.RangesParam) (engine.RelData, error) {
		account, _ := defines.GetAccountId(ctx)
		entered <- account
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return nil, errors.New("test release before pipeline cancellation")
		}
	})
	scope := &Scope{IsRemote: true, Proc: child, NodeInfo: engine.Node{CNCNT: 1, Mcpu: 1, Data: readutil.NewBlockListRelationData(0)},
		DataSource: &Source{node: node, SchemaName: "db", RelationName: "customer", TableDef: def, Timestamp: ts}}
	go func() {
		defer close(finished)
		result <- scope.getRelData(c, nil)
	}()
	defer func() {
		cancelChild()
		close(release)
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("range selection did not exit")
		}
	}()
	select {
	case account := <-entered:
		require.Equal(t, uint32(42), account, "root account must take precedence over the child account")
	case <-time.After(5 * time.Second):
		t.Fatal("range selection did not start")
	}
	cancelChild()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("range selection ignored pipeline cancellation")
	}
}

func TestM0RemoteSingleScopeRetainsShippedTombstones(t *testing.T) {
	enableSharedRemoteGuard(t)
	ctrl := gomock.NewController(t)
	c := newMergeTopFallbackTestCompile(t)
	t.Cleanup(c.proc.Free)
	ts := timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4}
	watermark := &m0WatermarkClient{result: ts.Prev()}
	c.proc.Base.TxnClient = watermark
	inner := mock_frontend.NewMockTxnOperator(ctrl)
	inner.EXPECT().GetWorkspace().Return(&disttae.Transaction{}).AnyTimes()
	inner.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	inner.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: ts}).AnyTimes()
	inner.EXPECT().SnapshotTS().Return(ts).AnyTimes()
	op := &m0StampedTxn{TxnOperator: inner}
	c.proc.Base.TxnOperator = op
	def := &plan.TableDef{TblId: 42, Version: 1, Name: "customer", Cols: []*plan.ColDef{
		{ColId: 11, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
	}}
	node := &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer", Obj: 42}, TableDef: def, Stats: &plan.Stats{},
		AggList: []*plan.Expr{{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "starcount"}}}}},
	}
	row := types.RandomRowid()
	localRow := types.NewRowid(row.BorrowBlockID(), row.GetRowOffset()^1)
	tombstones := readutil.NewEmptyTombstoneData()
	require.NoError(t, tombstones.AppendInMemory(row))
	tombstones.SortInMemory()
	shipped := readutil.NewBlockListRelationData(0)
	require.NoError(t, shipped.AttachTombstones(tombstones))
	sender := &Scope{Magic: Remote, NodeInfo: engine.Node{Mcpu: 1, CNCNT: 1, Data: shipped}, Proc: c.proc.NewNoContextChildProc(0), RootOp: table_scan.NewArgument(), DataSource: &Source{
		SchemaName: "db", RelationName: "customer", Attributes: []string{"a"}, TableDef: def, node: node, Timestamp: ts,
	}}
	t.Cleanup(sender.release)
	wire, err := encodeRemoteScope(sender, c.proc)
	require.NoError(t, err)
	received, err := decodeScope(wire, c.proc, true, nil)
	require.NoError(t, err)
	t.Cleanup(received.release)
	received.Proc.Ctx = c.proc.Ctx
	require.True(t, received.NodeInfo.Data.GetTombstones().HasAnyInMemoryTombstone())
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	c.e = eng
	eng.EXPECT().Database(gomock.Any(), "db", op).Return(db, nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), "customer", gomock.Any()).Return(rel, nil).AnyTimes()
	rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
	receivedData := received.NodeInfo.Data
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, context.Canceled)
	require.ErrorIs(t, received.getRelData(c, nil), context.Canceled)
	require.Same(t, receivedData, received.NodeInfo.Data, "canceled selection must retain the received tombstones")
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil)
	require.Error(t, received.getRelData(c, nil), "nil ranges must fail before publishing a marker")
	require.False(t, received.m0SingleRemoteHasShipped)
	require.Same(t, receivedData, received.NodeInfo.Data, "failed range selection must retain the received tombstones")
	// A real partitioned relation can return CombinedRelData. Its individual
	// ranges retain their own tombstones, so accepting the sender data must not
	// silently replace those tombstones. This fixture only verifies the receiver
	// boundary; it does not establish that production scheduling reaches it.
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(&disttae.CombinedRelData{}, nil)
	err = received.getRelData(c, nil)
	require.ErrorContains(t, err, "single remote combined ranges cannot bind shipped tombstones")
	require.False(t, received.m0SingleRemoteHasShipped)
	require.Same(t, receivedData, received.NodeInfo.Data, "failed member binding must retain the received tombstones")
	// A certified sender with no visible workspace deletes carries an empty
	// payload. The receiver must keep its own per-member ranges and use the
	// normal relation-reader policy after admitting the snapshot watermark.
	received.NodeInfo.Data = readutil.NewBlockListRelationData(0)
	combined := &disttae.CombinedRelData{}
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(combined, nil)
	rel.EXPECT().BuildReaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ any, _ *plan.Expr, data engine.RelData, _, _ int, _ bool, policy engine.TombstoneApplyPolicy, _ engine.FilterHint) ([]engine.Reader, error) {
			require.Same(t, combined, data)
			require.False(t, m0read.IsSingleRemote(ctx))
			require.Equal(t, engine.TombstoneApplyPolicy(engine.Policy_CheckAll), policy)
			return []engine.Reader{new(readutil.EmptyReader)}, nil
		})
	watermark.result = ts
	rebuiltReaders, _, err := received.buildReaders(c)
	require.NoError(t, err)
	require.Len(t, rebuiltReaders, 1)
	require.NoError(t, rebuiltReaders[0].Close())
	require.Nil(t, received.NodeInfo.Data, "the constructed reader owns the selected ranges")
	require.False(t, received.m0SingleRemoteHasShipped)
	watermark.result = ts.Prev()
	received.NodeInfo.Data = receivedData
	attachErr := errors.New("injected tombstone attach failure")
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(&readerPathAttachFailure{
		RelData: readutil.NewBlockListRelationData(0), err: attachErr,
	}, nil)
	require.ErrorIs(t, received.getRelData(c, nil), attachErr)
	require.Same(t, receivedData, received.NodeInfo.Data, "failed attach must retain the received tombstones")
	rangeCalls := 0
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, engine.RangesParam) (engine.RelData, error) {
		rangeCalls++
		data := readutil.NewBlockListRelationData(0)
		local := readutil.NewEmptyTombstoneData()
		require.NoError(t, local.AppendInMemory(localRow))
		local.SortInMemory()
		require.NoError(t, data.AttachTombstones(local))
		return data, nil
	}).AnyTimes()
	require.NoError(t, received.getRelData(c, nil))
	require.False(t, received.StarCountOnly, "sender tombstones require the normal reader")
	require.NotNil(t, received.NodeInfo.Data.GetTombstones(), "single remote scan lost sender tombstones")
	require.True(t, received.NodeInfo.Data.GetTombstones().HasAnyInMemoryTombstone())
	for _, candidate := range []types.Rowid{row, localRow} {
		left := received.NodeInfo.Data.GetTombstones().ApplyInMemTombstones(candidate.BorrowBlockID(), []int64{int64(candidate.GetRowOffset())}, nil)
		require.Empty(t, left, "receiver and sender tombstones must both be retained")
	}
	rel.EXPECT().BuildReaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ any, _ *plan.Expr, data engine.RelData, _, _ int, _ bool, _ engine.TombstoneApplyPolicy, _ engine.FilterHint) ([]engine.Reader, error) {
			require.True(t, m0read.IsSingleRemote(ctx))
			require.NotNil(t, data.GetTombstones())
			require.True(t, data.GetTombstones().HasAnyInMemoryTombstone())
			return []engine.Reader{new(readutil.EmptyReader)}, nil
		})
	before := rangeCalls
	readers, _, err := received.buildReaders(c)
	require.ErrorContains(t, err, "watermark precedes")
	require.Empty(t, readers)
	require.Equal(t, before, rangeCalls, "stale watermark must fail before ranges")
	require.Equal(t, ts.Prev(), watermark.want)
	watermark.result = ts
	readers, _, err = received.buildReaders(c)
	require.NoError(t, err)
	require.Len(t, readers, 1)
	require.NoError(t, readers[0].Close())
	// A subsequent range selection with no sender tombstones must not retain
	// this execution's marker, even if the local ranges have tombstones.
	received.DataSource.node.AggList = nil
	received.NodeInfo.Data = readutil.NewBlockListRelationData(0)
	require.NoError(t, received.getRelData(c, nil))
	require.False(t, received.m0SingleRemoteHasShipped)
}

func TestM0RemotePublishedTableReaderAccount(t *testing.T) {
	enableSharedRemoteGuard(t)
	ctrl := gomock.NewController(t)
	c := newMergeTopFallbackTestCompile(t)
	t.Cleanup(c.proc.Free)
	c.proc.Ctx = defines.AttachAccountId(c.proc.Ctx, 99)
	ts := timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4}
	inner := mock_frontend.NewMockTxnOperator(ctrl)
	inner.EXPECT().GetWorkspace().Return(&disttae.Transaction{}).AnyTimes()
	inner.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	inner.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: ts}).AnyTimes()
	inner.EXPECT().SnapshotTS().Return(ts).AnyTimes()
	op := &m0StampedTxn{TxnOperator: inner}
	c.proc.Base.TxnOperator = op
	def := &plan.TableDef{TblId: 42, Version: 1, Name: "customer", Cols: []*plan.ColDef{
		{ColId: 11, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
	}}
	node := &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer", PubInfo: &plan.PubInfo{TenantId: 42}}, TableDef: def, Stats: &plan.Stats{}}
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(readutil.NewBlockListRelationData(0), nil)
	db := mock_frontend.NewMockDatabase(ctrl)
	db.EXPECT().Relation(gomock.Any(), "customer", gomock.Any()).Return(rel, nil)
	eng := &readerPathCaptureEngine{database: db}
	c.e = eng
	s := &Scope{IsRemote: true, NodeInfo: engine.Node{CNCNT: 2, Mcpu: 1, Data: readutil.NewBlockListRelationData(0)}, DataSource: &Source{
		SchemaName: "db", RelationName: "customer", TableDef: def, node: node, Timestamp: ts,
	}}
	readers, _, err := s.buildReaders(c)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, reader := range readers {
			_ = reader.Close()
		}
	})
	require.Equal(t, 1, eng.buildBlockReadersCalls)
	account, err := defines.GetAccountId(eng.readerContext)
	require.NoError(t, err)
	require.Equal(t, uint32(42), account)
}

func TestM0RemoteHistoricalSnapshotReaderAccount(t *testing.T) {
	enableSharedRemoteGuard(t)
	for _, tc := range []struct {
		name      string
		pub       *plan.PubInfo
		tableType string
		dbName    string
		tableName string
		want      uint32
	}{
		{name: "snapshot tenant", want: 42},
		{name: "publisher overrides snapshot", pub: &plan.PubInfo{TenantId: 17}, want: 17},
		{name: "system table overrides snapshot", tableType: catalog.SystemClusterRel, want: catalog.System_Account},
		{name: "publisher overrides system table", pub: &plan.PubInfo{TenantId: 17}, tableType: catalog.SystemClusterRel, want: 17},
		{name: "logging overrides publisher", pub: &plan.PubInfo{TenantId: 17}, dbName: "system", tableName: "statement_info", want: catalog.System_Account},
	} {
		for _, cnCount := range []int32{1, 2} {
			t.Run(fmt.Sprintf("%s/cn-count=%d", tc.name, cnCount), func(t *testing.T) {
				dbName, tableName := tc.dbName, tc.tableName
				if dbName == "" {
					dbName = "db"
				}
				if tableName == "" {
					tableName = "customer"
				}
				ctrl := gomock.NewController(t)
				c := newMergeTopFallbackTestCompile(t)
				t.Cleanup(c.proc.Free)
				c.proc.Ctx = defines.AttachAccountId(c.proc.Ctx, 99)
				c.proc.ReplaceTopCtx(defines.AttachAccountId(c.proc.GetTopContext(), 99))
				now := timestamp.Timestamp{PhysicalTime: 200}
				c.proc.Base.TxnClient = &m0WatermarkClient{result: now}
				old := timestamp.Timestamp{PhysicalTime: 123}
				parent := mock_frontend.NewMockTxnOperator(ctrl)
				parent.EXPECT().GetWorkspace().Return(&disttae.Transaction{}).AnyTimes()
				parent.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
				parent.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: now}).AnyTimes()
				parent.EXPECT().SnapshotTS().Return(now).AnyTimes()
				child := mock_frontend.NewMockTxnOperator(ctrl)
				child.EXPECT().GetWorkspace().Return(&disttae.Transaction{}).AnyTimes()
				child.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
				child.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: old}).AnyTimes()
				child.EXPECT().SnapshotTS().Return(old).AnyTimes()
				c.proc.Base.TxnOperator = &m0StampedTxn{TxnOperator: parent}
				boundClone := &m0StampedTxn{TxnOperator: child}
				// Select a scan-local transaction even when a process-level
				// clone exists. Ranges and readers share this selected binding.
				parent.EXPECT().CloneSnapshotOp(old).Return(boundClone).Times(1)
				if tc.name != "snapshot tenant" || cnCount != 1 {
					c.proc.SetCloneTxnOperator(boundClone)
				}
				def := &plan.TableDef{TblId: 42, Version: 1, Name: tableName, TableType: tc.tableType, Cols: []*plan.ColDef{
					{ColId: 11, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
				}}
				node := &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: dbName, ObjName: tableName, PubInfo: tc.pub}, TableDef: def, Stats: &plan.Stats{},
					ScanSnapshot: &plan.Snapshot{TS: &old, Tenant: &plan.SnapshotTenant{TenantID: 42}}}
				rel := mock_frontend.NewMockRelation(ctrl)
				rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
				rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ engine.RangesParam) (engine.RelData, error) {
					account, err := defines.GetAccountId(ctx)
					require.NoError(t, err)
					require.Equal(t, tc.want, account, "ranges must use the selected account")
					return readutil.NewBlockListRelationData(0), nil
				})
				var readerCtx context.Context
				if cnCount == 1 {
					rel.EXPECT().BuildReaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						DoAndReturn(func(ctx context.Context, _ any, _ *plan.Expr, _ engine.RelData, _, _ int, _ bool, _ engine.TombstoneApplyPolicy, _ engine.FilterHint) ([]engine.Reader, error) {
							readerCtx = ctx
							return []engine.Reader{new(readutil.EmptyReader)}, nil
						})
				}
				db := mock_frontend.NewMockDatabase(ctrl)
				db.EXPECT().Relation(gomock.Any(), tableName, gomock.Any()).Return(rel, nil)
				eng := &readerPathCaptureEngine{database: db}
				c.e = eng
				pipeline := c.proc.NewNoContextChildProc(0)
				t.Cleanup(pipeline.Free)
				var cancel context.CancelFunc
				pipeline.Ctx, cancel = context.WithCancel(c.proc.Ctx)
				t.Cleanup(cancel)
				s := &Scope{Proc: pipeline, IsRemote: true, NodeInfo: engine.Node{CNCNT: cnCount, Mcpu: 1, Data: readutil.NewBlockListRelationData(0)}, DataSource: &Source{
					SchemaName: dbName, RelationName: tableName, Attributes: []string{"a"}, TableDef: def, node: node, Timestamp: old,
				}}
				readers, _, err := s.buildReaders(c)
				require.NoError(t, err)
				t.Cleanup(func() {
					for _, reader := range readers {
						_ = reader.Close()
					}
				})
				if cnCount != 1 {
					readerCtx = eng.readerContext
				}
				require.NotNil(t, readerCtx)
				account, err := defines.GetAccountId(readerCtx)
				require.NoError(t, err)
				require.Equal(t, tc.want, account, "reader must use the same account as ranges")
				cancel()
				require.ErrorIs(t, readerCtx.Err(), context.Canceled, "reader must retain pipeline cancellation")
			})
		}
	}
}

type m0WatermarkClient struct {
	client.TxnClient
	result timestamp.Timestamp
	want   timestamp.Timestamp
}

func (c *m0WatermarkClient) WaitLogTailAppliedAt(ctx context.Context, target timestamp.Timestamp) (timestamp.Timestamp, error) {
	if err := ctx.Err(); err != nil {
		return timestamp.Timestamp{}, err
	}
	c.want = target
	return c.result, nil
}
