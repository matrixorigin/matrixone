//go:build mo_m0

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

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type m0StampedTxn struct {
	client.TxnOperator
	revision uint64
}

func TestM0DistinctSnapshotScanTxnBinding(t *testing.T) {
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

// The relation and ranges path must keep the selected account from the root
// binding while also observing the source pipeline's independent cancellation.
func TestM0RemoteRangesUsePipelineCancellationAndBoundAccount(t *testing.T) {
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

type m0MutableSchedulingWorkspace struct {
	*Ws
	readOnly bool
	unstable bool
	revision uint64
	advance  bool
}

type m0DispatchStampedTxn struct {
	client.TxnOperator
	workspace client.Workspace
	revision  uint64
	snapshot  timestamp.Timestamp
}

func (op *m0DispatchStampedTxn) CatalogReadStamp() (client.CatalogReadStamp, error) {
	return client.CatalogReadStamp{TransactionID: "txn", Workspace: op.workspace, Revision: op.revision, Snapshot: op.snapshot}, nil
}

func (w *m0MutableSchedulingWorkspace) Readonly() bool { return w.readOnly }
func (w *m0MutableSchedulingWorkspace) CatalogVisibility() (uint64, bool) {
	revision := w.revision
	if w.advance {
		w.revision++
	}
	return revision, !w.unstable
}

func (op *m0StampedTxn) CatalogReadStamp() (client.CatalogReadStamp, error) {
	return client.CatalogReadStamp{TransactionID: "txn", Snapshot: op.SnapshotTS(), Revision: op.revision}, nil
}

type m0CloseTrackingReader struct {
	engine.Reader
	closed bool
}

func (r *m0CloseTrackingReader) Close() error { r.closed = true; return nil }

type m0RefreshDuringBuildEngine struct {
	*readerPathCaptureEngine
	txn         *m0StampedTxn
	reader      *m0CloseTrackingReader
	cancel      context.CancelFunc
	skipRefresh bool
}

func (e *m0RefreshDuringBuildEngine) BuildBlockReaders(
	_ context.Context, _ any, _ timestamp.Timestamp, _ *plan.Expr, _ *plan.TableDef,
	_ engine.RelData, _ int, _ ...engine.FilterHint,
) ([]engine.Reader, error) {
	if !e.skipRefresh {
		e.txn.revision += 2
	}
	if e.cancel != nil {
		e.cancel()
	}
	return []engine.Reader{e.reader}, nil
}

// This checks the existing scope wire representation, not object-membership
// authorization or a live multi-CN deployment.
func TestM0RemoteScopeBindingFieldsRoundTrip(t *testing.T) {
	c := newMergeTopFallbackTestCompile(t)
	t.Cleanup(c.proc.Free)
	ts := timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4}
	def := &plan.TableDef{TblId: 42, Version: 1, Name: "customer", Cols: []*plan.ColDef{
		{ColId: 11, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
		{ColId: 12, Name: "c", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}},
	}}
	node := &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer", Obj: 42}, TableDef: def}
	scope := &Scope{Magic: Remote, NodeInfo: engine.Node{Mcpu: 1}, Proc: c.proc.NewNoContextChildProc(0), RootOp: table_scan.NewArgument(), DataSource: &Source{
		SchemaName: "db", RelationName: "customer", Attributes: []string{"a", "c"}, TableDef: def, node: node, Timestamp: ts,
	}}
	t.Cleanup(scope.release)
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	restored, err := decodeScope(data, c.proc, true, nil)
	require.NoError(t, err)
	t.Cleanup(restored.release)
	require.True(t, restored.IsRemote)
	require.Equal(t, ts, restored.DataSource.Timestamp)
	require.Equal(t, []string{"a", "c"}, restored.DataSource.Attributes)
	require.Equal(t, uint64(42), restored.DataSource.TableDef.TblId)
	require.Equal(t, uint32(1), restored.DataSource.TableDef.Version)
	require.Equal(t, uint64(12), restored.DataSource.TableDef.Cols[1].ColId)
	require.Equal(t, uint32(1), restored.DataSource.TableDef.Cols[1].Seqnum)
	require.Equal(t, "db", restored.DataSource.node.ObjRef.SchemaName)
	require.Equal(t, "customer", restored.DataSource.node.ObjRef.ObjName)
	require.NoError(t, validateM0RemoteReadBinding(t.Context(), restored.DataSource, def, ts, nil))
	t.Run("object reference identity", func(t *testing.T) {
		original := restored.DataSource.node.ObjRef.Obj
		restored.DataSource.node.ObjRef.Obj++
		t.Cleanup(func() { restored.DataSource.node.ObjRef.Obj = original })
		require.Error(t, validateM0RemoteReadBinding(t.Context(), restored.DataSource, def, ts, nil))
	})
	for _, tc := range []struct {
		name   string
		mutate func(*plan.TableDef)
	}{
		{"other relation", func(d *plan.TableDef) { d.TblId++ }},
		{"other schema version", func(d *plan.TableDef) { d.Version++ }},
		{"other column identity", func(d *plan.TableDef) { d.Cols[1].ColId++ }},
		{"other column type", func(d *plan.TableDef) { d.Cols[1].Typ.Width++ }},
		{"missing prefix", func(d *plan.TableDef) { d.Cols = d.Cols[1:] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			local := proto.Clone(def).(*plan.TableDef)
			tc.mutate(local)
			require.Error(t, validateM0RemoteReadBinding(t.Context(), restored.DataSource, local, ts, nil))
		})
	}
	stale := ts
	stale.LogicalTime++
	require.Error(t, validateM0RemoteReadBinding(t.Context(), restored.DataSource, def, stale, nil))
	t.Run("projected plan schema", func(t *testing.T) {
		original := restored.DataSource.node.TableDef
		originalAttrs := restored.DataSource.Attributes
		restored.DataSource.node.TableDef = proto.Clone(original).(*plan.TableDef)
		restored.DataSource.node.TableDef.Cols = restored.DataSource.node.TableDef.Cols[1:]
		restored.DataSource.Attributes = []string{"c"}
		t.Cleanup(func() { restored.DataSource.node.TableDef = original; restored.DataSource.Attributes = originalAttrs })
		require.NoError(t, validateM0RemoteReadBinding(t.Context(), restored.DataSource, def, ts, nil))
	})
	t.Run("attribute outside plan", func(t *testing.T) {
		original := restored.DataSource.Attributes
		restored.DataSource.Attributes = []string{"a", "unrelated"}
		t.Cleanup(func() { restored.DataSource.Attributes = original })
		require.Error(t, validateM0RemoteReadBinding(t.Context(), restored.DataSource, def, ts, nil))
	})
	for _, tc := range []struct {
		name   string
		mutate func(*plan.TableDef)
	}{
		{"plan relation", func(d *plan.TableDef) { d.TblId++ }},
		{"plan schema version", func(d *plan.TableDef) { d.Version++ }},
		{"plan column identity", func(d *plan.TableDef) { d.Cols[1].ColId++ }},
		{"plan column type", func(d *plan.TableDef) { d.Cols[1].Typ.Width++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := restored.DataSource.node.TableDef
			restored.DataSource.node.TableDef = proto.Clone(original).(*plan.TableDef)
			t.Cleanup(func() { restored.DataSource.node.TableDef = original })
			tc.mutate(restored.DataSource.node.TableDef)
			require.Error(t, validateM0RemoteReadBinding(t.Context(), restored.DataSource, def, ts, nil))
		})
	}
}

// Exercise the remote ranges entry itself: a mismatched received definition
// must fail before the relation is allowed to enumerate any objects.
func TestM0RemoteRangesChecksBindingBeforeSelection(t *testing.T) {
	ts := timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4}
	def := &plan.TableDef{TblId: 42, Version: 1, Name: "customer", Cols: []*plan.ColDef{
		{ColId: 11, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
	}}
	for _, tc := range []struct {
		name    string
		mutate  func(*plan.TableDef, *timestamp.Timestamp)
		ok      bool
		refresh bool
	}{
		{"matching", func(*plan.TableDef, *timestamp.Timestamp) {}, true, false},
		{"foreign relation", func(d *plan.TableDef, _ *timestamp.Timestamp) { d.TblId++ }, false, false},
		{"stale schema", func(d *plan.TableDef, _ *timestamp.Timestamp) { d.Version++ }, false, false},
		{"wrong type", func(d *plan.TableDef, _ *timestamp.Timestamp) { d.Cols[0].Typ.Width++ }, false, false},
		{"stale snapshot", func(_ *plan.TableDef, t *timestamp.Timestamp) { t.LogicalTime++ }, false, false},
		{"snapshot refreshed during selection", func(*plan.TableDef, *timestamp.Timestamp) {}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c := newMergeTopFallbackTestCompile(t)
			t.Cleanup(c.proc.Free)
			inner := mock_frontend.NewMockTxnOperator(ctrl)
			inner.EXPECT().GetWorkspace().Return(&disttae.Transaction{}).AnyTimes()
			inner.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
			inner.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: ts}).AnyTimes()
			currentTS := ts
			inner.EXPECT().SnapshotTS().DoAndReturn(func() timestamp.Timestamp { return currentTS }).AnyTimes()
			op := &m0StampedTxn{TxnOperator: inner}
			c.proc.Base.TxnOperator = op
			eng := mock_frontend.NewMockEngine(ctrl)
			db := mock_frontend.NewMockDatabase(ctrl)
			rel := mock_frontend.NewMockRelation(ctrl)
			c.e = eng
			eng.EXPECT().Database(gomock.Any(), "db", op).Return(db, nil)
			db.EXPECT().Relation(gomock.Any(), "customer", gomock.Any()).Return(rel, nil)
			local := proto.Clone(def).(*plan.TableDef)
			wireTS := ts
			tc.mutate(local, &wireTS)
			rel.EXPECT().GetTableDef(gomock.Any()).Return(local).AnyTimes()
			sentinel := errors.New("ranges reached")
			if tc.ok {
				rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, sentinel)
			} else if tc.refresh {
				rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, engine.RangesParam) (engine.RelData, error) {
					currentTS.LogicalTime++
					return readutil.NewBlockListRelationData(0), nil
				})
			}
			node := &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer"}, TableDef: def, Stats: &plan.Stats{}}
			s := &Scope{IsRemote: true, NodeInfo: engine.Node{CNCNT: 1}, DataSource: &Source{
				SchemaName: "db", RelationName: "customer", TableDef: def, node: node, Timestamp: wireTS,
			}}
			err := s.getRelData(c, nil)
			if tc.ok {
				require.ErrorIs(t, err, sentinel)
			} else {
				require.Error(t, err)
				require.False(t, errors.Is(err, sentinel))
			}
		})
	}
}

// This is an isolated receiver counterexample. Whether a single remote scope can
// legitimately carry uncommitted tombstones is a separate scheduling question.
func TestM0RemoteSingleScopeRetainsShippedTombstones(t *testing.T) {
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
	rebuiltReaders, err := received.buildReaders(c)
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
	readers, err := received.buildReaders(c)
	require.ErrorContains(t, err, "watermark precedes")
	require.Empty(t, readers)
	require.Equal(t, before, rangeCalls, "stale watermark must fail before ranges")
	require.Equal(t, ts.Prev(), watermark.want)
	watermark.result = ts
	readers, err = received.buildReaders(c)
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

func TestM0WritableSingletonScanStaysOnCoordinator(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, txnOp := newTestTxnClientAndOp(ctrl, &Ws{})
	c := NewMockCompile(t)
	c.proc.Base.TxnOperator = txnOp
	c.addr = "ingress:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		candidates: engine.QueryCandidates{
			{Service: metadata.CNService{ServiceID: "ingress", PipelineServiceAddress: "ingress:6001", WorkState: metadata.WorkState_Working}, Mcpu: 8},
			{Service: metadata.CNService{ServiceID: "remote", PipelineServiceAddress: "remote:6001", WorkState: metadata.WorkState_Working}, Mcpu: 8},
		},
		resolvedNodes: engine.Nodes{
			{Id: "ingress", Addr: "ingress:6001", Mcpu: 8, WorkState: metadata.WorkState_Working},
			{Id: "remote", Addr: "remote:6001", Mcpu: 8, WorkState: metadata.WorkState_Working},
		},
	}
	workers, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.True(t, c.queryPlacement.RequireCurrentCN)
	c.cnList = workers
	c.e = newStubEngineForGenerateNodes("testdb", "t")
	node := &plan.Node{NodeType: plan.Node_TABLE_SCAN,
		ObjRef:   &plan.ObjectRef{SchemaName: "testdb", ObjName: "t"},
		TableDef: &plan.TableDef{Name: "t"},
		Stats:    &plan.Stats{ForceOneCN: true, Dop: 2},
	}
	scans, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Len(t, scans, 1)
	require.Equal(t, c.addr, scans[0].Addr)
	require.Equal(t, int32(1), scans[0].CNCNT)
}

// The sender verifies that workspace tombstones are empty while committed
// tombstones are reconstructed at the receiver's admitted snapshot.
func TestM0ReadOnlySingletonRemoteRebuildsCommittedTombstones(t *testing.T) {
	ctrl := gomock.NewController(t)
	workspace := &m0MutableSchedulingWorkspace{Ws: &Ws{}, readOnly: true}
	_, txnOp := newTestTxnClientAndOp(ctrl, workspace)
	c := NewMockCompile(t)
	c.proc.Base.TxnOperator = &m0DispatchStampedTxn{TxnOperator: txnOp, workspace: workspace, snapshot: timestamp.Timestamp{PhysicalTime: 200}}
	c.addr = "ingress:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "remote", Addr: "remote:6001", Mcpu: 2, WorkState: metadata.WorkState_Working}}
	eng := newStubEngineForGenerateNodes("testdb", "t")
	rel := eng.dbs["testdb"].rels["t"]
	tombstones := readutil.NewEmptyTombstoneData()
	require.NoError(t, tombstones.AppendInMemory(types.RandomRowid()))
	tombstones.SortInMemory()
	rel.collectTombstonesFn = func(_ context.Context, _ int, policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
		if policy == engine.Policy_CollectUncommittedTombstones {
			return readutil.NewEmptyTombstoneData(), nil
		}
		return tombstones, nil
	}
	c.e = eng
	node := &plan.Node{NodeType: plan.Node_TABLE_SCAN,
		ObjRef:   &plan.ObjectRef{SchemaName: "testdb", ObjName: "t"},
		TableDef: &plan.TableDef{Name: "t"},
		Stats:    &plan.Stats{ForceOneCN: true, Dop: 2},
	}
	scans, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Len(t, scans, 1)
	require.Equal(t, "remote:6001", scans[0].Addr)
	require.Equal(t, int32(1), scans[0].CNCNT)
	require.Equal(t, 1, rel.collectTombstonesCall)
	require.False(t, scans[0].Data.GetTombstones().HasAnyInMemoryTombstone())
	_, certified := scans[0].Data.(*m0SingleRemoteReadData)
	require.True(t, certified)
	// The regular scan-scope constructor must retain the coordinator-local
	// certificate until remote dispatch, even though the codec omits it.
	c.anal = &AnalyzeModule{}
	scope, err := c.compileTableScanWithNode(node, scans[0], false)
	require.NoError(t, err)
	t.Cleanup(scope.release)
	scope.DataSource.Timestamp = timestamp.Timestamp{PhysicalTime: 200}
	require.NoError(t, validateM0SingleRemoteReadData(scope))
	certificate := scope.NodeInfo.Data
	for _, replacement := range []engine.RelData{nil, readutil.BuildEmptyRelData()} {
		scope.NodeInfo.Data = replacement
		require.Error(t, validateM0SingleRemoteReadData(scope), "a single remote scan must not dispatch without its workspace certificate")
	}
	scope.NodeInfo.Data = certificate
	workspace.revision++
	require.True(t, moerr.IsMoErrCode(validateM0SingleRemoteReadData(scope), moerr.ErrTxnNeedRetry))
}

// Models a new scheduling attempt after the workspace becomes writable. It
// does not itself trigger the automatic RC retry machinery.
func TestM0ReschedulingRechecksWritableWorkspace(t *testing.T) {
	ctrl := gomock.NewController(t)
	workspace := &m0MutableSchedulingWorkspace{Ws: &Ws{}, readOnly: true}
	_, txnOp := newTestTxnClientAndOp(ctrl, workspace)
	c := NewMockCompile(t)
	c.proc.Base.TxnOperator = txnOp
	c.addr = "ingress:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	provider := &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		candidates: engine.QueryCandidates{
			{Service: metadata.CNService{ServiceID: "ingress", PipelineServiceAddress: "ingress:6001", WorkState: metadata.WorkState_Working}, Mcpu: 8},
			{Service: metadata.CNService{ServiceID: "remote", PipelineServiceAddress: "remote:6001", WorkState: metadata.WorkState_Working}, Mcpu: 8},
		},
		resolvedNodes: engine.Nodes{
			{Id: "ingress", Addr: "ingress:6001", Mcpu: 8, WorkState: metadata.WorkState_Working},
			{Id: "remote", Addr: "remote:6001", Mcpu: 8, WorkState: metadata.WorkState_Working},
		},
	}
	c.e = provider
	_, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.False(t, c.queryPlacement.RequireCurrentCN)
	workspace.readOnly = false
	workers, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.True(t, c.queryPlacement.RequireCurrentCN)
	c.cnList = workers
	c.e = newStubEngineForGenerateNodes("testdb", "t")
	node := &plan.Node{NodeType: plan.Node_TABLE_SCAN,
		ObjRef:   &plan.ObjectRef{SchemaName: "testdb", ObjName: "t"},
		TableDef: &plan.TableDef{Name: "t"},
		Stats:    &plan.Stats{ForceOneCN: true, Dop: 2},
	}
	scans, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Len(t, scans, 1)
	require.Equal(t, c.addr, scans[0].Addr)
}

func TestM0SingleRemoteSenderRechecksWorkspace(t *testing.T) {
	for _, tc := range []struct {
		name      string
		readOnly  bool
		wantError bool
	}{{"writable", false, true}, {"read-only", true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			workspace := &m0MutableSchedulingWorkspace{Ws: &Ws{}, readOnly: tc.readOnly}
			_, op := newTestTxnClientAndOp(ctrl, workspace)
			c := NewMockCompile(t)
			c.proc.Base.TxnOperator = &m0DispatchStampedTxn{TxnOperator: op, workspace: workspace, snapshot: timestamp.Timestamp{PhysicalTime: 200}}
			rel := newStubRelation("t")
			rel.tombstones = readutil.NewEmptyTombstoneData()
			planNode := &plan.Node{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "t"}, TableDef: &plan.TableDef{Name: "t"}}
			attacher := &remoteScanTombstoneAttacher{c: c, node: planNode, rel: rel}
			worker := &engine.Node{Addr: "remote:6001", CNCNT: 1}
			err := attacher.attach(worker)
			if tc.wantError {
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
				require.Zero(t, rel.collectTombstonesCall)
				require.Nil(t, worker.Data)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, rel.collectTombstonesCall)
				require.NotNil(t, worker.Data)
			}
		})
	}
	t.Run("becomes writable after scheduling", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		workspace := &m0MutableSchedulingWorkspace{Ws: &Ws{}, readOnly: true}
		_, op := newTestTxnClientAndOp(ctrl, workspace)
		c := NewMockCompile(t)
		c.proc.Base.TxnOperator = op
		require.False(t, c.executionRequiresCurrentCN())
		workspace.readOnly = false
		rel := newStubRelation("t")
		planNode := &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "t"}, TableDef: &plan.TableDef{Name: "t"}}
		attacher := &remoteScanTombstoneAttacher{c: c, node: planNode, rel: rel}
		worker := &engine.Node{Addr: "remote:6001", CNCNT: 1}
		err := attacher.attach(worker)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
		require.Zero(t, rel.collectTombstonesCall)
		require.Nil(t, worker.Data)
	})
}

// A prepared scope can be scheduled while read-only and dispatched after an
// earlier statement has changed the workspace. Exercise the actual send entry
// point; a dispatch rejection must happen before encoding or opening a stream.
func TestM0SingleRemoteDispatchRechecksWorkspace(t *testing.T) {
	ctrl := gomock.NewController(t)
	workspace := &m0MutableSchedulingWorkspace{Ws: &Ws{}, readOnly: true}
	_, op := newTestTxnClientAndOp(ctrl, workspace)
	c := NewMockCompile(t)
	c.proc.Base.TxnOperator = &m0DispatchStampedTxn{TxnOperator: op, workspace: workspace}
	scan := &Scope{
		NodeInfo:   engine.Node{Addr: "remote:6001", CNCNT: 1},
		DataSource: &Source{node: &plan.Node{NodeType: plan.Node_TABLE_SCAN}},
		Proc:       c.proc,
	}
	require.NoError(t, validateM0SingleRemoteDispatch(scan, c))
	workspace.unstable = true
	require.True(t, moerr.IsMoErrCode(validateM0SingleRemoteDispatch(scan, c), moerr.ErrTxnNeedRetry), "an in-flight workspace mutation is not read-only authority")
	workspace.unstable = false
	workspace.advance = true
	require.True(t, moerr.IsMoErrCode(validateM0SingleRemoteDispatch(scan, c), moerr.ErrTxnNeedRetry), "revision changes invalidate admission")
	workspace.advance = false
	workspace.readOnly = false
	_, err := scan.remoteRun(c)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
	outer := &Scope{PreScopes: []*Scope{nil, scan}}
	require.True(t, moerr.IsMoErrCode(validateM0SingleRemoteDispatch(outer, c), moerr.ErrTxnNeedRetry))
	scan.NodeInfo.CNCNT = 2
	require.NoError(t, validateM0SingleRemoteDispatch(outer, c), "do not change multi-CN dispatch")
	scan.NodeInfo.CNCNT = 1
	scan.DataSource.node.NodeType = plan.Node_TABLE_CLONE
	require.True(t, moerr.IsMoErrCode(validateM0SingleRemoteDispatch(outer, c), moerr.ErrTxnNeedRetry), "clone scans carry the same workspace boundary")
	scan.DataSource.node.NodeType = plan.Node_VALUE_SCAN
	require.NoError(t, validateM0SingleRemoteDispatch(outer, c), "do not reject a non-table scan")
	scan.DataSource.node.NodeType = plan.Node_TABLE_SCAN
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scan.Proc.Ctx = ctx
	_, err = scan.remoteRun(c)
	require.ErrorIs(t, err, context.Canceled)
}

func TestM0SingleRemoteDispatchRechecksAfterStreamOpen(t *testing.T) {
	for _, change := range []string{"writable", "workspace revision", "transaction generation", "snapshot", "transaction replacement", "canceled"} {
		t.Run(change, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			workspace := &m0MutableSchedulingWorkspace{Ws: &Ws{}, readOnly: true}
			_, op := newTestTxnClientAndOp(ctrl, workspace)
			c := NewMockCompile(t)
			ctx, cancel := context.WithCancel(c.proc.Ctx)
			defer cancel()
			c.proc.Ctx = ctx
			stamped := &m0DispatchStampedTxn{TxnOperator: op, workspace: workspace, snapshot: timestamp.Timestamp{PhysicalTime: 200}}
			c.proc.Base.TxnOperator = stamped
			c.addr = "ingress:6001"
			stream := mock_morpc.NewMockStream(ctrl)
			stream.EXPECT().Receive().Return(nil, nil)
			stream.EXPECT().Close(true).Return(nil)
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).Times(0)
			opened := false
			client := &testPipelineClient{genStream: func(context.Context, string) (morpc.Stream, error) {
				opened = true
				switch change {
				case "writable":
					workspace.readOnly = false
				case "workspace revision":
					workspace.revision++
				case "transaction generation":
					stamped.revision++
				case "snapshot":
					stamped.snapshot.PhysicalTime++
				case "transaction replacement":
					c.proc.Base.TxnOperator = &m0DispatchStampedTxn{TxnOperator: op, workspace: workspace}
				case "canceled":
					cancel()
				}
				return stream, nil
			}}
			rt := runtime.ServiceRuntime(c.proc.GetService())
			previous, existed := rt.GetGlobalVariables(runtime.PipelineClient)
			rt.SetGlobalVariables(runtime.PipelineClient, client)
			t.Cleanup(func() {
				if existed {
					rt.SetGlobalVariables(runtime.PipelineClient, previous)
				} else {
					rt.CompareAndDeleteGlobalVariables(runtime.PipelineClient, client)
				}
			})
			opRoot := connector.NewArgument()
			opRoot.Reg = &process.WaitRegister{Ch2: make(chan process.PipelineSignal, 1)}
			def := &plan.TableDef{TblId: 42, Name: "customer"}
			scope := &Scope{
				Magic: Remote, Proc: c.proc, RootOp: opRoot, ScopeAnalyzer: &ScopeAnalyzer{isStoped: true},
				NodeInfo: engine.Node{Addr: "remote:6001", CNCNT: 1, Mcpu: 1},
				DataSource: &Source{SchemaName: "db", RelationName: "customer", TableDef: def,
					node: &plan.Node{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer"}, TableDef: def}},
			}
			admitM0RemoteDispatchFixture(t, c, scope)
			sender, err := scope.remoteRun(c)
			require.True(t, opened, "the stream must open before the workspace changes")
			if change == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
			}
			require.NotNil(t, sender)
			sender.close(context.Background())
		})
	}
}

func TestM0RemotePublishedTableReaderAccount(t *testing.T) {
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
	readers, err := s.buildReaders(c)
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
				if tc.name == "snapshot tenant" && cnCount == 1 {
					// Ranges and reader construction must share the one selected
					// clone, rather than creating equal-TS workspaces independently.
					parent.EXPECT().CloneSnapshotOp(old).Return(boundClone).Times(1)
				} else {
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
				readers, err := s.buildReaders(c)
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

func TestM0RemoteRangesRejectsSnapshotABA(t *testing.T) {
	ctrl := gomock.NewController(t)
	c := newMergeTopFallbackTestCompile(t)
	t.Cleanup(c.proc.Free)
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
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, engine.RangesParam) (engine.RelData, error) {
		// A snapshot refresh away and back to the same timestamp still creates a
		// different catalog generation; final timestamp equality is insufficient.
		op.revision += 2
		return readutil.NewBlockListRelationData(0), nil
	})
	db := mock_frontend.NewMockDatabase(ctrl)
	db.EXPECT().Relation(gomock.Any(), "customer", gomock.Any()).Return(rel, nil)
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", op).Return(db, nil)
	c.e = eng
	node := &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer"}, TableDef: def, Stats: &plan.Stats{}}
	receivedData := readutil.NewBlockListRelationData(0)
	s := &Scope{IsRemote: true, NodeInfo: engine.Node{CNCNT: 1, Data: receivedData}, DataSource: &Source{
		SchemaName: "db", RelationName: "customer", TableDef: def, node: node, Timestamp: ts,
	}}
	err := s.getRelData(c, nil)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
	require.Same(t, receivedData, s.NodeInfo.Data, "a changed generation must not publish stale ranges")
}

func TestM0RemoteHistoricalRangesRejectParentGenerationChange(t *testing.T) {
	testM0RemoteHistoricalRangesParentChange(t, false)
}

func TestM0RemoteHistoricalRangesCanceledDuringParentChange(t *testing.T) {
	testM0RemoteHistoricalRangesParentChange(t, true)
}

func testM0RemoteHistoricalRangesParentChange(t *testing.T, cancelDuringRanges bool) {
	ctrl := gomock.NewController(t)
	c := newMergeTopFallbackTestCompile(t)
	t.Cleanup(c.proc.Free)
	pipelineCtx, cancel := context.WithCancel(c.proc.Ctx)
	t.Cleanup(cancel)
	c.proc.Ctx = pipelineCtx
	now := timestamp.Timestamp{PhysicalTime: 200}
	old := timestamp.Timestamp{PhysicalTime: 123}
	parentInner := mock_frontend.NewMockTxnOperator(ctrl)
	parentInner.EXPECT().GetWorkspace().Return(&disttae.Transaction{}).AnyTimes()
	parentInner.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	parentInner.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: now}).AnyTimes()
	parentInner.EXPECT().SnapshotTS().Return(now).AnyTimes()
	parent := &m0StampedTxn{TxnOperator: parentInner}
	childInner := mock_frontend.NewMockTxnOperator(ctrl)
	childInner.EXPECT().GetWorkspace().Return(&disttae.Transaction{}).AnyTimes()
	childInner.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	childInner.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: old}).AnyTimes()
	childInner.EXPECT().SnapshotTS().Return(old).AnyTimes()
	child := &m0StampedTxn{TxnOperator: childInner}
	c.proc.Base.TxnOperator = parent
	c.proc.SetCloneTxnOperator(child)
	def := &plan.TableDef{TblId: 42, Version: 1, Name: "customer", Cols: []*plan.ColDef{
		{ColId: 11, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
	}}
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, engine.RangesParam) (engine.RelData, error) {
		parent.revision += 2
		if cancelDuringRanges {
			cancel()
		}
		return readutil.NewBlockListRelationData(0), nil
	})
	db := mock_frontend.NewMockDatabase(ctrl)
	db.EXPECT().Relation(gomock.Any(), "customer", gomock.Any()).Return(rel, nil)
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", child).Return(db, nil)
	c.e = eng
	node := &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer"}, TableDef: def, Stats: &plan.Stats{}, ScanSnapshot: &plan.Snapshot{TS: &old}}
	receivedData := readutil.NewBlockListRelationData(0)
	s := &Scope{IsRemote: true, NodeInfo: engine.Node{CNCNT: 1, Data: receivedData}, DataSource: &Source{
		SchemaName: "db", RelationName: "customer", TableDef: def, node: node, Timestamp: old,
	}}
	err := s.getRelData(c, nil)
	require.Error(t, err)
	if cancelDuringRanges {
		require.ErrorIs(t, err, context.Canceled)
	} else {
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
	}
	require.Same(t, receivedData, s.NodeInfo.Data, "changed parent generation must not publish ranges")
}

func TestM0RemoteReaderBuildRejectsSnapshotABAAndClosesReader(t *testing.T) {
	testM0RemoteReaderBuildGenerationChange(t, false, true)
}

func TestM0RemoteReaderBuildCancellationClosesReader(t *testing.T) {
	testM0RemoteReaderBuildGenerationChange(t, true, false)
}

func TestM0RemoteReaderBuildCancellationDuringGenerationChange(t *testing.T) {
	testM0RemoteReaderBuildGenerationChange(t, true, true)
}

func testM0RemoteReaderBuildGenerationChange(t *testing.T, cancelDuringBuild, refreshDuringBuild bool) {
	ctrl := gomock.NewController(t)
	c := newMergeTopFallbackTestCompile(t)
	t.Cleanup(c.proc.Free)
	pipelineCtx, cancel := context.WithCancel(c.proc.Ctx)
	t.Cleanup(cancel)
	c.proc.Ctx = pipelineCtx
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
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(readutil.NewBlockListRelationData(0), nil)
	db := mock_frontend.NewMockDatabase(ctrl)
	db.EXPECT().Relation(gomock.Any(), "customer", gomock.Any()).Return(rel, nil)
	tracked := &m0CloseTrackingReader{}
	buildEngine := &m0RefreshDuringBuildEngine{readerPathCaptureEngine: &readerPathCaptureEngine{database: db}, txn: op, reader: tracked}
	buildEngine.skipRefresh = !refreshDuringBuild
	if cancelDuringBuild {
		buildEngine.cancel = cancel
	}
	c.e = buildEngine
	node := &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer"}, TableDef: def, Stats: &plan.Stats{}}
	s := &Scope{IsRemote: true, NodeInfo: engine.Node{CNCNT: 2, Mcpu: 1, Data: readutil.NewBlockListRelationData(0)}, DataSource: &Source{
		SchemaName: "db", RelationName: "customer", TableDef: def, node: node, Timestamp: ts,
	}}
	readers, err := s.buildReaders(c)
	require.Error(t, err)
	if cancelDuringBuild {
		require.ErrorIs(t, err, context.Canceled)
	} else {
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
	}
	require.Empty(t, readers)
	require.True(t, tracked.closed)
}

func TestM0RemoteBindingAcceptsEngineSyntheticRowID(t *testing.T) {
	rowID := plan2.MakeRowIdColDef()
	def := &plan.TableDef{TblId: 42, Version: 1, Name: "customer", Cols: []*plan.ColDef{
		{ColId: 11, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
		rowID,
	}}
	ts := timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4}
	node := &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer"}, TableDef: def}
	source := &Source{SchemaName: "db", RelationName: "customer", Attributes: []string{"a", rowID.Name}, TableDef: def, node: node, Timestamp: ts}
	require.NoError(t, validateM0RemoteReadBinding(t.Context(), source, def, ts, nil))
	badPlan := proto.Clone(def).(*plan.TableDef)
	badPlan.Cols[1].Typ.Id = int32(types.T_int32)
	source.node = &plan.Node{ObjRef: node.ObjRef, TableDef: badPlan}
	require.Error(t, validateM0RemoteReadBinding(t.Context(), source, def, ts, nil), "rowid must not bind to the first user column")
}

func TestM0RemoteBindingLegacyTinyTextPlan(t *testing.T) {
	local := &plan.TableDef{
		TblId: 42, LogicalId: 42, Name: "customer", DbName: "db", TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE customer (a INT, payload TINYTEXT)",
		Cols: []*plan.ColDef{
			{ColId: 11, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
			{ColId: 12, Name: "payload", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_text)}},
		},
	}
	planned := proto.Clone(local).(*plan.TableDef)
	require.NoError(t, plan2.RecoverLegacyTinyTextFromCreateSQL(t.Context(), planned))
	require.Equal(t, int32(types.MaxTinyTextLen), planned.Cols[1].Typ.Width)
	ts := timestamp.Timestamp{PhysicalTime: 123}
	source := &Source{SchemaName: "db", RelationName: "customer", Attributes: []string{"payload"}, TableDef: local, node: &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer", Obj: 42}, TableDef: planned,
	}, Timestamp: ts}
	require.NoError(t, validateM0RemoteReadBinding(t.Context(), source, local, ts, nil))
	require.Zero(t, local.Cols[1].Typ.Width, "planner recovery must not mutate the stored catalog definition")
	forged := proto.Clone(planned).(*plan.TableDef)
	forged.Cols[1].Typ.Width++
	source.node.TableDef = forged
	require.Error(t, validateM0RemoteReadBinding(t.Context(), source, local, ts, nil))
	source.node.TableDef = planned
	untrusted := proto.Clone(local).(*plan.TableDef)
	untrusted.LogicalId++
	source.TableDef = untrusted
	require.Error(t, validateM0RemoteReadBinding(t.Context(), source, untrusted, ts, nil))
	source.TableDef = local
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, validateM0RemoteReadBinding(canceled, source, local, ts, nil), context.Canceled)
}

func TestM0RemoteBindingLegacyTinyTextCreateLikePlan(t *testing.T) {
	base := &plan.TableDef{
		TblId: 41, LogicalId: 41, Name: "base", DbName: "db", TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE base (a INT, payload TINYTEXT)",
		Cols: []*plan.ColDef{
			{ColId: 1, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
			{ColId: 2, Name: "payload", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_text)}},
		},
	}
	local := proto.Clone(base).(*plan.TableDef)
	local.TblId, local.LogicalId, local.Name = 42, 42, "customer"
	local.Createsql = "CREATE TABLE customer LIKE base"
	planned := proto.Clone(local).(*plan.TableDef)
	resolver := func(_ context.Context, db, name string) (*plan.TableDef, error) {
		require.Equal(t, "db", db)
		require.Equal(t, "base", name)
		return base, nil
	}
	require.NoError(t, plan2.RecoverLegacyTinyText(t.Context(), planned, resolver))
	require.Equal(t, int32(types.MaxTinyTextLen), planned.Cols[1].Typ.Width)
	ts := timestamp.Timestamp{PhysicalTime: 123}
	source := &Source{SchemaName: "db", RelationName: "customer", Attributes: []string{"payload"}, TableDef: local, node: &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer", Obj: 42}, TableDef: planned,
	}, Timestamp: ts}
	require.NoError(t, validateM0RemoteReadBinding(t.Context(), source, local, ts, resolver))
}

func TestM0RemoteCreateLikeResolvesAtPublisherSnapshot(t *testing.T) {
	base := &plan.TableDef{
		TblId: 41, LogicalId: 41, Name: "base", DbName: "db", TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE base (a INT, payload TINYTEXT)",
		Cols: []*plan.ColDef{
			{ColId: 1, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
			{ColId: 2, Name: "payload", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_text)}},
		},
	}
	local := proto.Clone(base).(*plan.TableDef)
	local.TblId, local.LogicalId, local.Name = 42, 42, "customer"
	local.Createsql = "CREATE TABLE customer LIKE base"
	planned := proto.Clone(local).(*plan.TableDef)
	require.NoError(t, plan2.RecoverLegacyTinyText(t.Context(), planned, func(context.Context, string, string) (*plan.TableDef, error) { return base, nil }))
	require.Equal(t, int32(types.MaxTinyTextLen), planned.Cols[1].Typ.Width)

	for _, trusted := range []bool{true, false} {
		t.Run(fmt.Sprintf("trusted=%t", trusted), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c := newMergeTopFallbackTestCompile(t)
			t.Cleanup(c.proc.Free)
			ts := timestamp.Timestamp{PhysicalTime: 123}
			inner := mock_frontend.NewMockTxnOperator(ctrl)
			inner.EXPECT().GetWorkspace().Return(&disttae.Transaction{}).AnyTimes()
			inner.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
			inner.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: ts}).AnyTimes()
			inner.EXPECT().SnapshotTS().Return(ts).AnyTimes()
			op := &m0StampedTxn{TxnOperator: inner}
			c.proc.Base.TxnOperator = op
			rel := mock_frontend.NewMockRelation(ctrl)
			rel.EXPECT().GetTableDef(gomock.Any()).Return(local).AnyTimes()
			storedBase := proto.Clone(base).(*plan.TableDef)
			if !trusted {
				storedBase.LogicalId++
			}
			baseRel := mock_frontend.NewMockRelation(ctrl)
			baseRel.EXPECT().GetTableDef(gomock.Any()).Return(storedBase)
			db := mock_frontend.NewMockDatabase(ctrl)
			db.EXPECT().Relation(gomock.Any(), "customer", gomock.Any()).Return(rel, nil)
			db.EXPECT().Relation(gomock.Any(), "base", nil).Return(baseRel, nil)
			eng := mock_frontend.NewMockEngine(ctrl)
			eng.EXPECT().Database(gomock.Any(), "db", op).DoAndReturn(func(ctx context.Context, _ string, _ client.TxnOperator) (engine.Database, error) {
				account, err := defines.GetAccountId(ctx)
				require.NoError(t, err)
				require.Equal(t, uint32(42), account)
				return db, nil
			}).Times(2)
			c.e = eng
			sentinel := errors.New("ranges reached")
			if trusted {
				rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, sentinel)
			}
			node := &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer", Obj: 42, PubInfo: &plan.PubInfo{TenantId: 42}}, TableDef: planned, Stats: &plan.Stats{}}
			s := &Scope{IsRemote: true, NodeInfo: engine.Node{CNCNT: 1}, DataSource: &Source{
				SchemaName: "db", RelationName: "customer", Attributes: []string{"payload"}, TableDef: local, node: node, Timestamp: ts,
			}}
			err := s.getRelData(c, nil)
			if trusted {
				require.ErrorIs(t, err, sentinel)
			} else {
				require.Error(t, err)
				require.False(t, errors.Is(err, sentinel))
			}
		})
	}
}
