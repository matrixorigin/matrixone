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

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

// The relation and ranges path must keep the selected account from the root
// binding while also observing the source pipeline's independent cancellation.

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

// A prepared scope can be scheduled while read-only and dispatched after an
// earlier statement has changed the workspace. Exercise the actual send entry
// point; a dispatch rejection must happen before encoding or opening a stream.

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
	parentInner.EXPECT().CloneSnapshotOp(old).Return(child).Times(1)
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
	readers, _, err := s.buildReaders(c)
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
