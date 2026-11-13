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
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// These guards are compiled in both binaries. Exercise their state/ownership
// contract in both test modes without installing the tagged planner or catalog
// hooks. The receiver uses the same watermark check in both test modes.
// Tests are serial and restore the production mode immediately.
func enableSharedRemoteGuard(t *testing.T) {
	t.Helper()
	old := activeM0Enabled
	oldWait := activeM0WaitRemoteReadSnapshot
	activeM0Enabled = true
	activeM0WaitRemoteReadSnapshot = waitM0RemoteReadSnapshot
	t.Cleanup(func() { activeM0Enabled = old; activeM0WaitRemoteReadSnapshot = oldWait })
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

type m0RecordingTombstoneRelation struct {
	*stubRelation
	policy engine.TombstoneCollectPolicy
	result engine.Tombstoner
	err    error
}

func (r *m0RecordingTombstoneRelation) CollectTombstones(_ context.Context, _ int, policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
	r.policy = policy
	return r.result, r.err
}

func admitM0RemoteDispatchFixture(t *testing.T, c *Compile, s *Scope) {
	t.Helper()
	stamp, err := c.proc.GetTxnOperator().(interface {
		CatalogReadStamp() (client.CatalogReadStamp, error)
	}).CatalogReadStamp()
	require.NoError(t, err)
	s.DataSource.Timestamp = stamp.Snapshot
	rel := &m0RecordingTombstoneRelation{stubRelation: newStubRelation(s.DataSource.RelationName), result: readutil.NewEmptyTombstoneData()}
	require.NoError(t, (&remoteScanTombstoneAttacher{c: c, node: s.DataSource.node, rel: rel}).attach(&s.NodeInfo))
}

func TestM0SingleRemoteRebuildRequiresEmptyVisiblePrefix(t *testing.T) {
	enableSharedRemoteGuard(t)
	for _, tc := range []struct {
		name       string
		compileOff int
		wantError  bool
	}{
		{name: "empty prefix"},
		{name: "nonempty statement prefix", compileOff: 1, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			workspace := &m0MutableSchedulingWorkspace{Ws: &Ws{}, readOnly: true}
			_, op := newTestTxnClientAndOp(ctrl, workspace)
			c := NewMockCompile(t)
			c.proc.Base.TxnOperator = &m0DispatchStampedTxn{TxnOperator: op, workspace: workspace, snapshot: timestamp.Timestamp{PhysicalTime: 200}}
			c.TxnOffset = tc.compileOff
			node := &plan.Node{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "t"}, TableDef: &plan.TableDef{Name: "t"}}
			rel := &m0RecordingTombstoneRelation{stubRelation: newStubRelation("t"), result: readutil.NewEmptyTombstoneData()}
			worker := &engine.Node{Addr: "remote:6001", CNCNT: 1}
			err := (&remoteScanTombstoneAttacher{c: c, node: node, rel: rel}).attach(worker)
			if tc.wantError {
				require.Error(t, err)
				require.Nil(t, worker.Data)
			} else {
				require.NoError(t, err)
				require.NotNil(t, worker.Data)
				c.TxnOffset = 1
				scope := &Scope{Proc: c.proc, NodeInfo: *worker, DataSource: &Source{node: node, Timestamp: timestamp.Timestamp{PhysicalTime: 200}}}
				require.Error(t, validateM0SingleRemoteReadData(scope), "a changed statement prefix before dispatch must invalidate the certificate")
			}
		})
	}
}

func TestM0RemoteScopeWireDoesNotCarryScanOffset(t *testing.T) {
	enableSharedRemoteGuard(t)
	c := NewMockCompile(t)
	ts := timestamp.Timestamp{PhysicalTime: 200}
	def := &plan.TableDef{Name: "t"}
	node := &plan.Node{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "t"}, TableDef: def}
	sender := &Scope{Magic: Remote, Proc: c.proc.NewNoContextChildProc(0), RootOp: table_scan.NewArgument(),
		NodeInfo:   engine.Node{Mcpu: 1, CNCNT: 1, Data: readutil.NewBlockListRelationData(0)},
		DataSource: &Source{node: node, SchemaName: "db", RelationName: "t", TableDef: def, Timestamp: ts}, TxnOffset: 7}
	t.Cleanup(sender.release)
	wire, err := encodeRemoteScope(sender, c.proc)
	require.NoError(t, err)
	received, err := decodeScope(wire, c.proc, true, nil)
	require.NoError(t, err)
	t.Cleanup(received.release)
	require.Equal(t, 7, sender.TxnOffset)
	require.Zero(t, received.TxnOffset, "scan offset must not be assumed to cross the current scope wire")
}

func TestM0SingleRemoteDispatchRejectsUncertifiedNonzeroOffset(t *testing.T) {
	enableSharedRemoteGuard(t)
	for _, change := range []string{"compile", "scope", "canceled"} {
		t.Run(change, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			workspace := &m0MutableSchedulingWorkspace{Ws: &Ws{}, readOnly: true}
			_, op := newTestTxnClientAndOp(ctrl, workspace)
			c := NewMockCompile(t)
			c.proc.Base.TxnOperator = &m0DispatchStampedTxn{TxnOperator: op, workspace: workspace, snapshot: timestamp.Timestamp{PhysicalTime: 200}}
			scope := &Scope{Proc: c.proc, NodeInfo: engine.Node{CNCNT: 1}, DataSource: &Source{node: &plan.Node{NodeType: plan.Node_TABLE_SCAN}}}
			if change == "compile" || change == "canceled" {
				c.TxnOffset = 1
			} else {
				scope.TxnOffset = 1
			}
			if change == "canceled" {
				ctx, cancel := context.WithCancel(c.proc.Ctx)
				cancel()
				c.proc.Ctx = ctx
			}
			err := validateM0SingleRemoteDispatch(scope, c)
			if change == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry), "nonzero %s offset must be rejected before dispatch", change)
			}
		})
	}
}

func TestM0SingleRemoteRebuildCertificate(t *testing.T) {
	enableSharedRemoteGuard(t)
	for _, change := range []string{"none", "historical", "future request", "workspace", "transaction", "snapshot", "compile offset", "scope offset", "source timestamp", "relation", "schema", "scan snapshot", "cn count", "canceled", "canceled with changed scope", "empty initial snapshot"} {
		t.Run(change, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			workspace := &m0MutableSchedulingWorkspace{Ws: &Ws{}, readOnly: true}
			_, op := newTestTxnClientAndOp(ctrl, workspace)
			stamped := &m0DispatchStampedTxn{TxnOperator: op, workspace: workspace, snapshot: timestamp.Timestamp{PhysicalTime: 200}}
			if change == "empty initial snapshot" {
				stamped.snapshot = timestamp.Timestamp{}
			}
			c := NewMockCompile(t)
			ctx, cancel := context.WithCancel(c.proc.Ctx)
			defer cancel()
			c.proc.Ctx = ctx
			c.proc.Base.TxnOperator = stamped
			node := &plan.Node{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "t"}, TableDef: &plan.TableDef{Name: "t"}}
			wantSnapshot := stamped.snapshot
			if change == "historical" {
				wantSnapshot = timestamp.Timestamp{PhysicalTime: 123}
				node.ScanSnapshot = &plan.Snapshot{TS: &wantSnapshot}
			} else if change == "future request" {
				requested := timestamp.Timestamp{PhysicalTime: 300}
				node.ScanSnapshot = &plan.Snapshot{TS: &requested}
			}
			rel := &m0RecordingTombstoneRelation{stubRelation: newStubRelation("t"), result: readutil.NewEmptyTombstoneData()}
			worker := &engine.Node{Addr: "remote:6001", CNCNT: 1}
			attachErr := (&remoteScanTombstoneAttacher{c: c, node: node, rel: rel}).attach(worker)
			if change == "empty initial snapshot" {
				require.Error(t, attachErr)
				require.Nil(t, worker.Data)
				return
			}
			require.NoError(t, attachErr)
			certified, ok := worker.Data.(*m0SingleRemoteReadData)
			require.True(t, ok)
			encoded, err := certified.MarshalBinary()
			require.NoError(t, err)
			ordinary, err := certified.RelData.MarshalBinary()
			require.NoError(t, err)
			require.True(t, bytes.Equal(encoded, ordinary), "local certificate must not change the wire payload")
			scope := &Scope{Proc: c.proc, NodeInfo: *worker, DataSource: &Source{node: node, Timestamp: wantSnapshot}, TxnOffset: c.TxnOffset}
			switch change {
			case "workspace":
				workspace.revision++
			case "transaction":
				stamped.revision++
			case "snapshot":
				stamped.snapshot.PhysicalTime++
			case "compile offset":
				c.TxnOffset++
			case "scope offset":
				scope.TxnOffset++
			case "source timestamp":
				scope.DataSource.Timestamp.PhysicalTime++
			case "relation":
				node.ObjRef.ObjName = "other"
			case "schema":
				node.TableDef.Name = "other"
			case "scan snapshot":
				node.ScanSnapshot = &plan.Snapshot{}
			case "cn count":
				scope.NodeInfo.CNCNT++
			case "canceled":
				cancel()
			case "canceled with changed scope":
				scope.TxnOffset++
				cancel()
			}
			err = validateM0SingleRemoteReadData(scope)
			if change == "none" || change == "historical" || change == "future request" {
				require.NoError(t, err)
				sender := &Scope{Magic: Remote, Proc: c.proc.NewNoContextChildProc(0), RootOp: table_scan.NewArgument(), NodeInfo: *worker,
					DataSource: &Source{node: node, SchemaName: "db", RelationName: "t", TableDef: node.TableDef, Timestamp: wantSnapshot}, TxnOffset: c.TxnOffset}
				t.Cleanup(sender.release)
				wire, encodeErr := encodeRemoteScope(sender, c.proc)
				require.NoError(t, encodeErr)
				received, decodeErr := decodeScope(wire, c.proc, true, nil)
				require.NoError(t, decodeErr)
				t.Cleanup(received.release)
				require.Zero(t, received.TxnOffset, "the scope wire does not carry the sender's scan offset")
				_, localCertificateOnWire := received.NodeInfo.Data.(*m0SingleRemoteReadData)
				require.False(t, localCertificateOnWire)
				require.NotNil(t, received.NodeInfo.Data.GetTombstones())
				require.False(t, received.NodeInfo.Data.GetTombstones().HasAnyInMemoryTombstone())
			} else if change == "canceled" || change == "canceled with changed scope" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry), "changed %s must reject before dispatch", change)
			}
			if change != "none" && change != "historical" && change != "future request" {
				// The ordinary dispatch stamp is captured only after attachment;
				// the attached certificate must still reject this stale scope.
				_, err = scope.remoteRun(c)
				if change == "canceled" || change == "canceled with changed scope" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
				}
			}
		})
	}
}

func TestM0SingleRemoteRebuildAdmission(t *testing.T) {
	enableSharedRemoteGuard(t)
	for _, tc := range []struct {
		name        string
		cnCount     int32
		nodeType    plan.Node_NodeType
		uncommitted bool
		file        bool
		missing     bool
		fail        bool
		wantPolicy  engine.TombstoneCollectPolicy
		wantError   bool
	}{
		{name: "single empty workspace", cnCount: 1, nodeType: plan.Node_TABLE_SCAN, wantPolicy: engine.Policy_CollectUncommittedTombstones},
		{name: "single visible workspace delete", cnCount: 1, nodeType: plan.Node_TABLE_SCAN, uncommitted: true, wantPolicy: engine.Policy_CollectUncommittedTombstones, wantError: true},
		{name: "single visible workspace delete file", cnCount: 1, nodeType: plan.Node_TABLE_SCAN, file: true, wantPolicy: engine.Policy_CollectUncommittedTombstones, wantError: true},
		{name: "single missing workspace result", cnCount: 1, nodeType: plan.Node_TABLE_SCAN, missing: true, wantPolicy: engine.Policy_CollectUncommittedTombstones, wantError: true},
		{name: "single workspace collection error", cnCount: 1, nodeType: plan.Node_TABLE_SCAN, fail: true, wantPolicy: engine.Policy_CollectUncommittedTombstones, wantError: true},
		{name: "multi retains shipped committed", cnCount: 2, nodeType: plan.Node_TABLE_SCAN, wantPolicy: engine.Policy_CollectAllTombstones},
		{name: "other scan keeps original contract", cnCount: 1, nodeType: plan.Node_INDEX_SEARCH_SCAN, wantPolicy: engine.Policy_CollectAllTombstones},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			workspace := &m0MutableSchedulingWorkspace{Ws: &Ws{}, readOnly: true}
			_, op := newTestTxnClientAndOp(ctrl, workspace)
			c := NewMockCompile(t)
			c.proc.Base.TxnOperator = &m0DispatchStampedTxn{TxnOperator: op, workspace: workspace, snapshot: timestamp.Timestamp{PhysicalTime: 200}}
			row := types.RandomRowid()
			tombstones := readutil.NewEmptyTombstoneData()
			if tc.uncommitted {
				require.NoError(t, tombstones.AppendInMemory(row))
			}
			if tc.file {
				var stats objectio.ObjectStats
				objectio.SetObjectStatsLocation(&stats, objectio.NewRandomLocation(1, 1111))
				tombstones.AppendFiles(stats)
			}
			rel := &m0RecordingTombstoneRelation{stubRelation: newStubRelation("t"), result: tombstones}
			if tc.missing {
				rel.result = nil
			}
			if tc.fail {
				rel.err = errors.New("workspace collection failed")
			}
			node := &plan.Node{NodeType: tc.nodeType, ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "t"}, TableDef: &plan.TableDef{Name: "t"}}
			worker := &engine.Node{Addr: "remote:6001", CNCNT: tc.cnCount}
			err := (&remoteScanTombstoneAttacher{c: c, node: node, rel: rel}).attach(worker)
			require.Equal(t, tc.wantPolicy, rel.policy)
			if tc.wantError {
				require.Error(t, err)
				require.Nil(t, worker.Data)
			} else {
				require.NoError(t, err)
				require.NotNil(t, worker.Data)
			}
		})
	}
}

func TestM0SingleRemoteSenderRechecksWorkspace(t *testing.T) {
	enableSharedRemoteGuard(t)
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

func TestM0SingleRemoteDispatchRechecksWorkspace(t *testing.T) {
	enableSharedRemoteGuard(t)
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
	enableSharedRemoteGuard(t)
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

func TestSharedRemoteDispatchRejectsMissingOwners(t *testing.T) {
	enableSharedRemoteGuard(t)
	for _, state := range []string{"process", "transaction", "stamp", "workspace", "visibility"} {
		t.Run(state, func(t *testing.T) {
			c := NewMockCompile(t)
			t.Cleanup(c.proc.Free)
			scope := &Scope{Proc: c.proc, NodeInfo: engine.Node{CNCNT: 1}, DataSource: &Source{node: &plan.Node{NodeType: plan.Node_TABLE_SCAN}}}
			inner := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			c.proc.Base.TxnOperator = &m0DispatchStampedTxn{TxnOperator: inner}
			switch state {
			case "process":
				c.proc = nil
			case "transaction":
				c.proc.Base.TxnOperator = nil
			case "stamp":
				c.proc.Base.TxnOperator = inner
			case "visibility":
				c.proc.Base.TxnOperator = &m0DispatchStampedTxn{TxnOperator: inner, workspace: &Ws{}}
			}
			require.Error(t, validateM0SingleRemoteDispatch(scope, c), "missing %s must prevent dispatch", state)
			validate, err := captureM0SingleRemoteDispatch(scope, c)
			require.Error(t, err)
			require.Nil(t, validate, "a failed admission must not publish a reusable validator")
		})
	}
}

func TestSharedRemoteReadCertificateFailure(t *testing.T) {
	enableSharedRemoteGuard(t)
	require.NoError(t, validateM0SingleRemoteReadData(nil))
	for _, data := range []*m0SingleRemoteReadData{nil, {}, {RelData: readutil.NewBlockListRelationData(0)}} {
		child := &Scope{NodeInfo: engine.Node{Data: data}}
		require.Error(t, validateM0SingleRemoteReadData(&Scope{PreScopes: []*Scope{nil, child}}))
	}
	snapshot := timestamp.Timestamp{PhysicalTime: 200}
	node := &plan.Node{NodeType: plan.Node_TABLE_SCAN, TableDef: &plan.TableDef{Name: "t"}}
	failure := errors.New("statement owner no longer valid")
	data := &m0SingleRemoteReadData{RelData: readutil.NewBlockListRelationData(0), node: node, snapshot: snapshot, validate: func() error { return failure }}
	scope := &Scope{NodeInfo: engine.Node{CNCNT: 1, Data: data}, DataSource: &Source{node: node, Timestamp: snapshot}}
	require.ErrorIs(t, validateM0SingleRemoteReadData(scope), failure)
}

func TestOrdinaryRemoteDispatchKeepsDefaultContract(t *testing.T) {
	old := activeM0Enabled
	activeM0Enabled = false
	t.Cleanup(func() { activeM0Enabled = old })
	scope := &Scope{NodeInfo: engine.Node{CNCNT: 1}, TxnOffset: 7, DataSource: &Source{node: &plan.Node{NodeType: plan.Node_TABLE_SCAN}}}
	c := &Compile{TxnOffset: 7}
	require.NoError(t, validateM0SingleRemoteDispatch(scope, c))
	validate, err := captureM0SingleRemoteDispatch(scope, c)
	require.NoError(t, err)
	require.Nil(t, validate)
	require.NoError(t, validateM0SingleRemoteReadData(scope), "ordinary scans do not require an M0 certificate")
}
