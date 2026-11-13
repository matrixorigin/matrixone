// Copyright 2026 Matrix Origin
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

package compile

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// 每次克隆返回不可变的独立事务，预期值直接来自测试输入而非生产选择逻辑。
type scanSnapshotTestTxn struct {
	client.TxnOperator
	ts     timestamp.Timestamp
	parent *scanSnapshotTestTxn
}

func (op *scanSnapshotTestTxn) Txn() txn.TxnMeta {
	return txn.TxnMeta{SnapshotTS: op.ts}
}
func (op *scanSnapshotTestTxn) Status() txn.TxnStatus          { return txn.TxnStatus_Active }
func (op *scanSnapshotTestTxn) GetWorkspace() client.Workspace { return &Ws{} }
func (op *scanSnapshotTestTxn) CloneSnapshotOp(ts timestamp.Timestamp) client.TxnOperator {
	return &scanSnapshotTestTxn{ts: ts, parent: op}
}

func snapshotScanTestNode(ts *timestamp.Timestamp) *plan.Node {
	return &plan.Node{
		ObjRef:       &plan.ObjectRef{SchemaName: "app", ObjName: "t"},
		TableDef:     &plan.TableDef{Name: "t"},
		ScanSnapshot: &plan.Snapshot{TS: ts, Tenant: &plan.SnapshotTenant{TenantID: 7}},
	}
}

func TestScanSnapshotTransactionsAreRelationLocal(t *testing.T) {
	for _, path := range []string{"datasource", "local-relation", "remote-relation", "preinsert"} {
		t.Run(path, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			parent := &scanSnapshotTestTxn{ts: timestamp.Timestamp{PhysicalTime: 30}}
			proc.Base.TxnOperator = parent
			eng := mock_frontend.NewMockEngine(ctrl)
			db := mock_frontend.NewMockDatabase(ctrl)
			rel := mock_frontend.NewMockRelation(ctrl)
			var opened client.TxnOperator
			var openedCtx context.Context
			eng.EXPECT().Database(gomock.Any(), "app", gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ string, op client.TxnOperator) (engine.Database, error) {
					opened, openedCtx = op, ctx
					return db, nil
				}).AnyTimes()
			db.EXPECT().Relation(gomock.Any(), "t", proc).Return(rel, nil).AnyTimes()
			c := &Compile{proc: proc, e: eng}
			s1 := timestamp.Timestamp{PhysicalTime: 10, LogicalTime: 1}
			s2 := timestamp.Timestamp{PhysicalTime: 10, LogicalTime: 2}
			future := timestamp.Timestamp{PhysicalTime: 40}
			for _, ts := range []*timestamp.Timestamp{&s1, &s2, &s1, nil, {}, &parent.ts, &future, &s2} {
				node := snapshotScanTestNode(ts)
				var err error
				switch path {
				case "datasource":
					opened, openedCtx, err = c.getCompileTableScanDataSourceTxn(&Scope{DataSource: &Source{node: node}})
				case "local-relation", "remote-relation":
					_, _, _, err = c.handleDbRelContext(node, path == "remote-relation")
				case "preinsert":
					node.PreInsertCtx = &plan.PreInsertCtx{Ref: node.ObjRef, TableDef: node.TableDef}
					op, buildErr := constructPreInsert(nil, node, eng, proc)
					err = buildErr
					if op != nil {
						op.Release()
					}
				}
				require.NoError(t, err)
				wantTS, wantAccount := parent.ts, uint32(0)
				if ts != nil && !ts.IsEmpty() && ts.Less(parent.ts) {
					wantTS, wantAccount = *ts, 7
					require.NotSame(t, parent, opened)
				} else {
					require.Same(t, parent, opened)
				}
				require.Equal(t, wantTS, opened.Txn().SnapshotTS, "path=%s scan=%v", path, ts)
				account, err := defines.GetAccountId(openedCtx)
				require.NoError(t, err)
				require.Equal(t, wantAccount, account)
				account, err = defines.GetAccountId(proc.GetTopContext())
				require.NoError(t, err)
				require.Zero(t, account, "源租户不能修改调用方上下文")
				require.Same(t, parent, proc.GetTxnOperator())
			}
		})
	}
}

func TestScanSnapshotReexecutionUsesCurrentParent(t *testing.T) {
	proc := testutil.NewProcess(t)
	ts := timestamp.Timestamp{PhysicalTime: 10}
	node := snapshotScanTestNode(&ts)
	c := &Compile{proc: proc}
	for _, current := range []int64{20, 30} {
		parent := &scanSnapshotTestTxn{ts: timestamp.Timestamp{PhysicalTime: current}}
		proc.Base.TxnOperator = parent
		op, _, err := c.getCompileTableScanDataSourceTxn(&Scope{DataSource: &Source{node: node}})
		require.NoError(t, err)
		require.Equal(t, ts, op.Txn().SnapshotTS)
		require.Same(t, parent, op.(*scanSnapshotTestTxn).parent)
		// 模拟其他消费者留下的 clone；当前扫描不得借用它，reset 后也一样。
		proc.SetCloneTxnOperator(op)
	}
	proc.ResetCloneTxnOperator()
	op, _, err := c.getCompileTableScanDataSourceTxn(&Scope{DataSource: &Source{node: node}})
	require.NoError(t, err)
	require.Same(t, proc.GetTxnOperator(), op.(*scanSnapshotTestTxn).parent)
	require.Nil(t, proc.GetCloneTxnOperator())
}

func TestScanSnapshotTombstonesUseBoundRelation(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	proc.Base.TxnOperator = &scanSnapshotTestTxn{ts: timestamp.Timestamp{PhysicalTime: 30}}
	ts := timestamp.Timestamp{PhysicalTime: 10}
	node := snapshotScanTestNode(&ts)
	rel := mock_frontend.NewMockRelation(ctrl)
	wantErr := context.Canceled
	rel.EXPECT().CollectTombstones(gomock.Any(), 0, engine.TombstoneCollectPolicy(engine.Policy_CollectAllTombstones)).DoAndReturn(
		func(ctx context.Context, _ int, _ engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
			account, err := defines.GetAccountId(ctx)
			require.NoError(t, err)
			require.Equal(t, uint32(7), account)
			return nil, wantErr
		})
	_, err := collectTombstones(&Compile{proc: proc}, node, rel, engine.Policy_CollectAllTombstones)
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, proc.GetCloneTxnOperator(), "已绑定 relation 的路径无需再发布 clone")
}

func TestScanSnapshotConcurrentSelection(t *testing.T) {
	proc := testutil.NewProcess(t)
	parent := &scanSnapshotTestTxn{ts: timestamp.Timestamp{PhysicalTime: 30}}
	proc.Base.TxnOperator = parent
	c := &Compile{proc: proc}
	start := make(chan struct{})
	var wg sync.WaitGroup
	type result struct {
		op  client.TxnOperator
		err error
	}
	results := make([]result, 2)
	times := []timestamp.Timestamp{{PhysicalTime: 10}, {PhysicalTime: 20}}
	for i := range times {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i].op, _, results[i].err = c.getCompileTableScanDataSourceTxn(
				&Scope{DataSource: &Source{node: snapshotScanTestNode(&times[i])}})
		}()
	}
	close(start)
	wg.Wait()
	for i, result := range results {
		require.NoError(t, result.err)
		require.Equal(t, times[i], result.op.Txn().SnapshotTS)
	}
	require.Same(t, parent, proc.GetTxnOperator())
}

func TestScanSnapshotContextAndOpenFailure(t *testing.T) {
	for _, kind := range []string{"snapshot", "publisher", "cluster", "no-tenant"} {
		t.Run(kind, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			proc.Base.TxnOperator = &scanSnapshotTestTxn{ts: timestamp.Timestamp{PhysicalTime: 30}}
			ts := timestamp.Timestamp{PhysicalTime: 10}
			node := snapshotScanTestNode(&ts)
			wantAccount := uint32(7)
			switch kind {
			case "publisher":
				node.ObjRef.PubInfo = &plan.PubInfo{TenantId: 9}
				wantAccount = 9
			case "cluster":
				node.TableDef.TableType = catalog.SystemClusterRel
				wantAccount = 0
			case "no-tenant":
				node.ScanSnapshot.Tenant = nil
				wantAccount = 0
			}
			eng := mock_frontend.NewMockEngine(ctrl)
			openErr := errors.New("数据库打开失败")
			eng.EXPECT().Database(gomock.Any(), "app", gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ string, op client.TxnOperator) (engine.Database, error) {
					account, err := defines.GetAccountId(ctx)
					require.NoError(t, err)
					require.Equal(t, wantAccount, account)
					require.Equal(t, ts, op.Txn().SnapshotTS)
					return nil, openErr
				})
			c := &Compile{proc: proc, e: eng}
			_, _, _, err := c.handleDbRelContext(node, false)
			require.ErrorIs(t, err, openErr)
			// 历史打开失败后，当前扫描仍使用父事务。
			node.ScanSnapshot = nil
			op, _, err := c.getCompileTableScanDataSourceTxn(&Scope{DataSource: &Source{node: node}})
			require.NoError(t, err)
			require.Same(t, proc.GetTxnOperator(), op)
		})
	}
}
