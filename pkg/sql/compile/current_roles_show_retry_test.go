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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mockfrontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	executortest "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// TestShowRolesCompiledCacheGeneration uses operators compiled from an actual
// SHOW plan. The test explicitly retires the old cache and initializes the
// retry context; it does not exercise Compile.Run automatic retry.
func TestShowRolesCompiledCacheGeneration(t *testing.T) {
	mock := plan2.NewMockOptimizer(false)
	cc := mock.CurrentContext()
	proc := cc.GetProcess()
	ctrl := gomock.NewController(t)
	tx := mockfrontend.NewMockTxnOperator(ctrl)
	ws := mockfrontend.NewMockWorkspace(ctrl)
	proc.Base.TxnOperator = tx
	tx.EXPECT().GetWorkspace().Return(ws).AnyTimes()
	tx.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte("txn")}).AnyTimes()
	ws.EXPECT().GetSnapshotWriteOffset().Return(0).AnyTimes()
	ws.EXPECT().WriteOffset().Return(uint64(0)).AnyTimes()
	ws.EXPECT().UpdateSnapshotWriteOffset().AnyTimes()
	ws.EXPECT().GetHaveDDL().Return(false).AnyTimes()
	proc.Base.SessionInfo.CurrentRoleContext = process.CurrentRoleContext{Mode: 1, AccountID: 7, AccountName: "tenant", UserID: 8, UserName: "alice", PrimaryRoleID: 9}
	started, canceled := make(chan struct{}), make(chan struct{})
	nextErr := errors.New("second generation reached catalog")
	sql := executortest.NewMockSQLExecutor(ctrl)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previousSQL, hadSQL := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	previousVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadSQL {
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, previousSQL)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, sql)
		}
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previousVersion)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12))
		}
	})
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, sql)
	gomock.InOrder(
		sql.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ string, _ executor.Options) (executor.Result, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			return executor.Result{}, ctx.Err()
		}),
		sql.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(executor.Result{}, nextErr),
	)
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12))
	stmt, err := mysql.ParseOne(cc.GetContext(), "show tables from tpch", 1)
	require.NoError(t, err)
	defer stmt.Free()
	pn, err := plan2.BuildPlan(cc, stmt, false)
	require.NoError(t, err)
	eng := newStubEngine()
	for _, node := range pn.GetQuery().GetNodes() {
		if node.GetNodeType() == 0 {
			continue
		}
		ref := node.GetObjRef()
		if ref == nil {
			continue
		}
		db := eng.dbs[ref.GetSchemaName()]
		if db == nil {
			db = newStubDatabase(ref.GetSchemaName())
			eng.dbs[ref.GetSchemaName()] = db
		}
		db.rels[ref.GetObjName()] = newStubRelation(ref.GetObjName())
	}
	c := NewCompile("127.0.0.1:1", "tpch", "show tables from tpch", "", "", eng, proc, stmt, false, nil, time.Now())
	defer c.Release()
	require.NoError(t, c.Compile(cc.GetContext(), pn, nil))
	proc.ReplaceTopCtx(defines.AttachAccountId(context.Background(), 7))
	c.InitPipelineContextToExecuteQuery()
	require.NotNil(t, c.currentRolesCache)
	old := c.currentRolesCache
	findTF := func(c *Compile) (*table_function.TableFunction, *process.Process) {
		var tf *table_function.TableFunction
		var p *process.Process
		for _, root := range c.scopes {
			walkScopeTree(root, func(s *Scope) bool {
				_ = vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
					if f, ok := op.(*table_function.TableFunction); ok && f.FuncName == "mo_current_roles" && tf == nil {
						tf, p = f, s.Proc
					}
					return nil
				})
				return false
			})
		}
		require.NotNil(t, tf)
		return tf, p
	}
	tf, tfProc := findTF(c)
	require.NoError(t, tf.Prepare(tfProc))
	done := make(chan error, 1)
	go func() { _, err := tf.Call(tfProc); done <- err }()
	defer func() {
		c.closeCurrentRolesExecutionCache()
		proc.Cancel(nil)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("loader did not stop")
		}
		tf.Free(tfProc, false, nil)
	}()
	select {
	case <-started:
	case err := <-done:
		done <- err
		t.Fatalf("loader returned before start: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("loader did not start")
	}
	c.closeCurrentRolesExecutionCache()
	select {
	case <-canceled:
	case <-time.After(10 * time.Second):
		t.Fatal("loader not canceled")
	}
	select {
	case err := <-done:
		done <- err
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("loader did not return")
	}
	next, err := c.buildRetryCompile(false)
	require.NoError(t, err)
	defer next.Release()
	next.InitPipelineContextToRetryQuery()
	defer next.closeCurrentRolesExecutionCache()
	require.NotNil(t, next.currentRolesCache)
	require.NotSame(t, old, next.currentRolesCache)
	nextTF, nextProc := findTF(next)
	require.NoError(t, nextTF.Prepare(nextProc))
	defer nextTF.Free(nextProc, false, nil)
	_, err = nextTF.Call(nextProc)
	require.ErrorIs(t, err, nextErr)
}
