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
	"errors"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestVectorQueryConcurrentDataSourceFolds(t *testing.T) {
	for _, name := range []string{"active_local_reader", "remote_fold_registration", "snapshot_eager_before_selectors"} {
		t.Run(name, func(t *testing.T) {
			remote := name == "remote_fold_registration"
			c := NewMockCompile(t)
			c.e = newStubEngineForGenerateNodes("fold_init", "t")
			c.proc.Base.TxnClient, c.proc.Base.TxnOperator = newTestTxnClientAndOp(gomock.NewController(t))
			t.Cleanup(func() {
				c.freeFilterExprExecutors()
				require.Zero(t, c.proc.Mp().CurrNB())
			})

			makeScope := func(value int64) *Scope {
				col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "id", ColPos: 0}}}
				expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, ">=", []*plan.Expr{col, plan2.MakePlan2Int64ConstExprWithType(value)})
				require.NoError(t, err)
				return &Scope{Proc: c.proc, DataSource: &Source{node: &plan.Node{
					ObjRef:          &plan.ObjectRef{SchemaName: "fold_init", ObjName: "t"},
					TableDef:        &plan.TableDef{Name: "t", Cols: []*plan.ColDef{{Name: "id", Typ: col.Typ}}},
					FilterList:      []*plan.Expr{expr},
					BlockFilterList: []*plan.Expr{plan2.DeepCopyExpr(expr)},
				}}}
			}
			first, second, reader := makeScope(11), makeScope(23), makeScope(37)
			scopes := []*Scope{first, second}
			values := []int64{11, 23}
			if name == "snapshot_eager_before_selectors" {
				// planner 的 snapshot admission 将这些扫描保留在普通域：先串行
				// 初始化同一旧快照，再并发启动无快照 selector。不能让事务锁藏进 Fold 锁。
				owner := &foldSnapshotTxn{TxnOperator: c.proc.Base.TxnOperator, ts: timestamp.Timestamp{PhysicalTime: 2}}
				c.proc.Base.TxnOperator = owner
				for _, value := range []int64{41, 43} {
					s := makeScope(value)
					s.DataSource.node.ScanSnapshot = &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1}}
					scopes = append(scopes, s)
					values = append(values, value)
				}
				require.NoError(t, (&Scope{PreScopes: scopes[2:]}).InitAllDataSource(c))
				require.Equal(t, 1, owner.clones)
				require.Equal(t, int64(1), c.proc.GetCloneTxnOperator().Txn().SnapshotTS.PhysicalTime)
			}
			if remote {
				reader.IsRemote = true
				reader.DataSource.BlockFilterList = reader.DataSource.node.BlockFilterList
			} else {
				require.NoError(t, reader.InitAllDataSource(c))
			}

			// 两个 selector 的激活边界可同时初始化 datasource；已有 pipeline
			// 还可能同时求值 block filters。启动屏障后不增加串行化测试锁。
			start := make(chan struct{})
			errors := make(chan error, 3)
			var wg sync.WaitGroup
			run := func(f func() error) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					errors <- f()
				}()
			}
			run(func() error { return first.InitAllDataSource(c) })
			run(func() error { return second.InitAllDataSource(c) })
			var readerFilters []*plan.Expr
			run(func() (err error) {
				readerFilters, err = reader.handleRuntimeFilters(c, nil)
				return err
			})
			close(start)
			wg.Wait()
			close(errors)
			for err := range errors {
				require.NoError(t, err)
			}

			ids := make(map[int32]bool)
			check := func(expr *plan.Expr, value int64) {
				fold := expr.GetF().Args[1].GetFold()
				require.NotNil(t, fold)
				require.False(t, ids[fold.Id], "Fold ID must identify exactly one registered executor")
				ids[fold.Id] = true
				require.Len(t, fold.Data, 8)
				require.Equal(t, value, types.DecodeInt64(fold.Data))
			}
			for i, scope := range scopes {
				value := values[i]
				filters, err := scope.handleRuntimeFilters(c, nil)
				require.NoError(t, err)
				check(scope.DataSource.FilterList[0], value)
				check(filters[0], value)
				require.False(t, plan2.HasFoldExprForList(scope.DataSource.node.FilterList))
			}
			check(readerFilters[0], 37)
			if !remote {
				check(reader.DataSource.FilterList[0], 37)
			} else {
				require.False(t, plan2.HasFoldExprForList(reader.DataSource.BlockFilterList), "remote template stays reusable")
			}
			require.Len(t, c.filterExprExes, len(ids), "no duplicate or lost Fold registrations")
			// 已 join 后复用：不重新分配本地 scan 的 Fold ID，也不保留旧求值状态。
			c.resetFilterExprExecutors()
			for _, scope := range []*Scope{first, second} {
				// stubRelation 没有可重置的真实事务句柄，重新取得同一不可变 stub。
				scope.DataSource.Rel = nil
				require.NoError(t, scope.InitAllDataSource(c))
				_, err := scope.handleRuntimeFilters(c, nil)
				require.NoError(t, err)
			}
			require.Len(t, c.filterExprExes, len(ids), "prepared reuse keeps existing Fold IDs")
		})
	}
}

type foldSnapshotTxn struct {
	client.TxnOperator
	ts     timestamp.Timestamp
	clones int
}

func (t *foldSnapshotTxn) Txn() txn.TxnMeta {
	meta := t.TxnOperator.Txn()
	meta.SnapshotTS = t.ts
	return meta
}

func (t *foldSnapshotTxn) CloneSnapshotOp(ts timestamp.Timestamp) client.TxnOperator {
	t.clones++
	return &foldSnapshotTxn{TxnOperator: t.TxnOperator, ts: ts}
}

func TestCompileFoldLockTerminalPaths(t *testing.T) {
	for _, name := range []string{"replace_error", "replace_panic", "eval_error", "eval_panic"} {
		t.Run(name, func(t *testing.T) {
			c := NewMockCompile(t)
			t.Cleanup(func() { c.freeFilterExprExecutors(); require.Zero(t, c.proc.Mp().CurrNB()) })
			switch name {
			case "replace_error":
				col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
				expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, ">=", []*plan.Expr{col, plan2.MakePlan2Int64ConstExprWithType(1)})
				require.NoError(t, err)
				expr.GetF().Args[1] = &plan.Expr{Typ: col.Typ}
				require.Error(t, c.replaceFoldExpr(c.proc, expr))
			case "replace_panic":
				expr := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: -1}}}}
				require.Panics(t, func() { _ = c.replaceFoldExpr(c.proc, expr) })
			case "eval_error":
				sentinel := errors.New("fold evaluation failed")
				c.filterExprExes = []colexec.ExpressionExecutor{&foldExecutorProbe{err: sentinel}}
				require.ErrorIs(t, c.evalFoldExpr(c.proc, &plan.Expr{Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{Id: 0}}}), sentinel)
			case "eval_panic":
				require.Panics(t, func() {
					_ = c.evalFoldExpr(c.proc, &plan.Expr{Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{Id: 0}}})
				})
			}
			unlocked := c.filterExprMu.TryLock()
			// 只有当前测试线程访问该锁；回归时也释放遗留锁，避免 cleanup 卡住。
			c.filterExprMu.Unlock()
			require.True(t, unlocked, "error and panic must release the Compile Fold lock")
		})
	}
}

type foldExecutorProbe struct {
	colexec.ExpressionExecutor
	err           error
	resets, frees int
}

func (e *foldExecutorProbe) Eval(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return nil, e.err
}
func (e *foldExecutorProbe) ResetForNextQuery() { e.resets++ }
func (e *foldExecutorProbe) Free()              { e.frees++ }

func TestCompileFoldExecutorLifecycle(t *testing.T) {
	executor := &foldExecutorProbe{}
	c := &Compile{filterExprExes: []colexec.ExpressionExecutor{executor}}
	for generation := 1; generation <= 3; generation++ {
		c.resetFilterExprExecutors()
		require.Equal(t, generation, executor.resets)
		require.Len(t, c.filterExprExes, 1)
	}
	c.freeFilterExprExecutors()
	require.Empty(t, c.filterExprExes)
	require.Equal(t, 1, executor.frees)
	c.freeFilterExprExecutors()
	require.Equal(t, 1, executor.frees, "Compile release must not free a cleared registry twice")
}
