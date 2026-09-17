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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/adaptivetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/unionall"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

type observedWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *observedWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestLazyBranchCompletionWaitsForCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	observed := &observedWaitContext{Context: ctx, entered: make(chan struct{})}
	completion := newLazyBranchCompletion()
	result := make(chan error, 1)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		result <- completion.wait(observed)
	}()
	t.Cleanup(func() {
		cancel()
		<-workerDone
	})
	select {
	case <-observed.entered:
	case <-ctx.Done():
		t.Fatal("wait 未到达完成屏障")
	}
	select {
	case err := <-result:
		t.Fatalf("分支清理尚未完成，wait 却返回：%v", err)
	default:
	}
	want := errors.New("late cleanup error")
	completion.finish(scopeRunResult{err: want})
	select {
	case err := <-result:
		require.ErrorIs(t, err, want)
	case <-ctx.Done():
		t.Fatal("分支完成后 wait 未释放")
	}
	// 结果可重复观察，不消耗 MergeRun 的聚合错误证据。
	require.ErrorIs(t, completion.wait(context.Background()), want)
}

func TestLazyBranchCompletionCancellationDoesNotRetireProducer(t *testing.T) {
	completion := newLazyBranchCompletion()
	ctx, cancel := context.WithCancelCause(context.Background())
	want := errors.New("query canceled")
	cancel(want)
	require.ErrorIs(t, completion.wait(ctx), want)
	select {
	case <-completion.done:
		t.Fatal("取消 wait 不得冒充 producer 清理完成")
	default:
	}
	completion.finish(scopeRunResult{})
	require.NoError(t, completion.wait(context.Background()))
}

func TestInstallLazyBranchLifecycle(t *testing.T) {
	a := adaptivetop.NewArgument()
	defer a.Release()
	clear, deferFirst, err := installSequentialBranchStarter(a, func(int) error { return nil }, func(int) error { return nil })
	require.NoError(t, err)
	require.True(t, deferFirst)
	clear()
	_, _, err = installSequentialBranchStarter(a, func(int) error { return nil }, nil)
	require.ErrorContains(t, err, "completion barrier")

	u := unionall.NewArgument()
	defer u.Release()
	clear, deferFirst, err = installSequentialBranchStarter(u, func(int) error { return nil }, nil)
	require.NoError(t, err)
	require.False(t, deferFirst, "原 UNION ALL 仍立即启动第一分支")
	clear()

	_, _, err = installSequentialBranchStarter(colexec.NewMockOperator(), nil, nil)
	require.ErrorContains(t, err, "no branch starter")
	u.AppendChild(a)
	_, _, err = installSequentialBranchStarter(u, nil, nil)
	require.ErrorContains(t, err, "multiple branch starters")
	u.Children = nil
}

func TestAdaptiveLazyScopeSelectsLocalCandidate(t *testing.T) {
	for _, tc := range []struct {
		name         string
		limit        uint64
		left, right  []int8
		wantPrepares [2]int32
	}{
		{"zero starts neither", 0, []int8{1}, []int8{2}, [2]int32{0, 0}},
		{"full post skips rest", 2, []int8{1, 2}, []int8{3, 4}, [2]int32{1, 0}},
		{"short post advances", 2, []int8{1}, []int8{3, 4}, [2]int32{1, 1}},
		{"empty terminal finishes", 2, nil, nil, [2]int32{1, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newLazyUnionAllTestCompile(t)
			var leftPrepares, rightPrepares atomic.Int32
			root := c.compileUnionAll(&planpb.Node{},
				[]*Scope{newPreparingLazyUnionAllLeaf(c, newLazyUnionAllInt8Batch(c, tc.left...), &leftPrepares)},
				[]*Scope{newPreparingLazyUnionAllLeaf(c, newLazyUnionAllInt8Batch(c, tc.right...), &rightPrepares)}, true)[0]
			old := root.RootOp.(*unionall.UnionAll)
			child := old.GetChildren(0).(*merge.Merge)
			child.WithPartial(0, 0)
			a := adaptivetop.NewArgument()
			a.Branches = 2
			a.LimitExpr = plan2.MakePlan2Uint64ConstExprWithType(tc.limit)
			a.OperatorInfo = old.OperatorInfo
			a.AppendChild(child)
			old.Children = nil
			old.Release()
			root.RootOp = a
			registry, err := mpool.NewAllocationAccountRegistry(1, 1<<14)
			require.NoError(t, err)
			account, err := registry.Open(math.MaxInt64)
			require.NoError(t, err)
			require.NoError(t, a.SetAllocationAccount(account))
			t.Cleanup(func() {
				defer func() {
					c.proc.Free()
					require.Zero(t, c.proc.Mp().CurrNB())
				}()
				defer root.release()
				root.FreeOperator(c)
				require.NoError(t, a.ClearAllocationAccount(account))
				snapshot, first, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
				require.True(t, first)
				require.Zero(t, snapshot.Used)
			})
			c.scopes = []*Scope{root}
			c.InitPipelineContextToExecuteQuery()
			require.NoError(t, root.MergeRun(c))
			require.Equal(t, tc.wantPrepares[0], leftPrepares.Load())
			require.Equal(t, tc.wantPrepares[1], rightPrepares.Load())
		})
	}
}
