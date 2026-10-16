// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/window"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Literal sources avoid storage and scheduling dependencies while retaining the
// actual WINDOW/LEFT JOIN/two-AGG scalar fallback and its allocation owners.
func TestScalarSetFallbackResourceLifecycle(t *testing.T) {
	const query = `select o.id,cast((select i.v from (select 1 id,cast(-0e0 as double) v union all select 2,cast(-0e0 as double)) i where i.id>o.id union select i.v from (select 1 id,cast(-0e0 as double) v union all select 2,cast(-0e0 as double)) i where i.id>o.id) as varchar) from (select 0 id union all select 3) o order by o.id`
	budget := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
	for index, mode := range []string{"normal", "budget-rejection", "cancel-after-materialization", "cancel-window-input", "cancel-group-before-reload", "corrupt-spill-reload", "forced-spill-control", "fresh-after-failures"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(defines.AttachAccountId(context.Background(), catalog.System_Account))
			defer cancel()
			baseFS := testutil.NewFS(t)
			t.Cleanup(func() { baseFS.Close(context.Background()) })
			local, err := fileservice.Get[fileservice.MutableFileService](baseFS, defines.LocalFileServiceName)
			require.NoError(t, err)
			observed := &scalarFallbackSpillFS{MutableFileService: local, live: make(map[string]*os.File), corrupt: mode == "corrupt-spill-reload"}
			if mode == "cancel-group-before-reload" {
				observed.cancelOnReload = cancel
			}
			shared, err := fileservice.Get[fileservice.FileService](baseFS, defines.SharedFileServiceName)
			require.NoError(t, err)
			etl, err := fileservice.Get[fileservice.FileService](baseFS, defines.ETLFileServiceName)
			require.NoError(t, err)
			fs, err := fileservice.NewFileServices("", observed, shared, etl)
			require.NoError(t, err)
			proc := testutil.NewProcess(t, testutil.WithFileService(fs))
			t.Cleanup(proc.Free)
			proc.Ctx = ctx
			proc.ReplaceTopCtx(ctx)
			cap := uint64(64 << 20)
			if mode == "budget-rejection" {
				cap = 4096
			}
			generation, err := budget.OpenGenerationWithCap(uint64(index+1), cap)
			require.NoError(t, err)
			t.Cleanup(generation.Close)
			require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
			registry, err := generation.AllocationAccountRegistry()
			require.NoError(t, err)
			compilerCtx := plan2.NewEmptyCompilerContext(proc)
			compilerCtx.SetContext(ctx)
			stmts, err := mysql.Parse(ctx, query, 1)
			require.NoError(t, err)
			t.Cleanup(stmts[0].Free)
			logical, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(stmts[0], false)
			require.NoError(t, err)
			forceSpill := strings.Contains(mode, "spill") || mode == "cancel-group-before-reload"
			windows, aggs, lefts := 0, 0, 0
			for _, n := range logical.Nodes {
				if n.NodeType == plan.Node_WINDOW {
					windows++
				}
				if n.NodeType == plan.Node_AGG {
					if forceSpill {
						n.SpillMem = 1
					}
					aggs++
				}
				if n.NodeType == plan.Node_JOIN && n.JoinType == plan.Node_LEFT {
					lefts++
				}
			}
			require.Positive(t, windows)
			require.GreaterOrEqual(t, aggs, 2)
			require.Positive(t, lefts)
			proc.Base.TxnClient, proc.Base.TxnOperator = newTestTxnClientAndOpWithIsolation(gomock.NewController(t), txn.TxnIsolation_RC)
			var got []string
			var gotIDs []int64
			cancelOutput := mode == "cancel-after-materialization"
			fill := func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
				if bat == nil {
					return nil
				}
				ids := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])
				for i := 0; i < bat.RowCount(); i++ {
					gotIDs = append(gotIDs, ids[i])
					if bat.Vecs[1].IsNull(uint64(i)) {
						got = append(got, "NULL")
					} else {
						got = append(got, string(bat.Vecs[1].GetBytesAt(i)))
					}
					if ids[i] != 0 && ids[i] != 3 {
						return fmt.Errorf("unexpected outer identity %d", ids[i])
					}
				}
				if cancelOutput {
					cancel()
					return context.Canceled
				}
				return nil
			}
			c := NewCompile("test", "test", query, "", "", newStubEngine(), proc, stmts[0], false, nil, time.Now())
			released := false
			t.Cleanup(func() {
				if !released {
					c.FreeOperator()
					c.Release()
				}
			})
			c.SetIsPrepare(true)
			require.NoError(t, c.Compile(ctx, &plan.Plan{Plan: &plan.Plan_Query{Query: logical}}, fill))
			var injection *scalarFallbackCancelInput
			if mode == "cancel-window-input" {
				require.True(t, c.IsTpQuery(), "test injection supports the TP fixture only")
				var visitScope func(*Scope)
				visitScope = func(scope *Scope) {
					require.NoError(t, vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
						if injection != nil {
							return nil
						}
						selectParent := false
						switch op.(type) {
						case *window.Window:
							selectParent = mode == "cancel-window-input"
						}
						if !selectParent || len(op.GetOperatorBase().Children) != 1 {
							return nil
						}
						child := op.GetOperatorBase().GetChildren(0)
						if _, owned := child.(executionAllocationAccountOwner); owned {
							return nil
						}
						injection = &scalarFallbackCancelInput{Operator: child, cancel: cancel}
						op.GetOperatorBase().SetChild(injection, 0)
						return nil
					}))
					for _, pre := range scope.PreScopes {
						visitScope(pre)
					}
				}
				for _, scope := range c.scopes {
					visitScope(scope)
				}
				require.NotNil(t, injection, "find unowned child edge in actual fallback")
			}
			_, err = c.Run(0)
			switch mode {
			case "budget-rejection":
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				t.Logf("budget rejection: %v", err)
				require.Empty(t, got)
			case "cancel-group-before-reload":
				require.ErrorIs(t, err, context.Canceled)
				require.True(t, observed.cancelledReload)
				require.Empty(t, got)
			case "cancel-window-input":
				require.ErrorIs(t, err, context.Canceled)
				require.True(t, injection.fired, "must reach boundary after delivering nonempty input")
				require.Empty(t, got)
			case "corrupt-spill-reload":
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
				require.True(t, observed.corrupted, "must reopen after spill writer flush/close")
				require.Empty(t, got)
			case "cancel-after-materialization":
				require.ErrorIs(t, err, context.Canceled)
				require.NotEmpty(t, got)
			default:
				require.NoError(t, err)
				require.Equal(t, []string{"-0", "NULL"}, got)
				require.Equal(t, []int64{0, 3}, gotIDs)
			}
			first := generation.Snapshot()
			require.Zero(t, first.Used)
			require.Zero(t, first.SpillDiskUsed)
			require.Zero(t, first.SpillFDUsed)
			require.Zero(t, registry.LiveAllocationMetadata())
			assertScalarFallbackSpillClosed(t, observed, "first-attempt")
			tp := c.IsTpQuery()
			if !tp {
				require.ErrorIs(t, c.Reset(proc, time.Now(), fill, query), cantCompileForPrepareErr)
			} else if mode != "budget-rejection" {
				if injection != nil {
					injection.enabledOff = true
				}
				observed.corrupt = false
				observed.cancelOnReload = nil
				// A new top context plus real Compile.Reset exercises cached TP
				// reuse after success and after downstream cancellation.
				require.Zero(t, generation.Snapshot().Used)
				got = nil
				gotIDs = nil
				cancelOutput = false
				proc.ReplaceTopCtx(defines.AttachAccountId(context.Background(), catalog.System_Account))
				require.NoError(t, c.Reset(proc, time.Now(), fill, query))
				_, err = c.Run(0)
				require.NoError(t, err)
				require.Equal(t, []string{"-0", "NULL"}, got)
				require.Equal(t, []int64{0, 3}, gotIDs)
			}
			require.Zero(t, generation.Snapshot().Used, "terminal Run must release reservations before fixture destruction")
			require.Zero(t, registry.LiveAllocationMetadata())
			assertScalarFallbackSpillClosed(t, observed, "final-attempt")
			observed.mu.Lock()
			created, opened, removed, peak, corrupted, liveCount := observed.created, observed.opened, observed.removed, observed.peak, observed.corrupted, len(observed.live)
			observed.mu.Unlock()
			require.Zero(t, liveCount)
			if forceSpill {
				require.Positive(t, created)
				require.Positive(t, opened)
				require.Equal(t, created, removed)
			}
			t.Logf("spill-files mode=%s created=%d reopened=%d removed=%d peak-live=%d corrupted=%v", mode, created, opened, removed, peak, corrupted)

			c.FreeOperator()
			c.Release()
			released = true
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
			snapshot := generation.Snapshot()
			require.Zero(t, snapshot.Used)
			require.Zero(t, snapshot.SpillDiskUsed)
			require.Zero(t, snapshot.SpillFDUsed)
			require.Zero(t, snapshot.MemoryGrowthParticipants)
			require.Zero(t, registry.LiveAllocationMetadata())
			if mode != "budget-rejection" {
				require.Positive(t, snapshot.PeakUsed)
			}
			t.Logf("compiled fallback mode=%s TP=%v terminal=%+v", mode, tp, snapshot)
		})
		require.Zero(t, budget.AggregateUsed(), "fresh generation must not inherit live reservations")
	}
}

// The original child remains the sole destruction owner. Only unaccounted
// edges are decorated, so ownership discovery and draining interfaces stay
// visible on every real allocation owner. TP avoids operator-clone assertions.
type scalarFallbackCancelInput struct {
	vm.Operator
	cancel                  context.CancelFunc
	seen, fired, enabledOff bool
}

func (input *scalarFallbackCancelInput) Call(proc *process.Process) (vm.CallResult, error) {
	result, err := input.Operator.Call(proc)
	if err != nil || input.enabledOff {
		return result, err
	}
	if input.seen {
		input.fired = true
		input.cancel()
		return vm.CancelResult, context.Canceled
	}
	if result.Batch != nil && !result.Batch.IsEmpty() {
		input.seen = true
	}
	return result, nil
}

// The fixture owns this local filesystem. OpenFile is GROUP's synchronous
// post-flush/reopen boundary, so corruption never races the writer.
type scalarFallbackSpillFS struct {
	cancelOnReload  context.CancelFunc
	cancelledReload bool
	fileservice.MutableFileService
	mu                             sync.Mutex
	live                           map[string]*os.File
	handles                        []*os.File
	created, opened, removed, peak int
	corrupt, corrupted             bool
}

func (fs *scalarFallbackSpillFS) CreateFile(ctx context.Context, name string) (*os.File, error) {
	file, err := fs.MutableFileService.CreateFile(ctx, name)
	if err != nil {
		return nil, err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.live[name] = file
	fs.handles = append(fs.handles, file)
	fs.created++
	if len(fs.live) > fs.peak {
		fs.peak = len(fs.live)
	}
	return file, nil
}
func (fs *scalarFallbackSpillFS) OpenFile(ctx context.Context, name string) (*os.File, error) {
	file, err := fs.MutableFileService.OpenFile(ctx, name)
	if err != nil {
		return nil, err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.opened++
	fs.handles = append(fs.handles, file)
	if fs.cancelOnReload != nil {
		fs.cancelledReload = true
		fs.cancelOnReload()
		file.Close()
		return nil, context.Canceled
	}
	if fs.corrupt && !fs.corrupted {
		if err = file.Truncate(1); err != nil {
			file.Close()
			return nil, err
		}
		fs.corrupted = true
	}
	return file, nil
}
func (fs *scalarFallbackSpillFS) RemoveFile(ctx context.Context, name string) error {
	err := fs.MutableFileService.RemoveFile(ctx, name)
	if err != nil {
		return err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.live, name)
	fs.removed++
	return nil
}

func assertScalarFallbackSpillClosed(t *testing.T, fs *scalarFallbackSpillFS, phase string) {
	t.Helper()
	fs.mu.Lock()
	liveCount := len(fs.live)
	handles := append([]*os.File(nil), fs.handles...)
	created, opened, removed := fs.created, fs.opened, fs.removed
	fs.mu.Unlock()
	require.Zero(t, liveCount)
	require.Equal(t, created, removed)
	for _, handle := range handles {
		_, err := handle.Stat()
		require.ErrorIs(t, err, os.ErrClosed)
	}
	t.Logf("spill-terminal phase=%s created=%d reopened=%d removed=%d closed-handles=%d", phase, created, opened, removed, len(handles))
}
