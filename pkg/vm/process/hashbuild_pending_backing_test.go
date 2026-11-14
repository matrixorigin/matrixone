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

package process

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type pausedMemoryCapacityController struct {
	mpool.AllocationCapacityController
	admitted chan struct{}
	resume   chan struct{}
	paused   atomic.Bool
}

func (c *pausedMemoryCapacityController) AcquireAllocationCapacity(size uint64) error {
	if err := c.AllocationCapacityController.AcquireAllocationCapacity(size); err != nil {
		return err
	}
	if c.paused.CompareAndSwap(false, true) {
		close(c.admitted)
		<-c.resume
	}
	return nil
}

func TestExecutionMemoryRefreshPreservesPendingBacking(t *testing.T) {
	for _, tc := range []struct {
		name               string
		recovery           bool
		commitDuringSample bool
	}{
		{name: "ordinary"},
		{name: "recovery", recovery: true},
		{name: "completion during sample", commitDuringSample: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := MustNewHashBuildBudget(1000, 800)
			now := time.Unix(1, 0)
			available := uint64(300)
			var duringSample func()
			budget.memoryHeadroomProvider = func() (uint64, bool) {
				if duringSample != nil {
					duringSample()
				}
				return available, true
			}
			budget.memoryHeadroomSafety = 100
			budget.memoryHeadroomTTL = time.Second
			budget.memoryHeadroomNow = func() time.Time { return now }
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			registry, err := generation.AllocationAccountRegistry()
			require.NoError(t, err)
			mp := mpool.MustNewZero()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			controller := &pausedMemoryCapacityController{
				AllocationCapacityController: generation,
				admitted:                     make(chan struct{}), resume: make(chan struct{}),
			}
			var defaultController mpool.AllocationCapacityController = controller
			if tc.recovery {
				defaultController = generation
			}
			account, err := registry.OpenWithController(800, defaultController)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, _, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
				require.Zero(t, registry.CommittedCapacity())
				require.Zero(t, generation.Used())
			})
			class := mpool.AllocationCapacityClassDefault
			if tc.recovery {
				recovery, err := NewHashBuildRecoveryCapacity(generation)
				require.NoError(t, err)
				controller.AllocationCapacityController = recovery
				class, err = account.RegisterCapacityController(controller)
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, recovery.Close())
					require.NoError(t, account.UnregisterCapacityController(class, controller))
				})
				require.NoError(t, recovery.EnsureCapacity(200))
			}
			type result struct {
				buf []byte
				err error
			}
			finished := make(chan result, 1)
			go func() {
				buf, err := mp.AllocAccountedWithCapacityClass(200, account, mpool.AllocationOwner(1), 1, class)
				finished <- result{buf, err}
			}()
			var resumeOnce sync.Once
			var first result
			var joinOnce sync.Once
			join := func() {
				resumeOnce.Do(func() { close(controller.resume) })
				joinOnce.Do(func() { first = <-finished })
			}
			t.Cleanup(func() { join(); mp.Free(first.buf) })
			select {
			case <-controller.admitted:
			case <-time.After(5 * time.Second):
				t.Fatal("allocation did not reach the admission boundary")
			}
			require.Zero(t, mp.CurrNB(), "admitted allocation has no physical backing yet")
			now = now.Add(2 * time.Second)
			if tc.commitDuringSample {
				duringSample = join
			}
			second, err := mp.AllocAccounted(200, account, mpool.AllocationOwner(1), 2)
			duringSample = nil
			t.Cleanup(func() { mp.Free(second) })
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.ErrorIs(t, err, ErrHashBuildBudgetAdmission)
			require.Equal(t, uint64(200), generation.Used())
			join()
			require.NoError(t, first.err)
			require.Equal(t, uint64(200), registry.CommittedCapacity())
			// Once backing exists, a new physical sample may grant genuinely free
			// space again. Pending protection must not become a sticky lower cap.
			available = 500
			now = now.Add(2 * time.Second)
			grown, err := mp.AllocAccounted(200, account, mpool.AllocationOwner(1), 2)
			require.NoError(t, err)
			t.Cleanup(func() { mp.Free(grown) })
		})
	}
}
