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

package materialized

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestMaterializedSourceAllocationOwner(t *testing.T) {
	for _, owner := range []mpool.AllocationOwner{0, mpool.AllocationOwnerCTE, mpool.AllocationOwnerTop} {
		for _, spill := range []bool{false, true} {
			t.Run(owner.String()+map[bool]string{false: "/memory", true: "/spill"}[spill], func(t *testing.T) {
				mp := mpool.MustNewZeroNoFixed()
				t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
				registry, err := mpool.NewAllocationAccountRegistry(1, 1<<14)
				require.NoError(t, err)
				account, err := registry.Open(math.MaxInt64)
				require.NoError(t, err)
				budget := newTestSpillBudget(math.MaxUint64, math.MaxUint64, 1)
				config := budget.config(testSpillFactory(t.TempDir()))
				config.AllocationAccount = account
				config.AllocationOwner = owner
				source := NewSource(1)
				if spill {
					source.memoryLimit = 0
				}
				t.Cleanup(source.Close)
				require.NoError(t, source.Begin(mp, config))
				input := testInt64Batch(t, mp, 42)
				t.Cleanup(func() { input.Clean(mp) })
				require.NoError(t, source.Append(input))
				source.Finish(nil)
				out, end, err := source.Next(context.Background(), 0, 0)
				require.NoError(t, err)
				require.False(t, end)
				require.Equal(t, []int64{42}, vector.MustFixedColWithTypeCheck[int64](out.Vecs[0]))
				out.Clean(mp)
				source.ReleaseReader(0)
				memory, disk, fd := budget.usage()
				require.Zero(t, memory)
				require.Zero(t, disk)
				require.Zero(t, fd)
				snapshot, first, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
				require.True(t, first)
				require.Zero(t, snapshot.Used)
				require.Len(t, snapshot.Owners, 1)
				want := owner
				if want == 0 {
					want = mpool.AllocationOwnerCTE
				}
				require.Equal(t, want, snapshot.Owners[0].Owner)
				require.Positive(t, snapshot.Owners[0].Peak)
			})
		}
	}
}

func TestMaterializedSourceRejectsUnsupportedAllocationOwner(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)
	budget := newTestSpillBudget(math.MaxUint64, math.MaxUint64, 1)
	config := budget.config(nil)
	config.AllocationOwner = mpool.AllocationOwner(255)
	source := NewSource(1)
	defer source.Close()
	require.ErrorIs(t, source.Begin(mp, config), mpool.ErrAllocationAccountInvalid)
	config.AllocationOwner = mpool.AllocationOwnerTop
	require.NoError(t, source.Begin(mp, config))
	source.Finish(nil)
	source.ReleaseReader(0)
	require.Zero(t, mp.CurrNB())
}
