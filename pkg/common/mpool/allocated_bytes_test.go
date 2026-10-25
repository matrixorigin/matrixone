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

package mpool

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAllocatedBytesOwnership(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "go", true: "native"}[native], func(t *testing.T) {
			owner := MustNew("allocation-owner")
			other := MustNew("allocation-consumer")
			defer DeleteMPool(owner)
			defer DeleteMPool(other)
			b, err := owner.Alloc(129, native)
			require.NoError(t, err)
			require.Equal(t, int64(cap(b)), owner.AllocatedBytes())
			if !native {
				require.Zero(t, owner.CurrNB(), "public native statistics retain their semantics")
			}
			b, err = owner.Grow(b, 9001, native)
			require.NoError(t, err)
			require.Equal(t, int64(cap(b)), owner.AllocatedBytes())
			b, err = owner.ReallocZero(b, cap(b)+4096, native)
			require.NoError(t, err)
			require.Equal(t, int64(cap(b)), owner.AllocatedBytes())
			other.Free(b)
			require.Zero(t, owner.AllocatedBytes())
			require.Zero(t, other.AllocatedBytes())
			_, err = owner.Alloc(-1, native)
			require.Error(t, err)
			require.Zero(t, owner.AllocatedBytes())
		})
	}
}

func TestAllocatedBytesTransferredBeyondPoolRegistry(t *testing.T) {
	owner := MustNew("deleted-go-owner")
	consumer := MustNew("live-consumer")
	defer DeleteMPool(consumer)
	b, err := owner.Alloc(100, false)
	require.NoError(t, err)
	DeleteMPool(owner)
	require.Equal(t, int64(100), owner.AllocatedBytes(), "deleting a registry entry cannot retire a live owner")
	consumer.Free(b)
	require.Zero(t, owner.AllocatedBytes())
	_, exists := allocationCounters.Load(owner.id)
	require.False(t, exists)
}

func TestGrowCapacityPredictsActualNewAllocation(t *testing.T) {
	m := MustNew("capacity")
	defer DeleteMPool(m)
	var b []byte
	for _, size := range []int{1, 4, 4000, 4097, 9001, 65000} {
		want := GrowCapacity(int64(cap(b)), int64(size))
		next, err := m.Grow(b, size, false)
		require.NoError(t, err)
		if want > 0 {
			require.Equal(t, want, int64(cap(next)))
		} else {
			require.Equal(t, cap(b), cap(next))
		}
		b = next
	}
	m.Free(b)
}
