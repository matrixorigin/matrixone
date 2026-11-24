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

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// A vector allocates only from the pool it was created with: every call that
// passes a pool must pass exactly that one.
func TestVectorRejectsForeignPool(t *testing.T) {
	mp := mpool.MustNewZero()
	other := mpool.MustNewZero()
	vec := NewVec(types.T_int64.ToType(), mp)
	require.Same(t, mp, vec.MPool())
	require.False(t, vec.IsReadOnly())

	require.Error(t, AppendFixed(vec, int64(1), false, other))
	require.Panics(t, func() { _ = AppendFixed(vec, int64(1), false, nil) })
	require.Zero(t, other.CurrNB())
	require.NoError(t, AppendFixed(vec, int64(1), false, mp))
	require.Panics(t, func() { vec.Free(other) })

	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

// A nil pool makes a read-only reference vector: it never allocates, never
// mutates, and Free only drops its references.
func TestReadOnlyVectorIsImmutable(t *testing.T) {
	mp := mpool.MustNewZero()
	owned := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixedList(owned, []int64{1, 2, 3}, nil, mp))
	encoded, err := owned.MarshalBinary()
	require.NoError(t, err)

	ro := NewVec(types.T_any.ToType(), nil)
	require.True(t, ro.IsReadOnly())
	require.NoError(t, ro.UnmarshalBinary(encoded))
	require.Equal(t, []int64{1, 2, 3}, MustFixedColNoTypeCheck[int64](ro))
	// No-copy: the read-only vector references the encoding.
	require.True(t, byteSlicesOverlap(ro.GetData(), encoded))

	require.Error(t, AppendFixed(ro, int64(4), false, mp))
	require.Error(t, ro.Shrink([]int64{0}, false, mp))
	require.Equal(t, 3, ro.Length())

	// The window of a read-only vector is another read-only reference.
	w, err := ro.Window(1, 3)
	require.NoError(t, err)
	require.True(t, w.IsReadOnly())
	require.Equal(t, []int64{2, 3}, MustFixedColNoTypeCheck[int64](w))
	w.Free(mp)

	// A copy is owned by the pool it is made in.
	dup, err := ro.Dup(mp)
	require.NoError(t, err)
	require.Same(t, mp, dup.MPool())
	require.NoError(t, AppendFixed(dup, int64(4), false, mp))
	dup.Free(mp)

	ro.Free(mp)
	owned.Free(mp)
	require.Zero(t, mp.CurrNB())
}

// A window is a read-only reference to its source's rows, owned by no pool;
// CloneWindow and decodes into a vector with a pool copy into that pool.
func TestOwnedWindowAndDecodeCopy(t *testing.T) {
	mp := mpool.MustNewZero()
	src := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, AppendBytesList(src, [][]byte{
		[]byte("a"), []byte("a value longer than the inline varlena size"), []byte("c"),
	}, nil, mp))
	allocated := mp.CurrNB()

	w, err := src.Window(1, 3)
	require.NoError(t, err)
	require.True(t, w.IsReadOnly())
	require.Equal(t, allocated, mp.CurrNB())
	require.Equal(t, "a value longer than the inline varlena size", w.GetStringAt(0))
	require.True(t, byteSlicesOverlap(w.GetData(), src.GetData()))
	require.Error(t, AppendBytes(w, []byte("x"), false, mp))
	w.Free(nil)

	c, err := src.CloneWindow(1, 3, mp)
	require.NoError(t, err)
	require.Same(t, mp, c.MPool())
	require.Equal(t, "a value longer than the inline varlena size", c.GetStringAt(0))
	require.False(t, byteSlicesOverlap(c.GetData(), src.GetData()))
	require.False(t, byteSlicesOverlap(c.GetArea(), src.GetArea()))
	c.Free(mp)

	encoded, err := src.MarshalBinary()
	require.NoError(t, err)
	decoded := NewVec(types.T_any.ToType(), mp)
	require.NoError(t, decoded.UnmarshalBinary(encoded))
	require.False(t, byteSlicesOverlap(decoded.GetArea(), encoded))
	for i := range encoded {
		encoded[i] = 0
	}
	require.Equal(t, "a value longer than the inline varlena size", decoded.GetStringAt(1))
	decoded.Free(mp)

	src.Free(mp)
	require.Zero(t, mp.CurrNB())
}

// Fixtures for tests that allocate vectors through an accounted MPool view.
// Accounting belongs to mpool; a vector only knows the pool it was created
// with, so an accounted vector is NewVec(typ, state.view(t, mp)).

const (
	testVectorAllocationOwner    mpool.AllocationOwner = 1
	testVectorDataAllocationSite mpool.AllocationSite  = 1
)

type testVectorAllocationAccount struct {
	registry *mpool.AllocationAccountRegistry
	account  *mpool.AllocationAccount
}

// view returns an accounted view of mp that charges the test account.
func (s testVectorAllocationAccount) view(t testing.TB, mp *mpool.MPool) *mpool.MPool {
	t.Helper()
	view, err := mp.WithAccount(
		s.account,
		testVectorAllocationOwner,
		testVectorDataAllocationSite,
		mpool.AllocationCapacityClassDefault,
	)
	require.NoError(t, err)
	return view
}

func newTestVectorAllocationAccount(
	t testing.TB,
	limit uint64,
	allocationSlots uint64,
) testVectorAllocationAccount {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, allocationSlots)
	require.NoError(t, err)
	account, err := registry.Open(limit)
	require.NoError(t, err)
	return testVectorAllocationAccount{
		registry: registry,
		account:  account,
	}
}

func finalizeTestVectorAllocationAccount(
	t testing.TB,
	state testVectorAllocationAccount,
) {
	t.Helper()
	snapshot := state.account.Seal()
	require.Zero(t, snapshot.Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())
	_, err := state.registry.Finalize(state.account)
	require.NoError(t, err)
}
