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

package containers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// A lent vector returns its buffers to the pool, and a stale second release
// of it cannot reach the next borrower of the same buffers.
func TestReleaseVectorPooled(t *testing.T) {
	mp := mpool.MustNewZero()
	pool := NewVectorPool(t.Name(), 2, WithMPool(mp)) // one fixed-size and one varlena slot
	defer pool.Destory()
	typ := types.T_int64.ToType()

	first := pool.GetVector(&typ)
	require.True(t, isLent(first))
	for i := 0; i < 100; i++ {
		first.MustAppendValue(int64(i), false)
	}
	held := pool.Allocated()
	require.Greater(t, held, 0, "the pool measures a lent vector")
	usedCnt, _ := pool.Used(false)
	require.Equal(t, 1, usedCnt)

	ReleaseVector(first)
	require.False(t, isLent(first))
	require.Equal(t, 0, first.Length(), "a released lent vector is left empty")
	require.Equal(t, held, pool.Allocated(), "the buffers went back to the pool")
	usedCnt, _ = pool.Used(false)
	require.Equal(t, 0, usedCnt)

	// the only element is lent again, as a different vector
	second := pool.GetVector(&typ)
	require.True(t, isLent(second))
	require.NotSame(t, first, second)
	second.MustAppendValue(int64(7), false)

	// a stale release of first does nothing to second
	ReleaseVector(first)
	require.True(t, isLent(second))
	require.Equal(t, 1, second.Length())
	require.Equal(t, int64(7), second.GetValue(0))

	ReleaseVector(second)
	ReleaseVector(second)
	usedCnt, _ = pool.Used(false)
	require.Equal(t, 0, usedCnt)
}

// A vector the pool did not lend is freed, and releasing it again is a no-op.
func TestReleaseVectorUnpooled(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := MakeVector(types.T_varchar.ToType(), mp)
	vec.MustAppendValue([]byte("a value longer than an inline varlena"), false)
	require.Greater(t, mp.CurrNB(), int64(0))
	ReleaseVector(vec)
	require.Zero(t, mp.CurrNB())
	ReleaseVector(vec)
	require.Zero(t, mp.CurrNB())

	// a read-only vector only drops its references
	owner := vector.NewVec(types.T_int32.ToType(), mp)
	require.NoError(t, vector.AppendFixed(owner, int32(1), false, mp))
	view, err := owner.Window(0, 1)
	require.NoError(t, err)
	require.True(t, view.IsReadOnly())
	ReleaseVector(view)
	require.Equal(t, 1, owner.Length())
	owner.Free(mp)
	require.Zero(t, mp.CurrNB())

	// the shared placeholder is never released
	ReleaseVector(EMPTY_VECTOR)
	require.Equal(t, types.T_int8, EMPTY_VECTOR.GetType().Oid)
	ReleaseVector(nil)
}

// Closing the pool while a vector is lent: the vector stays usable, and its
// buffers are freed when it is released.
func TestReleaseVectorAfterPoolClose(t *testing.T) {
	mp := mpool.MustNewZero()
	pool := NewVectorPool(t.Name(), 2, WithMPool(mp)) // one fixed-size and one varlena slot
	typ := types.T_int64.ToType()
	vec := pool.GetVector(&typ)
	require.True(t, isLent(vec))
	vec.MustAppendValue(int64(1), false)
	pool.Close()
	require.Equal(t, int64(1), vec.GetValue(0))
	ReleaseVector(vec)
	require.Zero(t, mp.CurrNB(), "the lent buffers are freed once returned to a closed pool")
}

// Mutating a read-only vector fails instead of copying it.
func TestMutatingReadOnlyPanics(t *testing.T) {
	mp := mpool.MustNewZero()
	owner := vector.NewVec(types.T_int32.ToType(), mp)
	defer owner.Free(mp)
	require.NoError(t, vector.AppendFixed(owner, int32(1), false, mp))
	view, err := owner.Window(0, 1)
	require.NoError(t, err)
	require.Panics(t, func() { view.MustAppendValue(int32(2), false) })
	require.Panics(t, func() { UpdateValue(view, uint32(0), int32(2), false, view.MPool()) })
	require.Error(t, view.Shrink([]int64{0}, true, view.MPool()))
	require.Equal(t, int32(1), owner.GetValue(0), "the owner is untouched")
}
