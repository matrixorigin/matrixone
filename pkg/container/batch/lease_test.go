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

package batch

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// lentBatch returns a one-column batch whose vector aliases backing; the batch
// is the lender. The lease's release poisons backing, modelling a cache entry
// that is reused after its last pin goes.
func lentBatch(t *testing.T, values []int64) (*Batch, *vector.RefCountedBufferLease, []byte) {
	backing := types.EncodeSlice(append([]int64(nil), values...))
	lease, err := vector.NewRefCountedBufferLease(backing, int64(cap(backing)), func() {
		for i := range backing {
			backing[i] = 0xFF
		}
	})
	require.NoError(t, err)
	vec, err := vector.NewBorrowedFixedVector(types.T_int64.ToType(), len(values), backing)
	require.NoError(t, err)
	bat := NewOffHeapWithSize(1)
	require.NoError(t, bat.HoldLease(lease))
	lease.Release()
	bat.Vecs[0] = vec
	bat.SetRowCount(len(values))
	return bat, lease, backing
}

func TestBatchReleasesLeasesWhenItDropsColumns(t *testing.T) {
	mp := mpool.MustNewZero()
	for name, drop := range map[string]func(*Batch){
		"Clean":         func(b *Batch) { b.Clean(mp) },
		"CleanOnlyData": func(b *Batch) { b.CleanOnlyData() },
		"FreeColumns":   func(b *Batch) { b.FreeColumns(mp) },
		"Unmarshal": func(b *Batch) {
			source := NewWithSize(1)
			source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(5), false, mp))
			source.SetRowCount(1)
			data, err := source.MarshalBinary()
			require.NoError(t, err)
			source.Clean(mp)
			require.NoError(t, b.UnmarshalBinaryWithAnyMp(data, mp))
		},
	} {
		t.Run(name, func(t *testing.T) {
			bat, lease, _ := lentBatch(t, []int64{1, 2, 3})
			require.Equal(t, 1, bat.HeldLeases())
			require.NotNil(t, lease.Bytes())
			drop(bat)
			require.Zero(t, bat.HeldLeases())
			require.Nil(t, lease.Bytes(), "the lender's release is the last reference")
			bat.Clean(mp)
		})
	}
	require.Zero(t, mp.CurrNB())
}

// A vector moved out of a lending batch must be materialized before the
// batch releases what it lent; the moved copy then survives the release.
func TestBatchMovedBorrowedVectorIsMaterializedBeforeRelease(t *testing.T) {
	mp := mpool.MustNewZero()
	bat, lease, backing := lentBatch(t, []int64{7, 8, 9})
	moved := bat.Vecs[0]
	bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.True(t, moved.HasBorrowedBacking())
	require.NoError(t, moved.MaterializeOwned(mp))
	require.False(t, moved.HasBorrowedBacking())

	bat.CleanOnlyData()
	require.Nil(t, lease.Bytes())
	require.Equal(t, []byte{0xFF, 0xFF}, backing[:2], "the lent backing was reused")
	require.Equal(t, []int64{7, 8, 9}, vector.MustFixedColNoTypeCheck[int64](moved))
	moved.Free(mp)
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestBatchMoveLeasesFrom(t *testing.T) {
	mp := mpool.MustNewZero()
	dst, oldLease, _ := lentBatch(t, []int64{1})
	src, newLease, _ := lentBatch(t, []int64{2, 3})

	// The caller replaces dst's columns with src's, as a reader publishing a
	// freshly converted batch does.
	dst.Vecs[0].Free(mp)
	dst.Vecs[0], src.Vecs[0] = src.Vecs[0], nil
	dst.MoveLeasesFrom(src)
	dst.SetRowCount(2)
	src.Clean(mp)

	require.Nil(t, oldLease.Bytes(), "dst's previous lent backing is released")
	require.NotNil(t, newLease.Bytes(), "src's lease moved with its vector")
	require.Equal(t, 1, dst.HeldLeases())
	require.Zero(t, src.HeldLeases())
	require.Equal(t, []int64{2, 3}, vector.MustFixedColNoTypeCheck[int64](dst.Vecs[0]))

	dst.MoveLeasesFrom(dst) // self-move is a no-op
	require.Equal(t, 1, dst.HeldLeases())
	dst.Clean(mp)
	require.Nil(t, newLease.Bytes())
	require.Zero(t, mp.CurrNB())
}
