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

package iscp

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestAtomicBatchRetainsEncodedKeysAcrossPackerReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	atomic := NewAtomicBatch(mp)
	t.Cleanup(atomic.Close)
	packer := types.NewPacker()
	t.Cleanup(packer.Close)
	makeInput := func(values ...int32) (*batch.Batch, func()) {
		bat := batch.NewWithSize(2)
		owned := true
		t.Cleanup(func() {
			if owned {
				bat.Clean(mp)
			}
		})
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		for _, value := range values {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], types.BuildTS(1, 0), false, mp))
		}
		bat.SetRowCount(len(values))
		return bat, func() { owned = false }
	}
	first, releaseFirst := makeInput(2, 1, 2)
	atomic.Append(packer, first, 1, 0)
	releaseFirst()
	second, releaseSecond := makeInput(3)
	atomic.Append(packer, second, 1, 0)
	releaseSecond()

	iter := atomic.Rows.Iter()
	defer iter.Release()
	for i, expected := range []struct {
		value  int32
		offset int
		source *batch.Batch
	}{{1, 1, first}, {2, 2, first}, {3, 0, second}} {
		require.True(t, iter.Next(), "row %d", i)
		packer.Reset()
		packer.EncodeInt32(expected.value)
		require.Equal(t, packer.Bytes(), iter.Item().Pk)
		require.Equal(t, expected.offset, iter.Item().Offset)
		require.Same(t, expected.source, iter.Item().Src)
	}
	require.False(t, iter.Next())
}
