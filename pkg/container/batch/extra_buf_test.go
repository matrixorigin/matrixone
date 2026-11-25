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

package batch

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func newExtraBufTestBatch(t *testing.T, mp *mpool.MPool, payload []byte) *Batch {
	t.Helper()
	bat := NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int64{1, 2, 3}, nil, mp))
	bat.SetRowCount(3)
	require.NoError(t, bat.SetExtraBuf(mp, payload))
	return bat
}

func sameBacking(a, b []byte) bool {
	return len(a) != 0 && len(b) != 0 && &a[0] == &b[0]
}

func TestExtraBufSetCopiesFromPoolAndCleanFrees(t *testing.T) {
	mp := mpool.MustNewZero()
	payload := []byte("partial aggregate state")
	bat := newExtraBufTestBatch(t, mp, payload)
	require.Equal(t, payload, bat.ExtraBuf())
	require.False(t, sameBacking(payload, bat.ExtraBuf()), "SetExtraBuf copies")

	// data may alias the current payload.
	require.NoError(t, bat.SetExtraBuf(mp, bat.ExtraBuf()[:7]))
	require.Equal(t, []byte("partial"), bat.ExtraBuf())

	// Releasing the payload keeps the batch's pool.
	bat.DropExtraBuffer()
	require.Empty(t, bat.ExtraBuf())
	require.Same(t, mp, bat.mp)

	require.NoError(t, bat.SetExtraBuf(mp, payload))
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestExtraBufDecodeOwnership(t *testing.T) {
	mp := mpool.MustNewZero()
	source := newExtraBufTestBatch(t, mp, []byte("payload"))
	data, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Clean(mp)
	require.Zero(t, mp.CurrNB())

	t.Run("binary with pool", func(t *testing.T) {
		decoded := NewWithSize(0)
		require.NoError(t, decoded.UnmarshalBinaryWithAnyMp(data, mp))
		require.Equal(t, []byte("payload"), decoded.ExtraBuf())
		require.Same(t, mp, decoded.mp)
		require.False(t, sameBacking(data[len(data)-16:], decoded.ExtraBuf()))
		decoded.Clean(mp)
		require.Zero(t, mp.CurrNB())
	})

	t.Run("binary without pool borrows the input", func(t *testing.T) {
		decoded := NewWithSize(0)
		require.NoError(t, decoded.UnmarshalBinary(data))
		require.Equal(t, []byte("payload"), decoded.ExtraBuf())
		require.Nil(t, decoded.mp)
		at := bytes.Index(data, []byte("payload"))
		require.True(t, sameBacking(data[at:at+7], decoded.ExtraBuf()))
		decoded.Clean(mp) // frees nothing it does not own
		require.Zero(t, mp.CurrNB())
	})

	t.Run("reader with pool", func(t *testing.T) {
		decoded := NewWithSize(0)
		require.NoError(t, decoded.UnmarshalFromReader(bytes.NewReader(data), mp))
		require.Equal(t, []byte("payload"), decoded.ExtraBuf())
		require.Same(t, mp, decoded.mp)
		decoded.Clean(mp)
		require.Zero(t, mp.CurrNB())
	})

	t.Run("decode into a batch that owns a payload from another pool", func(t *testing.T) {
		other := mpool.MustNewZero()
		decoded := newExtraBufTestBatch(t, other, []byte("old"))
		decoded.FreeColumns(other)
		decoded.Vecs = nil
		require.NoError(t, decoded.UnmarshalBinaryWithAnyMp(data, mp))
		require.Zero(t, other.CurrNB(), "the old payload goes back to its own pool")
		require.Equal(t, []byte("payload"), decoded.ExtraBuf())
		decoded.Clean(mp)
		require.Zero(t, mp.CurrNB())
	})
}

func TestExtraBufMoveFromOwnerAndFromView(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("nil and self moves are no-ops", func(t *testing.T) {
		var nilBatch *Batch
		require.NoError(t, nilBatch.MoveExtraBufferFrom(nil, mp))
		bat := newExtraBufTestBatch(t, mp, []byte("kept"))
		require.NoError(t, bat.MoveExtraBufferFrom(nil, mp))
		require.NoError(t, bat.MoveExtraBufferFrom(bat, mp))
		require.Equal(t, []byte("kept"), bat.ExtraBuf())
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	})

	t.Run("owner moves its payload", func(t *testing.T) {
		source := newExtraBufTestBatch(t, mp, []byte("owned"))
		backing := source.ExtraBuf()
		destination := NewWithSize(0)
		require.NoError(t, destination.MoveExtraBufferFrom(source, mp))
		require.Empty(t, source.ExtraBuf())
		require.True(t, sameBacking(backing, destination.ExtraBuf()))
		source.Clean(mp)
		require.Equal(t, []byte("owned"), destination.ExtraBuf())
		destination.Clean(mp)
		require.Zero(t, mp.CurrNB())
	})

	t.Run("a view's payload is copied", func(t *testing.T) {
		for name, makeView := range map[string]func(parent, view *Batch){
			"ViewInto":     func(parent, view *Batch) { parent.ViewInto(view) },
			"PhysicalView": func(parent, view *Batch) { parent.PhysicalView(view) },
		} {
			t.Run(name, func(t *testing.T) {
				parent := newExtraBufTestBatch(t, mp, []byte("borrowed"))
				view := &Batch{}
				makeView(parent, view)
				require.Nil(t, view.mp, "a view owns nothing")
				require.True(t, sameBacking(parent.ExtraBuf(), view.ExtraBuf()))

				destination := NewWithSize(0)
				require.NoError(t, destination.MoveExtraBufferFrom(view, mp))
				require.False(t, sameBacking(parent.ExtraBuf(), destination.ExtraBuf()))
				parent.Clean(mp)
				require.Equal(t, []byte("borrowed"), destination.ExtraBuf())
				destination.Clean(mp)
				require.Zero(t, mp.CurrNB())
			})
		}
	})
}
