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
	"fmt"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

// requireSameBatch checks that got holds want's rows, column by column.
func requireSameBatch(t *testing.T, want, got *Batch) {
	t.Helper()
	require.Equal(t, want.RowCount(), got.RowCount())
	require.Len(t, got.Vecs, len(want.Vecs))
	for c, vec := range want.Vecs {
		other := got.Vecs[c]
		require.Equal(t, *vec.GetType(), *other.GetType(), "column %d", c)
		require.Equal(t, vec.Length(), other.Length(), "column %d", c)
		for r := 0; r < vec.Length(); r++ {
			require.Equal(t, vec.IsNull(uint64(r)), other.IsNull(uint64(r)), "column %d row %d", c, r)
			if !vec.IsNull(uint64(r)) {
				require.Equal(t, vec.GetRawBytesAt(r), other.GetRawBytesAt(r), "column %d row %d", c, r)
			}
		}
	}
	require.Equal(t, want.ExtraBuf(), got.ExtraBuf())
}

// newRandomBatch builds a batch of rows rows whose columns mix fixed and
// varlen types, nulls, constants and constant nulls.
func newRandomBatch(t *testing.T, rng *rand.Rand, mp *mpool.MPool, rows int) *Batch {
	cols := 1 + rng.Intn(6)
	bat := NewWithSize(cols)
	for c := range bat.Vecs {
		nullRate := rng.Intn(4) // 0: no nulls
		switch kind := rng.Intn(5); kind {
		case 0:
			vec := vector.NewVec(types.T_int64.ToType(), mp)
			for r := 0; r < rows; r++ {
				require.NoError(t, vector.AppendFixed(vec, rng.Int63(), nullRate > 0 && rng.Intn(nullRate+1) == 0, mp))
			}
			bat.Vecs[c] = vec
		case 1, 2:
			vec := vector.NewVec(types.T_varchar.ToType(), mp)
			for r := 0; r < rows; r++ {
				value := bytes.Repeat([]byte{byte('a' + rng.Intn(26))}, rng.Intn(60))
				require.NoError(t, vector.AppendBytes(vec, value, nullRate > 0 && rng.Intn(nullRate+1) == 0, mp))
			}
			bat.Vecs[c] = vec
		case 3:
			vec, err := vector.NewConstFixed(types.T_int32.ToType(), rng.Int31(), rows, mp)
			require.NoError(t, err)
			bat.Vecs[c] = vec
		default:
			bat.Vecs[c] = vector.NewConstNull(types.T_varchar.ToType(), rows, mp)
		}
	}
	bat.SetRowCount(rows)
	if rng.Intn(2) == 0 {
		require.NoError(t, bat.SetExtraBuf(mp, bytes.Repeat([]byte("x"), rng.Intn(300))))
	}
	bat.Recursive = rng.Int31()
	bat.ShuffleIDX = rng.Int31()
	return bat
}

// Over random batches, Encode writes MarshalBinary's bytes into the message
// buffer, the Entry round-trips them, and both views decode the rows.
func TestEncodeRandomBatchesRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	mp := mpool.MustNewZero()
	for i := 0; i < 200; i++ {
		rows := []int{0, 1, 2, 63, 64, 65, 500}[rng.Intn(7)]
		bat := newRandomBatch(t, rng, mp, rows)
		want, err := bat.MarshalBinary()
		require.NoError(t, err)
		encoded, err := Encode(bat)
		require.NoError(t, err)
		require.Equal(t, len(want), encoded.Len())

		wire, err := (&api.Entry{TableName: "t", Bat: encoded}).Marshal()
		require.NoError(t, err)
		var got api.Entry
		require.NoError(t, got.Unmarshal(wire))
		require.Equal(t, want, got.Bat.Bytes(), "batch %d", i)

		view := new(Batch)
		require.NoError(t, ViewTrustedBinaryInto(view, got.Bat.Bytes()))
		requireSameBatch(t, bat, view)
		checked, err := ViewBinary(got.Bat.Bytes())
		require.NoError(t, err)
		requireSameBatch(t, bat, checked)
		require.Equal(t, bat.Recursive, checked.Recursive)
		require.Equal(t, bat.ShuffleIDX, checked.ShuffleIDX)
		bat.Clean(mp)
	}
	require.Zero(t, mp.CurrNB())
}

// Encode borrows its batch until the Entry is marshaled. Every change that
// alters the encoding's size fails the marshal, without writing a byte past
// the encoding, rather than producing a corrupt message.
func TestEncodeDetectsChangedBatch(t *testing.T) {
	mutations := map[string]func(*testing.T, *Batch, *mpool.MPool){
		"append-row": func(t *testing.T, bat *Batch, mp *mpool.MPool) {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
		},
		"grow-varlen": func(t *testing.T, bat *Batch, mp *mpool.MPool) {
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], bytes.Repeat([]byte("z"), 100), false, mp))
		},
		"drop-vector": func(t *testing.T, bat *Batch, mp *mpool.MPool) {
			bat.Vecs = bat.Vecs[:len(bat.Vecs)-1]
		},
		"add-vector": func(t *testing.T, bat *Batch, mp *mpool.MPool) {
			bat.Vecs = append(bat.Vecs, vector.NewConstNull(types.T_int64.ToType(), bat.RowCount(), mp))
		},
		"nil-vector": func(t *testing.T, bat *Batch, mp *mpool.MPool) {
			bat.Vecs[0] = nil
		},
		"extra-buffer": func(t *testing.T, bat *Batch, mp *mpool.MPool) {
			require.NoError(t, bat.SetExtraBuf(mp, []byte("longer extra")))
		},
		"selection": func(t *testing.T, bat *Batch, mp *mpool.MPool) {
			var sel bitmap.Bitmap
			bat.SetSelection(&sel)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			bat := newEncodeTestBatch(t, mp, 10)
			require.NoError(t, bat.SetExtraBuf(mp, []byte("e")))
			owned := append([]*vector.Vector(nil), bat.Vecs...)
			encoded, err := Encode(bat)
			require.NoError(t, err)
			mutate(t, bat, mp)
			defer func() {
				// Restore the vectors the batch owns, free any the mutation
				// added, and check nothing leaked.
				for _, vec := range bat.Vecs[min(len(bat.Vecs), len(owned)):] {
					vec.Free(mp)
				}
				bat.ClearSelection()
				bat.Vecs = owned
				bat.Clean(mp)
				require.Zero(t, mp.CurrNB())
			}()

			dst := bytes.Repeat([]byte{0xAA}, encoded.Len()+64)
			_, err = encoded.MarshalTo(dst)
			require.Error(t, err)
			require.Equal(t, bytes.Repeat([]byte{0xAA}, 64), dst[encoded.Len():],
				"a failed marshal writes nothing past the encoding")
			_, err = (&api.Entry{TableName: "t", Bat: encoded}).Marshal()
			require.Error(t, err, "the Entry's marshal fails too")
			require.Nil(t, encoded.Bytes())
		})
	}
}

// Encode on read-only windows of a batch's vectors.
func TestEncodeWindows(t *testing.T) {
	mp := mpool.MustNewZero()
	src := newEncodeTestBatch(t, mp, 300)
	defer func() {
		src.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()
	for _, span := range [][2]int{{0, 0}, {0, 300}, {1, 2}, {63, 130}, {70, 300}} {
		t.Run(fmt.Sprint(span), func(t *testing.T) {
			bat := NewWithSize(len(src.Vecs))
			for i, vec := range src.Vecs {
				window, err := vec.Window(span[0], span[1])
				require.NoError(t, err)
				bat.Vecs[i] = window
			}
			bat.SetRowCount(span[1] - span[0])
			encoded, err := Encode(bat)
			require.NoError(t, err)
			wire, err := encoded.Marshal()
			require.NoError(t, err)
			for _, view := range []func([]byte) (*Batch, error){ViewBinary, ViewTrustedBinary} {
				got, err := view(wire)
				require.NoError(t, err)
				requireSameBatch(t, bat, got)
			}
		})
	}
}

// A reused view batch follows each decode: it grows when an encoding has
// more vectors, allocates nothing once warm, survives a failed decode, and
// after DropViews keeps no reference to the bytes.
func TestViewTrustedBinaryIntoLifecycle(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(11))
	narrow := NewWithSize(2)
	narrow.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	narrow.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	for i := 0; i < 20; i++ {
		require.NoError(t, vector.AppendFixed(narrow.Vecs[0], int64(i), false, mp))
		require.NoError(t, vector.AppendBytes(narrow.Vecs[1], bytes.Repeat([]byte("n"), 30), i == 3, mp))
	}
	narrow.SetRowCount(20)
	wide := newRandomBatch(t, rng, mp, 40)
	for len(wide.Vecs) < 4 {
		wide.Vecs = append(wide.Vecs, vector.NewConstNull(types.T_int64.ToType(), 40, mp))
	}
	defer func() {
		narrow.Clean(mp)
		wide.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()
	narrowBytes, err := narrow.MarshalBinary()
	require.NoError(t, err)
	wideBytes, err := wide.MarshalBinary()
	require.NoError(t, err)

	view := new(Batch)
	require.NoError(t, ViewTrustedBinaryInto(view, narrowBytes))
	requireSameBatch(t, narrow, view)
	require.NoError(t, ViewTrustedBinaryInto(view, wideBytes))
	requireSameBatch(t, wide, view)
	require.Zero(t, testing.AllocsPerRun(10, func() {
		DropViews(view)
		err = ViewTrustedBinaryInto(view, wideBytes)
	}))
	require.NoError(t, err)
	requireSameBatch(t, wide, view)

	require.Error(t, ViewTrustedBinaryInto(view, wideBytes[:len(wideBytes)-3]), "truncated")
	require.Error(t, ViewTrustedBinaryInto(view, nil), "empty")
	require.NoError(t, ViewTrustedBinaryInto(view, narrowBytes), "a failed decode leaves a usable view")
	requireSameBatch(t, narrow, view)

	DropViews(view)
	require.Zero(t, view.RowCount())
	require.Nil(t, view.ExtraBuf())
	for _, vec := range view.Vecs {
		require.Nil(t, vec.GetData())
		require.Nil(t, vec.GetArea())
		require.Zero(t, vec.Length())
	}
}

// Views of an off-heap receive buffer: once the views are dropped and the
// buffer freed (and poisoned), a detached copy still holds the rows and the
// view keeps no reference that could read freed memory.
func TestViewsOfFreedReceiveBuffer(t *testing.T) {
	mpool.EnableDebugPoisonOnFree()
	defer mpool.DisableDebugPoisonOnFree()
	mp := mpool.MustNewZero()
	bat := newEncodeTestBatch(t, mp, 100)
	defer func() {
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()
	encoded, err := Encode(bat)
	require.NoError(t, err)

	recvPool := mpool.MustNewZero()
	recv := mpool.NewMessageBuffer(recvPool, 1<<20)
	data, err := recv.Resize(encoded.Len())
	require.NoError(t, err)
	_, err = encoded.MarshalTo(data)
	require.NoError(t, err)
	var entry api.EncodedBatch
	require.NoError(t, entry.Unmarshal(data))
	detached := entry.Detach()

	view := new(Batch)
	require.NoError(t, ViewTrustedBinaryInto(view, entry.Bytes()))
	requireSameBatch(t, bat, view)
	DropViews(view)
	recv.Free()
	require.Zero(t, recvPool.CurrNB())

	for _, vec := range view.Vecs {
		require.Nil(t, vec.GetData(), "a dropped view keeps no reference to the freed buffer")
	}
	got, err := ViewTrustedBinary(detached.Bytes())
	require.NoError(t, err)
	requireSameBatch(t, bat, got)
}
