// Copyright 2021 Matrix Origin
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

package bitmap

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10
	BenchmarkRows = 8192
)

func newBm(n int) *Bitmap {
	var bm Bitmap
	bm.InitWithSize(int64(n))
	return &bm
}

type shortMarshalWriter struct{}

func (shortMarshalWriter) Write(value []byte) (int, error) {
	return len(value) - 1, nil
}

func TestNulls(t *testing.T) {
	np := newBm(Rows)
	np.AddRange(0, 0)
	np.AddRange(1, 10)
	require.Equal(t, 9, np.Count())
	np.Reset()

	ok := np.IsEmpty()
	require.Equal(t, true, ok)
	np.TryExpandWithSize(10)
	np.Add(0)
	ok = np.Contains(0)
	require.Equal(t, true, ok)
	require.Equal(t, 1, np.Count())

	itr := np.Iterator()
	require.Equal(t, uint64(0), itr.PeekNext())
	require.Equal(t, true, itr.HasNext())
	require.Equal(t, uint64(0), itr.Next())

	np.Remove(0)
	ok = np.IsEmpty()
	require.Equal(t, true, ok)

	np.AddMany([]uint64{1, 2, 3})
	require.Equal(t, 3, np.Count())
	np.RemoveRange(1, 3)
	require.Equal(t, 1, np.Count())
	require.True(t, np.Contains(3))

	np.AddMany([]uint64{1, 2, 3})
	np.Filter([]int64{0})
	fmt.Printf("%v\n", np.String())
	fmt.Printf("size: %v\n", np.Size())
	fmt.Printf("numbers: %v\n", np.Count())

	nq := newBm(Rows)
	encoded := np.Marshal()
	var streamed bytes.Buffer
	require.NoError(t, np.MarshalTo(&streamed))
	require.Equal(t, encoded, streamed.Bytes())
	require.Equal(t, len(encoded), np.MarshalSize())
	require.ErrorIs(t, np.MarshalTo(shortMarshalWriter{}), io.ErrShortWrite)
	nq.Unmarshal(encoded)

	require.Equal(t, np.ToArray(), nq.ToArray())

	np.Reset()
}

func TestRemoveRangeSameWordBoundary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		length int
		start  uint64
	}{
		{name: "one word", length: 64, start: 52},
		{name: "aggregate chunk boundary", length: 8192, start: 8180},
	} {
		t.Run(tc.name, func(t *testing.T) {
			np := newBm(tc.length)
			np.AddRange(0, uint64(tc.length))
			np.RemoveRange(tc.start, uint64(tc.length))
			require.Equal(t, int(tc.start), np.Count())
			require.True(t, np.Contains(tc.start-1))
			require.False(t, np.Contains(tc.start))
		})
	}
}

func BenchmarkAdd(b *testing.B) {
	np := newBm(BenchmarkRows)
	for i := 0; i < b.N; i++ {
		for j := 0; j < BenchmarkRows; j++ {
			np.Add(uint64(j))
		}
		for j := 0; j < BenchmarkRows; j++ {
			np.Contains(uint64(j))
		}
		for j := 0; j < BenchmarkRows; j++ {
			np.Remove(uint64(j))
		}
	}
}

func TestC(t *testing.T) {
	bm3 := newBm(10000)
	bm5 := newBm(10000)

	require.True(t, bm3.IsEmpty())
	require.True(t, bm3.C_IsEmpty())
	require.True(t, bm3.Count() == 0)
	require.True(t, bm3.C_Count() == 0)

	for i := 0; i < 10000; i += 3 {
		bm3.Add(uint64(i))
	}
	for i := 0; i < 10000; i += 5 {
		bm3.Add(uint64(i))
	}

	require.False(t, bm3.IsEmpty())
	require.False(t, bm3.C_IsEmpty())

	bm3Copy := bm3.Clone()
	require.True(t, bm3.IsSame(bm3Copy))
	require.True(t, bm3.C_Count() == bm3Copy.C_Count())
	bm5Copy := bm5.Clone()
	require.True(t, bm5.IsSame(bm5Copy))
	require.True(t, bm5.C_Count() == bm5Copy.C_Count())

	bm3CopyAgain := bm3.Clone()

	bm3.Or(bm5)
	bm3Copy.C_Or(bm5)
	require.True(t, bm3.IsSame(bm3Copy))
	require.True(t, bm3.Count() == int(bm3Copy.C_Count()))

	bm5.And(bm3CopyAgain)
	bm5Copy.C_And(bm3CopyAgain)
	require.True(t, bm5.IsSame(bm5Copy))
	require.True(t, bm5.Count() == int(bm5Copy.C_Count()))
}

func TestBitmapIterator_Next(t *testing.T) {
	np := newBm(BenchmarkRows)
	np.AddRange(0, 64)
	require.Equal(t, 64, np.Count())

	// | 63 -- 0 | 127 -- 64 | 191 -- 128 | 255 -- 192 | 319 -- 256 | 383 -- 320 | ... |
	rows := []uint64{127, 192, 320} // add some boundary case to check if loops over word
	np.AddMany(rows)

	itr := np.Iterator()
	i := uint64(0)
	for itr.HasNext() {
		r := itr.Next()
		if r < uint64(64) {
			require.Equal(t, i, r)
			i++
		} else {
			t.Logf("r now is %d\n", r)
		}
	}
}

func TestBitmap_Compatibility(t *testing.T) {
	np := newBm(BenchmarkRows)
	np.AddRange(0, 64)

	rows := []uint64{127, 192, 320}
	np.AddMany(rows)

	npV1 := &Bitmap{
		taggedLen: np.taggedLen,
		data:      np.data,
	}
	data := npV1.MarshalV1()

	np2 := newBm(BenchmarkRows)
	np2.UnmarshalV1(data)
	require.Equal(t, np.Count(), np2.Count())
	require.Equal(t, np.ToArray(), np2.ToArray())
	require.Equal(t, np.ToI64Array(nil), np2.ToI64Array(nil))

	np3 := newBm(BenchmarkRows)
	np3.UnmarshalNoCopyV1(data)
	require.Equal(t, np.Count(), np3.Count())
	require.Equal(t, np.ToArray(), np3.ToArray())
	require.Equal(t, np.ToI64Array(nil), np3.ToI64Array(nil))
}

func TestBitmap_Clear(t *testing.T) {
	np := newBm(BenchmarkRows)
	np.AddRange(100, 1000)
	require.Equal(t, 900, np.Count())
	np.Clear()
	require.True(t, np.IsEmpty())
	require.Equal(t, 0, np.Count())
}

func TestBitmap_Or(t *testing.T) {
	np := newBm(BenchmarkRows)
	np.AddRange(100, 1000)
	require.Equal(t, 900, np.Count())
	np2 := newBm(BenchmarkRows)
	np2.AddRange(500, 1500)
	require.Equal(t, 1000, np2.Count())
	np.Or(np2)
	require.Equal(t, 1400, np.Count())
}

func TestBitmap_And(t *testing.T) {
	np := newBm(BenchmarkRows)
	np.AddRange(100, 1000)
	require.Equal(t, 900, np.Count())
	np2 := newBm(BenchmarkRows)
	np2.AddRange(500, 1500)
	require.Equal(t, 1000, np2.Count())
	np.And(np2)
	require.Equal(t, 500, np.Count())
}

func TestBitmap_And2(t *testing.T) {
	np := newBm(BenchmarkRows)
	np.AddRange(0, 1000)
	require.Equal(t, 1000, np.Count())
	np2 := newBm(BenchmarkRows)
	np2.AddRange(500, 600)
	require.Equal(t, 100, np2.Count())
	np.And(np2)
	require.Equal(t, 100, np.Count())
}

func TestBitmapExternalStorageLifecycle(t *testing.T) {
	require.Equal(
		t,
		2*unsafe.Sizeof(int64(0))+unsafe.Sizeof([]uint64(nil)),
		unsafe.Sizeof(Bitmap{}),
	)
	storage := []uint64{^uint64(0), ^uint64(0)}
	var value Bitmap
	require.Nil(t, value.InstallExternalStorage(storage))
	require.True(t, value.HasExternalStorage())
	require.Equal(t, 2, value.ExternalStorageCapacity())

	value.InitWithSize(65)
	require.Equal(t, []uint64{0, 0}, storage)
	value.Add(64)
	require.True(t, value.Contains(64))

	value.Reset()
	require.True(t, value.HasExternalStorage())
	require.Equal(t, 2, value.ExternalStorageCapacity())
	require.Equal(t, []uint64{0, 0}, storage)
	value.TryExpandWithSize(128)
	value.Add(127)
	require.True(t, value.Contains(127))
	require.Panics(t, func() {
		value.TryExpandWithSize(129)
	})

	released := value.ReleaseExternalStorage()
	require.Equal(t, storage, released)
	require.False(t, value.HasExternalStorage())
	value.TryExpandWithSize(129)
	value.Add(128)
	require.True(t, value.Contains(128))
}

func TestBitmapRemapOrdered(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(fmt.Sprintf("external=%t", external), func(t *testing.T) {
			var value Bitmap
			if external {
				value.InstallExternalStorage(make([]uint64, 3))
			}
			value.InitWithSize(130)
			value.AddMany([]uint64{0, 2, 63, 64, 65, 128, 129})

			value.RemapOrdered([]int64{0, 2, 64, 65, 129}, false)
			require.Equal(t, int64(5), value.Len())
			require.Equal(t, 5, value.Count())
			for row := uint64(0); row < 5; row++ {
				require.True(t, value.Contains(row))
			}

			value.InitWithSize(130)
			value.AddMany([]uint64{0, 2, 63, 64, 65, 128, 129})
			value.RemapOrdered([]int64{1, 63, 128}, true)
			require.Equal(t, int64(130), value.Len())
			require.Equal(t, 5, value.Count())
			require.True(t, value.Contains(0))
			require.True(t, value.Contains(1))
			require.True(t, value.Contains(62))
			require.True(t, value.Contains(63))
			require.True(t, value.Contains(126))
		})
	}
}

func TestBitmapRemapMaskOrdered(t *testing.T) {
	var value Bitmap
	value.InitWithSize(130)
	value.AddMany([]uint64{1, 63, 64, 127, 129})

	selection := newBm(130)
	selection.AddMany([]uint64{1, 64, 129})
	value.RemapMaskOrdered(selection, false)
	require.Equal(t, int64(3), value.Len())
	require.Equal(t, 3, value.Count())
	require.True(t, value.Contains(0))
	require.True(t, value.Contains(1))
	require.True(t, value.Contains(2))

	value.InitWithSize(130)
	value.AddMany([]uint64{1, 63, 64, 127, 129})
	value.RemapMaskOrdered(selection, true)
	require.Equal(t, int64(130), value.Len())
	require.Equal(t, 2, value.Count())
	require.True(t, value.Contains(62))
	require.True(t, value.Contains(125))
}

func TestBitmapExternalStorageUnmarshal(t *testing.T) {
	source := newBm(128)
	source.Add(1)
	source.Add(127)
	encoded := source.Marshal()

	storage := make([]uint64, 2)
	var copied Bitmap
	copied.InstallExternalStorage(storage)
	copied.Unmarshal(encoded)
	require.True(t, copied.IsSame(source))
	require.True(t, copied.HasExternalStorage())

	var streamed Bitmap
	streamStorage := make([]uint64, 2)
	streamed.InstallExternalStorage(streamStorage)
	payload, err := streamed.PrepareExternalUnmarshal(
		encoded[:MarshalHeaderSize],
		len(encoded),
	)
	require.NoError(t, err)
	copy(payload, encoded[MarshalHeaderSize:])
	require.True(t, streamed.IsSame(source))

	_, err = streamed.PrepareExternalUnmarshal(
		encoded[:MarshalHeaderSize],
		len(encoded)-1,
	)
	require.Error(t, err)
}
