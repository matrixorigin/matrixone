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
		numBits: np.numBits,
		data:    np.data,
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

func TestBitmapOrBounded(t *testing.T) {
	dst := newBm(70)
	dst.AddMany([]uint64{1, 69})
	src := newBm(130)
	src.AddMany([]uint64{0, 1, 63, 64, 69, 70, 129})

	dst.OrBounded(src, 70)

	require.EqualValues(t, 70, dst.Len())
	require.Equal(t, 5, dst.Count())
	for _, row := range []uint64{0, 1, 63, 64, 69} {
		require.True(t, dst.Contains(row), "row %d", row)
	}
	for _, row := range []uint64{70, 129} {
		require.False(t, dst.Contains(row), "row %d", row)
	}
	require.NoError(t, dst.Validate())
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

func TestBitmapSize(t *testing.T) {
	require.Equal(
		t,
		2*unsafe.Sizeof(int64(0))+unsafe.Sizeof([]uint64(nil)),
		unsafe.Sizeof(Bitmap{}),
	)
}

func TestBitmapRemapOrdered(t *testing.T) {
	{
		t.Run("go", func(t *testing.T) {
			var value Bitmap
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

// Sparse bitmaps represent the remaining rows of their owning vector as clear.
// Ordered compaction cannot move a set bit beyond the original bitmap extent.
func TestBitmapOrderedRemapSparseTail(t *testing.T) {
	for _, length := range []int{0, 1, 63, 64, 65, 127, 128, 129} {
		{
			for _, mode := range []string{"slice", "mask", "offset"} {
				for _, tailOnly := range []bool{false, true} {
					t.Run(fmt.Sprintf("length=%d/%s/tail=%t", length, mode, tailOnly), func(t *testing.T) {
						value := newBm(length)
						for row := 0; row < length; row++ {
							if row%3 == 0 || row == length-1 {
								value.Add(uint64(row))
							}
						}
						backing := unsafe.SliceData(value.data)
						capacity := cap(value.data)
						original := value.Clone()
						start := int64(0)
						if tailOnly {
							start = int64(length)
						}
						var offset uint64
						if mode == "offset" {
							offset = 1
						}
						selection := newBm(length*2 + 66)
						sels := make([]int64, length+65)
						want := make([]uint64, 0)
						for i := range sels {
							row := start + int64(i) + int64(offset)
							sels[i] = row
							selection.Add(uint64(row) - offset)
							if original.Contains(uint64(row)) {
								want = append(want, uint64(i))
							}
						}
						if mode == "slice" {
							value.RemapOrdered(sels, false)
						} else {
							value.RemapMaskOrderedWithOffset(selection, false, offset)
						}
						require.Equal(t, want, value.ToArray())
						require.Equal(t, len(want), value.Count())
						require.Equal(t, int64(length), value.Len())
						for i, row := range sels {
							require.Equal(t, original.Contains(uint64(row)), value.Contains(uint64(i)), "row %d", i)
						}
						require.NoError(t, value.Validate())
						require.True(t, backing == unsafe.SliceData(value.data), "remap must retain the original backing")
						require.Equal(t, capacity, cap(value.data))

						// Repeated compaction and reuse must not resurrect discarded bits.
						value.RemapOrdered(nil, false)
						require.NoError(t, value.Validate())
						value.TryExpandWithSize(length)
						require.Zero(t, value.Count())
						require.NoError(t, value.Validate())
					})
				}
			}
		}
	}
}

func TestBitmapOrderedRemapRejectsInvalidTail(t *testing.T) {
	for _, sels := range [][]int64{{-1}, {0, 0}, {0, 2, 1}} {
		t.Run(fmt.Sprint(sels), func(t *testing.T) {
			value := newBm(1)
			value.Add(0)
			require.PanicsWithValue(t, "bitmap ordered remap requires strictly increasing non-negative rows", func() {
				value.RemapOrdered(sels, false)
			})
			require.Equal(t, int64(1), value.Len())
			require.Equal(t, []uint64{0}, value.ToArray())
			require.NoError(t, value.Validate())
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

func TestBitmapRemapMaskOrderedWithOffset(t *testing.T) {
	selection := newBm(129)
	selection.AddMany([]uint64{0, 62, 128})

	var value Bitmap
	value.InitWithSize(130)
	value.AddMany([]uint64{1, 63, 64, 127, 129})
	value.RemapMaskOrderedWithOffset(selection, false, 1)
	require.Equal(t, int64(3), value.Len())
	require.Equal(t, 3, value.Count())
	require.True(t, value.Contains(0))
	require.True(t, value.Contains(1))
	require.True(t, value.Contains(2))

	value.InitWithSize(130)
	value.AddMany([]uint64{1, 63, 64, 127, 129})
	value.RemapMaskOrderedWithOffset(selection, true, 1)
	require.Equal(t, 2, value.Count())
	require.True(t, value.Contains(62))
	require.True(t, value.Contains(125))
}

func TestBitmapPrepareUnmarshal(t *testing.T) {
	source := newBm(128)
	source.Add(1)
	source.Add(127)
	encoded := source.Marshal()

	var streamed Bitmap
	payload, err := streamed.PrepareUnmarshal(
		encoded[:MarshalHeaderSize],
		len(encoded),
	)
	require.NoError(t, err)
	copy(payload, encoded[MarshalHeaderSize:])
	require.True(t, streamed.IsSame(source))
	require.Equal(t, source.Count(), streamed.Count())
	require.NoError(t, streamed.Validate())

	_, err = streamed.PrepareUnmarshal(
		encoded[:MarshalHeaderSize],
		len(encoded)-1,
	)
	require.Error(t, err)
}

func TestBitmapIteratorBounds(t *testing.T) {
	collect := func(n *Bitmap) []uint64 {
		var got []uint64
		itr := n.IteratorValue()
		for itr.HasNext() {
			got = append(got, itr.Next())
		}
		require.False(t, itr.HasNext())
		require.Zero(t, itr.PeekNext())
		return got
	}

	var empty Bitmap
	require.Nil(t, collect(&empty))

	// Set bits at or beyond numBits are never returned.
	stray := Bitmap{numBits: 70, data: []uint64{1 << 3, 1<<5 | 1<<6 | 1<<63}}
	require.Equal(t, []uint64{3, 69}, collect(&stray))
	stray.numBits = 69
	require.Equal(t, []uint64{3}, collect(&stray))

	// Words missing from data read as zero.
	short := Bitmap{numBits: 200, data: []uint64{1 << 1}}
	require.Equal(t, []uint64{1}, collect(&short))

	full := newBm(130)
	full.AddMany([]uint64{0, 63, 64, 127, 128, 129})
	itr := full.IteratorValue()
	require.Equal(t, uint64(0), itr.PeekNext())
	require.Equal(t, []uint64{0, 63, 64, 127, 128, 129}, collect(full))
}
