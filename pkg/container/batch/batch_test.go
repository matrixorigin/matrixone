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

package batch

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10 // default rows
)

// add unit tests for cases
type batchTestCase struct {
	bat   *Batch
	types []types.Type
}

var (
	tcs []batchTestCase
)

func init() {
	tcs = []batchTestCase{
		newTestCase([]types.Type{types.T_int8.ToType()}),
	}
}

func TestBatchMarshalAndUnmarshal(t *testing.T) {
	mp := mpool.MustNewZero()

	for _, tc := range tcs {
		data, err := tc.bat.MarshalBinary()
		require.NoError(t, err)
		size, err := tc.bat.MarshalBinarySize()
		require.NoError(t, err)
		require.Equal(t, len(data), size)
		var streamed bytes.Buffer
		require.NoError(t, tc.bat.MarshalBinaryTo(&streamed))
		require.Equal(t, data, streamed.Bytes())
		require.ErrorIs(
			t,
			tc.bat.MarshalBinaryTo(shortBatchMarshalWriter{}),
			io.ErrShortWrite,
		)

		rbat := new(Batch)
		err = rbat.UnmarshalBinary(data)
		require.NoError(t, err)

		require.Equal(t, tc.bat.extraBuf, rbat.extraBuf)

		for i, vec := range rbat.Vecs {
			require.Equal(t, vector.MustFixedColWithTypeCheck[int8](tc.bat.Vecs[i]), vector.MustFixedColWithTypeCheck[int8](vec))
		}
	}

	var buf bytes.Buffer
	for _, tc := range tcs {
		data, err := tc.bat.MarshalBinaryWithBuffer(&buf, true)
		require.NoError(t, err)

		rbat := new(Batch)
		err = rbat.UnmarshalBinary(data)
		require.NoError(t, err)
		for i, vec := range rbat.Vecs {
			require.Equal(t, vector.MustFixedColWithTypeCheck[int8](tc.bat.Vecs[i]), vector.MustFixedColWithTypeCheck[int8](vec))
		}

		reader := bytes.NewReader(data)
		rbat = new(Batch)
		err = rbat.UnmarshalFromReader(reader, mp)
		require.NoError(t, err)
		for i, vec := range rbat.Vecs {
			require.Equal(t, vector.MustFixedColWithTypeCheck[int8](tc.bat.Vecs[i]), vector.MustFixedColWithTypeCheck[int8](vec))
		}
	}
}

func TestBatchWindowBroadcastsScalarConstants(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(3)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixedList(
		source.Vecs[0], []int64{10, 20, 30, 40}, nil, mp,
	))
	var err error
	source.Vecs[1], err = vector.NewConstFixed(
		types.T_int64.ToType(), int64(7), 1, mp,
	)
	require.NoError(t, err)
	source.Vecs[2] = vector.NewConstNull(types.T_int64.ToType(), 1, mp)
	source.SetRowCount(4)
	defer func() {
		source.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()

	_, err = source.Vecs[1].Window(2, 4)
	require.Error(t, err, "standalone vector windows retain physical bounds")

	window, err := source.Window(2, 4)
	require.NoError(t, err)
	require.Equal(t, 2, window.RowCount())
	require.Equal(t, []int64{30, 40}, vector.MustFixedColWithTypeCheck[int64](window.Vecs[0]))
	require.True(t, window.Vecs[1].IsConst())
	require.Equal(t, 2, window.Vecs[1].Length())
	require.Equal(t, int64(7), vector.GetFixedAtWithTypeCheck[int64](window.Vecs[1], 1))
	require.True(t, window.Vecs[2].IsConstNull())
	require.Equal(t, 2, window.Vecs[2].Length())
	window.FreeWindow()
}

func TestBatchWindowRejectsMissingRowMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, vec := range []*vector.Vector{
		vector.NewVec(types.T_int64.ToType(), mp),
		vector.NewConstNull(types.T_int64.ToType(), 0, mp),
	} {
		if !vec.IsConst() {
			require.NoError(t, vector.AppendFixed(vec, int64(1), false, mp))
		}
		source := NewWithSize(1)
		source.Vecs[0] = vec
		source.SetRowCount(4)
		window, err := source.Window(0, 4)
		require.Error(t, err)
		require.Nil(t, window)
		source.Clean(mp)
	}
	require.Zero(t, mp.CurrNB())
}

type shortBatchMarshalWriter struct{}

func (shortBatchMarshalWriter) Write(value []byte) (int, error) {
	return len(value) - 1, nil
}

type mpoolTrackingReader struct {
	reader    *bytes.Reader
	mp        *mpool.MPool
	maxCurrNB int64
}

func (r *mpoolTrackingReader) Read(value []byte) (int, error) {
	if current := r.mp.CurrNB(); current > r.maxCurrNB {
		r.maxCurrNB = current
	}
	return r.reader.Read(value)
}

func TestMarshalBinarySizeRejectsInvalidBatch(t *testing.T) {
	var nilBatch *Batch
	_, err := nilBatch.MarshalBinarySize()
	require.Error(t, err)

	invalid := NewWithSize(1)
	_, err = invalid.MarshalBinarySize()
	require.Error(t, err)
}

func TestBatch(t *testing.T) {
	for _, tc := range tcs {
		data, err := types.Encode(tc.bat)
		require.NoError(t, err)
		rbat := new(Batch)
		err = types.Decode(data, rbat)
		require.NoError(t, err)
		for i, vec := range rbat.Vecs {
			require.Equal(t, vector.MustFixedColWithTypeCheck[int8](tc.bat.Vecs[i]), vector.MustFixedColWithTypeCheck[int8](vec))
		}
	}
}

// TestBatchUnmarshalWithAnyMpRejectsTruncatedData verifies that every truncated
// prefix of a valid batch encoding is rejected without panicking. Mutation
// protected: deleting any boundary check makes a truncated valid MarshalBinary
// encoding panic or return nil.
func TestBatchUnmarshalWithAnyMpRejectsTruncatedData(t *testing.T) {
	mp := mpool.MustNewZero()

	source := NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(42), false, mp))
	source.SetRowCount(1)

	data, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Clean(mp)

	for end := len(data) - 1; end >= 0; end-- {
		target := new(Batch)
		var unmarshalErr error
		require.NotPanics(t, func() {
			unmarshalErr = target.UnmarshalBinaryWithAnyMp(data[:end], mp)
		}, "truncated at %d bytes", end)
		require.Error(t, unmarshalErr, "truncated at %d bytes", end)

		require.NoError(t, target.UnmarshalBinaryWithAnyMp(data, mp), "reuse after truncation at %d bytes", end)
		target.Clean(mp)
	}

	require.Equal(t, int64(0), mp.CurrNB())

	vectorEnd := 16 + int(types.DecodeUint32(data[12:16]))
	t.Run("malformed_vector_framing", func(t *testing.T) {
		for _, mutate := range []func([]byte){
			func(corrupted []byte) {
				zero := uint32(0)
				copy(corrupted[12:16], types.EncodeUint32(&zero))
			},
			func(corrupted []byte) {
				oversized := uint32(len(data))
				dataLenOffset := 16 + 1 + types.TSize + 4
				copy(corrupted[dataLenOffset:dataLenOffset+4], types.EncodeUint32(&oversized))
			},
		} {
			corrupted := append([]byte(nil), data...)
			mutate(corrupted)
			target := NewWithSize(0)
			var unmarshalErr error
			require.NotPanics(t, func() {
				unmarshalErr = target.UnmarshalBinaryWithAnyMp(corrupted, mp)
			})
			require.Error(t, unmarshalErr)
			target.Clean(mp)
			require.Equal(t, int64(0), mp.CurrNB())
		}
	})
	t.Run("undersized_fixed_vector_payload", func(t *testing.T) {
		corrupted := append([]byte(nil), data...)
		rowCount := int64(2)
		vectorLength := uint32(2)
		copy(corrupted[:8], types.EncodeInt64(&rowCount))
		vectorLengthOffset := 16 + 1 + types.TSize
		copy(corrupted[vectorLengthOffset:vectorLengthOffset+4], types.EncodeUint32(&vectorLength))

		target := NewWithSize(0)
		var unmarshalErr error
		require.NotPanics(t, func() {
			unmarshalErr = target.UnmarshalBinaryWithAnyMp(corrupted, mp)
		})
		require.Error(t, unmarshalErr)
		target.Clean(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	})
	t.Run("forged_fixed_vector_type_size", func(t *testing.T) {
		corrupted := append([]byte(nil), data...)
		rowCount := int64(2)
		vectorLength := uint32(2)
		forgedTypeSize := int32(1)
		copy(corrupted[:8], types.EncodeInt64(&rowCount))
		vectorLengthOffset := 16 + 1 + types.TSize
		copy(corrupted[vectorLengthOffset:vectorLengthOffset+4], types.EncodeUint32(&vectorLength))
		vectorTypeSizeOffset := 16 + 1 + 4
		copy(corrupted[vectorTypeSizeOffset:vectorTypeSizeOffset+4], types.EncodeInt32(&forgedTypeSize))

		target := NewWithSize(0)
		var unmarshalErr error
		require.NotPanics(t, func() {
			unmarshalErr = target.UnmarshalBinaryWithAnyMp(corrupted, mp)
		})
		require.Error(t, unmarshalErr)
		target.Clean(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	})
	t.Run("invalid_null_bitmap_metadata", func(t *testing.T) {
		source := NewWithSize(1)
		source.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
		require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(0), true, mp))
		source.SetRowCount(1)
		corrupted, err := source.MarshalBinary()
		require.NoError(t, err)
		source.Clean(mp)

		vectorDataOffset := 16
		dataLenOffset := vectorDataOffset + 1 + types.TSize + 4
		dataLen := int(types.DecodeUint32(corrupted[dataLenOffset : dataLenOffset+4]))
		areaLenOffset := dataLenOffset + 4 + dataLen
		areaLen := int(types.DecodeUint32(corrupted[areaLenOffset : areaLenOffset+4]))
		nspDataOffset := areaLenOffset + 4 + areaLen + 4
		bitmapLen := uint64(64)
		bitmapDataLen := uint64(0)
		copy(corrupted[nspDataOffset+8:nspDataOffset+16], types.EncodeUint64(&bitmapLen))
		copy(corrupted[nspDataOffset+16:nspDataOffset+24], types.EncodeUint64(&bitmapDataLen))

		target := NewWithSize(0)
		var unmarshalErr error
		require.NotPanics(t, func() {
			unmarshalErr = target.UnmarshalBinaryWithAnyMp(corrupted, mp)
		})
		require.Error(t, unmarshalErr)
		target.Clean(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	})
	t.Run("varlen_offsets_must_stay_within_area", func(t *testing.T) {
		source := NewWithSize(1)
		source.Vecs[0] = vector.NewVec(types.T_varchar.ToType(), mp)
		require.NoError(t, vector.AppendBytes(source.Vecs[0], bytes.Repeat([]byte("x"), types.VarlenaInlineSize+1), false, mp))
		source.SetRowCount(1)
		corrupted, err := source.MarshalBinary()
		require.NoError(t, err)
		source.Clean(mp)

		vectorDataOffset := 16
		varlenaOffset := vectorDataOffset + 1 + types.TSize + 4 + 4
		invalidAreaOffset := uint32(len(corrupted))
		copy(corrupted[varlenaOffset+4:varlenaOffset+8], types.EncodeUint32(&invalidAreaOffset))

		target := NewWithSize(0)
		var unmarshalErr error
		require.NotPanics(t, func() {
			unmarshalErr = target.UnmarshalBinaryWithAnyMp(corrupted, mp)
		})
		require.Error(t, unmarshalErr)
		target.Clean(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	})
	t.Run("preallocated_nil_vector", func(t *testing.T) {
		target := NewWithSize(1)
		var unmarshalErr error
		require.NotPanics(t, func() {
			unmarshalErr = target.UnmarshalBinaryWithAnyMp(data[:vectorEnd], mp)
		})
		require.Error(t, unmarshalErr)

		require.NoError(t, target.UnmarshalBinaryWithAnyMp(data, mp))
		target.Clean(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	})

	t.Run("owned_vector", func(t *testing.T) {
		target := NewWithSize(1)
		target.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
		require.NoError(t, vector.AppendFixed(target.Vecs[0], int64(-1), false, mp))
		require.Positive(t, mp.CurrNB())

		require.Error(t, target.UnmarshalBinaryWithAnyMp(data[:vectorEnd], mp))
		require.NoError(t, target.UnmarshalBinaryWithAnyMp(data, mp))
		target.Clean(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	})
}

func TestBatchUnmarshalPreservesIndependentRowCount(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	source.SetRowCount(1)
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Clean(mp)

	target := NewWithSize(0)
	require.NoError(t, target.UnmarshalBinaryWithAnyMp(encoded, mp))
	require.Equal(t, 1, target.RowCount())
	require.Zero(t, target.Vecs[0].Length())
	target.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestBatchUnmarshalPreservesShortNonEmptyVector(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_binary.ToType(), mp)
	require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte("object stats"), false, mp))
	source.SetRowCount(6)
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Clean(mp)

	target := NewWithSize(0)
	require.NoError(t, target.UnmarshalBinaryWithAnyMp(encoded, mp))
	require.Equal(t, 6, target.RowCount())
	require.Equal(t, 1, target.Vecs[0].Length())
	require.Equal(t, []byte("object stats"), target.Vecs[0].GetBytesAt(0))
	target.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestBatchUnmarshalRetainsRowCountWhenVectorCountChanges(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(2)
	for i := range source.Vecs {
		source.Vecs[i] = vector.NewVec(types.T_int64.ToType(), mp)
		require.NoError(t, vector.AppendFixed(source.Vecs[i], int64(i+1), false, mp))
	}
	source.SetRowCount(1)
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Clean(mp)

	target := NewWithSize(1)
	target.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixed(target.Vecs[0], int64(-1), false, mp))

	require.NoError(t, target.UnmarshalBinaryWithAnyMp(encoded, mp))
	require.Equal(t, 1, target.RowCount())
	require.Len(t, target.Vecs, 2)
	require.Equal(t, int64(1), vector.GetFixedAtWithTypeCheck[int64](target.Vecs[0], 0))
	require.Equal(t, int64(2), vector.GetFixedAtWithTypeCheck[int64](target.Vecs[1], 0))
	target.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestBatchUnmarshalSeparatesAliasedReuseVectors(t *testing.T) {
	for _, columnCount := range []int{2, 3, 64, 65} {
		t.Run(fmt.Sprintf("%d_columns", columnCount), func(t *testing.T) {
			mp := mpool.MustNewZero()
			source := NewWithSize(columnCount)
			for i := range source.Vecs {
				source.Vecs[i] = vector.NewVec(types.T_int64.ToType(), mp)
				require.NoError(t, vector.AppendFixed(source.Vecs[i], int64(i+1), false, mp))
			}
			source.SetRowCount(1)
			encoded, err := source.MarshalBinary()
			require.NoError(t, err)
			source.Clean(mp)

			target := NewWithSize(columnCount)
			shared := vector.NewVec(types.T_int64.ToType(), mp)
			require.NoError(t, vector.AppendFixed(shared, int64(-1), false, mp))
			for i := range target.Vecs {
				target.Vecs[i] = shared
			}
			t.Cleanup(func() {
				target.Clean(mp)
				require.Equal(t, int64(0), mp.CurrNB())
			})

			require.NoError(t, target.UnmarshalBinaryWithAnyMp(encoded, mp))
			for i := range target.Vecs {
				require.Equal(t, int64(i+1), vector.GetFixedAtWithTypeCheck[int64](target.Vecs[i], 0))
				if i > 0 {
					require.NotSame(t, target.Vecs[i-1], target.Vecs[i])
				}
			}
		})
	}
}

func TestBatchUnmarshalSeparatesBorrowedAliasedReuseVectorsWithoutMpool(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(2)
	for i := range source.Vecs {
		source.Vecs[i] = vector.NewVec(types.T_int64.ToType(), mp)
		require.NoError(t, vector.AppendFixed(source.Vecs[i], int64(i+1), false, mp))
	}
	source.SetRowCount(1)
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Clean(mp)

	seed := vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixed(seed, int64(-1), false, mp))
	seedData, err := seed.MarshalBinary()
	require.NoError(t, err)
	seed.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	shared := new(vector.Vector)
	require.NoError(t, shared.UnmarshalBinary(seedData))
	target := NewWithSize(2)
	target.Vecs[0] = shared
	target.Vecs[1] = shared
	t.Cleanup(func() {
		target.Clean(nil)
	})

	require.NoError(t, target.UnmarshalBinary(encoded))
	require.NotSame(t, target.Vecs[0], target.Vecs[1])
	require.Equal(t, int64(1), vector.GetFixedAtWithTypeCheck[int64](target.Vecs[0], 0))
	require.Equal(t, int64(2), vector.GetFixedAtWithTypeCheck[int64](target.Vecs[1], 0))
}

func TestBatchShrink(t *testing.T) {
	bat := newBatch([]types.Type{types.T_int8.ToType()}, 4)
	mp := bat.Vecs[0].MPool()
	require.NoError(t, bat.Shrink([]int64{0}, true, mp))
	require.Equal(t, 3, bat.rowCount)
	require.NoError(t, bat.Shrink([]int64{0, 2}, false, mp))
	require.Equal(t, 2, bat.rowCount)
}

func TestBatch_ReplaceVector(t *testing.T) {
	v1, v2, v3 := new(vector.Vector), new(vector.Vector), new(vector.Vector)
	bat := &Batch{
		Vecs: []*vector.Vector{
			v1,
			v1,
			v1,
			v2,
			v2,
		},
	}
	bat.ReplaceVector(bat.Vecs[0], v3, 0)
	require.Equal(t, v3, bat.Vecs[0])
	require.Equal(t, v3, bat.Vecs[1])
	require.Equal(t, v3, bat.Vecs[2])
	require.Equal(t, v2, bat.Vecs[3])
}

func newTestCase(ts []types.Type) batchTestCase {
	return batchTestCase{
		types: ts,
		bat:   newBatch(ts, Rows),
	}
}

// create a new block based on the type information, flgs[i] == ture: has null
func newBatch(ts []types.Type, rows int) *Batch {
	mp := mpool.MustNewZero()
	bat := NewWithSize(len(ts))
	bat.SetRowCount(rows)
	for i, typ := range ts {
		switch typ.Oid {
		case types.T_int8:
			vec := vector.NewVec(typ, mp)
			err := vec.PreExtend(rows, mp)
			if err != nil {
				panic(err)
			}
			vec.SetLength(rows)
			vs := vector.MustFixedColWithTypeCheck[int8](vec)
			for j := range vs {
				vs[j] = int8(j)
			}
			bat.Vecs[i] = vec
		}
	}

	bat.extraBuf = []byte("extra buf")
	return bat
}

func TestBatch_UnionOne(t *testing.T) {
	mp := mpool.MustNewZero()

	bat1 := NewWithSize(2)
	bat1.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType(), mp)

	bat2 := NewWithSize(2)
	bat2.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	bat2.Vecs[1] = vector.NewVec(types.T_int32.ToType(), mp)

	for i := 0; i < 100; i++ {
		vector.AppendFixed[int32](bat2.Vecs[0], int32(i), false, mp)
		vector.AppendFixed[int32](bat2.Vecs[1], int32(i*2), false, mp)
	}
	bat2.SetRowCount(bat2.Vecs[0].Length())

	for i := 0; i < bat2.RowCount(); i++ {
		require.Nil(t, bat1.UnionOne(bat2, int64(i), mp))
	}

	require.Equal(t, bat1.RowCount(), bat2.RowCount())
	row1 := vector.MustFixedColNoTypeCheck[int32](bat1.Vecs[0])
	row2 := vector.MustFixedColNoTypeCheck[int32](bat2.Vecs[0])
	require.Equal(t, row1, row2)

	row1 = vector.MustFixedColNoTypeCheck[int32](bat1.Vecs[1])
	row2 = vector.MustFixedColNoTypeCheck[int32](bat2.Vecs[1])
	require.Equal(t, row1, row2)
}

func requireStringValues(t *testing.T, vec *vector.Vector, values ...string) {
	t.Helper()
	require.Equal(t, len(values), vec.Length())
	for row, value := range values {
		require.Equal(t, value, vec.GetStringAt(row), "row %d", row)
	}
}

// A record with bytes after the batch is rejected, by both decoders, and the
// receiver decodes the next valid record.
func TestBatchDecodersRejectTrailingBytesAndReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(2)
	source.Vecs[0] = vector.NewVec(types.T_text.ToType(), mp)
	source.Vecs[1] = vector.NewVec(types.T_int64.ToType(), mp)
	for i, value := range []string{"1.5", "plain", "9.5"} {
		require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte(value), false, mp))
		require.NoError(t, vector.AppendFixed(source.Vecs[1], int64(i+1), false, mp))
	}
	source.SetRowCount(3)
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	encoded = append([]byte(nil), encoded...)
	source.Clean(mp)
	trailing := append(append([]byte(nil), encoded...), 0)

	for name, decode := range map[string]func(*Batch, []byte) error{
		"bytes": func(b *Batch, data []byte) error { return b.UnmarshalBinaryWithAnyMp(data, mp) },
		"sized": func(b *Batch, data []byte) error {
			return b.UnmarshalSizedFrom(bytes.NewReader(data), int64(len(data)), mp)
		},
	} {
		t.Run(name, func(t *testing.T) {
			target := NewWithSize(0)
			require.NoError(t, decode(target, encoded))
			require.ErrorContains(t, decode(target, trailing), "trailing bytes")
			require.NoError(t, decode(target, encoded))
			requireStringValues(t, target.Vecs[0], "1.5", "plain", "9.5")
			require.Equal(t, int64(3), vector.GetFixedAtWithTypeCheck[int64](target.Vecs[1], 2))
			target.Clean(mp)
		})
	}
	require.Zero(t, mp.CurrNB())
}

func TestShrinkFailureLeavesRowCountUnpublished(t *testing.T) {
	for _, mask := range []bool{false, true} {
		t.Run(fmt.Sprintf("mask=%t", mask), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			bat := NewWithSize(2)
			t.Cleanup(func() { bat.Clean(mp) })
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
			require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int64{10, 20, 30}, nil, mp))
			values := []int64{1, 2, 3}
			data := types.EncodeSlice(values)
			var err error
			bat.Vecs[1], err = vector.NewVecWithData(types.T_int64.ToType(), len(values), data, nil, nil), error(nil)
			require.NoError(t, err)
			bat.SetRowCount(3)
			if mask {
				var sels bitmap.Bitmap
				require.NoError(t, sels.InitWithSize(3, mp))
				t.Cleanup(sels.Free)
				sels.AddMany([]uint64{1, 2})
				require.Error(t, bat.ShrinkByMask(&sels, false, 0, mp))
			} else {
				require.Error(t, bat.Shrink([]int64{1, 2}, false, mp))
			}
			require.Equal(t, 3, bat.RowCount())
			require.Equal(t, []int64{20, 30}, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0]))
			require.Equal(t, values, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[1]))
		})
	}
}

func TestBatchShrinkByMaskRowCount(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sels   []uint64
		negate bool
		want   []int64
	}{
		{"all rows", []uint64{0, 1, 2}, false, []int64{10, 20, 30}},
		{"selection", []uint64{1, 2}, false, []int64{20, 30}},
		{"negation", []uint64{1}, true, []int64{10, 30}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			bat := NewWithSize(1)
			t.Cleanup(func() { bat.Clean(mp) })
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
			require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int64{10, 20, 30}, nil, mp))
			bat.SetRowCount(3)
			var sels bitmap.Bitmap
			require.NoError(t, sels.InitWithSize(3, mp))
			t.Cleanup(sels.Free)
			sels.AddMany(tc.sels)
			require.NoError(t, bat.ShrinkByMask(&sels, tc.negate, 0, mp))
			require.Equal(t, len(tc.want), bat.RowCount())
			require.Equal(t, tc.want, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0]))
		})
	}
}

// When a later Dup fails, OwnReadOnlyVectors still frees the read-only
// originals it already replaced; one of them owns its nulls bitmap.
func TestOwnReadOnlyVectorsLeavesBatchOnDupError(t *testing.T) {
	srcMP := mpool.MustNewZero()
	nspMP := mpool.MustNewZero()
	dstMP, err := mpool.NewMPool("own_read_only_cap", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(dstMP)

	small := vector.NewVec(types.T_int64.ToType(), srcMP)
	for i := 0; i < 8; i++ {
		require.NoError(t, vector.AppendFixed(small, int64(i), i%2 == 0, srcMP))
	}
	// Larger than dstMP's 1 MB cap, so its Dup fails.
	big := vector.NewVec(types.T_int64.ToType(), srcMP)
	for i := 0; i < 200_000; i++ {
		require.NoError(t, vector.AppendFixed(big, int64(i), false, srcMP))
	}
	w1, err := small.Window(0, 8)
	require.NoError(t, err)
	require.NoError(t, w1.DetachNulls(nspMP))
	require.Greater(t, nspMP.CurrNB(), int64(0))
	w2, err := big.Window(0, big.Length())
	require.NoError(t, err)

	// w1 sits in two columns, around the column whose copy fails.
	bat := NewWithSize(3)
	bat.Vecs[0], bat.Vecs[1], bat.Vecs[2] = w1, w2, w1
	require.Error(t, bat.OwnReadOnlyVectors(dstMP))

	require.Same(t, w1, bat.Vecs[0], "a failed call leaves the batch unchanged")
	require.Same(t, w2, bat.Vecs[1])
	require.Same(t, w1, bat.Vecs[2])
	require.Equal(t, 8, w1.Length(), "the aliased original stays intact")
	require.True(t, w1.GetNulls().Contains(0))
	require.Zero(t, dstMP.CurrNB(), "the copies already made are freed")
	w1.Free(nil)
	require.Zero(t, nspMP.CurrNB())
	small.Free(srcMP)
	big.Free(srcMP)
	require.Zero(t, srcMP.CurrNB())
}

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
