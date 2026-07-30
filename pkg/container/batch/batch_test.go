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
	"testing"

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

		rbat := new(Batch)
		err = rbat.UnmarshalBinary(data)
		require.NoError(t, err)

		require.Equal(t, tc.bat.ExtraBuf, rbat.ExtraBuf)

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
	source.Attrs = []string{"value"}
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
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
			target := NewOffHeapEmpty()
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

		target := NewOffHeapEmpty()
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

		target := NewOffHeapEmpty()
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
		source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
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

		target := NewOffHeapEmpty()
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
		source.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(source.Vecs[0], bytes.Repeat([]byte("x"), types.VarlenaInlineSize+1), false, mp))
		source.SetRowCount(1)
		corrupted, err := source.MarshalBinary()
		require.NoError(t, err)
		source.Clean(mp)

		vectorDataOffset := 16
		varlenaOffset := vectorDataOffset + 1 + types.TSize + 4 + 4
		invalidAreaOffset := uint32(len(corrupted))
		copy(corrupted[varlenaOffset+4:varlenaOffset+8], types.EncodeUint32(&invalidAreaOffset))

		target := NewOffHeapEmpty()
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
		target := NewOffHeapWithSize(1)
		target.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
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
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	source.SetRowCount(1)
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Clean(mp)

	target := NewOffHeapEmpty()
	require.NoError(t, target.UnmarshalBinaryWithAnyMp(encoded, mp))
	require.Equal(t, 1, target.RowCount())
	require.Zero(t, target.Vecs[0].Length())
	target.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestBatchUnmarshalPreservesShortNonEmptyVector(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_binary.ToType())
	require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte("object stats"), false, mp))
	source.SetRowCount(6)
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Clean(mp)

	target := NewOffHeapEmpty()
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
		source.Vecs[i] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(source.Vecs[i], int64(i+1), false, mp))
	}
	source.SetRowCount(1)
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Clean(mp)

	target := NewOffHeapWithSize(1)
	target.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(target.Vecs[0], int64(-1), false, mp))

	require.NoError(t, target.UnmarshalBinaryWithAnyMp(encoded, mp))
	require.Equal(t, 1, target.RowCount())
	require.Len(t, target.Vecs, 2)
	require.Equal(t, int64(1), vector.GetFixedAtWithTypeCheck[int64](target.Vecs[0], 0))
	require.Equal(t, int64(2), vector.GetFixedAtWithTypeCheck[int64](target.Vecs[1], 0))
	target.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestBatchUnmarshalRejectsOwnedVectorCountChangeWithoutMpool(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(2)
	for i := range source.Vecs {
		source.Vecs[i] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(source.Vecs[i], int64(i+1), false, mp))
	}
	source.SetRowCount(1)
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Clean(mp)

	target := NewWithSize(1)
	target.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(target.Vecs[0], int64(-1), false, mp))
	t.Cleanup(func() {
		target.Clean(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	})

	var unmarshalErr error
	require.NotPanics(t, func() {
		unmarshalErr = target.UnmarshalBinary(encoded)
	})
	require.Error(t, unmarshalErr)
}

func TestBatchUnmarshalSeparatesAliasedReuseVectors(t *testing.T) {
	for _, columnCount := range []int{2, 3} {
		t.Run(fmt.Sprintf("%d_columns", columnCount), func(t *testing.T) {
			mp := mpool.MustNewZero()
			source := NewWithSize(columnCount)
			for i := range source.Vecs {
				source.Vecs[i] = vector.NewVec(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixed(source.Vecs[i], int64(i+1), false, mp))
			}
			source.SetRowCount(1)
			encoded, err := source.MarshalBinary()
			require.NoError(t, err)
			source.Clean(mp)

			target := NewOffHeapWithSize(columnCount)
			shared := vector.NewOffHeapVecWithType(types.T_int64.ToType())
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
		source.Vecs[i] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(source.Vecs[i], int64(i+1), false, mp))
	}
	source.SetRowCount(1)
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Clean(mp)

	seed := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(seed, int64(-1), false, mp))
	seedData, err := seed.MarshalBinary()
	require.NoError(t, err)
	seed.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	shared := vector.NewVecFromReuse()
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
	bat.Shrink([]int64{0}, true)
	require.Equal(t, 3, bat.rowCount)
	bat.Shrink([]int64{0, 2}, false)
	require.Equal(t, 2, bat.rowCount)
}

func TestBatch_ReplaceVector(t *testing.T) {
	v1, v2, v3 := vector.NewVecFromReuse(), vector.NewVecFromReuse(), vector.NewVecFromReuse()
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
			vec := vector.NewVec(typ)
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

	bat.ExtraBuf = []byte("extra buf")
	bat.Attrs = []string{"1"}
	return bat
}

func TestBatch_UnionOne(t *testing.T) {
	mp := mpool.MustNewZero()

	bat1 := NewWithSize(2)
	bat1.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	bat2 := NewWithSize(2)
	bat2.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_int32.ToType())

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

// TestBatchUnmarshalWithAnyMp_Bug23156 tests the fix for bug #23156
// This test verifies that Vecs and Attrs length remain consistent when batch is reused
// The bug occurred when batch was reused with different Attrs/Vecs configurations,
// causing data mapping errors in UPDATE statements
func TestBatchUnmarshalWithAnyMp_Bug23156(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create first batch: DELETE tombstone data
	// Vecs: [rowid_vec, pk_vec], Attrs: ["rowid", "pk"]
	bat1 := NewWithSize(2)
	bat1.Attrs = []string{"rowid", "pk"}
	bat1.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	bat1.SetRowCount(0)

	// Create second batch: INSERT block info data
	// Vecs: [block_info_vec, object_stats_vec], Attrs: ["block_info", "object_stats"]
	bat2 := NewWithSize(2)
	bat2.Attrs = []string{"block_info", "object_stats"}
	bat2.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	bat2.SetRowCount(0)

	// Marshal both batches
	data1, err := bat1.MarshalBinary()
	require.NoError(t, err)
	data2, err := bat2.MarshalBinary()
	require.NoError(t, err)

	// Clean up original batches
	bat1.Clean(mp)
	bat2.Clean(mp)

	// Reuse the same batch object (simulating UPDATE scenario)
	reusedBat := &Batch{}
	reusedBat.offHeap = false

	// First unmarshal: DELETE tombstone data
	err = reusedBat.UnmarshalBinaryWithAnyMp(data1, mp)
	require.NoError(t, err)
	require.Equal(t, 2, len(reusedBat.Vecs), "Vecs length should be 2 after first unmarshal")
	require.Equal(t, 2, len(reusedBat.Attrs), "Attrs length should be 2 after first unmarshal")
	require.Equal(t, len(reusedBat.Vecs), len(reusedBat.Attrs), "Vecs and Attrs should have same length")
	require.Equal(t, "rowid", reusedBat.Attrs[0])
	require.Equal(t, "pk", reusedBat.Attrs[1])

	// Second unmarshal: INSERT block info data (reusing the same batch object)
	// This is the critical test - the batch object is reused
	err = reusedBat.UnmarshalBinaryWithAnyMp(data2, mp)
	require.NoError(t, err)
	require.Equal(t, 2, len(reusedBat.Vecs), "Vecs length should be 2 after second unmarshal")
	require.Equal(t, 2, len(reusedBat.Attrs), "Attrs length should be 2 after second unmarshal")
	require.Equal(t, len(reusedBat.Vecs), len(reusedBat.Attrs), "Vecs and Attrs must have same length after reuse")
	require.Equal(t, "block_info", reusedBat.Attrs[0], "Attrs[0] should be updated correctly")
	require.Equal(t, "object_stats", reusedBat.Attrs[1], "Attrs[1] should be updated correctly")

	// Clean up
	reusedBat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestBatchUnmarshalWithAnyMp_VecsAttrsLengthMismatch tests handling of normal case
// where Vecs and Attrs should have the same length
func TestBatchUnmarshalWithAnyMp_VecsAttrsLengthMismatch(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create a batch with Vecs length = 3, Attrs length = 3 (normal case)
	bat := NewWithSize(3)
	bat.Attrs = []string{"col1", "col2", "col3"}
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())
	bat.SetRowCount(0)

	// Marshal it
	data, err := bat.MarshalBinary()
	require.NoError(t, err)
	bat.Clean(mp)

	// Test unmarshal
	normalBat := &Batch{}
	err = normalBat.UnmarshalBinaryWithAnyMp(data, mp)
	require.NoError(t, err)
	require.Equal(t, 3, len(normalBat.Vecs), "Vecs length should be 3")
	require.Equal(t, 3, len(normalBat.Attrs), "Attrs length should be 3")
	require.Equal(t, len(normalBat.Vecs), len(normalBat.Attrs), "Vecs and Attrs must have same length")
	require.Equal(t, "col1", normalBat.Attrs[0])
	require.Equal(t, "col2", normalBat.Attrs[1])
	require.Equal(t, "col3", normalBat.Attrs[2])

	normalBat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestBatchUnmarshalWithAnyMp_ReuseDifferentLengths tests batch reuse with different lengths
// This is the key test case that captures the original bug
func TestBatchUnmarshalWithAnyMp_ReuseDifferentLengths(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create first batch: 3 Vecs, 3 Attrs
	bat1 := NewWithSize(3)
	bat1.Attrs = []string{"col1", "col2", "col3"}
	bat1.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat1.Vecs[2] = vector.NewVec(types.T_int32.ToType())
	bat1.SetRowCount(0)

	// Create second batch: 2 Vecs, 2 Attrs (different length)
	bat2 := NewWithSize(2)
	bat2.Attrs = []string{"attr1", "attr2"}
	bat2.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	bat2.SetRowCount(0)

	// Create third batch: 3 Vecs, 3 Attrs (back to original length)
	bat3 := NewWithSize(3)
	bat3.Attrs = []string{"x", "y", "z"}
	bat3.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat3.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat3.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	bat3.SetRowCount(0)

	// Marshal all
	data1, err := bat1.MarshalBinary()
	require.NoError(t, err)
	data2, err := bat2.MarshalBinary()
	require.NoError(t, err)
	data3, err := bat3.MarshalBinary()
	require.NoError(t, err)

	// Reuse the same batch object multiple times
	reusedBat := &Batch{}
	reusedBat.offHeap = false

	// First unmarshal: 3 Vecs, 3 Attrs
	err = reusedBat.UnmarshalBinaryWithAnyMp(data1, mp)
	require.NoError(t, err)
	require.Equal(t, 3, len(reusedBat.Vecs))
	require.Equal(t, 3, len(reusedBat.Attrs))
	require.Equal(t, len(reusedBat.Vecs), len(reusedBat.Attrs))
	require.Equal(t, "col1", reusedBat.Attrs[0])
	require.Equal(t, "col2", reusedBat.Attrs[1])
	require.Equal(t, "col3", reusedBat.Attrs[2])

	// Second unmarshal: 2 Vecs, 2 Attrs (different length - this is the critical case)
	err = reusedBat.UnmarshalBinaryWithAnyMp(data2, mp)
	require.NoError(t, err)
	require.Equal(t, 2, len(reusedBat.Vecs), "Vecs length should change to 2")
	require.Equal(t, 2, len(reusedBat.Attrs), "Attrs length should change to 2")
	require.Equal(t, len(reusedBat.Vecs), len(reusedBat.Attrs), "Vecs and Attrs must have same length")
	require.Equal(t, "attr1", reusedBat.Attrs[0])
	require.Equal(t, "attr2", reusedBat.Attrs[1])

	// Third unmarshal: 3 Vecs, 3 Attrs (back to original length)
	err = reusedBat.UnmarshalBinaryWithAnyMp(data3, mp)
	require.NoError(t, err)
	require.Equal(t, 3, len(reusedBat.Vecs), "Vecs length should change back to 3")
	require.Equal(t, 3, len(reusedBat.Attrs), "Attrs length should change back to 3")
	require.Equal(t, len(reusedBat.Vecs), len(reusedBat.Attrs), "Vecs and Attrs must have same length")
	require.Equal(t, "x", reusedBat.Attrs[0])
	require.Equal(t, "y", reusedBat.Attrs[1])
	require.Equal(t, "z", reusedBat.Attrs[2])

	// Clean up
	reusedBat.Clean(mp)
	bat1.Clean(mp)
	bat2.Clean(mp)
	bat3.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestBatchUnmarshalWithAnyMp_Bug21911 tests the fix for bug #21911
// This test verifies that Vecs length changes are handled correctly without panic
// The bug was: panic runtime error: index out of range [1] with length 1
// This occurred when Vecs length changed but the code tried to access vecs[i] beyond the old length
func TestBatchUnmarshalWithAnyMp_Bug21911(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create first batch: 1 Vec, 1 Attr
	bat1 := NewWithSize(1)
	bat1.Attrs = []string{"col1"}
	bat1.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat1.SetRowCount(0)

	// Create second batch: 2 Vecs, 2 Attrs (length increases - this is the critical case for #21911)
	bat2 := NewWithSize(2)
	bat2.Attrs = []string{"attr1", "attr2"}
	bat2.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_float64.ToType())
	bat2.SetRowCount(0)

	// Marshal both
	data1, err := bat1.MarshalBinary()
	require.NoError(t, err)
	data2, err := bat2.MarshalBinary()
	require.NoError(t, err)

	// Clean up original batches
	bat1.Clean(mp)
	bat2.Clean(mp)

	// Reuse the same batch object
	reusedBat := &Batch{}
	reusedBat.offHeap = false

	// First unmarshal: 1 Vec, 1 Attr
	err = reusedBat.UnmarshalBinaryWithAnyMp(data1, mp)
	require.NoError(t, err)
	require.Equal(t, 1, len(reusedBat.Vecs), "Vecs length should be 1 after first unmarshal")
	require.Equal(t, 1, len(reusedBat.Attrs), "Attrs length should be 1 after first unmarshal")
	require.Equal(t, len(reusedBat.Vecs), len(reusedBat.Attrs))
	require.Equal(t, "col1", reusedBat.Attrs[0])

	// Second unmarshal: 2 Vecs, 2 Attrs (length increases - this should not panic)
	// The bug #21911 occurred because Vecs was accessed beyond its old length
	err = reusedBat.UnmarshalBinaryWithAnyMp(data2, mp)
	require.NoError(t, err, "Should not panic when Vecs length increases")
	require.Equal(t, 2, len(reusedBat.Vecs), "Vecs length should change to 2")
	require.Equal(t, 2, len(reusedBat.Attrs), "Attrs length should change to 2")
	require.Equal(t, len(reusedBat.Vecs), len(reusedBat.Attrs), "Vecs and Attrs must have same length")
	require.Equal(t, "attr1", reusedBat.Attrs[0])
	require.Equal(t, "attr2", reusedBat.Attrs[1])

	// Clean up
	reusedBat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestBatchUnmarshalWithAnyMp_WithCleanOnlyData tests batch reuse with CleanOnlyData
// This simulates the actual UPDATE scenario where CleanOnlyData() is called before unmarshal
func TestBatchUnmarshalWithAnyMp_WithCleanOnlyData(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create first batch: DELETE tombstone data
	bat1 := NewWithSize(2)
	bat1.Attrs = []string{"rowid", "pk"}
	bat1.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	bat1.SetRowCount(0)

	// Create second batch: INSERT block info data
	bat2 := NewWithSize(2)
	bat2.Attrs = []string{"block_info", "object_stats"}
	bat2.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	bat2.SetRowCount(0)

	// Marshal both
	data1, err := bat1.MarshalBinary()
	require.NoError(t, err)
	data2, err := bat2.MarshalBinary()
	require.NoError(t, err)

	// Clean up original batches
	bat1.Clean(mp)
	bat2.Clean(mp)

	// Reuse the same batch object
	reusedBat := &Batch{}
	reusedBat.offHeap = false

	// First unmarshal: DELETE tombstone data
	err = reusedBat.UnmarshalBinaryWithAnyMp(data1, mp)
	require.NoError(t, err)
	require.Equal(t, 2, len(reusedBat.Vecs))
	require.Equal(t, 2, len(reusedBat.Attrs))
	require.Equal(t, "rowid", reusedBat.Attrs[0])
	require.Equal(t, "pk", reusedBat.Attrs[1])

	// Simulate CleanOnlyData() call (as done in multi_update.go)
	reusedBat.CleanOnlyData()

	// Second unmarshal: INSERT block info data (reusing the same batch object)
	// This is the critical test - the batch object is reused after CleanOnlyData()
	err = reusedBat.UnmarshalBinaryWithAnyMp(data2, mp)
	require.NoError(t, err)
	require.Equal(t, 2, len(reusedBat.Vecs), "Vecs length should be 2 after second unmarshal")
	require.Equal(t, 2, len(reusedBat.Attrs), "Attrs length should be 2 after second unmarshal")
	require.Equal(t, len(reusedBat.Vecs), len(reusedBat.Attrs), "Vecs and Attrs must have same length")
	require.Equal(t, "block_info", reusedBat.Attrs[0], "Attrs[0] should be updated correctly")
	require.Equal(t, "object_stats", reusedBat.Attrs[1], "Attrs[1] should be updated correctly")

	// Clean up
	reusedBat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestBatchUnmarshalWithAnyMp_OffHeap tests batch reuse with offHeap vectors
func TestBatchUnmarshalWithAnyMp_OffHeap(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create first batch: 2 Vecs, 2 Attrs
	bat1 := NewWithSize(2)
	bat1.Attrs = []string{"col1", "col2"}
	bat1.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat1.SetRowCount(0)

	// Create second batch: 3 Vecs, 3 Attrs (different length)
	bat2 := NewWithSize(3)
	bat2.Attrs = []string{"attr1", "attr2", "attr3"}
	bat2.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat2.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	bat2.SetRowCount(0)

	// Marshal both
	data1, err := bat1.MarshalBinary()
	require.NoError(t, err)
	data2, err := bat2.MarshalBinary()
	require.NoError(t, err)

	// Clean up original batches
	bat1.Clean(mp)
	bat2.Clean(mp)

	// Reuse the same batch object with offHeap
	reusedBat := &Batch{}
	reusedBat.offHeap = true

	// First unmarshal: 2 Vecs, 2 Attrs
	err = reusedBat.UnmarshalBinaryWithAnyMp(data1, mp)
	require.NoError(t, err)
	require.Equal(t, 2, len(reusedBat.Vecs))
	require.Equal(t, 2, len(reusedBat.Attrs))
	require.Equal(t, len(reusedBat.Vecs), len(reusedBat.Attrs))
	require.Equal(t, "col1", reusedBat.Attrs[0])
	require.Equal(t, "col2", reusedBat.Attrs[1])

	// Second unmarshal: 3 Vecs, 3 Attrs (length changes)
	err = reusedBat.UnmarshalBinaryWithAnyMp(data2, mp)
	require.NoError(t, err)
	require.Equal(t, 3, len(reusedBat.Vecs), "Vecs length should change to 3")
	require.Equal(t, 3, len(reusedBat.Attrs), "Attrs length should change to 3")
	require.Equal(t, len(reusedBat.Vecs), len(reusedBat.Attrs), "Vecs and Attrs must have same length")
	require.Equal(t, "attr1", reusedBat.Attrs[0])
	require.Equal(t, "attr2", reusedBat.Attrs[1])
	require.Equal(t, "attr3", reusedBat.Attrs[2])

	// Clean up
	reusedBat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestBatchUnmarshalWithAnyMp_FirstTime tests the first time unmarshal (nil Vecs)
func TestBatchUnmarshalWithAnyMp_FirstTime(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create batch: 3 Vecs, 3 Attrs
	bat := NewWithSize(3)
	bat.Attrs = []string{"a", "b", "c"}
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())
	bat.SetRowCount(0)

	// Marshal
	data, err := bat.MarshalBinary()
	require.NoError(t, err)
	bat.Clean(mp)

	// First time unmarshal (nil Vecs)
	newBat := &Batch{}
	newBat.offHeap = false
	err = newBat.UnmarshalBinaryWithAnyMp(data, mp)
	require.NoError(t, err)
	require.Equal(t, 3, len(newBat.Vecs), "Vecs length should be 3")
	require.Equal(t, 3, len(newBat.Attrs), "Attrs length should be 3")
	require.Equal(t, len(newBat.Vecs), len(newBat.Attrs), "Vecs and Attrs must have same length")
	require.Equal(t, "a", newBat.Attrs[0])
	require.Equal(t, "b", newBat.Attrs[1])
	require.Equal(t, "c", newBat.Attrs[2])

	// Clean up
	newBat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestBatchUnmarshalWithAnyMp_SameLengthReuse tests reuse with same Vecs/Attrs length
// but different content (common scenario in UPDATE)
func TestBatchUnmarshalWithAnyMp_SameLengthReuse(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create first batch: 2 Vecs, 2 Attrs
	bat1 := NewWithSize(2)
	bat1.Attrs = []string{"old1", "old2"}
	bat1.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat1.SetRowCount(0)

	// Create second batch: 2 Vecs, 2 Attrs (same length, different content)
	bat2 := NewWithSize(2)
	bat2.Attrs = []string{"new1", "new2"}
	bat2.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat2.SetRowCount(0)

	// Marshal both
	data1, err := bat1.MarshalBinary()
	require.NoError(t, err)
	data2, err := bat2.MarshalBinary()
	require.NoError(t, err)

	// Clean up original batches
	bat1.Clean(mp)
	bat2.Clean(mp)

	// Reuse the same batch object
	reusedBat := &Batch{}
	reusedBat.offHeap = false

	// First unmarshal
	err = reusedBat.UnmarshalBinaryWithAnyMp(data1, mp)
	require.NoError(t, err)
	require.Equal(t, 2, len(reusedBat.Vecs))
	require.Equal(t, 2, len(reusedBat.Attrs))
	require.Equal(t, "old1", reusedBat.Attrs[0])
	require.Equal(t, "old2", reusedBat.Attrs[1])

	// Second unmarshal: same length, different content
	// This tests that Attrs content is properly updated even when length doesn't change
	err = reusedBat.UnmarshalBinaryWithAnyMp(data2, mp)
	require.NoError(t, err)
	require.Equal(t, 2, len(reusedBat.Vecs), "Vecs length should remain 2")
	require.Equal(t, 2, len(reusedBat.Attrs), "Attrs length should remain 2")
	require.Equal(t, len(reusedBat.Vecs), len(reusedBat.Attrs), "Vecs and Attrs must have same length")
	require.Equal(t, "new1", reusedBat.Attrs[0], "Attrs[0] should be updated to new1")
	require.Equal(t, "new2", reusedBat.Attrs[1], "Attrs[1] should be updated to new2")

	// Clean up
	reusedBat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestBatchUnmarshalWithAnyMp_SerializedLengthMismatch tests handling of inconsistent serialized data
// where serialized Vecs length != serialized Attrs length (can occur in practice)
// The fix ensures Attrs length always matches Vecs length, using Vecs length as authoritative
func TestBatchUnmarshalWithAnyMp_SerializedLengthMismatch(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create a normal batch: 2 Vecs, 2 Attrs
	bat := NewWithSize(2)
	bat.Attrs = []string{"col1", "col2"}
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.SetRowCount(2)
	vector.AppendFixed(bat.Vecs[0], int32(1), false, mp)
	vector.AppendFixed(bat.Vecs[0], int32(2), false, mp)
	vector.AppendFixed(bat.Vecs[1], int32(10), false, mp)
	vector.AppendFixed(bat.Vecs[1], int32(20), false, mp)

	// Marshal it
	data, err := bat.MarshalBinary()
	require.NoError(t, err)

	// Manually modify the serialized data to create inconsistency:
	// Change Attrs length from 2 to 3 in the serialized data
	// Format: | rowCount(8) | VecsLen(4) | Vecs... | AttrsLen(4) | Attrs... | ...
	offset := 8 + 4 // skip rowCount and VecsLen
	// Skip Vecs data to find AttrsLen position
	for i := 0; i < 2; i++ {
		vecSize := types.DecodeInt32(data[offset:])
		offset += 4 + int(vecSize)
	}
	// Now offset points to AttrsLen
	// Change it from 2 to 3 and add an extra attr entry
	three := int32(3)
	attrsLenBytes := types.EncodeInt32(&three)
	newData := make([]byte, 0, len(data)+13)
	newData = append(newData, data[:offset]...)
	newData = append(newData, attrsLenBytes...)
	// Keep existing two attrs, then add third
	offset += 4
	// Copy existing two attrs
	attrsDataOffset := offset
	for i := 0; i < 2; i++ {
		attrSize := types.DecodeInt32(data[attrsDataOffset:])
		newData = append(newData, data[attrsDataOffset:attrsDataOffset+4+int(attrSize)]...)
		attrsDataOffset += 4 + int(attrSize)
	}
	// Add third attr
	extraAttrSize := int32(5) // "extra" is 5 bytes
	newData = append(newData, types.EncodeInt32(&extraAttrSize)...)
	newData = append(newData, []byte("extra")...)
	// Copy rest of data
	newData = append(newData, data[attrsDataOffset:]...)
	data = newData

	// Clean up original batch
	bat.Clean(mp)

	// Unmarshal the inconsistent data
	// Vecs length = 2 (from serialized data), but serialized Attrs length = 3
	// The fix should ensure Attrs length matches Vecs length (2), ignoring the extra Attr
	testBat := &Batch{}
	testBat.offHeap = false
	err = testBat.UnmarshalBinaryWithAnyMp(data, mp)
	require.NoError(t, err)

	// Attrs length should match Vecs length (2), not serialized Attrs length (3)
	require.Equal(t, 2, len(testBat.Vecs), "Vecs length should be 2")
	require.Equal(t, 2, len(testBat.Attrs), "Attrs length should match Vecs length (2), ignoring serialized length (3)")
	require.Equal(t, len(testBat.Vecs), len(testBat.Attrs), "Vecs and Attrs must have same length")
	require.Equal(t, "col1", testBat.Attrs[0], "First attr should be correct")
	require.Equal(t, "col2", testBat.Attrs[1], "Second attr should be correct")

	// Clean up
	testBat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestBatchUnmarshalWithAnyMp_SerializedAttrsLenLessThanVecsLen tests the case where
// serialized Attrs length < Vecs length (data inconsistency)
// This ensures remaining Attrs are cleared to prevent stale values
func TestBatchUnmarshalWithAnyMp_SerializedAttrsLenLessThanVecsLen(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create a batch: 3 Vecs, 3 Attrs
	bat := NewWithSize(3)
	bat.Attrs = []string{"col1", "col2", "col3"}
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())
	bat.SetRowCount(0)

	// Marshal it
	data, err := bat.MarshalBinary()
	require.NoError(t, err)

	// Manually modify the serialized data to create inconsistency:
	// Change Attrs length from 3 to 2 in the serialized data
	// Format: | rowCount(8) | VecsLen(4) | Vecs... | AttrsLen(4) | Attrs... | ...
	offset := 8 + 4 // skip rowCount and VecsLen
	// Skip Vecs data to find AttrsLen position
	for i := 0; i < 3; i++ {
		vecSize := types.DecodeInt32(data[offset:])
		offset += 4 + int(vecSize)
	}
	// Now offset points to AttrsLen
	// Change it from 3 to 2
	two := int32(2)
	attrsLenBytes := types.EncodeInt32(&two)
	newData := make([]byte, 0, len(data))
	newData = append(newData, data[:offset]...)
	newData = append(newData, attrsLenBytes...)
	// Copy only first two attrs
	offset += 4
	attrsDataOffset := offset
	for i := 0; i < 2; i++ {
		attrSize := types.DecodeInt32(data[attrsDataOffset:])
		newData = append(newData, data[attrsDataOffset:attrsDataOffset+4+int(attrSize)]...)
		attrsDataOffset += 4 + int(attrSize)
	}
	// Skip the third attr (size + content) and copy rest of data
	thirdAttrSize := types.DecodeInt32(data[attrsDataOffset:])
	attrsDataOffset += 4 + int(thirdAttrSize) // Skip third attr completely
	newData = append(newData, data[attrsDataOffset:]...)
	data = newData

	// Clean up original batch
	bat.Clean(mp)

	// Reuse a batch object (simulating UPDATE scenario)
	reusedBat := &Batch{}
	reusedBat.offHeap = false
	// First unmarshal with 3 Vecs, 3 Attrs to simulate stale Attrs
	reusedBat.Attrs = []string{"old1", "old2", "old3"} // Simulate stale Attrs
	reusedBat.Vecs = make([]*vector.Vector, 3)
	for i := range reusedBat.Vecs {
		reusedBat.Vecs[i] = vector.NewVecFromReuse()
	}

	// Unmarshal the inconsistent data
	// Vecs length = 3 (from serialized data), but serialized Attrs length = 2
	// The fix should ensure Attrs length matches Vecs length (3), clearing the third Attr
	err = reusedBat.UnmarshalBinaryWithAnyMp(data, mp)
	require.NoError(t, err)

	// Attrs length should match Vecs length (3), not serialized Attrs length (2)
	require.Equal(t, 3, len(reusedBat.Vecs), "Vecs length should be 3")
	require.Equal(t, 3, len(reusedBat.Attrs), "Attrs length should match Vecs length (3)")
	require.Equal(t, len(reusedBat.Vecs), len(reusedBat.Attrs), "Vecs and Attrs must have same length")
	require.Equal(t, "col1", reusedBat.Attrs[0], "First attr should be correct")
	require.Equal(t, "col2", reusedBat.Attrs[1], "Second attr should be correct")
	require.Equal(t, "", reusedBat.Attrs[2], "Third attr should be cleared (empty string) to prevent stale value")

	// Clean up
	reusedBat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}
