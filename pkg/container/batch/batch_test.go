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

	"github.com/matrixorigin/matrixone/pkg/container/bufferlease"

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

// legacyNoStringSource, legacyUniformStringSource and legacyRowsStringSource
// encode one vector's string-source section as written by the retired
// version-2 transport writer.
func legacyNoStringSource() []byte { return []byte{batchMetadataModeNone} }

func legacyUniformStringSource(source types.StringSource) []byte {
	return []byte{batchMetadataModeUniform, byte(source)}
}

func legacyRowsStringSource(sources ...types.StringSource) []byte {
	count := int32(len(sources))
	out := append([]byte{batchMetadataModeRows}, types.EncodeInt32(&count)...)
	for _, source := range sources {
		out = append(out, byte(source))
	}
	return out
}

// legacyV2TransportWire rewrites the version-1 transport trailer of encoded
// as an older (version-2) writer emitted it: sections[i] follows vector i's
// string-domain record. Without a trailer, one is synthesized with empty
// string-domain records, as older writers did for source-only batches. Bytes
// after the trailer (a pipeline grouping section) are preserved.
func legacyV2TransportWire(t *testing.T, encoded []byte, sections ...[]byte) []byte {
	t.Helper()
	prefix, err := stableBatchPayloadLength(encoded)
	require.NoError(t, err)
	stable := NewWithSize(0)
	require.NoError(t, stable.UnmarshalBinary(encoded[:prefix]))
	require.Len(t, sections, len(stable.Vecs))
	records := make([][]byte, len(stable.Vecs))
	suffix := encoded[prefix:]
	if len(suffix) >= 4 && suffix[0] == batchMetadataMagic0 &&
		suffix[1] == batchMetadataMagic1 && suffix[2] == batchMetadataMagic2 {
		require.Equal(t, batchMetadataVersionV1, suffix[3])
		require.Equal(t, int32(len(stable.Vecs)), types.DecodeInt32(suffix[4:8]))
		require.Equal(t, int64(stable.RowCount()), types.DecodeInt64(suffix[8:16]))
		pos := 16
		for i := range records {
			start := pos
			mode := suffix[pos] &^ (batchMetadataBinaryFlag | batchMetadataTextFlag)
			pos++
			switch mode {
			case batchMetadataModeNone:
			case batchMetadataModeUniform:
				pos++
			case batchMetadataModeRows:
				pos += 4 + int(types.DecodeInt32(suffix[pos:pos+4]))
			default:
				t.Fatalf("unexpected metadata mode %d", mode)
			}
			records[i] = suffix[start:pos]
		}
		require.Equal(t, uint32(pos+4), types.DecodeUint32(suffix[pos:pos+4]))
		suffix = suffix[pos+4:]
	} else {
		for i := range records {
			records[i] = []byte{batchMetadataModeNone}
		}
	}
	var trailer bytes.Buffer
	trailer.Write([]byte{batchMetadataMagic0, batchMetadataMagic1, batchMetadataMagic2, batchMetadataVersion})
	nVecs := int32(len(records))
	trailer.Write(types.EncodeInt32(&nVecs))
	rowCount := int64(stable.RowCount())
	trailer.Write(types.EncodeInt64(&rowCount))
	for i := range records {
		trailer.Write(records[i])
		trailer.Write(sections[i])
	}
	size := uint32(trailer.Len() + 4)
	trailer.Write(types.EncodeUint32(&size))
	out := append([]byte(nil), encoded[:prefix]...)
	out = append(out, trailer.Bytes()...)
	return append(out, suffix...)
}

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
	source.Attrs = []string{"value"}
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

	bat.ExtraBuf = []byte("extra buf")
	bat.Attrs = []string{"1"}
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

// legacyV1Rows encodes one vector's per-row record as written by the retired
// version-1 transport writer: binary (0x80) / text (0x40) flags, optionally
// with a retired kind (0..4) in the low bits.
func legacyV1Rows(rows ...byte) []byte {
	count := int32(len(rows))
	out := append([]byte{batchMetadataModeRows}, types.EncodeInt32(&count)...)
	return append(out, rows...)
}

// legacyV1TransportWire appends a version-1 transport trailer, as an older
// writer emitted it, to the stable Batch bytes. records[i] is vector i's
// string-domain record.
func legacyV1TransportWire(t *testing.T, stable []byte, records ...[]byte) []byte {
	t.Helper()
	decoded := NewWithSize(0)
	require.NoError(t, decoded.UnmarshalBinary(stable))
	require.Len(t, records, len(decoded.Vecs))
	var trailer bytes.Buffer
	trailer.Write([]byte{batchMetadataMagic0, batchMetadataMagic1, batchMetadataMagic2, batchMetadataVersionV1})
	nVecs := int32(len(records))
	trailer.Write(types.EncodeInt32(&nVecs))
	rowCount := int64(decoded.RowCount())
	trailer.Write(types.EncodeInt64(&rowCount))
	for _, record := range records {
		trailer.Write(record)
	}
	size := uint32(trailer.Len() + 4)
	trailer.Write(types.EncodeUint32(&size))
	return append(append([]byte(nil), stable...), trailer.Bytes()...)
}

func requireStringValues(t *testing.T, vec *vector.Vector, values ...string) {
	t.Helper()
	require.Equal(t, len(values), vec.Length())
	for row, value := range values {
		require.Equal(t, value, vec.GetStringAt(row), "row %d", row)
	}
}

func TestTransportDecoderDropsLegacyV1Trailer(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_text.ToType(), mp)
	require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte("5"), false, mp))
	require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte("6"), false, mp))
	source.SetRowCount(2)
	defer source.Clean(mp)

	legacy, err := source.MarshalBinary()
	require.NoError(t, err)
	var wire bytes.Buffer
	plain, err := source.MarshalBinaryWithBuffer(&wire, true)
	require.NoError(t, err)
	require.Equal(t, legacy, plain, "writers never emit the transport trailer")

	encoded := legacyV1TransportWire(t, legacy, legacyV1Rows(batchMetadataBinaryFlag, 0))
	decoded := NewWithSize(0)
	require.NoError(t, decoded.UnmarshalBinaryWithMetadata(encoded, mp))
	requireStringValues(t, decoded.Vecs[0], "5", "6")
	require.NoError(t, decoded.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(encoded), int64(len(encoded)), mp))
	requireStringValues(t, decoded.Vecs[0], "5", "6")

	// An older writer's version-2 trailer carries per-row string sources. Both
	// decoders validate and drop them.
	olderWriter := legacyV2TransportWire(t, encoded,
		legacyRowsStringSource(types.StringSourceCOMStmt, types.StringSourceSQLPrepare))
	require.Equal(t, batchMetadataVersion, olderWriter[len(legacy)+3])
	olderDecoded := NewWithSize(0)
	require.NoError(t, olderDecoded.UnmarshalBinaryWithMetadata(olderWriter, mp))
	requireStringValues(t, olderDecoded.Vecs[0], "5", "6")
	require.NoError(t, olderDecoded.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(olderWriter), int64(len(olderWriter)), mp))
	requireStringValues(t, olderDecoded.Vecs[0], "5", "6")
	olderDecoded.Clean(mp)

	corrupt := append([]byte(nil), olderWriter...)
	const sourceRowsOffset = 4 + 4 + 8 + 1 + 4 + 2 + 1 + 4
	corrupt[len(legacy)+sourceRowsOffset] = 255
	corruptDecoded := NewWithSize(0)
	require.ErrorContains(t,
		corruptDecoded.UnmarshalBinaryWithMetadata(corrupt, mp),
		"invalid string source metadata")
	corruptDecoded = NewWithSize(0)
	require.ErrorContains(t,
		corruptDecoded.UnmarshalFromReaderWithMetadata(bytes.NewReader(corrupt), int64(len(corrupt)), mp),
		"invalid string source metadata")
	corruptDecoded.Clean(mp)

	// Older writers stored a retired per-row kind (0..4) in the low bits of
	// each row byte. The reader validates and ignores it.
	const binaryRowsOffset = 4 + 4 + 8 + 1 + 4
	withKinds := append([]byte(nil), encoded...)
	withKinds[len(legacy)+binaryRowsOffset] |= 2
	withKinds[len(legacy)+binaryRowsOffset+1] |= 4
	kindsDecoded := NewWithSize(0)
	require.NoError(t, kindsDecoded.UnmarshalBinaryWithMetadata(withKinds, mp))
	requireStringValues(t, kindsDecoded.Vecs[0], "5", "6")
	kindsDecoded.Clean(mp)
	withKinds[len(legacy)+binaryRowsOffset+1] |= 5
	kindsDecoded = NewWithSize(0)
	require.Error(t, kindsDecoded.UnmarshalBinaryWithMetadata(withKinds, mp))
	kindsDecoded.Clean(mp)

	// A row flagged both binary and text is malformed.
	both := legacyV1TransportWire(t, legacy,
		legacyV1Rows(batchMetadataBinaryFlag|batchMetadataTextFlag, 0))
	bothDecoded := NewWithSize(0)
	require.ErrorContains(t, bothDecoded.UnmarshalBinaryWithMetadata(both, mp), "mutually exclusive")
	bothDecoded = NewWithSize(0)
	require.ErrorContains(t, bothDecoded.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(both), int64(len(both)), mp), "mutually exclusive")
	bothDecoded.Clean(mp)

	// Reusing the receiver with a trailer-free payload still decodes.
	require.NoError(t, decoded.UnmarshalBinaryWithMetadata(legacy, mp))
	requireStringValues(t, decoded.Vecs[0], "5", "6")
	decoded.Clean(mp)
}

func TestTransportDecoderDropsLegacyStringSourcesOnConstant(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	var err error
	source.Vecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte("value"), 2, mp)
	require.NoError(t, err)
	source.SetRowCount(2)
	defer source.Clean(mp)

	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	// An older writer could emit a per-row section (even a mixed one) for a
	// constant vector. It is validated and dropped; the constant survives.
	encoded = legacyV2TransportWire(t, encoded,
		legacyRowsStringSource(types.StringSourceLiteral, types.StringSourceSQLPrepare))

	check := func(decoded *Batch) {
		require.Equal(t, 2, decoded.RowCount())
		require.True(t, decoded.Vecs[0].IsConst())
		require.Equal(t, "value", decoded.Vecs[0].GetStringAt(1))
	}
	buffered := NewWithSize(0)
	require.NoError(t, buffered.UnmarshalBinaryWithMetadata(encoded, mp))
	check(buffered)
	buffered.Clean(mp)

	streaming := NewWithSize(0)
	require.NoError(t, streaming.UnmarshalFromReaderWithMetadata(bytes.NewReader(encoded), int64(len(encoded)), mp))
	check(streaming)
	streaming.Clean(mp)
}

func TestLegacyStringSourcesDroppedByBatchAndSpillCodecs(t *testing.T) {
	mp := mpool.MustNewZero()
	sources := []types.StringSource{
		types.StringSourceExpression,
		types.StringSourceLiteral,
		types.StringSourceUserVariable,
		types.StringSourceSQLPrepare,
		types.StringSourceCOMStmt,
	}
	values := []string{"v0", "v1", "v2", "v3", "v4"}
	bat := NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType(), mp)
	for _, value := range values {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(value), false, mp))
	}
	bat.SetRowCount(len(sources))
	defer bat.Clean(mp)

	// Every valid source value an older writer could emit is accepted and
	// dropped by the buffered and streaming decoders, together with the
	// older writer's per-row binary flags.
	stable, err := bat.MarshalBinary()
	require.NoError(t, err)
	encoded := legacyV1TransportWire(t, stable, legacyV1Rows(
		batchMetadataBinaryFlag, 0, batchMetadataBinaryFlag, 0, batchMetadataTextFlag))
	encoded = legacyV2TransportWire(t, encoded, legacyRowsStringSource(sources...))
	decoded := NewWithSize(0)
	require.NoError(t, decoded.UnmarshalBinaryWithMetadata(encoded, mp))
	requireStringValues(t, decoded.Vecs[0], values...)
	require.NoError(t, decoded.UnmarshalFromReaderWithMetadata(bytes.NewReader(encoded), int64(len(encoded)), mp))
	requireStringValues(t, decoded.Vecs[0], values...)
	decoded.Clean(mp)

	var wire bytes.Buffer
	require.NoError(t, bat.MarshalBinaryForSpillTo(&wire))
	spilled := NewWithSize(0)
	require.NoError(t, spilled.UnmarshalFromReaderForSpill(bytes.NewReader(wire.Bytes()), mp))
	requireStringValues(t, spilled.Vecs[0], values...)

	// The spill codec frames the same transport payload; an older
	// writer's version-2 trailer inside it is dropped as well.
	payloadSize := types.DecodeInt64(wire.Bytes()[:8])
	payload := wire.Bytes()[8 : 8+payloadSize]
	require.Len(t, wire.Bytes(), int(8+payloadSize))
	olderPayload := legacyV2TransportWire(t, payload, legacyRowsStringSource(sources...))
	olderSize := int64(len(olderPayload))
	olderWire := append(types.EncodeInt64(&olderSize), olderPayload...)
	require.NoError(t, spilled.UnmarshalFromReaderForSpill(bytes.NewReader(olderWire), mp))
	requireStringValues(t, spilled.Vecs[0], values...)
	spilled.Clean(mp)
}

func TestTransportDropsLegacyVectorFlags(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(3)
	for i := range source.Vecs {
		source.Vecs[i] = vector.NewVec(types.T_varbinary.ToType(), mp)
		require.NoError(t, vector.AppendBytesList(
			source.Vecs[i], [][]byte{[]byte("a"), []byte("b")}, nil, mp))
	}
	source.SetRowCount(2)
	defer source.Clean(mp)
	stable, err := source.MarshalBinary()
	require.NoError(t, err)

	// Older writers flagged a whole vector text (0x40) or binary (0x80), and
	// the oldest ones used a uniform mode with a retired kind byte.
	encoded := legacyV1TransportWire(t, stable,
		[]byte{batchMetadataModeNone | batchMetadataTextFlag},
		[]byte{batchMetadataModeNone | batchMetadataBinaryFlag},
		[]byte{batchMetadataModeUniform | batchMetadataBinaryFlag, 2},
	)
	for _, streaming := range []bool{false, true} {
		decoded := NewWithSize(0)
		if streaming {
			require.NoError(t, decoded.UnmarshalFromReaderWithMetadata(
				bytes.NewReader(encoded), int64(len(encoded)), mp))
		} else {
			require.NoError(t, decoded.UnmarshalBinaryWithMetadata(encoded, mp))
		}
		for _, vec := range decoded.Vecs {
			require.Equal(t, types.T_varbinary, vec.GetType().Oid)
			requireStringValues(t, vec, "a", "b")
		}
		decoded.Clean(mp)
	}

	for _, malformed := range [][][]byte{
		{
			{batchMetadataModeNone | batchMetadataTextFlag | batchMetadataBinaryFlag},
			{batchMetadataModeNone},
			{batchMetadataModeNone},
		},
		{
			{batchMetadataModeUniform, 0},
			{batchMetadataModeNone},
			{batchMetadataModeNone},
		},
		{
			{batchMetadataModeUniform, batchMetadataRetiredKindMax + 1},
			{batchMetadataModeNone},
			{batchMetadataModeNone},
		},
	} {
		wire := legacyV1TransportWire(t, stable, malformed...)
		decoded := NewWithSize(0)
		require.Error(t, decoded.UnmarshalBinaryWithMetadata(wire, mp))
		decoded.Clean(mp)
		decoded = NewWithSize(0)
		require.Error(t, decoded.UnmarshalFromReaderWithMetadata(
			bytes.NewReader(wire), int64(len(wire)), mp))
		decoded.Clean(mp)
	}
}

func TestTransportRejectsMalformedTrailer(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType(), mp)
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("5"), false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)
	stable, err := bat.MarshalBinary()
	require.NoError(t, err)
	encoded := legacyV1TransportWire(t, stable, []byte{batchMetadataModeNone | batchMetadataBinaryFlag})
	for _, malformed := range [][]byte{
		encoded[:len(encoded)-1],
		append(append([]byte(nil), encoded...), 0),
	} {
		reused := NewWithSize(0)
		require.Error(t, reused.UnmarshalBinaryWithMetadata(malformed, mp))
		reused.Clean(mp)
	}
}

func TestTransportRejectsMismatchedCountsBeforeAllocation(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_text.ToType(), mp)
	require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte("5"), false, mp))
	source.SetRowCount(1)
	defer source.Clean(mp)
	stable, err := source.MarshalBinary()
	require.NoError(t, err)
	encoded := legacyV1TransportWire(t, stable, []byte{batchMetadataModeNone | batchMetadataBinaryFlag})
	prefixLen, err := stableBatchPayloadLength(encoded)
	require.NoError(t, err)

	for _, nVecs := range []int32{0, 2} {
		malformed := append([]byte(nil), encoded...)
		copy(malformed[prefixLen+4:prefixLen+8], types.EncodeInt32(&nVecs))
		reused := NewWithSize(0)
		require.ErrorContains(t, reused.UnmarshalBinaryWithMetadata(malformed, mp),
			"vector count mismatch")
		reused.Clean(mp)
	}

	heterogeneous := NewWithSize(1)
	heterogeneous.Vecs[0] = vector.NewVec(types.T_text.ToType(), mp)
	for range 2 {
		require.NoError(t, vector.AppendBytes(heterogeneous.Vecs[0], []byte("5"), false, mp))
	}
	heterogeneous.SetRowCount(2)
	defer heterogeneous.Clean(mp)
	heterogeneousStable, err := heterogeneous.MarshalBinary()
	require.NoError(t, err)
	heterogeneousEncoded := legacyV1TransportWire(t, heterogeneousStable,
		legacyV1Rows(batchMetadataBinaryFlag, 0))
	heterogeneousPrefix, err := stableBatchPayloadLength(heterogeneousEncoded)
	require.NoError(t, err)
	// PPB header (16 bytes) + mode byte precede the rows count.
	rowCountOffset := heterogeneousPrefix + 17
	malformed := append([]byte(nil), heterogeneousEncoded...)
	amplified := int32(batchMetadataMaxRows)
	copy(malformed[rowCountOffset:rowCountOffset+4], types.EncodeInt32(&amplified))
	reused := NewWithSize(0)
	require.ErrorContains(t, reused.UnmarshalBinaryWithMetadata(malformed, mp),
		"row count mismatch")
	reused.Clean(mp)
}

func TestTransportStreamingRejectsTruncatedRowsBeforeAllocation(t *testing.T) {
	sourceMP := mpool.MustNewZero()
	source := NewWithSize(1)
	source.Vecs[0] = vector.NewConstNull(
		types.T_int8.ToType(),
		int(batchMetadataMaxRows),
		sourceMP,
	)
	source.SetRowCount(int(batchMetadataMaxRows))
	stable, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Clean(sourceMP)
	require.Zero(t, sourceMP.CurrNB())

	var trailer bytes.Buffer
	trailer.Write([]byte{
		batchMetadataMagic0,
		batchMetadataMagic1,
		batchMetadataMagic2,
		batchMetadataVersion,
	})
	nVecs := int32(1)
	trailer.Write(types.EncodeInt32(&nVecs))
	rowCount := int64(batchMetadataMaxRows)
	trailer.Write(types.EncodeInt64(&rowCount))
	trailer.WriteByte(batchMetadataModeRows)
	count := batchMetadataMaxRows
	trailer.Write(types.EncodeInt32(&count))
	trailer.WriteByte(2) // a retired per-row kind from an older writer
	trailerLen := uint32(trailer.Len() + 4)
	trailer.Write(types.EncodeUint32(&trailerLen))

	wire := append(append([]byte(nil), stable...), trailer.Bytes()...)
	mp := mpool.MustNewZero()
	reader := &mpoolTrackingReader{
		reader: bytes.NewReader(wire),
		mp:     mp,
	}
	target := NewWithSize(0)
	err = target.UnmarshalFromReaderWithMetadata(reader, int64(len(wire)), mp)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, int(batchMetadataMaxRows), target.Vecs[0].Length())
	require.Equal(t, mp.CurrNB(), reader.maxCurrNB,
		"truncated row metadata must not transiently amplify the MPool")
	target.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func makeTransportStreamingWire(t *testing.T) (encoded, legacy []byte) {
	t.Helper()
	mp := mpool.MustNewZero()
	source := NewWithSize(2)
	source.Vecs[0] = vector.NewVec(types.T_text.ToType(), mp)
	source.Vecs[1] = vector.NewVec(types.T_int64.ToType(), mp)
	for i, value := range []string{"1.5", "plain", "9.5"} {
		require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte(value), false, mp))
		require.NoError(t, vector.AppendFixed(source.Vecs[1], int64(i+1), false, mp))
	}
	source.SetRowCount(3)

	legacy, err := source.MarshalBinary()
	require.NoError(t, err)
	// An older writer's version-1 trailer with per-row binary flags.
	encoded = legacyV1TransportWire(t, legacy,
		legacyV1Rows(batchMetadataBinaryFlag, 0, batchMetadataBinaryFlag),
		[]byte{batchMetadataModeNone})
	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
	return encoded, legacy
}

func TestTransportStreamingRoundTrip(t *testing.T) {
	encoded, legacy := makeTransportStreamingWire(t)
	require.Greater(t, len(encoded), len(legacy))
	require.Equal(t, legacy, encoded[:len(legacy)],
		"the streaming extension must not change the stable Batch prefix")

	mp := mpool.MustNewZero()
	target := NewWithSize(0)
	require.NoError(t, target.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(encoded), int64(len(encoded)), mp))
	require.Equal(t, 3, target.RowCount())
	requireStreamingRows(t, target)
	require.Equal(t, int64(3), vector.GetFixedAtWithTypeCheck[int64](target.Vecs[1], 2))
	target.Clean(mp)

	spillTarget := NewWithSize(0)
	require.NoError(t, spillTarget.UnmarshalFromReaderWithMetadataForSpill(
		bytes.NewReader(encoded), int64(len(encoded)), mp))
	requireStreamingRows(t, spillTarget)
	spillTarget.Clean(mp)

	// Older writers stored a retired per-row kind (0..4) in the low bits of
	// each row byte; streaming decoders validate and drop it.
	withKinds := append([]byte(nil), encoded...)
	firstRow := len(legacy) + 4 + 4 + 8 + 1 + 4
	for row, kind := range []byte{2, 0, 3} {
		withKinds[firstRow+row] |= kind
	}
	kindsTarget := NewWithSize(0)
	require.NoError(t, kindsTarget.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(withKinds), int64(len(withKinds)), mp))
	requireStreamingRows(t, kindsTarget)
	kindsTarget.Clean(mp)
	kindsSpill := NewWithSize(0)
	require.NoError(t, kindsSpill.UnmarshalFromReaderWithMetadataForSpill(
		bytes.NewReader(withKinds), int64(len(withKinds)), mp))
	requireStreamingRows(t, kindsSpill)
	kindsSpill.Clean(mp)

	// An older writer's version-2 trailer adds a string-source section per
	// vector; streaming and spill decoders validate and drop it.
	olderWriter := legacyV2TransportWire(t, encoded,
		legacyRowsStringSource(types.StringSourceLiteral, types.StringSourceExpression, types.StringSourceCOMStmt),
		legacyUniformStringSource(types.StringSourceSQLPrepare))
	olderTarget := NewWithSize(0)
	require.NoError(t, olderTarget.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(olderWriter), int64(len(olderWriter)), mp))
	requireStreamingRows(t, olderTarget)
	require.Equal(t, int64(3), vector.GetFixedAtWithTypeCheck[int64](olderTarget.Vecs[1], 2))
	olderTarget.Clean(mp)
	olderSpill := NewWithSize(0)
	require.NoError(t, olderSpill.UnmarshalFromReaderWithMetadataForSpill(
		bytes.NewReader(olderWriter), int64(len(olderWriter)), mp))
	requireStreamingRows(t, olderSpill)
	olderSpill.Clean(mp)
	for _, section := range [][]byte{
		{batchMetadataModeUniform, byte(types.StringSourceExpression)},
		{batchMetadataModeUniform, 0xff},
		{0x7f},
	} {
		malformed := legacyV2TransportWire(t, encoded, legacyNoStringSource(), section)
		target := NewWithSize(0)
		require.Error(t, target.UnmarshalFromReaderWithMetadata(
			bytes.NewReader(malformed), int64(len(malformed)), mp))
		target.Clean(mp)
	}
	require.Zero(t, mp.CurrNB())
}

func requireStreamingRows(t *testing.T, bat *Batch) {
	t.Helper()
	requireStringValues(t, bat.Vecs[0], "1.5", "plain", "9.5")
}

func TestSpillStreamingRejectsBatchMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, test := range []struct {
		name  string
		attrs []string
		extra []byte
	}{
		{name: "attributes", attrs: []string{"value"}},
		{name: "extra buffer", extra: []byte("unaccounted payload")},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := NewWithSize(1)
			source.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
			require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(1), false, mp))
			source.Attrs = test.attrs
			source.ExtraBuf = test.extra
			source.SetRowCount(1)
			var wire bytes.Buffer
			require.NoError(t, source.MarshalBinaryTo(&wire))

			target := NewWithSize(0)
			err := target.UnmarshalFromReaderWithMetadataForSpill(
				bytes.NewReader(wire.Bytes()),
				int64(wire.Len()),
				mp,
			)
			require.Error(t, err)
			target.Clean(mp)
			source.Clean(mp)
		})
	}
	require.Zero(t, mp.CurrNB())
}

func TestTransportStreamingReservesRemainingRecordsAndFooter(t *testing.T) {
	encoded, legacy := makeTransportStreamingWire(t)
	// Header + first rows mode + count + three row bytes ends immediately
	// before the second vector's mode byte.
	firstRowsEnd := len(legacy) + 4 + 4 + 8 + 1 + 4 + 3
	require.Less(t, firstRowsEnd, len(encoded))

	t.Run("valid multi-vector record", func(t *testing.T) {
		mp := mpool.MustNewZero()
		target := NewWithSize(0)
		require.NoError(t, target.UnmarshalFromReaderWithMetadata(
			bytes.NewReader(encoded), int64(len(encoded)), mp))
		requireStreamingRows(t, target)
		target.Clean(mp)
		require.Zero(t, mp.CurrNB())
	})

	for _, tc := range []struct {
		name string
		wire []byte
	}{
		{name: "missing remaining vector record and footer", wire: encoded[:firstRowsEnd]},
		{name: "footer short by one byte", wire: encoded[:len(encoded)-1]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			reader := &mpoolTrackingReader{reader: bytes.NewReader(tc.wire), mp: mp}
			target := NewWithSize(0)
			err := target.UnmarshalFromReaderWithMetadata(reader, int64(len(tc.wire)), mp)
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			require.Equal(t, mp.CurrNB(), reader.maxCurrNB,
				"framing rejection must happen before row metadata allocation")
			target.Clean(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestTransportStreamingMalformedReuse(t *testing.T) {
	encoded, legacy := makeTransportStreamingWire(t)
	secondModeOffset := len(legacy) + 4 + 4 + 8 + 1 + 4 + 3
	malformed := append([]byte(nil), encoded...)
	malformed[secondModeOffset] = 0x3f

	mp := mpool.MustNewZero()
	target := NewWithSize(0)
	require.NoError(t, target.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(encoded), int64(len(encoded)), mp))
	requireStreamingRows(t, target)

	require.ErrorContains(t, target.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(malformed), int64(len(malformed)), mp),
		"invalid prepared parameter metadata mode")

	require.NoError(t, target.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(legacy), int64(len(legacy)), mp))
	requireStreamingRows(t, target)
	target.Clean(mp)
	require.Zero(t, mp.CurrNB())
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
	bat1.Vecs[0] = vector.NewVec(types.T_Rowid.ToType(), mp)
	bat1.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	bat1.SetRowCount(0)

	// Create second batch: INSERT block info data
	// Vecs: [block_info_vec, object_stats_vec], Attrs: ["block_info", "object_stats"]
	bat2 := NewWithSize(2)
	bat2.Attrs = []string{"block_info", "object_stats"}
	bat2.Vecs[0] = vector.NewVec(types.T_varchar.ToType(), mp)
	bat2.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
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
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType(), mp)
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
	bat1.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType(), mp)
	bat1.Vecs[2] = vector.NewVec(types.T_int32.ToType(), mp)
	bat1.SetRowCount(0)

	// Create second batch: 2 Vecs, 2 Attrs (different length)
	bat2 := NewWithSize(2)
	bat2.Attrs = []string{"attr1", "attr2"}
	bat2.Vecs[0] = vector.NewVec(types.T_varchar.ToType(), mp)
	bat2.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	bat2.SetRowCount(0)

	// Create third batch: 3 Vecs, 3 Attrs (back to original length)
	bat3 := NewWithSize(3)
	bat3.Attrs = []string{"x", "y", "z"}
	bat3.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	bat3.Vecs[1] = vector.NewVec(types.T_int64.ToType(), mp)
	bat3.Vecs[2] = vector.NewVec(types.T_int64.ToType(), mp)
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
	bat1.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	bat1.SetRowCount(0)

	// Create second batch: 2 Vecs, 2 Attrs (length increases - this is the critical case for #21911)
	bat2 := NewWithSize(2)
	bat2.Attrs = []string{"attr1", "attr2"}
	bat2.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	bat2.Vecs[1] = vector.NewVec(types.T_float64.ToType(), mp)
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
	bat1.Vecs[0] = vector.NewVec(types.T_Rowid.ToType(), mp)
	bat1.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	bat1.SetRowCount(0)

	// Create second batch: INSERT block info data
	bat2 := NewWithSize(2)
	bat2.Attrs = []string{"block_info", "object_stats"}
	bat2.Vecs[0] = vector.NewVec(types.T_varchar.ToType(), mp)
	bat2.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
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
	bat1.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType(), mp)
	bat1.SetRowCount(0)

	// Create second batch: 3 Vecs, 3 Attrs (different length)
	bat2 := NewWithSize(3)
	bat2.Attrs = []string{"attr1", "attr2", "attr3"}
	bat2.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	bat2.Vecs[1] = vector.NewVec(types.T_int64.ToType(), mp)
	bat2.Vecs[2] = vector.NewVec(types.T_int64.ToType(), mp)
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
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.SetRowCount(0)

	// Marshal
	data, err := bat.MarshalBinary()
	require.NoError(t, err)
	bat.Clean(mp)

	// First time unmarshal (nil Vecs)
	newBat := &Batch{}
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
	bat1.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType(), mp)
	bat1.SetRowCount(0)

	// Create second batch: 2 Vecs, 2 Attrs (same length, different content)
	bat2 := NewWithSize(2)
	bat2.Attrs = []string{"new1", "new2"}
	bat2.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	bat2.Vecs[1] = vector.NewVec(types.T_int64.ToType(), mp)
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
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType(), mp)
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
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType(), mp)
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
	// First unmarshal with 3 Vecs, 3 Attrs to simulate stale Attrs
	reusedBat.Attrs = []string{"old1", "old2", "old3"} // Simulate stale Attrs
	reusedBat.Vecs = make([]*vector.Vector, 3)
	for i := range reusedBat.Vecs {
		reusedBat.Vecs[i] = new(vector.Vector)
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
			lease, err := bufferlease.NewRefCounted(data, int64(cap(data)), nil)
			require.NoError(t, err)
			t.Cleanup(lease.Release)
			bat.Vecs[1], err = vector.NewVecWithData(types.T_int64.ToType(), len(values), data, nil, nil), error(nil)
			require.NoError(t, err)
			bat.SetRowCount(3)
			if mask {
				var sels bitmap.Bitmap
				sels.Init(bitmapTestMP)
				mustBitmapOK(sels.InitWithSize(3, bitmapTestMP))
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
			sels.Init(bitmapTestMP)
			mustBitmapOK(sels.InitWithSize(3, bitmapTestMP))
			sels.AddMany(tc.sels)
			require.NoError(t, bat.ShrinkByMask(&sels, tc.negate, 0, mp))
			require.Equal(t, len(tc.want), bat.RowCount())
			require.Equal(t, tc.want, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0]))
		})
	}
}

// When a later Dup fails, OwnReadOnlyVectors still frees the read-only
// originals it already replaced; one of them owns its nulls bitmap.
func TestOwnReadOnlyVectorsFreesReplacedOnDupError(t *testing.T) {
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

	bat := New([]string{"a", "b"})
	bat.Vecs[0], bat.Vecs[1] = w1, w2
	require.Error(t, bat.OwnReadOnlyVectors(dstMP))

	require.Zero(t, nspMP.CurrNB(), "the replaced original's nulls are freed")
	require.False(t, bat.Vecs[0].IsReadOnly(), "the first column was copied")
	require.Same(t, w2, bat.Vecs[1], "the failed column is left in place")
	bat.Vecs[0].Free(dstMP)
	require.Zero(t, dstMP.CurrNB())
	small.Free(srcMP)
	big.Free(srcMP)
	require.Zero(t, srcMP.CurrNB())
}

func TestBatchUnionByMask(t *testing.T) {
	mp := mpool.MustNewZero()
	src := New([]string{"a", "b"})
	src.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	src.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	for i := 0; i < 100; i++ {
		require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(i), i%10 == 0, mp))
		require.NoError(t, vector.AppendBytes(src.Vecs[1], []byte(fmt.Sprintf("row-%03d-long-enough-to-spill", i)), false, mp))
	}
	src.SetRowCount(100)
	mask := new(bitmap.Bitmap)
	require.NoError(t, mask.InitWithSize(100, mp))
	for r := 0; r < 100; r += 3 {
		mask.Add(uint64(r))
	}

	dst := New([]string{"a", "b"})
	dst.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	dst.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, dst.UnionByMask(src, mask, mp))
	require.Equal(t, 34, dst.RowCount())
	for i := 0; i < 34; i++ {
		r := 3 * i
		require.Equal(t, r%10 == 0, dst.Vecs[0].IsNull(uint64(i)))
		if r%10 != 0 {
			require.Equal(t, int64(r), vector.GetFixedAtNoTypeCheck[int64](dst.Vecs[0], i))
		}
		require.Equal(t, fmt.Sprintf("row-%03d-long-enough-to-spill", r), dst.Vecs[1].GetStringAt(i))
	}
	dst.Clean(mp)
	src.Clean(mp)
	mask.Free()
	require.Zero(t, mp.CurrNB())
}
