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

type failAfterBatchWriter struct {
	remaining int
}

func (w *failAfterBatchWriter) Write(p []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, io.ErrClosedPipe
	}
	if len(p) > w.remaining {
		n := w.remaining
		w.remaining = 0
		return n, io.ErrClosedPipe
	}
	w.remaining -= len(p)
	return len(p), nil
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
		transportSize, err := tc.bat.MarshalBinaryWithMetadataSize()
		require.NoError(t, err)
		require.Equal(t, len(data), transportSize)
		streamed.Reset()
		require.NoError(t, tc.bat.MarshalBinaryWithMetadataTo(&streamed))
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
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
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
	window.Clean(nil)
}

func TestBatchWindowBroadcastsConstRuntimeStringDomainAtOffset(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	var err error
	source.Vecs[0], err = vector.NewConstBytes(
		types.T_varbinary.ToType(), []byte("selected"), 1, mp)
	require.NoError(t, err)
	require.NoError(t, source.Vecs[0].SetRuntimeStringDomainWithMP(
		types.RuntimeStringText, mp))
	source.SetRowCount(4)
	defer func() {
		source.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()

	window, err := source.Window(2, 4)
	require.NoError(t, err)
	require.Equal(t, 2, window.RowCount())
	require.True(t, window.Vecs[0].IsConst())
	for row := 0; row < window.RowCount(); row++ {
		require.Equal(t, "selected", window.Vecs[0].GetStringAt(row))
		require.Equal(t, types.RuntimeStringText,
			window.Vecs[0].GetRuntimeStringDomainAt(row))
	}
	window.Clean(nil)
}

func TestBatchWindowRejectsMissingRowMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, vec := range []*vector.Vector{
		vector.NewVec(types.T_int64.ToType()),
		vector.NewConstNull(types.T_int64.ToType(), 0, mp),
		vector.NewRollupConst(types.T_int64.ToType(), 1, mp),
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
	for _, columnCount := range []int{2, 3, 64, 65} {
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
	require.NoError(t, bat.Shrink([]int64{0}, true, nil))
	require.Equal(t, 3, bat.rowCount)
	require.NoError(t, bat.Shrink([]int64{0, 2}, false, nil))
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

func TestClonePreservesConstantBinaryStringMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	var err error
	source.Vecs[0], err = vector.NewConstBytes(
		types.T_varchar.ToType(), []byte{0xe4, 0xbd, 0xa0}, 3, mp)
	require.NoError(t, err)
	source.Vecs[0].SetIsBinaryString(true)
	source.SetRowCount(3)
	defer source.Clean(mp)

	cloned, err := source.Dup(mp)
	require.NoError(t, err)
	defer cloned.Clean(mp)
	require.True(t, cloned.Vecs[0].GetIsBinaryString())
	for row := 0; row < 3; row++ {
		require.True(t, cloned.Vecs[0].GetIsBinaryStringAt(row))
	}
}

func TestClonePreservesNormalizedConstantBinaryStringRows(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	var err error
	source.Vecs[0], err = vector.NewConstBytes(
		types.T_varchar.ToType(), []byte("text"), 2, mp)
	require.NoError(t, err)
	require.NoError(t, source.Vecs[0].SetBinaryStringRowsWithMP([]bool{false, false}, mp))
	require.False(t, source.Vecs[0].HasBinaryStringRows())
	for row := range 2 {
		require.False(t, source.Vecs[0].GetIsBinaryStringAt(row))
	}
	source.SetRowCount(2)
	defer source.Clean(mp)

	cloned, err := source.Dup(mp)
	require.NoError(t, err)
	defer cloned.Clean(mp)
	require.False(t, cloned.Vecs[0].HasBinaryStringRows())
	for row := range 2 {
		require.False(t, cloned.Vecs[0].GetIsBinaryStringAt(row))
	}
}

func TestTransportMetadataRoundTripAndReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte("5"), false, mp))
	require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte("5"), false, mp))
	require.NoError(t, source.Vecs[0].SetBinaryStringRows([]bool{true, false}))
	source.SetRowCount(2)
	defer source.Clean(mp)

	legacy, err := source.MarshalBinary()
	require.NoError(t, err)
	var wire bytes.Buffer
	encoded, err := source.MarshalBinaryWithMetadata(&wire, true)
	require.NoError(t, err)
	require.Equal(t, wire.Bytes(), encoded)
	require.Greater(t, len(encoded), len(legacy))
	require.Equal(t, legacy, encoded[:len(legacy)])
	streamSize, err := source.MarshalBinaryWithMetadataSize()
	require.NoError(t, err)
	require.Equal(t, len(encoded), streamSize)
	var streamed bytes.Buffer
	require.NoError(t, source.MarshalBinaryWithMetadataTo(&streamed))
	require.Equal(t, encoded, streamed.Bytes())
	require.ErrorIs(t,
		source.MarshalBinaryWithMetadataTo(shortBatchMarshalWriter{}),
		io.ErrShortWrite,
	)

	decoded := NewOffHeapEmpty()
	require.NoError(t, decoded.UnmarshalBinaryWithMetadata(encoded, mp))
	require.True(t, decoded.Vecs[0].GetIsBinaryStringAt(0))
	require.False(t, decoded.Vecs[0].GetIsBinaryStringAt(1))
	require.Equal(t, batchMetadataVersionV1, encoded[len(legacy)+3],
		"writers no longer emit the string-source section")

	// An older writer's version-2 trailer carries per-row string sources. Both
	// decoders validate and drop them while restoring the binary-string rows.
	olderWriter := legacyV2TransportWire(t, encoded,
		legacyRowsStringSource(types.StringSourceCOMStmt, types.StringSourceSQLPrepare))
	require.Equal(t, batchMetadataVersion, olderWriter[len(legacy)+3])
	olderDecoded := NewOffHeapEmpty()
	require.NoError(t, olderDecoded.UnmarshalBinaryWithMetadata(olderWriter, mp))
	require.True(t, olderDecoded.Vecs[0].GetIsBinaryStringAt(0))
	require.False(t, olderDecoded.Vecs[0].GetIsBinaryStringAt(1))
	require.NoError(t, olderDecoded.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(olderWriter), int64(len(olderWriter)), mp))
	require.True(t, olderDecoded.Vecs[0].GetIsBinaryStringAt(0))
	require.False(t, olderDecoded.Vecs[0].GetIsBinaryStringAt(1))
	olderDecoded.Clean(mp)

	corrupt := append([]byte(nil), olderWriter...)
	const sourceRowsOffset = 4 + 4 + 8 + 1 + 4 + 2 + 1 + 4
	corrupt[len(legacy)+sourceRowsOffset] = 255
	corruptDecoded := NewOffHeapEmpty()
	require.ErrorContains(t,
		corruptDecoded.UnmarshalBinaryWithMetadata(corrupt, mp),
		"invalid string source metadata")
	corruptDecoded = NewOffHeapEmpty()
	require.ErrorContains(t,
		corruptDecoded.UnmarshalFromReaderWithMetadata(bytes.NewReader(corrupt), int64(len(corrupt)), mp),
		"invalid string source metadata")
	corruptDecoded.Clean(mp)

	// Older writers stored a retired per-row kind (0..4) in the low bits of
	// each binary-string row byte. The reader validates and ignores it while
	// restoring the binary-string rows.
	const binaryRowsOffset = 4 + 4 + 8 + 1 + 4
	withKinds := append([]byte(nil), encoded...)
	withKinds[len(legacy)+binaryRowsOffset] |= 2
	withKinds[len(legacy)+binaryRowsOffset+1] |= 4
	kindsDecoded := NewOffHeapEmpty()
	require.NoError(t, kindsDecoded.UnmarshalBinaryWithMetadata(withKinds, mp))
	require.True(t, kindsDecoded.Vecs[0].GetIsBinaryStringAt(0))
	require.False(t, kindsDecoded.Vecs[0].GetIsBinaryStringAt(1))
	kindsDecoded.Clean(mp)
	withKinds[len(legacy)+binaryRowsOffset+1] |= 5
	kindsDecoded = NewOffHeapEmpty()
	require.Error(t, kindsDecoded.UnmarshalBinaryWithMetadata(withKinds, mp))
	kindsDecoded.Clean(mp)

	// Reusing the receiver with a legacy payload must clear the previous
	// sidecar rather than leaking the first generation's provenance.
	require.NoError(t, decoded.UnmarshalBinaryWithMetadata(legacy, mp))
	require.False(t, decoded.Vecs[0].GetIsBinaryString())
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

	var wire bytes.Buffer
	encoded, err := source.MarshalBinaryWithMetadata(&wire, true)
	require.NoError(t, err)
	stable, err := source.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, stable, encoded, "a batch without string domains carries no trailer")
	// An older writer could emit a per-row section (even a mixed one) for a
	// constant vector. It is validated and dropped; the constant survives.
	encoded = legacyV2TransportWire(t, encoded,
		legacyRowsStringSource(types.StringSourceLiteral, types.StringSourceSQLPrepare))

	check := func(decoded *Batch) {
		require.Equal(t, 2, decoded.RowCount())
		require.True(t, decoded.Vecs[0].IsConst())
		require.Equal(t, "value", decoded.Vecs[0].GetStringAt(1))
		require.False(t, decoded.Vecs[0].HasBinaryStringMetadata())
	}
	buffered := NewOffHeapEmpty()
	require.NoError(t, buffered.UnmarshalBinaryWithMetadata(encoded, mp))
	check(buffered)
	buffered.Clean(mp)

	streaming := NewOffHeapEmpty()
	require.NoError(t, streaming.UnmarshalFromReaderWithMetadata(bytes.NewReader(encoded), int64(len(encoded)), mp))
	check(streaming)
	streaming.Clean(mp)
}

func TestLegacyStringSourcesDroppedByBatchAndGroupingCodecs(t *testing.T) {
	mp := mpool.MustNewZero()
	sources := []types.StringSource{
		types.StringSourceExpression,
		types.StringSourceLiteral,
		types.StringSourceUserVariable,
		types.StringSourceSQLPrepare,
		types.StringSourceCOMStmt,
	}
	bat := NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	for range sources {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("value"), false, mp))
	}
	binaryRows := []bool{true, false, true, false, false}
	require.NoError(t, bat.Vecs[0].SetBinaryStringRowsWithMP(binaryRows, mp))
	bat.Vecs[0].GetGrouping().Add(1, 3)
	bat.SetRowCount(len(sources))
	defer bat.Clean(mp)

	requireBinaryRows := func(decoded *Batch) {
		for row, binary := range binaryRows {
			require.Equal(t, binary, decoded.Vecs[0].GetIsBinaryStringAt(row), "row %d", row)
		}
	}

	// Every valid source value an older writer could emit is accepted and
	// dropped by the buffered and streaming decoders.
	var wire bytes.Buffer
	encoded, err := bat.MarshalBinaryWithMetadata(&wire, true)
	require.NoError(t, err)
	encoded = legacyV2TransportWire(t, encoded, legacyRowsStringSource(sources...))
	decoded := NewOffHeapEmpty()
	require.NoError(t, decoded.UnmarshalBinaryWithMetadata(encoded, mp))
	requireBinaryRows(decoded)
	require.NoError(t, decoded.UnmarshalFromReaderWithMetadata(bytes.NewReader(encoded), int64(len(encoded)), mp))
	requireBinaryRows(decoded)
	decoded.Clean(mp)

	wire.Reset()
	require.NoError(t, bat.MarshalBinaryWithGroupingTo(&wire))
	grouped := NewOffHeapEmpty()
	require.NoError(t, grouped.UnmarshalFromReaderWithGrouping(bytes.NewReader(wire.Bytes()), mp))
	requireBinaryRows(grouped)
	require.True(t, grouped.Vecs[0].GetGrouping().IsSame(bat.Vecs[0].GetGrouping()))

	// The grouping codec frames the same transport payload; an older
	// writer's version-2 trailer inside it is dropped as well.
	payloadSize := types.DecodeInt64(wire.Bytes()[:8])
	payload := wire.Bytes()[8 : 8+payloadSize]
	groupingTail := wire.Bytes()[8+payloadSize:]
	olderPayload := legacyV2TransportWire(t, payload, legacyRowsStringSource(sources...))
	olderSize := int64(len(olderPayload))
	olderWire := append(types.EncodeInt64(&olderSize), olderPayload...)
	olderWire = append(olderWire, groupingTail...)
	require.NoError(t, grouped.UnmarshalFromReaderWithGrouping(bytes.NewReader(olderWire), mp))
	requireBinaryRows(grouped)
	require.True(t, grouped.Vecs[0].GetGrouping().IsSame(bat.Vecs[0].GetGrouping()))
	grouped.Clean(mp)
}

func TestTransportPreservesUniformExplicitText(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_varbinary.ToType())
	require.NoError(t, vector.AppendBytesList(
		source.Vecs[0], [][]byte{[]byte("a"), []byte("b")}, nil, mp))
	require.NoError(t, source.Vecs[0].SetSelectedValueBinaryStringRowsWithMP(
		[]bool{false, false}, mp))
	source.SetRowCount(2)
	defer source.Clean(mp)
	metadataSize, err := source.TransportMetadataSize()
	require.NoError(t, err)
	require.Equal(t, 21, metadataSize, "uniform text metadata must be constant-sized")

	var wire bytes.Buffer
	encoded, err := source.MarshalBinaryWithMetadata(&wire, true)
	require.NoError(t, err)
	decoded := NewOffHeapEmpty()
	defer decoded.Clean(mp)
	require.NoError(t, decoded.UnmarshalBinaryWithMetadata(encoded, mp))
	for row := 0; row < decoded.Vecs[0].Length(); row++ {
		require.Equal(t, types.RuntimeStringText, decoded.Vecs[0].GetRuntimeStringDomainAt(row))
		require.False(t, decoded.Vecs[0].GetIsBinaryStringAt(row))
	}
}

func TestUniformExplicitTextMetadataDoesNotScaleWithRows(t *testing.T) {
	mp := mpool.MustNewZero()
	vec, err := vector.NewConstBytes(types.T_varbinary.ToType(), []byte("text"), 65536, mp)
	require.NoError(t, err)
	require.NoError(t, vec.SetRuntimeStringDomainWithMP(types.RuntimeStringText, mp))
	bat := NewWithSize(1)
	bat.Vecs[0] = vec
	bat.SetRowCount(65536)
	defer bat.Clean(mp)
	size, err := bat.TransportMetadataSize()
	require.NoError(t, err)
	require.Equal(t, 21, size)
}

func TestTransportMetadataSizeMatchesTrailer(t *testing.T) {
	mp := mpool.MustNewZero()
	var nilBatch *Batch
	size, err := nilBatch.TransportMetadataSize()
	require.NoError(t, err)
	require.Zero(t, size)
	require.False(t, nilBatch.HasBinaryStringMetadata())

	bat := NewWithSize(4)
	defer bat.Clean(mp)
	for i, typ := range []types.Type{
		types.T_text.ToType(),
		types.T_text.ToType(),
		types.T_text.ToType(),
		types.T_varbinary.ToType(),
	} {
		bat.Vecs[i] = vector.NewVec(typ)
		for range 2 {
			require.NoError(t, vector.AppendBytes(bat.Vecs[i], []byte("v"), false, mp))
		}
	}
	require.NoError(t, bat.Vecs[0].SetIsBinaryStringAt(0, true))
	bat.Vecs[2].SetIsBinaryString(true)
	bat.SetRowCount(2)

	require.True(t, bat.HasBinaryStringMetadata())
	size, err = bat.TransportMetadataSize()
	require.NoError(t, err)
	stable, err := bat.MarshalBinary()
	require.NoError(t, err)
	var wire bytes.Buffer
	encoded, err := bat.MarshalBinaryWithMetadata(&wire, true)
	require.NoError(t, err)
	require.Equal(t, size, len(encoded)-len(stable))

	invalid := NewWithSize(2)
	invalid.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(invalid.Vecs[0], []byte("v"), false, mp))
	invalid.Vecs[0].SetIsBinaryString(true)
	invalid.SetRowCount(1)
	_, err = invalid.TransportMetadataSize()
	require.ErrorContains(t, err, "nil vector")
	invalid.Clean(mp)

	plainStatic := NewWithSize(1)
	plainStatic.Vecs[0] = vector.NewVec(types.T_varbinary.ToType())
	require.NoError(t, vector.AppendBytes(plainStatic.Vecs[0], []byte("v"), false, mp))
	plainStatic.SetRowCount(1)
	require.False(t, plainStatic.HasBinaryStringMetadata())
	require.False(t, plainStatic.HasTransportMetadata())
	require.False(t, plainStatic.HasBinaryStringMetadata(),
		"static binary semantics are already carried by the vector type")
	plainStatic.Clean(mp)
}

func TestTransportMarshalInvalidBoundaryMatrix(t *testing.T) {
	mp := mpool.MustNewZero()
	var nilBatch *Batch
	var wire bytes.Buffer
	_, err := nilBatch.MarshalBinaryWithMetadata(&wire, true)
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.ErrorIs(t, nilBatch.MarshalBinaryWithMetadataTo(&wire), io.ErrClosedPipe)

	bat := NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("value"), false, mp))
	bat.SetRowCount(1)
	bat.Vecs[0].SetIsBinaryString(true)
	require.ErrorIs(t, bat.AppendTransportMetadataTo(nil), io.ErrClosedPipe)
	bat.Clean(mp)

	_, err = NewWithSize(0).MarshalBinaryWithMetadata(nil, true)
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.Zero(t, mp.CurrNB())
}

func TestTransportMetadataPropagatesEveryWriterFailure(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := NewWithSize(3)
	for column := range bat.Vecs {
		bat.Vecs[column] = vector.NewVec(types.T_text.ToType())
		for range 2 {
			require.NoError(t, vector.AppendBytes(
				bat.Vecs[column], []byte("value"), false, mp))
		}
	}
	require.NoError(t, bat.Vecs[0].SetBinaryStringRowsWithMP([]bool{true, false}, mp))
	bat.Vecs[1].SetIsBinaryString(true)
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	var encoded bytes.Buffer
	require.NoError(t, bat.AppendTransportMetadataTo(&encoded))
	require.NotZero(t, encoded.Len())
	for failAfter := 0; failAfter < encoded.Len(); failAfter++ {
		err := bat.AppendTransportMetadataTo(&failAfterBatchWriter{
			remaining: failAfter,
		})
		require.Error(t, err, "writer failure at byte %d must propagate", failAfter)
	}
}

func TestTransportRejectsMalformedTrailer(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("5"), false, mp))
	bat.Vecs[0].SetIsBinaryString(true)
	bat.SetRowCount(1)
	defer bat.Clean(mp)
	var wire bytes.Buffer
	encoded, err := bat.MarshalBinaryWithMetadata(&wire, true)
	require.NoError(t, err)
	for _, malformed := range [][]byte{
		encoded[:len(encoded)-1],
		append(append([]byte(nil), encoded...), 0),
	} {
		reused := NewOffHeapEmpty()
		require.Error(t, reused.UnmarshalBinaryWithMetadata(malformed, mp))
		reused.Clean(mp)
	}
}

func TestTransportRejectsMismatchedCountsBeforeAllocation(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte("5"), false, mp))
	source.Vecs[0].SetIsBinaryString(true)
	source.SetRowCount(1)
	defer source.Clean(mp)
	var wire bytes.Buffer
	encoded, err := source.MarshalBinaryWithMetadata(&wire, true)
	require.NoError(t, err)
	prefixLen, err := stableBatchPayloadLength(encoded)
	require.NoError(t, err)

	for _, nVecs := range []int32{0, 2} {
		malformed := append([]byte(nil), encoded...)
		copy(malformed[prefixLen+4:prefixLen+8], types.EncodeInt32(&nVecs))
		reused := NewOffHeapEmpty()
		require.ErrorContains(t, reused.UnmarshalBinaryWithMetadata(malformed, mp),
			"vector count mismatch")
		reused.Clean(mp)
	}

	heterogeneous := NewWithSize(1)
	heterogeneous.Vecs[0] = vector.NewVec(types.T_text.ToType())
	for range 2 {
		require.NoError(t, vector.AppendBytes(heterogeneous.Vecs[0], []byte("5"), false, mp))
	}
	require.NoError(t, heterogeneous.Vecs[0].SetBinaryStringRows([]bool{true, false}))
	heterogeneous.SetRowCount(2)
	defer heterogeneous.Clean(mp)
	wire.Reset()
	heterogeneousEncoded, err := heterogeneous.MarshalBinaryWithMetadata(&wire, true)
	require.NoError(t, err)
	heterogeneousPrefix, err := stableBatchPayloadLength(heterogeneousEncoded)
	require.NoError(t, err)
	// PPB header (16 bytes) + mode byte precede the rows count.
	rowCountOffset := heterogeneousPrefix + 17
	malformed := append([]byte(nil), heterogeneousEncoded...)
	amplified := int32(batchMetadataMaxRows)
	copy(malformed[rowCountOffset:rowCountOffset+4], types.EncodeInt32(&amplified))
	reused := NewOffHeapEmpty()
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
	target := NewOffHeapEmpty()
	err = target.UnmarshalFromReaderWithMetadata(reader, int64(len(wire)), mp)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, int(batchMetadataMaxRows), target.Vecs[0].Length())
	require.False(t, target.Vecs[0].HasBinaryStringMetadata())
	require.Equal(t, mp.CurrNB(), reader.maxCurrNB,
		"truncated row metadata must not transiently amplify the MPool")
	target.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func makeTransportStreamingWire(t *testing.T) (encoded, legacy []byte) {
	t.Helper()
	mp := mpool.MustNewZero()
	source := NewWithSize(2)
	source.Vecs[0] = vector.NewVec(types.T_text.ToType())
	source.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	for i, value := range []string{"1.5", "plain", "9.5"} {
		require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte(value), false, mp))
		require.NoError(t, vector.AppendFixed(source.Vecs[1], int64(i+1), false, mp))
	}
	require.NoError(t, source.Vecs[0].SetBinaryStringRowsWithMP([]bool{true, false, true}, mp))
	source.SetRowCount(3)

	legacy, err := source.MarshalBinary()
	require.NoError(t, err)
	var wire bytes.Buffer
	encoded, err = source.MarshalBinaryWithMetadata(&wire, true)
	require.NoError(t, err)
	encoded = append([]byte(nil), encoded...)
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
	target := NewOffHeapEmpty()
	require.NoError(t, target.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(encoded), int64(len(encoded)), mp))
	require.Equal(t, 3, target.RowCount())
	requireStreamingBinaryRows(t, target)
	require.Equal(t, int64(3), vector.GetFixedAtWithTypeCheck[int64](target.Vecs[1], 2))
	require.False(t, target.Vecs[1].HasBinaryStringMetadata())
	target.Clean(mp)

	spillTarget := NewOffHeapEmpty()
	require.NoError(t, spillTarget.UnmarshalFromReaderWithMetadataForSpill(
		bytes.NewReader(encoded), int64(len(encoded)), mp))
	requireStreamingBinaryRows(t, spillTarget)
	spillTarget.Clean(mp)

	// Older writers stored a retired per-row kind (0..4) in the low bits of
	// each row byte; streaming decoders validate and drop it.
	withKinds := append([]byte(nil), encoded...)
	firstRow := len(legacy) + 4 + 4 + 8 + 1 + 4
	for row, kind := range []byte{2, 0, 3} {
		withKinds[firstRow+row] |= kind
	}
	kindsTarget := NewOffHeapEmpty()
	require.NoError(t, kindsTarget.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(withKinds), int64(len(withKinds)), mp))
	requireStreamingBinaryRows(t, kindsTarget)
	kindsTarget.Clean(mp)
	kindsSpill := NewOffHeapEmpty()
	require.NoError(t, kindsSpill.UnmarshalFromReaderWithMetadataForSpill(
		bytes.NewReader(withKinds), int64(len(withKinds)), mp))
	requireStreamingBinaryRows(t, kindsSpill)
	kindsSpill.Clean(mp)

	// An older writer's version-2 trailer adds a string-source section per
	// vector; streaming and spill decoders validate and drop it.
	olderWriter := legacyV2TransportWire(t, encoded,
		legacyRowsStringSource(types.StringSourceLiteral, types.StringSourceExpression, types.StringSourceCOMStmt),
		legacyUniformStringSource(types.StringSourceSQLPrepare))
	olderTarget := NewOffHeapEmpty()
	require.NoError(t, olderTarget.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(olderWriter), int64(len(olderWriter)), mp))
	requireStreamingBinaryRows(t, olderTarget)
	require.Equal(t, int64(3), vector.GetFixedAtWithTypeCheck[int64](olderTarget.Vecs[1], 2))
	require.False(t, olderTarget.Vecs[1].HasBinaryStringMetadata())
	olderTarget.Clean(mp)
	olderSpill := NewOffHeapEmpty()
	require.NoError(t, olderSpill.UnmarshalFromReaderWithMetadataForSpill(
		bytes.NewReader(olderWriter), int64(len(olderWriter)), mp))
	requireStreamingBinaryRows(t, olderSpill)
	olderSpill.Clean(mp)
	for _, section := range [][]byte{
		{batchMetadataModeUniform, byte(types.StringSourceExpression)},
		{batchMetadataModeUniform, 0xff},
		{0x7f},
	} {
		malformed := legacyV2TransportWire(t, encoded, legacyNoStringSource(), section)
		target := NewOffHeapEmpty()
		require.Error(t, target.UnmarshalFromReaderWithMetadata(
			bytes.NewReader(malformed), int64(len(malformed)), mp))
		require.False(t, target.Vecs[0].HasBinaryStringMetadata())
		target.Clean(mp)
	}
	require.Zero(t, mp.CurrNB())
}

func requireStreamingBinaryRows(t *testing.T, bat *Batch) {
	t.Helper()
	require.True(t, bat.Vecs[0].HasBinaryStringRows())
	for row, binary := range []bool{true, false, true} {
		require.Equal(t, binary, bat.Vecs[0].GetIsBinaryStringAt(row), "row %d", row)
	}
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
			source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(1), false, mp))
			source.Attrs = test.attrs
			source.ExtraBuf = test.extra
			source.SetRowCount(1)
			var wire bytes.Buffer
			require.NoError(t, source.MarshalBinaryWithMetadataTo(&wire))

			target := NewOffHeapEmpty()
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
		target := NewOffHeapEmpty()
		require.NoError(t, target.UnmarshalFromReaderWithMetadata(
			bytes.NewReader(encoded), int64(len(encoded)), mp))
		requireStreamingBinaryRows(t, target)
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
			target := NewOffHeapEmpty()
			err := target.UnmarshalFromReaderWithMetadata(reader, int64(len(tc.wire)), mp)
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			require.False(t, target.Vecs[0].HasBinaryStringMetadata())
			require.Equal(t, mp.CurrNB(), reader.maxCurrNB,
				"framing rejection must happen before row metadata allocation")
			target.Clean(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestTransportStreamingMalformedReuseClearsMetadata(t *testing.T) {
	encoded, legacy := makeTransportStreamingWire(t)
	secondModeOffset := len(legacy) + 4 + 4 + 8 + 1 + 4 + 3
	malformed := append([]byte(nil), encoded...)
	malformed[secondModeOffset] = 0x3f

	mp := mpool.MustNewZero()
	target := NewOffHeapEmpty()
	require.NoError(t, target.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(encoded), int64(len(encoded)), mp))
	requireStreamingBinaryRows(t, target)

	require.ErrorContains(t, target.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(malformed), int64(len(malformed)), mp),
		"invalid prepared parameter metadata mode")
	require.False(t, target.Vecs[0].HasBinaryStringMetadata())

	require.NoError(t, target.UnmarshalFromReaderWithMetadata(
		bytes.NewReader(legacy), int64(len(legacy)), mp))
	require.False(t, target.Vecs[0].HasBinaryStringMetadata())
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

func TestShrinkFailureLeavesRowCountUnpublished(t *testing.T) {
	for _, mask := range []bool{false, true} {
		t.Run(fmt.Sprintf("mask=%t", mask), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			bat := NewWithSize(2)
			t.Cleanup(func() { bat.Clean(mp) })
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int64{10, 20, 30}, nil, mp))
			values := []int64{1, 2, 3}
			data := types.EncodeSlice(values)
			lease, err := vector.NewRefCountedBufferLease(data, int64(cap(data)), nil)
			require.NoError(t, err)
			t.Cleanup(lease.Release)
			bat.Vecs[1], err = vector.NewBorrowedFixedVector(types.T_int64.ToType(), len(values), data, lease)
			require.NoError(t, err)
			bat.SetRowCount(3)
			if mask {
				var sels bitmap.Bitmap
				sels.InitWithSize(3)
				sels.AddMany([]uint64{1, 2})
				require.Error(t, bat.ShrinkByMask(&sels, false, 0, nil))
			} else {
				require.Error(t, bat.Shrink([]int64{1, 2}, false, nil))
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
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int64{10, 20, 30}, nil, mp))
			bat.SetRowCount(3)
			var sels bitmap.Bitmap
			sels.InitWithSize(3)
			sels.AddMany(tc.sels)
			require.NoError(t, bat.ShrinkByMask(&sels, tc.negate, 0, mp))
			require.Equal(t, len(tc.want), bat.RowCount())
			require.Equal(t, tc.want, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0]))
		})
	}
}
