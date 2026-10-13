// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggexec

import (
	"bytes"
	"encoding"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// These helpers retain the pre-fix group framing and BinaryMarshaler decoder.
// They are an independent compatibility oracle and the benchmark baseline;
// calling two new streaming paths alone would not prove the old wire works.
func legacyMedianChunk[T numeric | types.Decimal64 | types.Decimal128, R types.FixedSizeTExceptStrType](exec *medianColumnExecSelf[T, R], chunk int, buf *bytes.Buffer) error {
	count := int64(exec.ret.getNthChunkSize(chunk))
	if err := types.WriteInt64(buf, count); err != nil {
		return err
	}
	if err := exec.ret.marshalChunkToBuffer(chunk, buf); err != nil {
		return err
	}
	if err := types.WriteInt64(buf, count); err != nil {
		return err
	}
	start := exec.ret.optInformation.chunkSize * chunk
	for i := 0; i < int(count); i++ {
		data, err := exec.groups[start+i].MarshalBinary()
		if err != nil {
			return err
		}
		if err = types.WriteSizeBytes(data, buf); err != nil {
			return err
		}
	}
	return types.WriteInt64(buf, 0)
}

func legacyMedianDecode[T numeric | types.Decimal64 | types.Decimal128, R types.FixedSizeTExceptStrType](exec *medianColumnExecSelf[T, R], reader io.Reader, mp *mpool.MPool) error {
	if err := unmarshalFromReaderNoGroup(reader, &exec.ret.optSplitResult); err != nil {
		return err
	}
	exec.ret.setupT()
	count, err := types.ReadInt64(reader)
	if err != nil {
		return err
	}
	exec.groups = make([]*Vectors[T], int(count))
	for i := range exec.groups {
		_, data, err := types.ReadSizeBytes(reader)
		if err != nil {
			return err
		}
		group := &Vectors[T]{}
		exec.groups[i] = group
		if err = group.Unmarshal(data, exec.argType, mp); err != nil {
			return err
		}
	}
	_, err = types.ReadInt64(reader)
	return err
}

func decimal64SerdeFixture(tb testing.TB, mp *mpool.MPool, rows int) *medianColumnDecimalExec[types.Decimal64] {
	tb.Helper()
	typ := types.New(types.T_decimal64, 16, 6)
	made, err := makeMedian(mp, AggIdOfMedian, false, typ)
	require.NoError(tb, err)
	exec := made.(*medianColumnDecimalExec[types.Decimal64])
	require.NoError(tb, exec.GroupGrow(1))
	// Set up the same bounded segments produced by BulkFill without a large
	// source vector or millions of single-row calls in the measured region.
	values := make([]types.Decimal64, MaxVectorLength)
	for i := range values {
		values[i] = types.Decimal64(i + 1)
	}
	for remaining := rows; remaining > 0; {
		count := min(remaining, len(values))
		vec := exec.groups[0].getAppendableVector()
		require.NoError(tb, vector.AppendFixedList(vec, values[:count], nil, mp))
		remaining -= count
	}
	if rows > 0 {
		exec.ret.bsFromEmptyList[0][0] = false
	}
	return exec
}

func TestMedianStreamSerdeLegacyWireAndReuse(t *testing.T) {
	for _, rows := range []int{0, 1, MaxVectorLength + 1} {
		t.Run(medianSerdeRowsName(rows), func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()) }()
			source := decimal64SerdeFixture(t, mp, rows)
			defer source.Free()
			var legacy, current bytes.Buffer
			require.NoError(t, legacyMedianChunk(&source.medianColumnExecSelf, 0, &legacy))
			require.NoError(t, source.SaveIntermediateResultOfChunk(0, &current))
			require.Equal(t, legacy.Bytes(), current.Bytes())
			var selected bytes.Buffer
			require.NoError(t, source.SaveIntermediateResult(1, [][]uint8{{1}}, &selected))
			require.Equal(t, legacy.Bytes(), selected.Bytes())

			target := decimal64SerdeFixture(t, mp, 7)
			defer target.Free()
			reader := bytes.NewReader(legacy.Bytes())
			require.NoError(t, target.UnmarshalFromReader(reader, mp))
			require.Zero(t, reader.Len())
			require.Equal(t, rows, target.groups[0].Length())
			// Repeat into an already populated target. The old vectors must be
			// released, rather than retained or appended to the new groups.
			require.NoError(t, target.UnmarshalFromReader(bytes.NewReader(legacy.Bytes()), mp))
			require.Equal(t, rows, target.groups[0].Length())

			oldTarget := decimal64SerdeFixture(t, mp, 0)
			defer oldTarget.Free()
			require.NoError(t, legacyMedianDecode(&oldTarget.medianColumnExecSelf, bytes.NewReader(current.Bytes()), mp))
			require.Equal(t, rows, oldTarget.groups[0].Length())
			for i := range source.groups[0].vecs {
				require.Equal(t, vector.MustFixedColNoTypeCheck[types.Decimal64](source.groups[0].vecs[i]), vector.MustFixedColNoTypeCheck[types.Decimal64](target.groups[0].vecs[i]))
				require.Equal(t, vector.MustFixedColNoTypeCheck[types.Decimal64](source.groups[0].vecs[i]), vector.MustFixedColNoTypeCheck[types.Decimal64](oldTarget.groups[0].vecs[i]))
			}
		})
	}
}

func medianSerdeRowsName(rows int) string {
	switch rows {
	case 0:
		return "empty"
	case 1:
		return "single"
	default:
		return "segmented"
	}
}

func TestMedianStreamSerdeSecondGroupTruncationHasCleanupOwner(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	source := decimal64SerdeFixture(t, mp, 3)
	defer source.Free()
	require.NoError(t, source.GroupGrow(1))
	require.NoError(t, AppendMultiFixed(source.groups[1], types.Decimal64(7), false, 2, mp))
	source.ret.bsFromEmptyList[0][1] = false
	var encoded bytes.Buffer
	require.NoError(t, legacyMedianChunk(&source.medianColumnExecSelf, 0, &encoded))
	baseAllocated := mp.CurrNB()
	target := decimal64SerdeFixture(t, mp, 0)
	require.Error(t, target.UnmarshalFromReader(bytes.NewReader(encoded.Bytes()[:encoded.Len()-10]), mp))
	require.Len(t, target.groups, 2)
	require.Equal(t, 3, target.groups[0].Length())
	target.Free()
	require.Equal(t, baseAllocated, mp.CurrNB())
}

func TestMedianStreamSerdeNilPool(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	source := decimal64SerdeFixture(t, mp, 9)
	defer source.Free()
	var encoded bytes.Buffer
	require.NoError(t, legacyMedianChunk(&source.medianColumnExecSelf, 0, &encoded))
	for _, legacy := range []bool{false, true} {
		target := decimal64SerdeFixture(t, mp, 0)
		if legacy {
			require.NoError(t, legacyMedianDecode(&target.medianColumnExecSelf, bytes.NewReader(encoded.Bytes()), nil))
		} else {
			require.NoError(t, target.UnmarshalFromReader(bytes.NewReader(encoded.Bytes()), nil))
		}
		require.Equal(t, 9, target.groups[0].Length())
		require.Equal(t, types.Decimal64(9), vector.MustFixedColNoTypeCheck[types.Decimal64](target.groups[0].vecs[0])[8])
		target.Free()
	}
}

// Find the original group-length prefix without depending on hard-coded result
// vector sizes; the result-vector codec remains independently exercised above.
func medianGroupPrefix(t *testing.T, encoded []byte, mp *mpool.MPool) int {
	t.Helper()
	reader := bytes.NewReader(encoded)
	probe := decimal64SerdeFixture(t, mp, 0)
	defer probe.Free()
	require.NoError(t, unmarshalFromReaderNoGroup(reader, &probe.ret.optSplitResult))
	_, err := types.ReadInt64(reader)
	require.NoError(t, err)
	return len(encoded) - reader.Len()
}

func TestMedianStreamSerdeRejectsCorruptionAndFreesPartialVectors(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	source := decimal64SerdeFixture(t, mp, MaxVectorLength+1)
	defer source.Free()
	var encoded bytes.Buffer
	require.NoError(t, legacyMedianChunk(&source.medianColumnExecSelf, 0, &encoded))
	prefix := medianGroupPrefix(t, encoded.Bytes(), mp)
	baseAllocated := mp.CurrNB()
	for _, cut := range []int{prefix, prefix + 3, prefix + 4 + 7, encoded.Len() - 10, encoded.Len() - 1} {
		target := decimal64SerdeFixture(t, mp, 3)
		err := target.UnmarshalFromReader(bytes.NewReader(encoded.Bytes()[:cut]), mp)
		require.Error(t, err, "cut=%d", cut)
		target.Free()
		require.Equal(t, baseAllocated, mp.CurrNB(), "cut=%d leaked an owned partial vector", cut)
	}
	for _, size := range []int32{-1, 0, 7, math.MaxInt32} {
		broken := append([]byte(nil), encoded.Bytes()...)
		copy(broken[prefix:prefix+4], types.EncodeInt32(&size))
		target := decimal64SerdeFixture(t, mp, 0)
		require.Error(t, target.UnmarshalFromReader(bytes.NewReader(broken), mp))
		target.Free()
		require.Equal(t, baseAllocated, mp.CurrNB())
	}
	broken := append([]byte(nil), encoded.Bytes()...)
	count := int64(-1)
	copy(broken[prefix+4:prefix+12], types.EncodeInt64(&count))
	target := decimal64SerdeFixture(t, mp, 0)
	require.Error(t, target.UnmarshalFromReader(bytes.NewReader(broken), mp))
	target.Free()
	require.Equal(t, baseAllocated, mp.CurrNB())
}

func TestMedianStreamSerdePreservesGroupPadding(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	source := decimal64SerdeFixture(t, mp, 3)
	defer source.Free()
	var encoded bytes.Buffer
	require.NoError(t, legacyMedianChunk(&source.medianColumnExecSelf, 0, &encoded))
	prefix := medianGroupPrefix(t, encoded.Bytes(), mp)
	size := types.DecodeInt32(encoded.Bytes()[prefix : prefix+4])
	end := prefix + 4 + int(size)
	padded := append([]byte(nil), encoded.Bytes()[:end]...)
	padded = append(padded, []byte("pad")...)
	padded = append(padded, encoded.Bytes()[end:]...)
	size += 3
	copy(padded[prefix:prefix+4], types.EncodeInt32(&size))
	for _, legacy := range []bool{false, true} {
		target := decimal64SerdeFixture(t, mp, 0)
		reader := bytes.NewReader(padded)
		if legacy {
			require.NoError(t, legacyMedianDecode(&target.medianColumnExecSelf, reader, mp))
		} else {
			require.NoError(t, target.UnmarshalFromReader(reader, mp))
		}
		require.Zero(t, reader.Len())
		require.Equal(t, 3, target.groups[0].Length())
		target.Free()
	}
}

func TestMedianStreamSerdeKeepsLegacyVectorValidation(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	source := decimal64SerdeFixture(t, mp, 3)
	defer source.Free()
	var encoded bytes.Buffer
	require.NoError(t, legacyMedianChunk(&source.medianColumnExecSelf, 0, &encoded))
	prefix := medianGroupPrefix(t, encoded.Bytes(), mp)
	vectorPrefix := prefix + 4 + 8
	vectorSize := types.DecodeUint32(encoded.Bytes()[vectorPrefix : vectorPrefix+4])
	end := vectorPrefix + 4 + int(vectorSize)
	badFlag := append([]byte(nil), encoded.Bytes()...)
	badFlag[end-1] = 2
	padded := append([]byte(nil), encoded.Bytes()[:end]...)
	padded = append(padded, []byte("pad")...)
	padded = append(padded, encoded.Bytes()[end:]...)
	vectorSize += 3
	copy(padded[vectorPrefix:vectorPrefix+4], types.EncodeUint32(&vectorSize))
	groupSize := types.DecodeInt32(padded[prefix:prefix+4]) + 3
	copy(padded[prefix:prefix+4], types.EncodeInt32(&groupSize))
	baseAllocated := mp.CurrNB()
	for _, data := range [][]byte{badFlag, padded} {
		for _, legacy := range []bool{false, true} {
			target := decimal64SerdeFixture(t, mp, 0)
			if legacy {
				require.Error(t, legacyMedianDecode(&target.medianColumnExecSelf, bytes.NewReader(data), mp))
			} else {
				require.Error(t, target.UnmarshalFromReader(bytes.NewReader(data), mp))
			}
			target.Free()
			require.Equal(t, baseAllocated, mp.CurrNB())
		}
	}
}

type serdeWriteLimit struct {
	limit int
	err   error
}

func (w serdeWriteLimit) Write(data []byte) (int, error) {
	if len(data) > w.limit {
		return w.limit, w.err
	}
	return len(data), nil
}

type oversizedSerdeGroup struct{ size int }

func (g oversizedSerdeGroup) MarshalBinary() ([]byte, error)  { panic("stream fallback") }
func (g oversizedSerdeGroup) MarshalBinarySize() (int, error) { return g.size, nil }
func (g oversizedSerdeGroup) MarshalBinaryTo(io.Writer) error { panic("overflow not rejected") }

func TestMedianStreamSerdeWriterErrorsAndGroupOverflow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	source := decimal64SerdeFixture(t, mp, 3)
	defer source.Free()
	group := source.groups[0]
	for _, limit := range []int{0, 3, 7, 20} {
		require.ErrorIs(t, group.MarshalBinaryTo(serdeWriteLimit{limit: limit}), io.ErrShortWrite)
	}
	failure := errors.New("writer failed")
	require.ErrorIs(t, group.MarshalBinaryTo(serdeWriteLimit{limit: 0, err: failure}), failure)
	require.ErrorIs(t, group.MarshalBinaryTo(nil), io.ErrClosedPipe)
	require.ErrorIs(t, writeRuntimeAggGroup(group, serdeWriteLimit{limit: 3}), io.ErrShortWrite)
	large := int64(math.MaxInt32) + 1
	for _, size := range []int{-1, int(large)} {
		var destination bytes.Buffer
		require.Error(t, writeRuntimeAggGroup(oversizedSerdeGroup{size: size}, &destination))
		require.Zero(t, destination.Len())
	}
	// Aggregate groups which do not opt into streaming retain the old path.
	var fallback bytes.Buffer
	require.NoError(t, writeRuntimeAggGroup(fakeSerdeGroup("legacy"), &fallback))
	_, data, err := types.ReadSizeBytes(bytes.NewReader(fallback.Bytes()))
	require.NoError(t, err)
	require.Equal(t, "legacy", string(data))
}

type serdeReadRequests struct {
	io.Reader
	maxRequest int
}

func (r *serdeReadRequests) Read(data []byte) (int, error) {
	r.maxRequest = max(r.maxRequest, len(data))
	return r.Reader.Read(data)
}

type serdeWriteRequests struct{ maxRequest int }

func (w *serdeWriteRequests) Write(data []byte) (int, error) {
	w.maxRequest = max(w.maxRequest, len(data))
	return len(data), nil
}

func TestMedianStreamSerdeUsesVectorSizedReadsAndWrites(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	source := decimal64SerdeFixture(t, mp, 2*MaxVectorLength+1)
	defer source.Free()
	newWriter := &serdeWriteRequests{}
	require.NoError(t, writeRuntimeAggGroup(source.groups[0], newWriter))
	require.LessOrEqual(t, newWriter.maxRequest, MaxVectorLength*8)
	oldWriter := &serdeWriteRequests{}
	// Erase the optional streaming methods; this is exactly the old group
	// BinaryMarshaler contract, so it still materializes the complete group.
	oldGroup := struct{ encoding.BinaryMarshaler }{source.groups[0]}
	require.NoError(t, writeRuntimeAggGroup(oldGroup, oldWriter))
	require.Greater(t, oldWriter.maxRequest, MaxVectorLength*8)
	var encoded bytes.Buffer
	require.NoError(t, legacyMedianChunk(&source.medianColumnExecSelf, 0, &encoded))
	for _, legacy := range []bool{false, true} {
		target := decimal64SerdeFixture(t, mp, 0)
		reader := &serdeReadRequests{Reader: bytes.NewReader(encoded.Bytes())}
		if legacy {
			require.NoError(t, legacyMedianDecode(&target.medianColumnExecSelf, reader, mp))
			require.Greater(t, reader.maxRequest, MaxVectorLength*8)
		} else {
			require.NoError(t, target.UnmarshalFromReader(reader, mp))
			require.LessOrEqual(t, reader.maxRequest, MaxVectorLength*8)
		}
		target.Free()
	}
}

func BenchmarkMedianGroupSerde(b *testing.B) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(b, mp.CurrNB()) }()
	// 32 MiB of Decimal64 state, enough to expose full-group heap copies while
	// keeping the experiment short and independent of a billion-row cluster.
	source := decimal64SerdeFixture(b, mp, 4*1024*1024)
	defer source.Free()
	var encoded bytes.Buffer
	require.NoError(b, legacyMedianChunk(&source.medianColumnExecSelf, 0, &encoded))
	for _, legacy := range []bool{true, false} {
		name := "stream"
		if legacy {
			name = "legacy"
		}
		b.Run("serialize/"+name, func(b *testing.B) {
			var output bytes.Buffer
			output.Grow(encoded.Len())
			b.ReportAllocs()
			b.SetBytes(int64(encoded.Len()))
			b.ResetTimer()
			for range b.N {
				output.Reset()
				if legacy {
					require.NoError(b, legacyMedianChunk(&source.medianColumnExecSelf, 0, &output))
				} else {
					require.NoError(b, source.SaveIntermediateResultOfChunk(0, &output))
				}
			}
		})
		b.Run("decode/"+name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(encoded.Len()))
			b.ResetTimer()
			for range b.N {
				made, err := makeMedian(mp, AggIdOfMedian, false, source.argType)
				require.NoError(b, err)
				target := made.(*medianColumnDecimalExec[types.Decimal64])
				reader := bytes.NewReader(encoded.Bytes())
				if legacy {
					err = legacyMedianDecode(&target.medianColumnExecSelf, reader, mp)
				} else {
					err = target.UnmarshalFromReader(reader, mp)
				}
				require.NoError(b, err)
				target.Free()
			}
		})
	}
}
