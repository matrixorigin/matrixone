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

package vector

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

type selectedRowsOneShotShortWriter struct {
	short bool
}

type selectedRowsFastBuffer struct {
	bytes.Buffer
	fastCalls    int
	varlenaCalls int
	short        bool
}

func (w *selectedRowsFastBuffer) WriteSelectedVarlenaRows(values []types.Varlena, area []byte, rows []int64) (int, error) {
	w.varlenaCalls++
	start := w.Len()
	for _, row := range rows {
		value := values[row].GetByteSlice(area)
		if err := types.WriteInt32(&w.Buffer, int32(len(value))); err != nil {
			return w.Len() - start, err
		}
		if _, err := w.Buffer.Write(value); err != nil {
			return w.Len() - start, err
		}
	}
	written := w.Len() - start
	if w.short && written > 0 {
		written--
	}
	return written, nil
}

func TestSelectedRowsVarlenaWriterMatchesReference(t *testing.T) {
	pool := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), pool)
	t.Cleanup(func() { source.Free(pool); require.Zero(t, pool.CurrNB()) })
	require.NoError(t, AppendStringList(source, []string{"", "short", string(bytes.Repeat([]byte("long"), 20))}, nil, pool))
	rows := []int64{2, 0, 1, 2}
	for _, tc := range []struct {
		name   string
		modify func()
		fast   bool
	}{
		{name: "plain", fast: true},
		{name: "null fallback", modify: func() { source.SetNull(1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() {
				source.GetNulls().Clear()
			})
			if tc.modify != nil {
				tc.modify()
			}
			var reference bytes.Buffer
			require.NoError(t, source.MarshalSelectedRowsTo(&reference, rows))
			fast := &selectedRowsFastBuffer{}
			require.NoError(t, source.MarshalSelectedRowsTo(fast, rows))
			require.Equal(t, reference.Bytes(), fast.Bytes())
			require.Equal(t, tc.fast, fast.varlenaCalls == 1)
		})
	}
	require.ErrorIs(t, source.MarshalSelectedRowsTo(&selectedRowsFastBuffer{short: true}, rows), io.ErrShortWrite)
}

func (w *selectedRowsFastBuffer) WriteSelectedFixedRows(
	data []byte,
	width int,
	rows []int64,
) (int, error) {
	w.fastCalls++
	written := 0
	for _, selected := range rows {
		row := int(selected)
		n, err := w.Buffer.Write(data[row*width : (row+1)*width])
		written += n
		if err != nil {
			return written, err
		}
	}
	if w.short && written > 0 {
		return written - 1, nil
	}
	return written, nil
}

func (w *selectedRowsOneShotShortWriter) Write(value []byte) (int, error) {
	if !w.short && len(value) != 0 {
		w.short = true
		return len(value) - 1, nil
	}
	return len(value), nil
}

func TestSelectedRowsCodecPreservesFixedWidthMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType(), mp)
	destination := NewVec(types.T_int64.ToType(), mp)
	t.Cleanup(func() {
		destination.Free(mp)
		source.Free(mp)
		require.Zero(t, mp.CurrNB())
	})
	require.NoError(t, AppendFixedList(source, []int64{10, 20, 30, 40}, nil, mp))
	source.SetNull(1)

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&encoded, []int64{3, 1, 0}))
	expected := []byte{
		3, 0, 0, 0, 0x01, 8, 0, 0, 0, // count, NULL flag, fixed width
		0, 40, 0, 0, 0, 0, 0, 0, 0, // ordinary value 40
		1,                          // NULL: no value bytes
		0, 10, 0, 0, 0, 0, 0, 0, 0, // ordinary value 10
	}
	require.Equal(t, expected, encoded.Bytes())
	state := newTestVectorAllocationAccount(t, 1024, 4)
	t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state) })
	primitive, err := mpool.NewAccountedBuffer(
		mp, state.account, testVectorAllocationOwner, testVectorDataAllocationSite)
	require.NoError(t, err)
	t.Cleanup(primitive.Free)
	require.NoError(t, source.MarshalSelectedRowsTo(primitive, []int64{3, 1, 0}))
	require.Equal(t, expected, primitive.Bytes())
	require.NoError(t, destination.UnmarshalSelectedRowsFrom(&encoded, 3, mp))
	require.Equal(t, []int64{40, 0, 10}, MustFixedColWithTypeCheck[int64](destination))
	require.False(t, destination.IsNull(0))
	require.True(t, destination.IsNull(1))
	require.False(t, destination.IsNull(2))
	require.Zero(t, encoded.Len())

	// NULL is the only defined bit. Any other header bit is rejected before
	// decoding starts, leaving the destination untouched; any other row-flag
	// bit is rejected mid-decode, leaving it empty.
	for bit := 1; bit < 8; bit++ {
		before := destination.Length()
		bad := append([]byte(nil), expected...)
		bad[4] |= 1 << bit
		require.ErrorContains(t, destination.UnmarshalSelectedRowsFrom(
			bytes.NewReader(bad), 3, mp), "invalid selected vector metadata", "header bit %d", bit)
		require.Equal(t, before, destination.Length())
		bad = append([]byte(nil), expected...)
		bad[18] |= 1 << bit
		require.ErrorContains(t, destination.UnmarshalSelectedRowsFrom(
			bytes.NewReader(bad), 3, mp), "row metadata", "row bit %d", bit)
		require.Zero(t, destination.Length())
	}
}

func TestSelectedRowsCodecFixedWidthWriterFastPathMatchesWire(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType(), mp)
	t.Cleanup(func() {
		source.Free(mp)
		require.Zero(t, mp.CurrNB())
	})
	values := make([]int64, 257)
	for i := range values {
		values[i] = int64(i*17 - 3)
	}
	require.NoError(t, AppendFixedList(source, values, nil, mp))
	rows := []int64{256, 0, 129, 7}

	var reference bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&reference, rows))
	fast := &selectedRowsFastBuffer{}
	require.NoError(t, source.MarshalSelectedRowsTo(fast, rows))
	require.Equal(t, 1, fast.fastCalls)
	require.Equal(t, reference.Bytes(), fast.Bytes())

	fast.Reset()
	fast.fastCalls = 0
	source.SetNull(7)
	require.NoError(t, source.MarshalSelectedRowsTo(fast, rows))
	require.Zero(t, fast.fastCalls, "row metadata requires the reference writer path")

	constant, err := NewConstFixed(types.T_int64.ToType(), int64(41), 4, mp)
	require.NoError(t, err)
	defer constant.Free(mp)
	fast.Reset()
	fast.fastCalls = 0
	require.NoError(t, constant.MarshalSelectedRowsTo(fast, []int64{3, 1}))
	require.Zero(t, fast.fastCalls, "constant vectors broadcast one physical value")

	fast.Reset()
	fast.fastCalls = 0
	source.GetNulls().Del(7)
	fast.short = true
	require.ErrorIs(t, source.MarshalSelectedRowsTo(fast, rows), io.ErrShortWrite)
	require.Equal(t, 1, fast.fastCalls)
}

func TestSelectedRowsCodecUniformMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType(), mp)
	defer func() { source.Free(mp); require.Zero(t, mp.CurrNB()) }()
	require.NoError(t, AppendFixedList(source, []int64{7, 8, 9, 10}, nil, mp))
	for _, rows := range [][]int64{{}, {0, 2, 3}, {3, 0, 3, 1}} {
		func() {
			selected := NewVec(types.T_int64.ToType(), mp)
			defer selected.Free(mp)
			require.NoError(t, selected.Union(source, rows, mp))
			all := make([]int64, len(rows))
			for i := range all {
				all[i] = int64(i)
			}
			// Encoding a selection equals encoding its materialized copy.
			var got, want bytes.Buffer
			require.NoError(t, source.MarshalSelectedRowsTo(&got, rows))
			require.NoError(t, selected.MarshalSelectedRowsTo(&want, all))
			require.Equal(t, want.Bytes(), got.Bytes())
		}()
	}
	require.Error(t, source.MarshalSelectedRowsTo(io.Discard, []int64{-1}))
	require.Error(t, source.MarshalSelectedRowsTo(io.Discard, []int64{4}))
	var encoded bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&encoded, []int64{3, 0}))
	for cut := 0; cut < encoded.Len(); cut++ {
		require.Error(t, source.MarshalSelectedRowsTo(
			&failSelectedRowsWriter{remaining: cut}, []int64{3, 0}), "cut=%d", cut)
	}

}

func TestSelectedRowsCodecFixedWidthEmptyAndAllNull(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType(), mp)
	destination := NewVec(types.T_int64.ToType(), mp)
	t.Cleanup(func() {
		destination.Free(mp)
		source.Free(mp)
		require.Zero(t, mp.CurrNB())
	})

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&encoded, nil))
	require.Equal(t, 4+1+4, encoded.Len())
	require.NoError(t, destination.UnmarshalSelectedRowsFrom(&encoded, 0, mp))
	require.Zero(t, destination.Length())
	require.Zero(t, encoded.Len())

	require.NoError(t, AppendFixedList(source, []int64{10, 20}, []bool{true, true}, mp))
	require.NoError(t, source.MarshalSelectedRowsTo(&encoded, []int64{1, 0}))
	require.NoError(t, destination.UnmarshalSelectedRowsFrom(&encoded, 2, mp))
	require.True(t, destination.IsNull(0))
	require.True(t, destination.IsNull(1))
	require.Zero(t, encoded.Len())
}

// A sparse selection (256 of 8192 rows) encodes in proportion to the
// selection, not the source.
func BenchmarkSelectedRowsSparse(b *testing.B) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType(), mp)
	values := make([]int64, 8192)
	for i := range values {
		values[i] = int64(i)
	}
	require.NoError(b, AppendFixedList(source, values, nil, mp))
	rows := make([]int64, 0, 256)
	for row := 0; row < len(values); row += len(values) / 256 {
		rows = append(rows, int64(row))
	}
	b.Cleanup(func() {
		source.Free(mp)
		require.Zero(b, mp.CurrNB())
	})
	b.ReportAllocs()
	for b.Loop() {
		if err := source.MarshalSelectedRowsTo(io.Discard, rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectedRowsFixedWidth(b *testing.B) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType(), mp)
	values := make([]int64, 8192)
	rows := make([]int64, 0, len(values)/2)
	for i := range values {
		values[i] = int64(i * 7)
		if i%2 == 0 {
			rows = append(rows, int64(i))
		}
	}
	require.NoError(b, AppendFixedList(source, values, nil, mp))
	var encoded bytes.Buffer
	require.NoError(b, source.MarshalSelectedRowsTo(&encoded, rows))
	payload := bytes.Clone(encoded.Bytes())
	b.Cleanup(func() {
		source.Free(mp)
		require.Zero(b, mp.CurrNB())
	})

	b.Run("encode", func(b *testing.B) {
		var output bytes.Buffer
		output.Grow(len(payload))
		b.ReportAllocs()
		b.SetBytes(int64(len(rows) * source.GetType().TypeSize()))
		for b.Loop() {
			output.Reset()
			if err := source.MarshalSelectedRowsTo(&output, rows); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(output.Len()), "encoded-B")
	})
	b.Run("decode", func(b *testing.B) {
		destination := NewVec(types.T_int64.ToType(), mp)
		defer destination.Free(mp)
		reader := bytes.NewReader(payload)
		b.ReportAllocs()
		b.SetBytes(int64(len(rows) * source.GetType().TypeSize()))
		for b.Loop() {
			reader.Reset(payload)
			if err := destination.UnmarshalSelectedRowsFrom(reader, len(rows), mp); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(payload)), "encoded-B")
	})
}

func BenchmarkSelectedRowsVarlenaDecode(b *testing.B) {
	for _, rows := range []int{256, 512, 8192} {
		for _, tc := range []struct {
			name     string
			typ      types.Type
			value    func(int) []byte
			nullable bool
		}{
			{"varchar_inline_23", types.T_varchar.ToType(), func(i int) []byte {
				return bytes.Repeat([]byte{byte(i)}, types.VarlenaInlineSize)
			}, false},
			{"varchar_nullable_23", types.T_varchar.ToType(), func(i int) []byte {
				return bytes.Repeat([]byte{byte(i)}, types.VarlenaInlineSize)
			}, true},
			{"varchar_long_64", types.T_varchar.ToType(), func(i int) []byte {
				return bytes.Repeat([]byte{byte(i)}, 64)
			}, false},
			{"varchar_area_24", types.T_varchar.ToType(), func(i int) []byte {
				return bytes.Repeat([]byte{byte(i)}, types.VarlenaInlineSize+1)
			}, false},
			{"vecf32_inline_16", types.T_array_float32.ToType(), func(i int) []byte {
				return types.ArrayToBytes([]float32{float32(i), 2, 3, 4})
			}, false},
			{"vecf64_area_32", types.T_array_float64.ToType(), func(i int) []byte {
				return types.ArrayToBytes([]float64{float64(i), 2, 3, 4})
			}, false},
		} {
			b.Run(fmt.Sprintf("%s/rows_%d", tc.name, rows), func(b *testing.B) {
				mp := mpool.MustNewZero()
				b.Cleanup(func() { mpool.DeleteMPool(mp) })
				source := NewVec(tc.typ, mp)
				account := newTestVectorAllocationAccount(b, 16<<20, 64)
				dmp := account.view(b, mp)
				destination := NewVec(tc.typ, dmp)
				b.Cleanup(func() {
					destination.Free(dmp)
					source.Free(mp)
					finalizeTestVectorAllocationAccount(b, account)
					require.Zero(b, mp.CurrNB())
				})
				selected := make([]int64, rows)
				for i := range rows {
					require.NoError(b, AppendBytes(source, tc.value(i), tc.nullable && i%4 == 0, mp))
					selected[i] = int64(i)
				}
				var encoded bytes.Buffer
				require.NoError(b, source.MarshalSelectedRowsTo(&encoded, selected))
				payload := encoded.Bytes()
				reader := bytes.NewReader(payload)
				// Warm the reusable, accounted destination before measuring the
				// per-record codec cost; setup and capacity growth are separate.
				require.NoError(b, destination.UnmarshalSelectedRowsFrom(reader, rows, dmp))
				b.ReportAllocs()
				b.SetBytes(int64(len(payload)))
				b.ResetTimer()
				for b.Loop() {
					reader.Reset(payload)
					if err := destination.UnmarshalSelectedRowsFrom(reader, rows, dmp); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				require.Equal(b, source.Length(), destination.Length())
				require.Zero(b, reader.Len())
				for row := range rows {
					require.Equal(b, source.IsNull(uint64(row)), destination.IsNull(uint64(row)))
					if !source.IsNull(uint64(row)) {
						require.Equal(b, source.GetBytesAt(row), destination.GetBytesAt(row))
					}
				}
				b.ReportMetric(float64(account.account.Snapshot().Peak), "account-peak-B")
			})
		}
	}
}

func TestSelectedRowsVarlenaDecodeInlineBoundaryAndReuse(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typ    types.Type
		values [][]byte
	}{
		{"varchar", types.T_varchar.ToType(), [][]byte{
			bytes.Repeat([]byte{'a'}, types.VarlenaInlineSize+1),
			bytes.Repeat([]byte{'b'}, types.VarlenaInlineSize), {}, []byte("short"),
		}},
		{"vecf32", types.T_array_float32.ToType(), [][]byte{
			types.ArrayToBytes([]float32{1, 2, 3, 4, 5, 6}),
			types.ArrayToBytes([]float32{-0.5, 2, 3, 4}), {},
			types.ArrayToBytes([]float32{7}),
		}},
		{"vecf64", types.T_array_float64.ToType(), [][]byte{
			types.ArrayToBytes([]float64{1, 2, 3}),
			types.ArrayToBytes([]float64{-0.5, 2}), {},
			types.ArrayToBytes([]float64{7}),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			source := NewVec(tc.typ, mp)
			account := newTestVectorAllocationAccount(t, 1<<20, 64)
			dmp := account.view(t, mp)
			destination := NewVec(tc.typ, dmp)
			t.Cleanup(func() {
				destination.Free(dmp)
				source.Free(mp)
				finalizeTestVectorAllocationAccount(t, account)
				require.Zero(t, mp.CurrNB())
			})
			require.NoError(t, AppendBytesList(source, tc.values, nil, mp))
			require.NoError(t, AppendBytes(source, nil, true, mp))
			// The second decode moves an inline value onto an earlier external
			// descriptor; its unused inline bytes must not retain old offsets.
			for _, selected := range [][]int64{{0, 1, 2, 3, 4}, {3, 2, 1, 0, 4}} {
				var encoded bytes.Buffer
				require.NoError(t, source.MarshalSelectedRowsTo(&encoded, selected))
				require.NoError(t, destination.UnmarshalSelectedRowsFrom(&encoded, len(selected), dmp))
				require.Zero(t, encoded.Len())
				descriptors := MustFixedColNoTypeCheck[types.Varlena](destination)
				for row, input := range selected {
					if source.IsNull(uint64(input)) {
						require.True(t, destination.IsNull(uint64(row)))
						continue
					}
					want := tc.values[input]
					require.False(t, destination.IsNull(uint64(row)))
					require.Equal(t, want, destination.GetBytesAt(row))
					if len(want) <= types.VarlenaInlineSize {
						require.Equal(t, make([]byte, types.VarlenaSize-1-len(want)),
							descriptors[row][1+len(want):])
					}
				}
			}
		})
	}
}

func TestSelectedRowsVarlenaDecodeAccountCapacityAndReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	source := NewVec(types.T_varchar.ToType(), mp)
	t.Cleanup(func() {
		source.Free(mp)
		require.Zero(t, mp.CurrNB())
	})
	require.NoError(t, AppendBytesList(source, [][]byte{
		[]byte("inline"), bytes.Repeat([]byte{'x'}, 80),
	}, nil, mp))
	var encoded, recovery bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&encoded, []int64{0, 1}))
	require.NoError(t, source.MarshalSelectedRowsTo(&recovery, []int64{0}))

	run := func(t *testing.T, limit uint64, rejected bool) uint64 {
		t.Helper()
		account := newTestVectorAllocationAccount(t, limit, 64)
		dmp := account.view(t, mp)
		destination := NewVec(source.typ, dmp)
		t.Cleanup(func() {
			destination.Free(dmp)
			finalizeTestVectorAllocationAccount(t, account)
		})
		err := destination.UnmarshalSelectedRowsFrom(bytes.NewReader(encoded.Bytes()), 2, dmp)
		if rejected {
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Zero(t, destination.Length(), "the earlier inline row must not be published")
			require.NoError(t, destination.UnmarshalSelectedRowsFrom(bytes.NewReader(recovery.Bytes()), 1, dmp))
			require.Equal(t, []byte("inline"), destination.GetBytesAt(0))
		} else {
			require.NoError(t, err)
			require.Equal(t, source.GetBytesAt(1), destination.GetBytesAt(1))
		}
		return account.account.Snapshot().Peak
	}
	var peak uint64
	t.Run("measured-peak", func(t *testing.T) { peak = run(t, 1<<20, false) })
	require.Positive(t, peak)
	t.Run("exact-peak", func(t *testing.T) { require.Equal(t, peak, run(t, peak, false)) })
	t.Run("one-byte-short", func(t *testing.T) { run(t, peak-1, true) })
}

func TestSelectedRowsVarlenaDecodeRejectsInvalidLength(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	destination := NewVec(types.T_varchar.ToType(), mp)
	t.Cleanup(func() {
		destination.Free(mp)
		require.Zero(t, mp.CurrNB())
	})
	for _, tc := range []struct {
		name string
		size int32
		want string
	}{
		{"negative", -1, "value size"},
		{"min-int32", -1 << 31, "value size"},
		{"truncated-inline", types.VarlenaInlineSize, "EOF"},
		{"truncated-area", types.VarlenaInlineSize + 1, "EOF"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var payload bytes.Buffer
			require.NoError(t, types.WriteInt32(&payload, 1))
			require.NoError(t, payload.WriteByte(0))
			require.NoError(t, types.WriteInt32(&payload, tc.size))
			require.ErrorContains(t,
				destination.UnmarshalSelectedRowsFrom(&payload, 1, mp), tc.want)
			require.Zero(t, destination.Length())
		})
	}
}

func TestSelectedRowsCodecPreservesMetadataAndVarlena(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), mp)
	defer source.Free(mp)
	require.NoError(t, AppendBytes(source, []byte("zero"), false, mp))
	require.NoError(t, AppendBytes(source, nil, true, mp))
	require.NoError(t, AppendBytes(source, bytes.Repeat([]byte("x"), 80), false, mp))
	require.NoError(t, AppendBytes(source, []byte("three"), false, mp))

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&encoded, []int64{3, 1, 0}))
	destination := NewVec(types.T_varchar.ToType(), mp)
	defer destination.Free(mp)
	require.NoError(t, destination.UnmarshalSelectedRowsFrom(&encoded, 3, mp))

	require.Equal(t, "three", string(destination.GetBytesAt(0)))
	require.True(t, destination.IsNull(1))
	require.Equal(t, "zero", string(destination.GetBytesAt(2)))
	require.False(t, destination.IsNull(0))
	require.False(t, destination.IsNull(2))
	require.Zero(t, encoded.Len())
}

func TestSelectedRowsCodecRejectsInvalidOrTruncatedRecords(t *testing.T) {
	mp := mpool.MustNewZero()
	destination := NewVec(types.T_int64.ToType(), mp)
	defer destination.Free(mp)
	var nilVector *Vector
	require.Error(t, nilVector.MarshalSelectedRowsTo(io.Discard, nil))
	require.Error(t, destination.marshalSelectedRowsTo(nil, 0, nil, func(int) int { return 0 }))
	require.Error(t, destination.marshalSelectedRowsTo(
		io.Discard, -1, nil, func(int) int { return 0 }))
	require.Error(t, destination.MarshalSelectedRowsTo(io.Discard, []int64{0}))
	require.Error(t, nilVector.UnmarshalSelectedRowsFrom(bytes.NewReader(nil), 0, mp))
	require.Error(t, destination.UnmarshalSelectedRowsFrom(nil, 0, mp))
	require.Error(t, destination.UnmarshalSelectedRowsFrom(bytes.NewReader(nil), 0, nil))
	require.Error(t, destination.UnmarshalSelectedRowsFrom(bytes.NewReader(nil), -1, mp))
	constant := NewVec(types.T_int64.ToType(), mp)
	constant.ToConst()
	defer constant.Free(mp)
	require.ErrorContains(t,
		constant.UnmarshalSelectedRowsFrom(bytes.NewReader(nil), 0, mp),
		"non-constant destination")

	var wrongCount bytes.Buffer
	require.NoError(t, types.WriteInt32(&wrongCount, 2))
	require.ErrorContains(t,
		destination.UnmarshalSelectedRowsFrom(&wrongCount, 1, mp),
		"does not match")

	source := NewVec(types.T_int64.ToType(), mp)
	defer source.Free(mp)
	require.NoError(t, AppendFixedList(source, []int64{42}, nil, mp))
	var encoded bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&encoded, []int64{0}))
	payload := encoded.Bytes()
	require.Greater(t, len(payload), 1)
	require.Error(t, destination.UnmarshalSelectedRowsFrom(
		bytes.NewReader(payload[:len(payload)-1]), 1, mp))
	require.Zero(t, destination.Length())

	short := &selectedRowsOneShotShortWriter{}
	require.ErrorIs(t,
		source.MarshalSelectedRowsTo(short, []int64{0}),
		io.ErrShortWrite,
	)
}

func TestSelectedRowsCodecRejectsInvalidMetadataBeforePublishingRows(t *testing.T) {
	mp := mpool.MustNewZero()
	destination := NewVec(types.T_int64.ToType(), mp)
	defer destination.Free(mp)

	tests := []struct {
		name    string
		payload func(*bytes.Buffer)
		want    string
	}{
		{
			name: "reserved-vector-metadata",
			payload: func(buf *bytes.Buffer) {
				require.NoError(t, types.WriteInt32(buf, 1))
				require.NoError(t, buf.WriteByte(0xc0))
			},
			want: "metadata",
		},
		{
			name: "undefined-vector-metadata-bit",
			payload: func(buf *bytes.Buffer) {
				require.NoError(t, types.WriteInt32(buf, 1))
				require.NoError(t, buf.WriteByte(1<<2))
			},
			want: "metadata",
		},
		{
			name: "undefined-row-flag-bit",
			payload: func(buf *bytes.Buffer) {
				require.NoError(t, types.WriteInt32(buf, 1))
				require.NoError(t, buf.WriteByte(selectedRowsHasNull))
				require.NoError(t, types.WriteInt32(buf, int32(types.T_int64.TypeLen())))
				require.NoError(t, buf.WriteByte(selectedRowsHasNull|1<<1))
			},
			want: "row metadata",
		},
		{
			name: "fixed-width-mismatch",
			payload: func(buf *bytes.Buffer) {
				require.NoError(t, types.WriteInt32(buf, 1))
				require.NoError(t, buf.WriteByte(0))
				require.NoError(t, types.WriteInt32(buf, 4))
			},
			want: "value size",
		},
		{
			name: "truncated-fixed-width-value",
			payload: func(buf *bytes.Buffer) {
				require.NoError(t, types.WriteInt32(buf, 1))
				require.NoError(t, buf.WriteByte(0))
				require.NoError(t, types.WriteInt32(buf, int32(types.T_int64.TypeLen())))
				require.NoError(t, types.WriteFull(buf, make([]byte, 7)))
			},
			want: "EOF",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var payload bytes.Buffer
			tc.payload(&payload)
			err := destination.UnmarshalSelectedRowsFrom(&payload, 1, mp)
			require.ErrorContains(t, err, tc.want)
			require.Zero(t, destination.Length())
		})
	}
}

func TestSelectedRowsCodecEveryWireBoundaryIsAtomic(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), mp)
	defer func() {
		source.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, AppendBytes(source, bytes.Repeat([]byte("x"), 80), false, mp))
	require.NoError(t, AppendBytes(source, []byte("ordinary"), false, mp))
	require.NoError(t, AppendBytes(source, nil, true, mp))

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&encoded, []int64{0, 1, 2}))
	// The NULL row makes every row carry a flag byte; every cut is rejected
	// atomically.
	payload := encoded.Bytes()
	for cut := 0; cut < len(payload); cut++ {
		func() {
			destination := NewVec(types.T_varchar.ToType(), mp)
			defer destination.Free(mp)
			require.Error(t, destination.UnmarshalSelectedRowsFrom(
				bytes.NewReader(payload[:cut]), 3, mp), "cut=%d", cut)
			require.Zero(t, destination.Length(), "cut=%d", cut)
			// Reuse the same storage after a partially decoded descriptor or area.
			require.NoError(t, destination.UnmarshalSelectedRowsFrom(bytes.NewReader(payload), 3, mp))
			require.Equal(t, source.GetBytesAt(0), destination.GetBytesAt(0))
			require.Equal(t, source.GetBytesAt(1), destination.GetBytesAt(1))
			require.True(t, destination.IsNull(2))
		}()
	}
	for cut := 0; cut < len(payload); cut++ {
		require.Error(t, source.MarshalSelectedRowsTo(
			&failSelectedRowsWriter{remaining: cut}, []int64{0, 1, 2}), "cut=%d", cut)
	}
}

type failSelectedRowsWriter struct {
	remaining int
}

func (w *failSelectedRowsWriter) Write(value []byte) (int, error) {
	if len(value) <= w.remaining {
		w.remaining -= len(value)
		return len(value), nil
	}
	if w.remaining == 0 {
		return 0, io.ErrClosedPipe
	}
	n := w.remaining
	w.remaining = 0
	return n, io.ErrClosedPipe
}
