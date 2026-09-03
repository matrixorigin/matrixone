// Copyright 2026 Matrix Origin
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

package sidecarflight

import (
	"bytes"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestNativeInputFrameIsOneQueryPoolAllocation(t *testing.T) {
	sourceMP := mpool.MustNewZero()
	frameMP := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Attrs = []string{"v"}
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for row := int64(0); row < 8; row++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], row, row == 3, sourceMP))
	}
	bat.SetRowCount(8)
	defer func() {
		bat.Clean(sourceMP)
		require.Zero(t, sourceMP.CurrNB())
		require.Zero(t, frameMP.CurrNB())
	}()

	payload, err := bat.MarshalBinary()
	require.NoError(t, err)
	frame, err := marshalNativeInputFrame(7, bat, len(payload), frameMP)
	require.NoError(t, err)
	require.Equal(t, int64(len(frame)), frameMP.CurrNB())
	sequence, directPayload, err := unmarshalNativeBatchFrame(frame, maxNativeInputBatchBytes)
	require.NoError(t, err)
	require.Equal(t, uint64(7), sequence)
	require.Equal(t, payload, directPayload)
	frameMP.Free(frame)
	require.Zero(t, frameMP.CurrNB())

	_, err = marshalNativeInputFrame(8, bat, len(payload)-1, frameMP)
	require.Error(t, err)
	require.Zero(t, frameMP.CurrNB())
	_, err = marshalNativeInputFrame(8, bat, len(payload)+1, frameMP)
	require.Error(t, err)
	require.Zero(t, frameMP.CurrNB())
}

func TestFixedNativeFrameWriterRejectsGrowth(t *testing.T) {
	w := fixedNativeFrameWriter{data: make([]byte, 4)}
	require.NoError(t, w.WriteUint32(7))
	require.Equal(t, 4, w.Len())
	require.ErrorIs(t, w.EnsureCapacity(5), io.ErrShortWrite)
	_, err := w.WriteString("x")
	require.ErrorIs(t, err, io.ErrShortWrite)
}

func TestPlanNativeWindowMatchesCompactClone(t *testing.T) {
	mp := mpool.MustNewZero()
	const rows = 130
	bat := batch.NewWithSize(4)
	bat.Attrs = []string{"fixed", "text", "constant", "constant_text"}
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	var err error
	bat.Vecs[2], err = vector.NewConstFixed(types.T_decimal64.ToType(), types.Decimal64(42), rows, mp)
	require.NoError(t, err)
	bat.Vecs[3], err = vector.NewConstBytes(
		types.T_varchar.ToType(), bytes.Repeat([]byte{'c'}, types.VarlenaInlineSize+1), rows, mp,
	)
	require.NoError(t, err)
	for row := 0; row < rows; row++ {
		null := row == 0 || row == 63 || row == 64 || row == 129
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(row), null, mp))
		value := bytes.Repeat([]byte{byte(row)}, 1+row%48)
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], value, null, mp))
	}
	bat.SetRowCount(rows)
	defer func() {
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()

	for _, bounds := range [][2]int{
		{0, 1}, {0, 63}, {0, 64}, {0, 65}, {1, 64}, {63, 65}, {64, 129}, {65, 130}, {129, 130},
	} {
		start, end := bounds[0], bounds[1]
		window, cloneErr := cloneNativeWindow(bat, start, end, mp)
		require.NoError(t, cloneErr)
		expected, sizeErr := window.MarshalBinarySize()
		window.Clean(mp)
		require.NoError(t, sizeErr)

		plan, planErr := planNativeWindow(bat, start, uint64(expected))
		require.NoError(t, planErr)
		require.Equal(t, end, plan.end, "window [%d,%d)", start, end)
		require.Equal(t, expected, plan.payloadBytes, "window [%d,%d)", start, end)
	}
}

func TestPlanNativeWindowHandlesConstantNullAtNonzeroOffset(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(2)
	bat.Attrs = []string{"constant_null", "constant_text"}
	bat.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 12, mp)
	var err error
	bat.Vecs[1], err = vector.NewConstBytes(
		types.T_varchar.ToType(), bytes.Repeat([]byte{'x'}, types.VarlenaInlineSize+1), 12, mp,
	)
	require.NoError(t, err)
	bat.SetRowCount(12)
	defer func() {
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()

	window, err := cloneNativeWindow(bat, 5, 12, mp)
	require.NoError(t, err)
	expected, err := window.MarshalBinarySize()
	window.Clean(mp)
	require.NoError(t, err)
	plan, err := planNativeWindow(bat, 5, uint64(expected))
	require.NoError(t, err)
	require.Equal(t, 12, plan.end)
	require.Equal(t, expected, plan.payloadBytes)
}

func TestPlanNativeWindowBoundsFourMiBWideBatch(t *testing.T) {
	mp := mpool.MustNewZero()
	const rows = 130
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	value := bytes.Repeat([]byte{'w'}, types.MaxVarcharLen)
	for row := 0; row < rows; row++ {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], value, false, mp))
	}
	bat.SetRowCount(rows)
	defer func() {
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()

	frames := 0
	for start := 0; start < rows; {
		plan, err := planNativeWindow(bat, start, maxNativeInputBatchBytes)
		require.NoError(t, err)
		require.Greater(t, plan.end, start)
		require.LessOrEqual(t, uint64(plan.payloadBytes), maxNativeInputBatchBytes)

		window, err := cloneNativeWindow(bat, start, plan.end, mp)
		require.NoError(t, err)
		actual, err := window.MarshalBinarySize()
		window.Clean(mp)
		require.NoError(t, err)
		require.Equal(t, plan.payloadBytes, actual)

		if plan.end < rows {
			tooLarge, err := cloneNativeWindow(bat, start, plan.end+1, mp)
			require.NoError(t, err)
			nextSize, err := tooLarge.MarshalBinarySize()
			tooLarge.Clean(mp)
			require.NoError(t, err)
			require.Greater(t, uint64(nextSize), maxNativeInputBatchBytes,
				"each planned frame must be maximal")
		}
		frames++
		start = plan.end
	}
	require.GreaterOrEqual(t, frames, 3)
}

func BenchmarkPlanNativeWindow(b *testing.B) {
	for _, tc := range []struct {
		name string
		rows int
	}{
		{name: "one_frame_4096_rows", rows: 4096},
		{name: "multi_frame_65536_rows", rows: 65536},
	} {
		b.Run(tc.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			value := bytes.Repeat([]byte{'x'}, 64)
			for row := 0; row < tc.rows; row++ {
				if err := vector.AppendFixed(bat.Vecs[0], int64(row), false, mp); err != nil {
					b.Fatal(err)
				}
				if err := vector.AppendBytes(bat.Vecs[1], value, false, mp); err != nil {
					b.Fatal(err)
				}
			}
			bat.SetRowCount(tc.rows)
			b.Cleanup(func() { bat.Clean(mp) })
			b.ReportAllocs()
			b.SetBytes(int64(tc.rows * (8 + len(value))))
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				for start := 0; start < tc.rows; {
					plan, err := planNativeWindow(bat, start, maxNativeInputBatchBytes)
					if err != nil {
						b.Fatal(err)
					}
					start = plan.end
				}
			}
		})
	}
}
