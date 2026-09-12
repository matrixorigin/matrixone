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

package group

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type groupSpillWriteOnly struct {
	io.Writer
}

type groupSpillCountingWriter struct {
	writes int
	bytes  int
}

type groupSpillPartialWriter struct {
	limit  int
	err    error
	writes int
	bytes  int
}

func (w *groupSpillPartialWriter) Write(value []byte) (int, error) {
	w.writes++
	n := min(w.limit, len(value))
	w.bytes += n
	return n, w.err
}

func (w *groupSpillCountingWriter) Write(value []byte) (int, error) {
	w.writes++
	w.bytes += len(value)
	return len(value), nil
}

func newGroupSpillPerfBudget(t testing.TB) (
	*process.ExecutionResourceBudget,
	*process.ExecutionResourceGeneration,
	*process.ExecutionSpillDiskReservation,
) {
	t.Helper()
	budget, err := process.NewExecutionResourceBudget(1<<40, 1<<39)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	disk, err := generation.ReserveSpillDisk(0)
	require.NoError(t, err)
	return budget, generation, disk
}

func TestGroupSpillWriterAccountsPhysicalWrites(t *testing.T) {
	budget, generation, disk := newGroupSpillPerfBudget(t)
	target := &groupSpillCountingWriter{}
	pool := mpool.MustNewZero()
	writer, err := newGroupSpillWriter(
		&container{mp: pool}, target, context.Background(), disk)
	require.NoError(t, err)
	record := spillRecordWriter{target: writer}
	before := generation.Snapshot().ReserveCount
	payload := make([]byte, 32)
	for range 1000 {
		_, err = record.Write(payload)
		require.NoError(t, err)
	}
	if got := disk.Size(); got != 0 {
		t.Errorf("buffered bytes charged before physical write: got %d", got)
	}
	require.NoError(t, writer.Flush())
	require.Equal(t, 1, target.writes)
	require.Equal(t, len(payload)*1000, target.bytes)
	require.Equal(t, uint64(len(payload)*1000), disk.Size())
	require.Equal(t, uint64(1), generation.Snapshot().ReserveCount-before)

	writer.Free()
	require.True(t, disk.Release())
	require.Zero(t, generation.Snapshot().SpillDiskUsed)
	generation.Close()
	budget.Close()
	require.Zero(t, pool.CurrNB())
}

func TestGroupSpillWriterAdmitsDiskBeforePhysicalWrite(t *testing.T) {
	budget, generation, disk := newGroupSpillPerfBudget(t)
	remaining := uint64(31)
	held, err := generation.ReserveSpillDisk(generation.SpillDiskCap() - remaining)
	require.NoError(t, err)
	target := &groupSpillCountingWriter{}
	pool := mpool.MustNewZero()
	writer, err := newGroupSpillWriter(
		&container{mp: pool}, target, context.Background(), disk)
	require.NoError(t, err)

	_, err = writer.Write(make([]byte, remaining+1))
	require.NoError(t, err)
	err = writer.Flush()
	require.Error(t, err)
	require.Zero(t, target.writes)
	require.Zero(t, target.bytes)
	require.Zero(t, disk.Size())

	writer.Free()
	require.True(t, disk.Release())
	require.True(t, held.Release())
	require.Zero(t, generation.Snapshot().SpillDiskUsed)
	generation.Close()
	budget.Close()
	require.Zero(t, pool.CurrNB())
}

func TestGroupSpillWriterDirectFallbackAccountsOneWrite(t *testing.T) {
	budget, generation, disk := newGroupSpillPerfBudget(t)
	target := &groupSpillCountingWriter{}
	pool := mpool.MustNewZero()
	writer, err := newGroupSpillWriter(
		&container{mp: pool}, target, context.Background(), disk)
	require.NoError(t, err)
	writer.disabled = true
	before := generation.Snapshot().ReserveCount

	payload := make([]byte, 32)
	n, err := writer.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.Equal(t, 1, target.writes)
	require.Equal(t, len(payload), target.bytes)
	require.Equal(t, uint64(len(payload)), disk.Size())
	require.Equal(t, uint64(1), generation.Snapshot().ReserveCount-before)

	writer.Free()
	require.True(t, disk.Release())
	require.Zero(t, generation.Snapshot().SpillDiskUsed)
	generation.Close()
	budget.Close()
	require.Zero(t, pool.CurrNB())
}

func TestGroupSpillWriterReconcilesPartialPhysicalWrites(t *testing.T) {
	for _, test := range []struct {
		name      string
		direct    bool
		targetErr error
		wantErr   error
	}{
		{name: "buffered short write", wantErr: io.ErrShortWrite},
		{name: "buffered partial error", targetErr: io.ErrUnexpectedEOF, wantErr: io.ErrUnexpectedEOF},
		{name: "direct short write", direct: true, wantErr: io.ErrShortWrite},
		{name: "direct partial error", direct: true, targetErr: io.ErrUnexpectedEOF, wantErr: io.ErrUnexpectedEOF},
	} {
		t.Run(test.name, func(t *testing.T) {
			budget, generation, disk := newGroupSpillPerfBudget(t)
			target := &groupSpillPartialWriter{limit: 4, err: test.targetErr}
			pool := mpool.MustNewZero()
			writer, err := newGroupSpillWriter(
				&container{mp: pool}, target, context.Background(), disk)
			require.NoError(t, err)
			writer.disabled = test.direct

			payload := make([]byte, 8)
			n, writeErr := writer.Write(payload)
			if test.direct {
				require.Equal(t, 4, n)
				require.ErrorIs(t, writeErr, test.wantErr)
			} else {
				require.Equal(t, len(payload), n)
				require.NoError(t, writeErr)
				require.ErrorIs(t, writer.Flush(), test.wantErr)
			}
			require.Equal(t, 1, target.writes)
			require.Equal(t, 4, target.bytes)
			require.Equal(t, uint64(4), disk.Size())

			writer.Free()
			require.True(t, disk.Release())
			require.Zero(t, generation.Snapshot().SpillDiskUsed)
			generation.Close()
			budget.Close()
			require.Zero(t, pool.CurrNB())
		})
	}
}

func TestGroupSpillWriterCoalescesSelectedFixedRowsWithoutChangingWire(t *testing.T) {
	pool := mpool.MustNewZero()
	source := vector.NewVec(types.T_int64.ToType())
	values := make([]int64, 8192)
	rows := make([]int32, 0, len(values))
	for i := range values {
		values[i] = int64(i*19 - 5)
		rows = append(rows, int32(len(values)-1-i))
	}
	require.NoError(t, vector.AppendFixedList(source, values, nil, pool))

	var reference bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&reference, rows))
	var fallbackTarget bytes.Buffer
	fallbackRecord := spillRecordWriter{target: &fallbackTarget}
	require.NoError(t, source.MarshalSelectedRowsTo(&fallbackRecord, rows))
	require.Equal(t, reference.Bytes(), fallbackTarget.Bytes())
	require.Equal(t, int64(reference.Len()), fallbackRecord.written)

	var target bytes.Buffer
	writer, err := newGroupSpillWriter(
		&container{mp: pool}, &target, context.Background(), nil)
	require.NoError(t, err)
	record := spillRecordWriter{target: writer}
	require.NoError(t, source.MarshalSelectedRowsTo(&record, rows))
	require.NoError(t, writer.Flush())
	require.Equal(t, int64(reference.Len()), record.written)
	require.Equal(t, reference.Bytes(), target.Bytes())

	writer.Free()
	source.Free(pool)
	require.Zero(t, pool.CurrNB())
}

func BenchmarkGroupSpillSelectedFixedRows(b *testing.B) {
	pool := mpool.MustNewZero()
	source := vector.NewVec(types.T_int64.ToType())
	values := make([]int64, 8192)
	rows := make([]int32, 0, 256)
	for i := range values {
		values[i] = int64(i)
		if i%32 == 0 {
			rows = append(rows, int32(i))
		}
	}
	require.NoError(b, vector.AppendFixedList(source, values, nil, pool))
	b.Cleanup(func() {
		source.Free(pool)
		require.Zero(b, pool.CurrNB())
	})

	for _, test := range []struct {
		name string
		fast bool
	}{
		{name: "reference"},
		{name: "coalesced", fast: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			writer, err := newGroupSpillWriter(
				&container{mp: pool}, io.Discard, context.Background(), nil)
			require.NoError(b, err)
			record := spillRecordWriter{target: writer}
			var target io.Writer = &record
			if !test.fast {
				target = groupSpillWriteOnly{Writer: &record}
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(rows) * types.T_int64.TypeLen()))
			b.ResetTimer()
			for b.Loop() {
				if err := source.MarshalSelectedRowsTo(target, rows); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			require.NoError(b, writer.Flush())
			writer.Free()
		})
	}
}

func BenchmarkGroupSpillWriterDiskAccounting(b *testing.B) {
	for _, test := range []struct {
		name    string
		workers int
	}{
		{name: "1-writer", workers: 1},
		{name: "14-writers", workers: 14},
	} {
		b.Run(test.name, func(b *testing.B) {
			oldProcs := runtime.GOMAXPROCS(test.workers)
			defer runtime.GOMAXPROCS(oldProcs)
			budget, err := process.NewExecutionResourceBudget(1<<40, 1<<39)
			require.NoError(b, err)
			generation, err := budget.OpenGeneration(1)
			require.NoError(b, err)
			pool := mpool.MustNewZero()
			payload := make([]byte, 32)
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				disk, err := generation.ReserveSpillDisk(0)
				require.NoError(b, err)
				writer, err := newGroupSpillWriter(
					&container{mp: pool}, io.Discard, context.Background(), disk)
				require.NoError(b, err)
				record := spillRecordWriter{target: writer}
				for pb.Next() {
					_, err = record.Write(payload)
					require.NoError(b, err)
				}
				require.NoError(b, writer.Flush())
				writer.Free()
				require.True(b, disk.Release())
			})
			b.StopTimer()
			generation.Close()
			budget.Close()
			require.Zero(b, pool.CurrNB())
		})
	}
}
