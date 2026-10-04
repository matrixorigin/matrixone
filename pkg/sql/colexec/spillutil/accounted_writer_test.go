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

package spillutil

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const accountedWriterTestSite mpool.AllocationSite = 1

func TestAccountedWriterOwnsAndReleasesBuffer(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("accounted-writer-lifecycle"),
	)
	defer proc.Free()
	state := newTestSpillAllocationAccount(t, 1<<20, 16)
	var target bytes.Buffer

	writer, err := NewAccountedWriter(
		proc.Ctx,
		proc.Mp(),
		state.account,
		mpool.AllocationOwnerOrder,
		accountedWriterTestSite,
		&target,
		64,
	)
	require.NoError(t, err)
	n, err := writer.Write([]byte("abc"))
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Empty(t, target.Bytes())
	require.Positive(t, state.account.Snapshot().Used)

	require.NoError(t, writer.Flush())
	require.Equal(t, "abc", target.String())
	writer.Free()
	require.Zero(t, state.account.Snapshot().Used)
	finalizeTestSpillAllocationAccount(t, state)
}

func TestAccountedWriterFallsBackToDirectWriteOnCapacity(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("accounted-writer-fallback"),
	)
	defer proc.Free()
	state := newTestSpillAllocationAccount(t, 32, 16)
	var target bytes.Buffer

	writer, err := NewAccountedWriter(
		proc.Ctx,
		proc.Mp(),
		state.account,
		mpool.AllocationOwnerOrder,
		accountedWriterTestSite,
		&target,
		64,
	)
	require.NoError(t, err)
	n, err := writer.Write([]byte("direct"))
	require.NoError(t, err)
	require.Equal(t, 6, n)
	require.Equal(t, "direct", target.String())
	require.Zero(t, state.account.Snapshot().Used)
	require.NoError(t, writer.Flush())

	writer.Free()
	finalizeTestSpillAllocationAccount(t, state)
}

type accountedWriterShortTarget struct{}

func (accountedWriterShortTarget) Write(value []byte) (int, error) {
	return len(value) - 1, nil
}

type spillWriterResult struct {
	written int
	err     error
}

type cancelingSpillWriter struct {
	cancel context.CancelFunc
}

func (w cancelingSpillWriter) Write(value []byte) (int, error) {
	w.cancel()
	return len(value), nil
}

func (w spillWriterResult) Write([]byte) (int, error) {
	return w.written, w.err
}

func TestAccountedWriterFailsClosedOnShortWrite(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("accounted-writer-short-write"),
	)
	defer proc.Free()
	state := newTestSpillAllocationAccount(t, 1<<20, 16)

	writer, err := NewAccountedWriter(
		proc.Ctx,
		proc.Mp(),
		state.account,
		mpool.AllocationOwnerOrder,
		accountedWriterTestSite,
		accountedWriterShortTarget{},
		64,
	)
	require.NoError(t, err)
	_, err = writer.Write([]byte("payload"))
	require.NoError(t, err)
	require.ErrorIs(t, writer.Flush(), io.ErrShortWrite)
	require.ErrorIs(t, writer.Flush(), io.ErrShortWrite)

	writer.Free()
	finalizeTestSpillAllocationAccount(t, state)
}

func TestAccountedWriterHonorsCancellation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("accounted-writer-cancel"),
	)
	defer proc.Free()
	state := newTestSpillAllocationAccount(t, 1<<20, 16)
	ctx, cancel := context.WithCancel(proc.Ctx)
	cancel()
	var target bytes.Buffer

	writer, err := NewAccountedWriter(
		ctx,
		proc.Mp(),
		state.account,
		mpool.AllocationOwnerOrder,
		accountedWriterTestSite,
		&target,
		64,
	)
	require.NoError(t, err)
	_, err = writer.Write([]byte("payload"))
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, target.Bytes())

	writer.Free()
	finalizeTestSpillAllocationAccount(t, state)
}

func TestSpillWriterBoundarySemantics(t *testing.T) {
	_, err := NewAccountedWriter(nil, nil, nil, 0, 0, io.Discard, 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	require.NoError(t, (*AccountedWriter)(nil).Flush())
	(*AccountedWriter)(nil).Free()
	require.Equal(t, 0, func() int {
		n, writeErr := (*AccountedWriter)(nil).Write([]byte("x"))
		require.ErrorIs(t, writeErr, io.ErrClosedPipe)
		return n
	}())

	direct, err := NewAccountedWriter(
		context.Background(), nil, nil, 0, 0, io.Discard, 4)
	require.NoError(t, err)
	n, err := direct.Write(nil)
	require.NoError(t, err)
	require.Zero(t, n)
	n, err = direct.Write([]byte("payload"))
	require.NoError(t, err)
	require.Equal(t, len("payload"), n)
	direct.Free()

	sentinel := io.ErrUnexpectedEOF
	failed := &AccountedWriter{
		target: io.Discard, ctx: context.Background(), failed: sentinel,
	}
	_, err = failed.Write([]byte("x"))
	require.ErrorIs(t, err, sentinel)
	require.ErrorIs(t, failed.Flush(), sentinel)
	failed.Free()

	ctx, cancel := context.WithCancel(context.Background())
	cancelled, err := NewAccountedWriter(ctx, nil, nil, 0, 0, io.Discard, 4)
	require.NoError(t, err)
	cancel()
	require.ErrorIs(t, cancelled.Flush(), context.Canceled)
	cancelled.Free()

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	state := newTestSpillAllocationAccount(t, 1<<20, 16)
	buffered, err := NewAccountedWriter(
		context.Background(), proc.Mp(), state.account,
		mpool.AllocationOwnerOrder, accountedWriterTestSite,
		accountedWriterShortTarget{}, 4)
	require.NoError(t, err)
	n, err = buffered.Write([]byte("abcdefgh"))
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, 4, n)
	buffered.Free()
	finalizeTestSpillAllocationAccount(t, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())

	proc = testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	state = newTestSpillAllocationAccount(t, 1<<20, 16)
	loopCtx, loopCancel := context.WithCancel(context.Background())
	buffered, err = NewAccountedWriter(
		loopCtx, proc.Mp(), state.account,
		mpool.AllocationOwnerOrder, accountedWriterTestSite,
		cancelingSpillWriter{cancel: loopCancel}, 4)
	require.NoError(t, err)
	n, err = buffered.Write([]byte("abcdefghijkl"))
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 8, n)
	buffered.Free()
	finalizeTestSpillAllocationAccount(t, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())

	for _, tc := range []struct {
		name string
		n    int
		err  error
	}{
		{name: "negative", n: -1},
		{name: "too-large", n: 99},
		{name: "short", n: 1},
		{name: "target-error", n: 0, err: io.ErrClosedPipe},
	} {
		t.Run(tc.name, func(t *testing.T) {
			written, writeErr := writeAll(
				spillWriterResult{written: tc.n, err: tc.err}, []byte("abc"))
			require.LessOrEqual(t, written, 3)
			require.Error(t, writeErr)
		})
	}

	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	reservation, err := generation.ReserveSpillDisk(0)
	require.NoError(t, err)
	diskWriter := NewDiskReservationWriter(
		spillWriterResult{written: 2}, reservation)
	n, err = diskWriter.Write([]byte("abcd"))
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, 2, n)
	require.Equal(t, uint64(2), reservation.Size())
	reservation.Release()
	generation.Close()
	budget.Close()

	for _, tc := range []struct {
		name string
		n    int
	}{
		{name: "negative", n: -1},
		{name: "too-large", n: 99},
	} {
		t.Run("disk-"+tc.name, func(t *testing.T) {
			writer := NewDiskReservationWriter(
				spillWriterResult{written: tc.n}, nil)
			written, writeErr := writer.Write([]byte("abc"))
			require.ErrorIs(t, writeErr, io.ErrShortWrite)
			require.GreaterOrEqual(t, written, 0)
			require.LessOrEqual(t, written, 3)
		})
	}

	n, err = (*DiskReservationWriter)(nil).Write([]byte("x"))
	require.Zero(t, n)
	require.ErrorIs(t, err, io.ErrClosedPipe)
}
