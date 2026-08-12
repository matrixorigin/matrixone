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
