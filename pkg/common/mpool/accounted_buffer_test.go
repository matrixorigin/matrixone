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

package mpool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccountedBufferLifecycle(t *testing.T) {
	registry, account := newTestAllocationAccount(t, 1<<20, 4)
	mp := MustNew("accounted-buffer")
	defer DeleteMPool(mp)
	buffer, err := NewAccountedBuffer(
		mp,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.NoError(t, err)

	require.NoError(t, buffer.EnsureCapacity(32))
	firstCapacity := buffer.Cap()
	require.GreaterOrEqual(t, firstCapacity, 32)
	require.Equal(t, uint64(firstCapacity), account.Snapshot().Used)
	_, err = buffer.WriteString("accounted")
	require.NoError(t, err)
	require.Equal(t, "accounted", string(buffer.Bytes()))

	buffer.Reset()
	require.Zero(t, buffer.Len())
	require.Equal(t, firstCapacity, buffer.Cap())
	_, err = buffer.Write([]byte("reuse"))
	require.NoError(t, err)
	require.Equal(t, uint64(firstCapacity), account.Snapshot().Used)

	require.NoError(t, buffer.EnsureCapacity(firstCapacity+1))
	secondCapacity := buffer.Cap()
	require.Greater(t, secondCapacity, firstCapacity)
	snapshot := account.Snapshot()
	require.Equal(t, uint64(secondCapacity), snapshot.Used)
	require.GreaterOrEqual(
		t,
		snapshot.Peak,
		uint64(firstCapacity+secondCapacity),
	)
	require.Equal(t, "reuse", string(buffer.Bytes()))

	buffer.Free()
	buffer.Free()
	require.Zero(t, account.Snapshot().Used)
	finalizeTestAllocationAccount(t, registry, account)
}

func TestAccountedBufferFailureRetainsPublishedData(t *testing.T) {
	registry, account := newTestAllocationAccount(t, 64, 2)
	mp := MustNew("accounted-buffer-failure")
	defer DeleteMPool(mp)
	buffer, err := NewAccountedBuffer(
		mp,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.NoError(t, err)
	_, err = buffer.Write([]byte("published"))
	require.NoError(t, err)
	before := append([]byte(nil), buffer.Bytes()...)
	snapshot := account.Snapshot()
	used := snapshot.Used

	err = buffer.EnsureCapacity(usedSizeToInt(t, snapshot.Limit))
	require.ErrorIs(t, err, ErrAllocationAccountCapacity)
	require.Equal(t, before, buffer.Bytes())
	require.Equal(t, used, account.Snapshot().Used)

	buffer.Free()
	finalizeTestAllocationAccount(t, registry, account)
}

func TestAccountedBufferConfiguration(t *testing.T) {
	_, err := NewAccountedBuffer(nil, nil, 0, 0)
	require.ErrorIs(t, err, ErrAllocationAccountInvalid)
	var buffer *AccountedBuffer
	require.Nil(t, buffer.Bytes())
	require.Zero(t, buffer.Len())
	require.Zero(t, buffer.Cap())
	_, err = buffer.Write([]byte("x"))
	require.ErrorIs(t, err, ErrAllocationAccountInvalid)
	buffer.Reset()
	buffer.Free()
}

func usedSizeToInt(t testing.TB, value uint64) int {
	t.Helper()
	require.LessOrEqual(t, value, uint64(^uint(0)>>1))
	return int(value)
}
