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

package bufferlease

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRefCountedTerminalLifecycle(t *testing.T) {
	var cleanup atomic.Int32
	lease, err := NewRefCounted([]byte("payload"), 64, func() { cleanup.Add(1) })
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), lease.Bytes())
	require.Equal(t, int64(64), lease.AccountedBytes())
	require.True(t, lease.Retain())
	lease.Release()
	require.Equal(t, int32(0), cleanup.Load())
	lease.Release()
	require.Equal(t, int32(1), cleanup.Load())
	require.Nil(t, lease.Bytes())
	require.False(t, lease.Retain(), "a terminal lease must not be resurrected")
	require.Panics(t, lease.Release, "release underflow is an ownership violation")
}

func TestRefCountedConcurrentLastRelease(t *testing.T) {
	const holders = 128
	var cleanup atomic.Int32
	lease, err := NewRefCounted(make([]byte, 8), 8, func() { cleanup.Add(1) })
	require.NoError(t, err)
	for range holders {
		require.True(t, lease.Retain())
	}

	var wait sync.WaitGroup
	var missing atomic.Int32
	wait.Add(holders)
	for range holders {
		go func() {
			defer wait.Done()
			if lease.Bytes() == nil {
				missing.Add(1)
			}
			lease.Release()
		}()
	}
	wait.Wait()
	require.Zero(t, missing.Load())
	require.Equal(t, int32(0), cleanup.Load())
	lease.Release()
	require.Equal(t, int32(1), cleanup.Load())
}

func TestRefCountedRejectsNegativeAccounting(t *testing.T) {
	lease, err := NewRefCounted(nil, -1, nil)
	require.Error(t, err)
	require.Nil(t, lease)
}
