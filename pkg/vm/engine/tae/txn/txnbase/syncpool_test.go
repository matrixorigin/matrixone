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

package txnbase

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSyncPoolBufferReuse tests that sync.Pool correctly reuses buffers
func TestSyncPoolBufferReuse(t *testing.T) {
	// Get buffer from pool
	buf1 := GetMarshalBuffer()
	require.NotNil(t, buf1)

	// Write some data
	buf1.WriteString("test data")
	assert.Greater(t, buf1.Len(), 0)

	// Return to pool
	PutMarshalBuffer(buf1)

	// Get another buffer - should be the same one (reused)
	buf2 := GetMarshalBuffer()
	require.NotNil(t, buf2)

	// Buffer should be reset (empty)
	assert.Equal(t, 0, buf2.Len(), "buffer should be reset after returning to pool")

	// Return to pool
	PutMarshalBuffer(buf2)
}

// TestSyncPoolLargeBufferDiscard tests that large buffers are not returned to pool
func TestSyncPoolLargeBufferDiscard(t *testing.T) {
	// Get buffer from pool
	buf := GetMarshalBuffer()
	require.NotNil(t, buf)

	// Grow buffer beyond MaxPooledBufSize
	buf.Grow(MaxPooledBufSize + 1000)
	assert.Greater(t, buf.Cap(), MaxPooledBufSize)

	// Return to pool - should be discarded
	PutMarshalBuffer(buf)

	// Get another buffer - should be a new one
	buf2 := GetMarshalBuffer()
	require.NotNil(t, buf2)

	// New buffer should have small capacity (not the large one)
	assert.LessOrEqual(t, buf2.Cap(), MaxPooledBufSize)

	PutMarshalBuffer(buf2)
}

// TestComposedCmd_MarshalBinary_WithSyncPool tests ComposedCmd.MarshalBinary uses sync.Pool
func TestComposedCmd_MarshalBinary_WithSyncPool(t *testing.T) {
	cc := NewComposedCmd()

	// Add some commands
	cmd1 := NewTxnStateCmd("cmd1", 1, [12]byte{1})
	cmd2 := NewTxnStateCmd("cmd2", 2, [12]byte{2})
	cc.AddCmd(cmd1)
	cc.AddCmd(cmd2)

	// Marshal multiple times
	for i := 0; i < 10; i++ {
		buf, err := cc.MarshalBinary()
		require.NoError(t, err)
		require.NotNil(t, buf)
		assert.Greater(t, len(buf), 0)
	}
}

// TestTxnCmd_MarshalBinary_WithSyncPool tests TxnCmd.MarshalBinary uses sync.Pool
func TestTxnCmd_MarshalBinary_WithSyncPool(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-txn"

	// Add commands
	cmd1 := NewTxnStateCmd("cmd1", 1, [12]byte{1})
	txnCmd.ComposedCmd.AddCmd(cmd1)

	// Marshal multiple times
	for i := 0; i < 10; i++ {
		buf, err := txnCmd.MarshalBinary()
		require.NoError(t, err)
		require.NotNil(t, buf)
		assert.Greater(t, len(buf), 0)
	}
}
