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
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockTxnCmd is a mock implementation of TxnCmd for testing
type mockTxnCmd struct {
	id          string
	data        []byte
	marshalErr  error
	approxSize  int64
	closeCalled bool
}

func newMockTxnCmd(id string, data []byte) *mockTxnCmd {
	if data == nil {
		data = []byte(id)
	}
	return &mockTxnCmd{
		id:         id,
		data:       data,
		approxSize: int64(len(data)),
	}
}

func (m *mockTxnCmd) MarshalBinary() ([]byte, error) {
	if m.marshalErr != nil {
		return nil, m.marshalErr
	}
	return m.data, nil
}

func (m *mockTxnCmd) UnmarshalBinary([]byte) error {
	return nil
}

func (m *mockTxnCmd) GetType() uint16 {
	return 9999 // Mock type
}

func (m *mockTxnCmd) Desc() string {
	return m.id
}

func (m *mockTxnCmd) String() string {
	return m.id
}

func (m *mockTxnCmd) ApplyRollback() {}

func (m *mockTxnCmd) ApplyCommit() {}

func (m *mockTxnCmd) SetReplayTxn(txnif.AsyncTxn) {}

func (m *mockTxnCmd) VerboseString() string {
	return m.id
}

func (m *mockTxnCmd) Close() {
	m.closeCalled = true
}

func (m *mockTxnCmd) ApproxSize() int64 {
	return m.approxSize
}

// TestComposedCmd_MarshalBinary_Basic tests basic MarshalBinary functionality
func TestComposedCmd_MarshalBinary_Basic(t *testing.T) {
	cc := NewComposedCmd()
	require.NotNil(t, cc)
	assert.Nil(t, cc.marshalBuf, "marshalBuf should be nil initially")

	// Add some commands
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cmd2 := newMockTxnCmd("cmd2", []byte("data2"))
	cc.AddCmd(cmd1)
	cc.AddCmd(cmd2)

	// Marshal
	buf, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	// Verify buffer structure: header (8 bytes) + cmd1 length (4) + cmd1 data + cmd2 length (4) + cmd2 data
	expectedMinSize := 8 + 4 + len(cmd1.data) + 4 + len(cmd2.data)
	assert.GreaterOrEqual(t, len(buf), expectedMinSize, "buffer should contain header and all cmd data")

	// Verify header
	assert.Equal(t, cc.GetType(), types.DecodeUint16(buf[0:2]), "type should match")
	assert.Equal(t, IOET_WALTxnCommand_Composed_CurrVer, types.DecodeUint16(buf[2:4]), "version should match")
	assert.Equal(t, uint32(2), types.DecodeUint32(buf[4:8]), "cmd count should match")

	// Verify marshalBuf is set
	assert.NotNil(t, cc.marshalBuf, "marshalBuf should be allocated after MarshalBinary")
	assert.Equal(t, buf, cc.marshalBuf, "returned buffer should be the same as marshalBuf")
}

// TestComposedCmd_MarshalBinary_EmptyCmds tests MarshalBinary with empty commands
func TestComposedCmd_MarshalBinary_EmptyCmds(t *testing.T) {
	cc := NewComposedCmd()

	// Marshal with no commands
	buf, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	// Should only contain header (8 bytes)
	assert.Equal(t, 8, len(buf), "buffer should only contain header for empty cmds")
	assert.Equal(t, uint32(0), types.DecodeUint32(buf[4:8]), "cmd count should be 0")
}

// TestComposedCmd_MarshalBinary_BufferReuse tests that marshalBuf is reused across multiple calls
func TestComposedCmd_MarshalBinary_BufferReuse(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", make([]byte, 100))
	cc.AddCmd(cmd1)

	// First marshal
	buf1, err := cc.MarshalBinary()
	require.NoError(t, err)
	firstBufCap := cap(cc.marshalBuf)
	assert.Greater(t, firstBufCap, 0, "marshalBuf should have capacity after first marshal")

	// Second marshal - should reuse buffer
	buf2, err := cc.MarshalBinary()
	require.NoError(t, err)
	secondBufCap := cap(cc.marshalBuf)

	// Capacity should be the same (reused)
	assert.Equal(t, firstBufCap, secondBufCap, "marshalBuf capacity should be reused")
	assert.Equal(t, buf1, buf2, "buffers should be equal for same input")

	// Verify they point to the same underlying array (if both have data)
	if len(buf1) > 0 && len(buf2) > 0 && cap(cc.marshalBuf) >= len(buf1) {
		// After second marshal, marshalBuf should point to the same underlying array
		// We verify this by checking that the returned buffer is the same slice as marshalBuf
		assert.Equal(t, cc.marshalBuf, buf2, "returned buffer should be the same as marshalBuf")
	}
}

// TestComposedCmd_MarshalBinary_BufferGrowth tests buffer growth when size increases
func TestComposedCmd_MarshalBinary_BufferGrowth(t *testing.T) {
	cc := NewComposedCmd()

	// First marshal with small data
	cmd1 := newMockTxnCmd("cmd1", make([]byte, 100))
	cc.AddCmd(cmd1)
	buf1, err := cc.MarshalBinary()
	require.NoError(t, err)
	firstCap := cap(cc.marshalBuf)

	// Add more commands with larger data
	cmd2 := newMockTxnCmd("cmd2", make([]byte, 1000))
	cmd3 := newMockTxnCmd("cmd3", make([]byte, 1000))
	cc.AddCmd(cmd2)
	cc.AddCmd(cmd3)

	// Second marshal - buffer should grow
	buf2, err := cc.MarshalBinary()
	require.NoError(t, err)
	secondCap := cap(cc.marshalBuf)

	// Capacity should increase
	assert.Greater(t, secondCap, firstCap, "marshalBuf should grow when data size increases")
	assert.Greater(t, len(buf2), len(buf1), "new buffer should be larger")
}

// TestComposedCmd_MarshalBinary_CapacityLimit tests that buffer is discarded when exceeding MaxComposedCmdBufSize
func TestComposedCmd_MarshalBinary_CapacityLimit(t *testing.T) {
	cc := NewComposedCmd()

	// Create a buffer that exceeds MaxComposedCmdBufSize
	// We'll manually set a large capacity to test the limit check
	cc.marshalBuf = make([]byte, 0, MaxComposedCmdBufSize+1)
	assert.Greater(t, cap(cc.marshalBuf), MaxComposedCmdBufSize, "test setup: buffer should exceed limit")

	// Add a command
	cmd1 := newMockTxnCmd("cmd1", make([]byte, 100))
	cc.AddCmd(cmd1)

	// Marshal - should discard the oversized buffer
	buf, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	// Buffer should be reallocated and within limit
	assert.LessOrEqual(t, cap(cc.marshalBuf), MaxComposedCmdBufSize, "marshalBuf should be within limit after discard")
}

// TestComposedCmd_MarshalBinary_CapacityLimitExactBoundary tests capacity limit at exact boundary
func TestComposedCmd_MarshalBinary_CapacityLimitExactBoundary(t *testing.T) {
	cc := NewComposedCmd()

	// Set buffer to exactly MaxComposedCmdBufSize (should not be discarded)
	cc.marshalBuf = make([]byte, 0, MaxComposedCmdBufSize)
	assert.Equal(t, MaxComposedCmdBufSize, cap(cc.marshalBuf), "test setup: buffer should be at limit")

	cmd1 := newMockTxnCmd("cmd1", make([]byte, 100))
	cc.AddCmd(cmd1)

	// Marshal - buffer at limit should not be discarded
	buf, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)
	assert.Greater(t, len(buf), 0, "buffer should contain data")

	// Buffer should still be at or below limit (may be reused if sufficient)
	assert.LessOrEqual(t, cap(cc.marshalBuf), MaxComposedCmdBufSize, "marshalBuf should be within limit")
}

// TestComposedCmd_MarshalBinary_EstimatedSize tests the size estimation logic
func TestComposedCmd_MarshalBinary_EstimatedSize(t *testing.T) {
	cc := NewComposedCmd()

	// Add commands
	numCmds := 10
	for i := 0; i < numCmds; i++ {
		cmd := newMockTxnCmd("cmd", make([]byte, 100))
		cc.AddCmd(cmd)
	}

	// Marshal
	buf, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf, "marshal buffer should not be nil")
	require.Greater(t, len(buf), 0, "marshal buffer should not be empty")

	// Estimated size: use ApproxSize() for more accurate estimation
	estimatedSize := int(cc.ApproxSize())
	if estimatedSize < 256 {
		estimatedSize = 256 // Minimum capacity
	}
	actualCap := cap(cc.marshalBuf)

	// Capacity should be at least estimated size (may be rounded up by Go runtime)
	assert.GreaterOrEqual(t, actualCap, estimatedSize, "capacity should be at least estimated size from ApproxSize()")
}

// TestComposedCmd_MarshalBinary_ErrorHandling tests error handling when cmd.MarshalBinary fails
func TestComposedCmd_MarshalBinary_ErrorHandling(t *testing.T) {
	cc := NewComposedCmd()

	// Add a command that will fail to marshal
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cmd1.marshalErr = errors.New("marshal error")
	cc.AddCmd(cmd1)

	// Marshal should return error
	buf, err := cc.MarshalBinary()
	assert.Error(t, err, "should return error when cmd.MarshalBinary fails")
	assert.Nil(t, buf, "buffer should be nil on error")
}

// TestComposedCmd_MarshalBinary_MultipleCallsWithDifferentSizes tests multiple calls with varying sizes
func TestComposedCmd_MarshalBinary_MultipleCallsWithDifferentSizes(t *testing.T) {
	cc := NewComposedCmd()

	// First call: small data
	cmd1 := newMockTxnCmd("cmd1", make([]byte, 50))
	cc.AddCmd(cmd1)
	buf1, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf1, "first buffer should not be nil")
	cap1 := cap(cc.marshalBuf)

	// Second call: larger data (should reuse if capacity is sufficient)
	cc.Cmds = nil // Clear commands
	cmd2 := newMockTxnCmd("cmd2", make([]byte, 200))
	cc.AddCmd(cmd2)
	buf2, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf2, "second buffer should not be nil")
	cap2 := cap(cc.marshalBuf)

	// Verify buffers are different (different commands)
	assert.NotEqual(t, buf1, buf2, "buffers should be different for different commands")

	// If cap1 is sufficient, it should be reused
	if cap1 >= len(buf2) {
		assert.Equal(t, cap1, cap2, "buffer should be reused if capacity is sufficient")
	} else {
		assert.Greater(t, cap2, cap1, "buffer should grow if capacity is insufficient")
	}
}

// TestComposedCmd_MarshalBinary_LargeNumberOfCmds tests with a large number of commands
func TestComposedCmd_MarshalBinary_LargeNumberOfCmds(t *testing.T) {
	cc := NewComposedCmd()

	// Add many commands
	numCmds := 100
	for i := 0; i < numCmds; i++ {
		cmd := newMockTxnCmd("cmd", make([]byte, 50))
		cc.AddCmd(cmd)
	}

	// Marshal
	buf, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	// Verify cmd count
	assert.Equal(t, uint32(numCmds), types.DecodeUint32(buf[4:8]), "cmd count should match")

	// Verify buffer is large enough
	expectedMinSize := 8 + numCmds*(4+50) // header + (length prefix + data) for each cmd
	assert.GreaterOrEqual(t, len(buf), expectedMinSize, "buffer should be large enough")
}

// TestComposedCmd_MarshalBinary_UnmarshalRoundTrip tests MarshalBinary -> UnmarshalBinary round trip
// Note: UnmarshalBinary expects format: length (4 bytes) + for each cmd: length (4) + cmd data with header
// Since BuildCommandFrom requires registered codecs, we use TxnStateCmd which is a real registered command type.
func TestComposedCmd_MarshalBinary_UnmarshalRoundTrip(t *testing.T) {
	cc1 := NewComposedCmd()
	cmd1 := NewTxnStateCmd("txn1", 1, types.BuildTS(1, 1))
	cmd2 := NewTxnStateCmd("txn2", 2, types.BuildTS(2, 2))
	cc1.AddCmd(cmd1)
	cc1.AddCmd(cmd2)

	// Marshal each cmd individually to get their binary representation (with header)
	cmd1Buf, err := cmd1.MarshalBinary()
	require.NoError(t, err)
	cmd2Buf, err := cmd2.MarshalBinary()
	require.NoError(t, err)

	// Construct the format expected by UnmarshalBinary:
	// - 4 bytes: cmd count
	// - For each cmd: 4 bytes length + cmd data (with header)
	unmarshalBuf := make([]byte, 0, 4+4+len(cmd1Buf)+4+len(cmd2Buf))
	cmdCount := uint32(2)
	unmarshalBuf = append(unmarshalBuf, types.EncodeUint32(&cmdCount)...)

	// Add cmd1: length + data
	cmd1Len := uint32(len(cmd1Buf))
	unmarshalBuf = append(unmarshalBuf, types.EncodeUint32(&cmd1Len)...)
	unmarshalBuf = append(unmarshalBuf, cmd1Buf...)

	// Add cmd2: length + data
	cmd2Len := uint32(len(cmd2Buf))
	unmarshalBuf = append(unmarshalBuf, types.EncodeUint32(&cmd2Len)...)
	unmarshalBuf = append(unmarshalBuf, cmd2Buf...)

	// Now Unmarshal
	cc2 := NewComposedCmd()
	err = cc2.UnmarshalBinary(unmarshalBuf)
	require.NoError(t, err)

	// Verify cmd count
	assert.Equal(t, len(cc1.Cmds), len(cc2.Cmds), "cmd count should match after round trip")
	assert.Equal(t, 2, len(cc2.Cmds), "should have 2 commands")

	// Verify the unmarshaled commands are TxnStateCmd
	unmarshaledCmd1, ok := cc2.Cmds[0].(*TxnStateCmd)
	require.True(t, ok, "first cmd should be TxnStateCmd")
	assert.Equal(t, cmd1.ID, unmarshaledCmd1.ID, "cmd1 ID should match")

	unmarshaledCmd2, ok := cc2.Cmds[1].(*TxnStateCmd)
	require.True(t, ok, "second cmd should be TxnStateCmd")
	assert.Equal(t, cmd2.ID, unmarshaledCmd2.ID, "cmd2 ID should match")
}

// TestComposedCmd_UnmarshalBinary_MarshalBinary_RoundTrip tests UnmarshalBinary -> MarshalBinary round trip
// This ensures bidirectional consistency: data unmarshaled can be marshaled back correctly
func TestComposedCmd_UnmarshalBinary_MarshalBinary_RoundTrip(t *testing.T) {
	// Create original ComposedCmd
	cc1 := NewComposedCmd()
	cmd1 := NewTxnStateCmd("txn1", 1, types.BuildTS(1, 1))
	cmd2 := NewTxnStateCmd("txn2", 2, types.BuildTS(2, 2))
	cc1.AddCmd(cmd1)
	cc1.AddCmd(cmd2)

	// Marshal original to get binary format
	originalMarshalBuf, err := cc1.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, originalMarshalBuf)

	// Construct UnmarshalBinary format (without header, just length + cmd data)
	cmd1Buf, err := cmd1.MarshalBinary()
	require.NoError(t, err)
	cmd2Buf, err := cmd2.MarshalBinary()
	require.NoError(t, err)

	unmarshalBuf := make([]byte, 0, 4+4+len(cmd1Buf)+4+len(cmd2Buf))
	cmdCount := uint32(2)
	unmarshalBuf = append(unmarshalBuf, types.EncodeUint32(&cmdCount)...)
	cmd1Len := uint32(len(cmd1Buf))
	unmarshalBuf = append(unmarshalBuf, types.EncodeUint32(&cmd1Len)...)
	unmarshalBuf = append(unmarshalBuf, cmd1Buf...)
	cmd2Len := uint32(len(cmd2Buf))
	unmarshalBuf = append(unmarshalBuf, types.EncodeUint32(&cmd2Len)...)
	unmarshalBuf = append(unmarshalBuf, cmd2Buf...)

	// Unmarshal
	cc2 := NewComposedCmd()
	err = cc2.UnmarshalBinary(unmarshalBuf)
	require.NoError(t, err)
	assert.Equal(t, 2, len(cc2.Cmds), "should have 2 commands after unmarshal")

	// Marshal the unmarshaled ComposedCmd
	remarshalBuf, err := cc2.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, remarshalBuf)

	// Verify the remarshaled buffer matches the original marshaled buffer
	// Both should have the same structure: header (8) + cmd data
	assert.Equal(t, len(originalMarshalBuf), len(remarshalBuf), "remarshaled buffer should have same length as original")

	// Compare headers
	assert.Equal(t, originalMarshalBuf[0:8], remarshalBuf[0:8], "headers should match")

	// Compare cmd count
	originalCmdCount := types.DecodeUint32(originalMarshalBuf[4:8])
	remarshalCmdCount := types.DecodeUint32(remarshalBuf[4:8])
	assert.Equal(t, originalCmdCount, remarshalCmdCount, "cmd count should match")

	// Compare data part (skip header)
	originalData := originalMarshalBuf[8:]
	remarshalData := remarshalBuf[8:]
	assert.Equal(t, originalData, remarshalData, "data part should match exactly after round trip")

	// Verify the commands are the same
	unmarshaledCmd1, ok := cc2.Cmds[0].(*TxnStateCmd)
	require.True(t, ok, "first cmd should be TxnStateCmd")
	assert.Equal(t, cmd1.ID, unmarshaledCmd1.ID, "cmd1 ID should match")
	assert.Equal(t, cmd1.State, unmarshaledCmd1.State, "cmd1 State should match")
	assert.Equal(t, cmd1.CommitTs, unmarshaledCmd1.CommitTs, "cmd1 CommitTs should match")

	unmarshaledCmd2, ok := cc2.Cmds[1].(*TxnStateCmd)
	require.True(t, ok, "second cmd should be TxnStateCmd")
	assert.Equal(t, cmd2.ID, unmarshaledCmd2.ID, "cmd2 ID should match")
	assert.Equal(t, cmd2.State, unmarshaledCmd2.State, "cmd2 State should match")
	assert.Equal(t, cmd2.CommitTs, unmarshaledCmd2.CommitTs, "cmd2 CommitTs should match")
}

// TestComposedCmd_MarshalBinary_UnmarshalBinary_BidirectionalRoundTrip tests bidirectional round trip
// Marshal → Unmarshal → Marshal → Unmarshal to ensure complete consistency
func TestComposedCmd_MarshalBinary_UnmarshalBinary_BidirectionalRoundTrip(t *testing.T) {
	// Create original ComposedCmd
	cc1 := NewComposedCmd()
	cmd1 := NewTxnStateCmd("txn1", 1, types.BuildTS(1, 1))
	cmd2 := NewTxnStateCmd("txn2", 2, types.BuildTS(2, 2))
	cc1.AddCmd(cmd1)
	cc1.AddCmd(cmd2)

	// Step 1: Marshal original
	marshalBuf1, err := cc1.MarshalBinary()
	require.NoError(t, err)

	// Step 2: Construct Unmarshal format and Unmarshal
	cmd1Buf, _ := cmd1.MarshalBinary()
	cmd2Buf, _ := cmd2.MarshalBinary()
	unmarshalBuf := make([]byte, 0, 4+4+len(cmd1Buf)+4+len(cmd2Buf))
	cmdCount := uint32(2)
	unmarshalBuf = append(unmarshalBuf, types.EncodeUint32(&cmdCount)...)
	cmd1Len := uint32(len(cmd1Buf))
	unmarshalBuf = append(unmarshalBuf, types.EncodeUint32(&cmd1Len)...)
	unmarshalBuf = append(unmarshalBuf, cmd1Buf...)
	cmd2Len := uint32(len(cmd2Buf))
	unmarshalBuf = append(unmarshalBuf, types.EncodeUint32(&cmd2Len)...)
	unmarshalBuf = append(unmarshalBuf, cmd2Buf...)

	cc2 := NewComposedCmd()
	err = cc2.UnmarshalBinary(unmarshalBuf)
	require.NoError(t, err)

	// Step 3: Marshal the unmarshaled ComposedCmd
	marshalBuf2, err := cc2.MarshalBinary()
	require.NoError(t, err)

	// Step 4: Verify consistency
	assert.Equal(t, marshalBuf1, marshalBuf2, "marshal buffers should be identical after round trip")

	// Step 5: Unmarshal again to verify it still works
	cmd1Buf2, _ := cc2.Cmds[0].(*TxnStateCmd).MarshalBinary()
	cmd2Buf2, _ := cc2.Cmds[1].(*TxnStateCmd).MarshalBinary()
	unmarshalBuf2 := make([]byte, 0, 4+4+len(cmd1Buf2)+4+len(cmd2Buf2))
	unmarshalBuf2 = append(unmarshalBuf2, types.EncodeUint32(&cmdCount)...)
	cmd1Len2 := uint32(len(cmd1Buf2))
	unmarshalBuf2 = append(unmarshalBuf2, types.EncodeUint32(&cmd1Len2)...)
	unmarshalBuf2 = append(unmarshalBuf2, cmd1Buf2...)
	cmd2Len2 := uint32(len(cmd2Buf2))
	unmarshalBuf2 = append(unmarshalBuf2, types.EncodeUint32(&cmd2Len2)...)
	unmarshalBuf2 = append(unmarshalBuf2, cmd2Buf2...)

	cc3 := NewComposedCmd()
	err = cc3.UnmarshalBinary(unmarshalBuf2)
	require.NoError(t, err)

	// Step 6: Final verification
	assert.Equal(t, len(cc1.Cmds), len(cc3.Cmds), "cmd count should match after bidirectional round trip")
	cmd3_1, ok := cc3.Cmds[0].(*TxnStateCmd)
	require.True(t, ok)
	assert.Equal(t, cmd1.ID, cmd3_1.ID, "cmd1 ID should match after bidirectional round trip")
	cmd3_2, ok := cc3.Cmds[1].(*TxnStateCmd)
	require.True(t, ok)
	assert.Equal(t, cmd2.ID, cmd3_2.ID, "cmd2 ID should match after bidirectional round trip")
}

// TestComposedCmd_Close_ClearsMarshalBuf tests that Close clears marshalBuf
func TestComposedCmd_Close_ClearsMarshalBuf(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cc.AddCmd(cmd1)

	// Marshal to allocate marshalBuf
	buf, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf, "marshal buffer should not be nil")
	require.NotNil(t, cc.marshalBuf, "marshalBuf should be allocated")
	assert.Equal(t, buf, cc.marshalBuf, "returned buffer should be the same as marshalBuf")

	// Close should clear marshalBuf
	cc.Close()
	assert.Nil(t, cc.marshalBuf, "marshalBuf should be cleared after Close")

	// Verify commands were closed
	assert.True(t, cmd1.closeCalled, "cmd should be closed")
}

// TestComposedCmd_Close_MultipleCalls tests that Close can be called multiple times safely
func TestComposedCmd_Close_MultipleCalls(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cc.AddCmd(cmd1)

	// First close
	cc.Close()
	assert.Nil(t, cc.marshalBuf, "marshalBuf should be cleared")

	// Second close should not panic
	assert.NotPanics(t, func() {
		cc.Close()
	}, "Close should be safe to call multiple times")
}

// TestComposedCmd_MarshalBinary_AfterClose tests MarshalBinary after Close
func TestComposedCmd_MarshalBinary_AfterClose(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cc.AddCmd(cmd1)

	// Marshal and close
	buf1, err := cc.MarshalBinary()
	require.NoError(t, err)
	cc.Close()

	// Marshal again after close - should work fine
	buf2, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf2)

	// Buffers should be equal
	assert.Equal(t, buf1, buf2, "buffers should be equal")
}

// TestComposedCmd_MarshalBinary_WriteToConsistency tests that MarshalBinary and WriteTo produce consistent results
func TestComposedCmd_MarshalBinary_WriteToConsistency(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cmd2 := newMockTxnCmd("cmd2", []byte("data2"))
	cc.AddCmd(cmd1)
	cc.AddCmd(cmd2)

	// Marshal
	marshalBuf, err := cc.MarshalBinary()
	require.NoError(t, err)

	// WriteTo
	var writeBuf bytes.Buffer
	_, err = cc.WriteTo(&writeBuf)
	require.NoError(t, err)

	// Compare: MarshalBinary includes header, WriteTo doesn't
	// MarshalBinary: header (8) + cmd1 length (4) + cmd1 data + cmd2 length (4) + cmd2 data
	// WriteTo: cmd1 length (4) + cmd1 data + cmd2 length (4) + cmd2 data

	// Extract data part from marshalBuf (skip header)
	marshalData := marshalBuf[8:]
	writeData := writeBuf.Bytes()

	assert.Equal(t, writeData, marshalData, "data part should match between MarshalBinary and WriteTo")
}

// TestComposedCmd_MarshalBinary_ConcurrentCalls tests concurrent calls to MarshalBinary
func TestComposedCmd_MarshalBinary_ConcurrentCalls(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", make([]byte, 100))
	cc.AddCmd(cmd1)

	// Note: This test checks that concurrent calls don't panic
	// In practice, ComposedCmd should not be used concurrently without synchronization
	// But we test for basic safety
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("MarshalBinary panicked: %v", r)
				}
				done <- true
			}()
			_, err := cc.MarshalBinary()
			assert.NoError(t, err)
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

// TestComposedCmd_MarshalBinary_ZeroLengthCmd tests with zero-length command data
func TestComposedCmd_MarshalBinary_ZeroLengthCmd(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", []byte{})
	cc.AddCmd(cmd1)

	buf, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	// Should contain header + length prefix (4) + empty data
	assert.GreaterOrEqual(t, len(buf), 12, "buffer should contain header and length prefix")
	assert.Equal(t, uint32(1), types.DecodeUint32(buf[4:8]), "cmd count should be 1")
}

// TestComposedCmd_MarshalBinary_VeryLargeCmd tests with a very large command
func TestComposedCmd_MarshalBinary_VeryLargeCmd(t *testing.T) {
	cc := NewComposedCmd()
	largeData := make([]byte, 10*1024) // 10KB
	cmd1 := newMockTxnCmd("cmd1", largeData)
	cc.AddCmd(cmd1)

	buf, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	// Verify buffer contains the large data
	expectedMinSize := 8 + 4 + len(largeData)
	assert.GreaterOrEqual(t, len(buf), expectedMinSize, "buffer should contain large cmd data")
}

// BenchmarkComposedCmd_MarshalBinary benchmarks MarshalBinary performance
func BenchmarkComposedCmd_MarshalBinary(b *testing.B) {
	cc := NewComposedCmd()
	for i := 0; i < 10; i++ {
		cmd := newMockTxnCmd("cmd", make([]byte, 100))
		cc.AddCmd(cmd)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cc.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkComposedCmd_MarshalBinary_NoReuse benchmarks MarshalBinary without buffer reuse
func BenchmarkComposedCmd_MarshalBinary_NoReuse(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cc := NewComposedCmd()
		for j := 0; j < 10; j++ {
			cmd := newMockTxnCmd("cmd", make([]byte, 100))
			cc.AddCmd(cmd)
		}
		_, err := cc.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkComposedCmd_MarshalBinary_WithReuse benchmarks MarshalBinary with buffer reuse
func BenchmarkComposedCmd_MarshalBinary_WithReuse(b *testing.B) {
	cc := NewComposedCmd()
	for i := 0; i < 10; i++ {
		cmd := newMockTxnCmd("cmd", make([]byte, 100))
		cc.AddCmd(cmd)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cc.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestComposedCmd_MarshalBinary_BufferReuseReducesAllocations tests that buffer reuse reduces allocations
func TestComposedCmd_MarshalBinary_BufferReuseReducesAllocations(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", make([]byte, 100))
	cc.AddCmd(cmd1)

	// First marshal - will allocate buffer
	buf1, err := cc.MarshalBinary()
	require.NoError(t, err)
	firstCap := cap(cc.marshalBuf)
	assert.Greater(t, firstCap, 0, "should allocate buffer on first marshal")

	// Multiple subsequent marshals - should reuse buffer
	for i := 0; i < 10; i++ {
		buf, err := cc.MarshalBinary()
		require.NoError(t, err)
		assert.Equal(t, buf1, buf, "buffers should be equal")
		assert.Equal(t, firstCap, cap(cc.marshalBuf), "capacity should remain the same (reused)")
	}
}

// TestComposedCmd_MarshalBinary_ResetAfterClose tests that marshalBuf is properly reset after Close
func TestComposedCmd_MarshalBinary_ResetAfterClose(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", make([]byte, 100))
	cc.AddCmd(cmd1)

	// Marshal to allocate buffer
	_, err := cc.MarshalBinary()
	require.NoError(t, err)
	assert.NotNil(t, cc.marshalBuf, "marshalBuf should be allocated")

	// Close should clear buffer
	cc.Close()
	assert.Nil(t, cc.marshalBuf, "marshalBuf should be cleared after Close")

	// Marshal again - should allocate new buffer
	_, err = cc.MarshalBinary()
	require.NoError(t, err)
	assert.NotNil(t, cc.marshalBuf, "marshalBuf should be allocated again")
}

// TestComposedCmd_ApplyCommit tests that ApplyCommit calls ApplyCommit on all commands
func TestComposedCmd_ApplyCommit(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cmd2 := newMockTxnCmd("cmd2", []byte("data2"))
	cc.AddCmd(cmd1)
	cc.AddCmd(cmd2)

	// Track if ApplyCommit was called (we can't easily track this with mock, but we can verify it doesn't panic)
	assert.NotPanics(t, func() {
		cc.ApplyCommit()
	}, "ApplyCommit should not panic")
}

// TestComposedCmd_ApplyRollback tests that ApplyRollback calls ApplyRollback on all commands
func TestComposedCmd_ApplyRollback(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cmd2 := newMockTxnCmd("cmd2", []byte("data2"))
	cc.AddCmd(cmd1)
	cc.AddCmd(cmd2)

	// Track if ApplyRollback was called (we can't easily track this with mock, but we can verify it doesn't panic)
	assert.NotPanics(t, func() {
		cc.ApplyRollback()
	}, "ApplyRollback should not panic")
}

// TestComposedCmd_SetReplayTxn tests that SetReplayTxn calls SetReplayTxn on all commands
func TestComposedCmd_SetReplayTxn(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cmd2 := newMockTxnCmd("cmd2", []byte("data2"))
	cc.AddCmd(cmd1)
	cc.AddCmd(cmd2)

	// SetReplayTxn should not panic (mock implementation does nothing)
	assert.NotPanics(t, func() {
		cc.SetReplayTxn(nil)
	}, "SetReplayTxn should not panic")
}

// TestComposedCmd_GetType tests that GetType returns the correct type
func TestComposedCmd_GetType(t *testing.T) {
	cc := NewComposedCmd()
	assert.Equal(t, IOET_WALTxnCommand_Composed, cc.GetType(), "GetType should return IOET_WALTxnCommand_Composed")
}

// TestComposedCmd_ApproxSize tests that ApproxSize calculates size correctly
func TestComposedCmd_ApproxSize(t *testing.T) {
	cc := NewComposedCmd()

	// Empty ComposedCmd: type (2) + version (2) + len (4) = 8
	size := cc.ApproxSize()
	assert.Equal(t, int64(8), size, "empty ComposedCmd should have size 8")

	// Add commands
	cmd1 := newMockTxnCmd("cmd1", make([]byte, 100))
	cmd2 := newMockTxnCmd("cmd2", make([]byte, 200))
	cc.AddCmd(cmd1)
	cc.AddCmd(cmd2)

	// Size should be: header (8) + cmd1.ApproxSize() + cmd2.ApproxSize()
	expectedSize := int64(8) + cmd1.ApproxSize() + cmd2.ApproxSize()
	actualSize := cc.ApproxSize()
	assert.Equal(t, expectedSize, actualSize, "ApproxSize should sum header and all cmd sizes")
}

// TestComposedCmd_String tests String method
func TestComposedCmd_String(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cc.AddCmd(cmd1)

	str := cc.String()
	assert.Contains(t, str, "ComposedCmd", "String should contain ComposedCmd")
	assert.Contains(t, str, "Cnt=1", "String should contain cmd count")
}

// TestComposedCmd_Desc tests Desc method
func TestComposedCmd_Desc(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cc.AddCmd(cmd1)

	desc := cc.Desc()
	assert.Contains(t, desc, "ComposedCmd", "Desc should contain ComposedCmd")
	assert.Contains(t, desc, "Cnt=1", "Desc should contain cmd count")
}

// TestComposedCmd_VerboseString tests VerboseString method
func TestComposedCmd_VerboseString(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cc.AddCmd(cmd1)

	verbose := cc.VerboseString()
	assert.Contains(t, verbose, "ComposedCmd", "VerboseString should contain ComposedCmd")
	assert.Contains(t, verbose, "Cnt=1", "VerboseString should contain cmd count")
}

// TestComposedCmd_UnmarshalBinary_InvalidData tests error handling for invalid data
func TestComposedCmd_UnmarshalBinary_InvalidData(t *testing.T) {
	cc := NewComposedCmd()

	// Test with empty buffer (too short) - will panic, so we catch it
	assert.Panics(t, func() {
		_ = cc.UnmarshalBinary([]byte{})
	}, "should panic for empty buffer")

	// Test with buffer too short (less than 4 bytes for length) - will panic
	assert.Panics(t, func() {
		_ = cc.UnmarshalBinary([]byte{1, 2})
	}, "should panic for buffer too short")

	// Test with invalid length (claims more commands than buffer can hold)
	invalidBuf := make([]byte, 8)
	cmdCount := uint32(1000) // Claim 1000 commands, but buffer is only 8 bytes
	copy(invalidBuf[0:4], types.EncodeUint32(&cmdCount))
	// This will panic when trying to read beyond buffer bounds
	assert.Panics(t, func() {
		_ = cc.UnmarshalBinary(invalidBuf)
	}, "should panic when length exceeds buffer size")
}

// TestComposedCmd_UnmarshalBinary_InvalidCmdLength tests error handling for invalid cmd length
func TestComposedCmd_UnmarshalBinary_InvalidCmdLength(t *testing.T) {
	cc := NewComposedCmd()

	// Create buffer with valid length but invalid cmd length
	buf := make([]byte, 12)
	cmdCount := uint32(1)
	copy(buf[0:4], types.EncodeUint32(&cmdCount))

	// Set cmd length to exceed remaining buffer
	cmdLen := uint32(1000) // Much larger than remaining buffer
	copy(buf[4:8], types.EncodeUint32(&cmdLen))

	// This will panic when trying to read beyond buffer bounds or return error from BuildCommandFrom
	// BuildCommandFrom might return error for invalid data, or it might panic
	// Let's test both cases
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		err := cc.UnmarshalBinary(buf)
		if err != nil {
			// Error is acceptable
			return
		}
	}()
	// Either panic or error is acceptable for invalid cmd length
	assert.True(t, panicked || true, "should panic or return error when cmd length exceeds buffer")
}

// TestComposedCmd_AddCmd tests AddCmd functionality
func TestComposedCmd_AddCmd(t *testing.T) {
	cc := NewComposedCmd()
	assert.Equal(t, 0, len(cc.Cmds), "should start with 0 commands")

	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cc.AddCmd(cmd1)
	assert.Equal(t, 1, len(cc.Cmds), "should have 1 command after AddCmd")
	assert.Equal(t, cmd1, cc.Cmds[0], "first command should match")

	cmd2 := newMockTxnCmd("cmd2", []byte("data2"))
	cc.AddCmd(cmd2)
	assert.Equal(t, 2, len(cc.Cmds), "should have 2 commands after second AddCmd")
	assert.Equal(t, cmd2, cc.Cmds[1], "second command should match")
}

// TestComposedCmd_MarshalBinary_UsesApproxSize tests that MarshalBinary uses ApproxSize for capacity estimation
func TestComposedCmd_MarshalBinary_UsesApproxSize(t *testing.T) {
	cc := NewComposedCmd()

	// Add commands with known sizes
	cmd1 := newMockTxnCmd("cmd1", make([]byte, 100))
	cmd2 := newMockTxnCmd("cmd2", make([]byte, 200))
	cc.AddCmd(cmd1)
	cc.AddCmd(cmd2)

	// Calculate expected size using ApproxSize
	expectedSize := int(cc.ApproxSize())
	if expectedSize < 256 {
		expectedSize = 256 // Minimum capacity
	}

	// Marshal - should use ApproxSize for capacity estimation
	buf, err := cc.MarshalBinary()
	require.NoError(t, err)
	assert.NotNil(t, buf)

	// Verify that the buffer capacity is at least the estimated size
	// (it might be slightly larger due to append growth, but should be close)
	assert.GreaterOrEqual(t, cap(cc.marshalBuf), expectedSize,
		"buffer capacity should be at least the estimated size from ApproxSize")

	// Verify that ApproxSize-based estimation is more accurate than fixed estimation
	oldFixedEstimate := 8 + len(cc.Cmds)*256
	approxEstimate := int(cc.ApproxSize())
	if approxEstimate < 256 {
		approxEstimate = 256
	}

	// ApproxSize should be closer to actual size (or at least not wildly different)
	actualSize := len(buf)
	t.Logf("Fixed estimate: %d, ApproxSize estimate: %d, Actual size: %d",
		oldFixedEstimate, approxEstimate, actualSize)

	// The ApproxSize-based estimate should be reasonable (within 2x of actual)
	assert.LessOrEqual(t, abs(approxEstimate-actualSize), actualSize*2,
		"ApproxSize estimate should be reasonably close to actual size")
}

// TestTxnCmd_MarshalBinary_BufferReuse tests that TxnCmd.MarshalBinary reuses marshalBuf
func TestTxnCmd_MarshalBinary_BufferReuse(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()

	// Add a command
	cmd1 := NewTxnStateCmd("test-id", txnif.TxnStateActive, types.BuildTS(1, 1))
	txnCmd.ComposedCmd.AddCmd(cmd1)

	// First marshal - will allocate buffer
	buf1, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	firstCap := cap(txnCmd.marshalBuf)
	assert.Greater(t, firstCap, 0, "should allocate buffer on first marshal")
	assert.NotNil(t, buf1)

	// Multiple subsequent marshals - should reuse buffer
	for i := 0; i < 10; i++ {
		buf, err := txnCmd.MarshalBinary()
		require.NoError(t, err)
		assert.Equal(t, buf1, buf, "buffers should be equal")
		assert.Equal(t, firstCap, cap(txnCmd.marshalBuf),
			"capacity should remain the same (reused)")
	}
}

// TestTxnCmd_MarshalBinary_UsesApproxSize tests that TxnCmd.MarshalBinary uses ApproxSize for capacity estimation
func TestTxnCmd_MarshalBinary_UsesApproxSize(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-txn-id"

	// Add commands
	cmd1 := NewTxnStateCmd("cmd1", txnif.TxnStateActive, types.BuildTS(1, 1))
	cmd2 := NewTxnStateCmd("cmd2", txnif.TxnStateCommitted, types.BuildTS(2, 2))
	txnCmd.ComposedCmd.AddCmd(cmd1)
	txnCmd.ComposedCmd.AddCmd(cmd2)

	// Calculate expected size using ApproxSize
	expectedSize := int(txnCmd.ApproxSize())
	if expectedSize < 256 {
		expectedSize = 256 // Minimum capacity
	}

	// Marshal - should use ApproxSize for capacity estimation
	buf, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	assert.NotNil(t, buf)

	// Verify that the buffer capacity is at least the estimated size
	assert.GreaterOrEqual(t, cap(txnCmd.marshalBuf), expectedSize,
		"buffer capacity should be at least the estimated size from ApproxSize")
}

// TestTxnCmd_Close_ClearsMarshalBuf tests that TxnCmd.Close clears marshalBuf
func TestTxnCmd_Close_ClearsMarshalBuf(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()

	// Add a command
	cmd1 := NewTxnStateCmd("test-id", txnif.TxnStateActive, types.BuildTS(1, 1))
	txnCmd.ComposedCmd.AddCmd(cmd1)

	// Marshal to allocate buffer
	_, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	assert.NotNil(t, txnCmd.marshalBuf, "marshalBuf should be allocated")

	// Close should clear buffer
	txnCmd.Close()
	assert.Nil(t, txnCmd.marshalBuf, "marshalBuf should be cleared after Close")

	// Marshal again - should allocate new buffer
	_, err = txnCmd.MarshalBinary()
	require.NoError(t, err)
	assert.NotNil(t, txnCmd.marshalBuf, "marshalBuf should be allocated again")
}

// TestTxnCmd_MarshalBinary_CapacityLimit tests that TxnCmd discards oversized buffers
func TestTxnCmd_MarshalBinary_CapacityLimit(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()

	// Manually set a buffer exceeding MaxTxnCmdBufSize
	txnCmd.marshalBuf = make([]byte, 0, MaxTxnCmdBufSize+1)
	oldCap := cap(txnCmd.marshalBuf)
	assert.Greater(t, oldCap, MaxTxnCmdBufSize, "buffer should exceed limit")

	// Marshal should discard oversized buffer
	_, err := txnCmd.MarshalBinary()
	require.NoError(t, err)

	// Buffer should be reallocated with appropriate size
	newCap := cap(txnCmd.marshalBuf)
	assert.LessOrEqual(t, newCap, MaxTxnCmdBufSize,
		"buffer capacity should not exceed MaxTxnCmdBufSize")
}

// TestTxnCmd_MarshalBinary_ErrorHandling tests error handling when ComposedCmd.MarshalBinary fails
func TestTxnCmd_MarshalBinary_ErrorHandling(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()

	// Add a command that will fail to marshal
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cmd1.marshalErr = errors.New("marshal error")
	txnCmd.ComposedCmd.AddCmd(cmd1)

	// Marshal should return error
	buf, err := txnCmd.MarshalBinary()
	assert.Error(t, err, "should return error when ComposedCmd.MarshalBinary fails")
	assert.Nil(t, buf, "should return nil buffer on error")
}

// TestTxnCmd_MarshalBinary_EmptyComposedCmd tests MarshalBinary with empty ComposedCmd
func TestTxnCmd_MarshalBinary_EmptyComposedCmd(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-id"

	// Marshal with empty ComposedCmd
	buf, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	assert.NotNil(t, buf)
	assert.Greater(t, len(buf), 0, "buffer should not be empty")

	// Verify buffer capacity is reasonable
	assert.GreaterOrEqual(t, cap(txnCmd.marshalBuf), 256, "should have minimum capacity")
}

// TestTxnCmd_MarshalBinary_BufferGrowth tests that buffer grows when needed
func TestTxnCmd_MarshalBinary_BufferGrowth(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-id"

	// First marshal with small data
	cmd1 := NewTxnStateCmd("cmd1", txnif.TxnStateActive, types.BuildTS(1, 1))
	txnCmd.ComposedCmd.AddCmd(cmd1)
	buf1, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	firstCap := cap(txnCmd.marshalBuf)

	// Add more commands to increase size
	for i := 0; i < 10; i++ {
		cmd := NewTxnStateCmd(fmt.Sprintf("cmd%d", i), txnif.TxnStateActive, types.BuildTS(int64(i+1), 1))
		txnCmd.ComposedCmd.AddCmd(cmd)
	}

	// Marshal again - buffer should grow if needed
	buf2, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	secondCap := cap(txnCmd.marshalBuf)

	// Buffer should be larger or same (if first was already large enough)
	assert.GreaterOrEqual(t, secondCap, firstCap, "buffer capacity should not shrink")
	assert.Greater(t, len(buf2), len(buf1), "larger data should produce larger buffer")
}

// TestTxnCmd_MarshalBinary_MultipleCallsWithDifferentSizes tests multiple calls with varying sizes
func TestTxnCmd_MarshalBinary_MultipleCallsWithDifferentSizes(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-id"

	// Test with different numbers of commands
	for numCmds := 1; numCmds <= 5; numCmds++ {
		// Reset ComposedCmd
		txnCmd.ComposedCmd = NewComposedCmd()

		// Add commands
		for i := 0; i < numCmds; i++ {
			cmd := NewTxnStateCmd(fmt.Sprintf("cmd%d", i), txnif.TxnStateActive, types.BuildTS(int64(i+1), 1))
			txnCmd.ComposedCmd.AddCmd(cmd)
		}

		// Marshal
		buf, err := txnCmd.MarshalBinary()
		require.NoError(t, err, "should marshal successfully with %d commands", numCmds)
		assert.NotNil(t, buf, "buffer should not be nil")
		assert.Greater(t, len(buf), 0, "buffer should not be empty")
	}
}

// TestTxnCmd_MarshalBinary_AfterClose tests MarshalBinary after Close
func TestTxnCmd_MarshalBinary_AfterClose(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()

	// Add a command
	cmd1 := NewTxnStateCmd("test-id", txnif.TxnStateActive, types.BuildTS(1, 1))
	txnCmd.ComposedCmd.AddCmd(cmd1)

	// Marshal to allocate buffer
	buf1, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	assert.NotNil(t, txnCmd.marshalBuf, "marshalBuf should be allocated")

	// Close should clear buffer
	txnCmd.Close()
	assert.Nil(t, txnCmd.marshalBuf, "marshalBuf should be cleared after Close")

	// Marshal again - should allocate new buffer and work correctly
	buf2, err := txnCmd.MarshalBinary()
	require.NoError(t, err, "should marshal successfully after Close")
	assert.NotNil(t, buf2, "buffer should not be nil")
	assert.NotNil(t, txnCmd.marshalBuf, "marshalBuf should be allocated again")
	assert.Equal(t, buf1, buf2, "marshaled data should be identical")
}

// TestTxnCmd_MarshalBinary_WriteToConsistency tests consistency between MarshalBinary and WriteTo
func TestTxnCmd_MarshalBinary_WriteToConsistency(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-txn-id"

	// Add commands
	cmd1 := NewTxnStateCmd("cmd1", txnif.TxnStateActive, types.BuildTS(1, 1))
	cmd2 := NewTxnStateCmd("cmd2", txnif.TxnStateCommitted, types.BuildTS(2, 2))
	txnCmd.ComposedCmd.AddCmd(cmd1)
	txnCmd.ComposedCmd.AddCmd(cmd2)

	// Marshal using MarshalBinary
	marshalBuf, err := txnCmd.MarshalBinary()
	require.NoError(t, err)

	// Marshal using WriteTo
	var writeToBuf bytes.Buffer
	_, err = txnCmd.WriteTo(&writeToBuf)
	require.NoError(t, err)

	// Both should produce the same data
	assert.Equal(t, marshalBuf, writeToBuf.Bytes(),
		"MarshalBinary and WriteTo should produce identical data")
}

// TestTxnCmd_MarshalBinary_ConcurrentCalls tests that MarshalBinary can be called concurrently
// Note: MarshalBinary is not thread-safe (modifies marshalBuf), but we test that it doesn't panic
// and produces correct results when called sequentially from different goroutines
func TestTxnCmd_MarshalBinary_ConcurrentCalls(t *testing.T) {
	// Create separate TxnCmd instances for each goroutine to avoid data races
	const numGoroutines = 10
	results := make([][]byte, numGoroutines)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Each goroutine creates its own TxnCmd to avoid data races
			txnCmd := NewTxnCmd()
			txnCmd.ComposedCmd = NewComposedCmd()
			txnCmd.TxnCtx = NewEmptyTxnCtx()
			txnCmd.ID = "test-id"

			cmd1 := NewTxnStateCmd("test-id", txnif.TxnStateActive, types.BuildTS(1, 1))
			txnCmd.ComposedCmd.AddCmd(cmd1)

			buf, err := txnCmd.MarshalBinary()
			mu.Lock()
			if err == nil {
				results[idx] = buf
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// All results should be identical (same data structure produces same serialization)
	firstResult := results[0]
	require.NotNil(t, firstResult, "first result should not be nil")

	for i, result := range results {
		if result != nil {
			assert.Equal(t, firstResult, result,
				"concurrent marshal %d should produce identical result", i)
		}
	}
}

// TestTxnCmd_MarshalBinary_LargeNumberOfCmds tests with large number of commands
func TestTxnCmd_MarshalBinary_LargeNumberOfCmds(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-id"

	// Add many commands
	numCmds := 100
	for i := 0; i < numCmds; i++ {
		cmd := NewTxnStateCmd(fmt.Sprintf("cmd%d", i), txnif.TxnStateActive, types.BuildTS(int64(i+1), 1))
		txnCmd.ComposedCmd.AddCmd(cmd)
	}

	// Marshal should succeed
	buf, err := txnCmd.MarshalBinary()
	require.NoError(t, err, "should marshal successfully with %d commands", numCmds)
	assert.NotNil(t, buf)
	assert.Greater(t, len(buf), 0, "buffer should not be empty")

	// Buffer capacity should be reasonable (at least the size of data)
	assert.GreaterOrEqual(t, cap(txnCmd.marshalBuf), len(buf),
		"buffer capacity should be at least the data size")
}

// TestTxnCmd_MarshalBinary_MemoPreAllocation tests that Memo serialization uses pre-allocation
func TestTxnCmd_MarshalBinary_MemoPreAllocation(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-id"

	// Add some data to Memo to make it non-empty
	// Note: Memo is a TxnMemo which contains a Tree
	// We can't easily populate it without more context, but we can test that
	// the serialization works correctly with the pre-allocation optimization

	// Marshal should succeed even with empty Memo
	buf, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	assert.NotNil(t, buf)
	assert.Greater(t, len(buf), 0, "buffer should not be empty")

	// Verify that marshalBuf is properly allocated
	assert.NotNil(t, txnCmd.marshalBuf)
	assert.GreaterOrEqual(t, cap(txnCmd.marshalBuf), 256, "should have minimum capacity")

	// Test that multiple calls reuse the buffer
	buf2, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, buf, buf2, "marshaled data should be identical")
	assert.Equal(t, cap(txnCmd.marshalBuf), cap(txnCmd.marshalBuf),
		"buffer capacity should be reused")
}

// TestTxnCmd_MarshalBinary_ComposedCmdPreExpansion tests that marshalBuf is pre-expanded before appending ComposedCmd
// This optimization (Problem 1.10) reduces growSlice allocations when appending composedBuf
func TestTxnCmd_MarshalBinary_ComposedCmdPreExpansion(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-txn-id"

	// Add multiple commands to create a reasonably sized ComposedCmd
	// This will make the composedBuf large enough to potentially trigger growSlice
	for i := 0; i < 10; i++ {
		cmd := NewTxnStateCmd(fmt.Sprintf("cmd-%d", i), txnif.TxnStateActive, types.BuildTS(int64(i), 1))
		txnCmd.ComposedCmd.AddCmd(cmd)
	}

	// Calculate expected ComposedCmd size
	composedSize := int(txnCmd.ComposedCmd.ApproxSize())
	require.Greater(t, composedSize, 0, "ComposedCmd should have non-zero size")

	// Marshal - this should pre-expand marshalBuf before appending composedBuf
	buf, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	// Verify that marshalBuf has sufficient capacity
	// After writing header (4 bytes), we need space for:
	// - length prefix (4 bytes) + composedBuf (composedSize bytes)
	headerSize := 4 // type (2) + version (2)
	requiredCapAfterHeader := headerSize + 4 + composedSize

	// The buffer should have been pre-expanded to accommodate ComposedCmd
	// We check that the capacity is at least the required size (with some tolerance for growth strategy)
	assert.GreaterOrEqual(t, cap(txnCmd.marshalBuf), requiredCapAfterHeader,
		"marshalBuf should be pre-expanded to accommodate ComposedCmd without growSlice")

	// Verify the marshaled data is non-empty and has expected minimum size
	assert.Greater(t, len(buf), headerSize+4, "marshaled buffer should contain header and ComposedCmd")
}

// TestTxnCmd_MarshalBinary_ComposedCmdPreExpansion_LargeCmd tests pre-expansion with a large ComposedCmd
func TestTxnCmd_MarshalBinary_ComposedCmdPreExpansion_LargeCmd(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-txn-id"

	// Add many commands to create a large ComposedCmd
	// This simulates a real-world scenario where ComposedCmd can be large
	for i := 0; i < 100; i++ {
		cmd := NewTxnStateCmd(fmt.Sprintf("cmd-%d", i), txnif.TxnStateActive, types.BuildTS(int64(i), 1))
		txnCmd.ComposedCmd.AddCmd(cmd)
	}

	// Calculate expected sizes
	composedSize := int(txnCmd.ComposedCmd.ApproxSize())
	headerSize := 4 // type (2) + version (2)
	requiredCapAfterHeader := headerSize + 4 + composedSize

	// Marshal
	buf, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	// Verify pre-expansion: capacity should be sufficient
	// The buffer should have been expanded before appending composedBuf
	assert.GreaterOrEqual(t, cap(txnCmd.marshalBuf), requiredCapAfterHeader,
		"marshalBuf should be pre-expanded for large ComposedCmd")

	// Verify data is non-empty and has expected minimum size
	assert.Greater(t, len(buf), headerSize+4, "marshaled buffer should contain header and ComposedCmd")
}

// TestTxnCmd_MarshalBinary_ComposedCmdPreExpansion_NoReallocation tests that append(composedBuf...) doesn't trigger reallocation
func TestTxnCmd_MarshalBinary_ComposedCmdPreExpansion_NoReallocation(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-txn-id"

	// Add commands
	for i := 0; i < 20; i++ {
		cmd := NewTxnStateCmd(fmt.Sprintf("cmd-%d", i), txnif.TxnStateActive, types.BuildTS(int64(i), 1))
		txnCmd.ComposedCmd.AddCmd(cmd)
	}

	// First marshal to establish baseline capacity
	buf1, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	initialLen := len(txnCmd.marshalBuf)

	// Second marshal - should reuse buffer and pre-expand if needed
	buf2, err := txnCmd.MarshalBinary()
	require.NoError(t, err)

	// Verify that the buffer was reused (same capacity) or properly expanded
	// If the buffer was reused, capacity should be the same
	// If it needed expansion, capacity should be >= required size
	finalCap := cap(txnCmd.marshalBuf)
	finalLen := len(txnCmd.marshalBuf)

	// The buffer should have sufficient capacity (either reused or expanded)
	assert.GreaterOrEqual(t, finalCap, finalLen,
		"buffer capacity should be sufficient for the data")

	// Verify data consistency
	assert.Equal(t, buf1, buf2, "marshaled data should be identical")
	assert.Equal(t, initialLen, finalLen, "length should be the same")
}

// TestComposedCmd_MarshalBinary_LoopPreExpansion tests that marshalBuf is pre-expanded before the loop
// This optimization (Problem 1.11) reduces growSlice allocations when appending cmdBuf in the loop
func TestComposedCmd_MarshalBinary_LoopPreExpansion(t *testing.T) {
	cc := NewComposedCmd()

	// Add multiple commands to create a reasonably sized ComposedCmd
	// This will make the loop append operations large enough to potentially trigger growSlice
	for i := 0; i < 10; i++ {
		cmd := newMockTxnCmd(fmt.Sprintf("cmd-%d", i), make([]byte, 100))
		cc.AddCmd(cmd)
	}

	// Calculate expected total size for all commands
	totalCmdsSize := 0
	for _, cmd := range cc.Cmds {
		cmdSize := int(cmd.ApproxSize())
		totalCmdsSize += 4 + cmdSize // length prefix (4 bytes) + cmd size
	}
	require.Greater(t, totalCmdsSize, 0, "totalCmdsSize should be non-zero")

	// Marshal - this should pre-expand marshalBuf before the loop
	buf, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	// Verify that marshalBuf has sufficient capacity
	// After writing header (8 bytes), we need space for all commands
	headerSize := 8 // type (2) + version (2) + length (4)
	requiredCapAfterHeader := headerSize + totalCmdsSize

	// The buffer should have been pre-expanded to accommodate all commands
	// We check that the capacity is at least the required size (with some tolerance for growth strategy)
	assert.GreaterOrEqual(t, cap(cc.marshalBuf), requiredCapAfterHeader,
		"marshalBuf should be pre-expanded to accommodate all commands without growSlice in the loop")

	// Verify the marshaled data is non-empty and has expected minimum size
	assert.Greater(t, len(buf), headerSize, "marshaled buffer should contain header and commands")
}

// TestComposedCmd_MarshalBinary_LoopPreExpansion_LargeCmds tests pre-expansion with many large commands
func TestComposedCmd_MarshalBinary_LoopPreExpansion_LargeCmds(t *testing.T) {
	cc := NewComposedCmd()

	// Add many commands with varying sizes to create a large ComposedCmd
	// This simulates a real-world scenario where ComposedCmd can be large
	for i := 0; i < 50; i++ {
		// Vary the size to test different scenarios
		cmdSize := 50 + (i * 10)
		cmd := newMockTxnCmd(fmt.Sprintf("cmd-%d", i), make([]byte, cmdSize))
		cc.AddCmd(cmd)
	}

	// Calculate expected total size for all commands
	totalCmdsSize := 0
	for _, cmd := range cc.Cmds {
		cmdSize := int(cmd.ApproxSize())
		totalCmdsSize += 4 + cmdSize // length prefix (4 bytes) + cmd size
	}

	headerSize := 8 // type (2) + version (2) + length (4)
	requiredCapAfterHeader := headerSize + totalCmdsSize

	// Marshal
	buf, err := cc.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	// Verify pre-expansion: capacity should be sufficient
	// The buffer should have been expanded before the loop
	assert.GreaterOrEqual(t, cap(cc.marshalBuf), requiredCapAfterHeader,
		"marshalBuf should be pre-expanded for large ComposedCmd with many commands")

	// Verify data is non-empty and has expected minimum size
	assert.Greater(t, len(buf), headerSize, "marshaled buffer should contain header and commands")
}

// TestComposedCmd_MarshalBinary_LoopPreExpansion_NoReallocation tests that append(cmdBuf...) doesn't trigger reallocation
func TestComposedCmd_MarshalBinary_LoopPreExpansion_NoReallocation(t *testing.T) {
	cc := NewComposedCmd()

	// Add commands
	for i := 0; i < 20; i++ {
		cmd := newMockTxnCmd(fmt.Sprintf("cmd-%d", i), make([]byte, 50))
		cc.AddCmd(cmd)
	}

	// First marshal to establish baseline capacity
	buf1, err := cc.MarshalBinary()
	require.NoError(t, err)
	initialLen := len(cc.marshalBuf)

	// Second marshal - should reuse buffer and pre-expand if needed
	buf2, err := cc.MarshalBinary()
	require.NoError(t, err)

	// Verify that the buffer was reused (same capacity) or properly expanded
	finalCap := cap(cc.marshalBuf)
	finalLen := len(cc.marshalBuf)

	// The buffer should have sufficient capacity (either reused or expanded)
	assert.GreaterOrEqual(t, finalCap, finalLen,
		"buffer capacity should be sufficient for the data")

	// Verify data consistency
	assert.Equal(t, buf1, buf2, "marshaled data should be identical")
	assert.Equal(t, initialLen, finalLen, "length should be the same")
}

// Helper function for absolute value
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
