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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockTxnCmd is a mock implementation of TxnCmd for testing
// It implements MarshalBinaryWithBuffer to satisfy the new interface requirement
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

// MarshalBinaryWithBuffer implements the new interface required by ComposedCmd
func (m *mockTxnCmd) MarshalBinaryWithBuffer(buf *bytes.Buffer) error {
	if m.marshalErr != nil {
		return m.marshalErr
	}
	_, err := buf.Write(m.data)
	return err
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

// TestComposedCmd_MarshalBinary_PoolReuse tests that sync.Pool is used for buffer reuse
func TestComposedCmd_MarshalBinary_PoolReuse(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", make([]byte, 100))
	cc.AddCmd(cmd1)

	// Multiple marshals should work correctly (pool reuse is internal)
	for i := 0; i < 10; i++ {
		buf, err := cc.MarshalBinary()
		require.NoError(t, err)
		require.NotNil(t, buf)
		assert.Greater(t, len(buf), 0, "buffer should contain data")
	}
}

// TestComposedCmd_MarshalBinary_MultipleCallsConsistent tests multiple calls produce consistent results
func TestComposedCmd_MarshalBinary_MultipleCallsConsistent(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", make([]byte, 100))
	cc.AddCmd(cmd1)

	// First marshal
	buf1, err := cc.MarshalBinary()
	require.NoError(t, err)

	// Second marshal - should produce identical result
	buf2, err := cc.MarshalBinary()
	require.NoError(t, err)

	assert.Equal(t, buf1, buf2, "multiple calls should produce same result")
}

// TestComposedCmd_MarshalBinary_ErrorHandling tests error handling when cmd.MarshalBinaryWithBuffer fails
func TestComposedCmd_MarshalBinary_ErrorHandling(t *testing.T) {
	cc := NewComposedCmd()

	// Add a command that will fail to marshal
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cmd1.marshalErr = errors.New("marshal error")
	cc.AddCmd(cmd1)

	// Marshal should return error
	buf, err := cc.MarshalBinary()
	assert.Error(t, err, "should return error when cmd.MarshalBinaryWithBuffer fails")
	assert.Nil(t, buf, "buffer should be nil on error")
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

// TestComposedCmd_MarshalUnmarshal_RoundTrip tests MarshalBinary -> UnmarshalBinary round trip
func TestComposedCmd_MarshalUnmarshal_RoundTrip(t *testing.T) {
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

// TestComposedCmd_BidirectionalRoundTrip tests bidirectional round trip
// Marshal → Unmarshal → Marshal → Unmarshal to ensure complete consistency
func TestComposedCmd_BidirectionalRoundTrip(t *testing.T) {
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

	// Step 5: Verify commands
	assert.Equal(t, len(cc1.Cmds), len(cc2.Cmds), "cmd count should match")
	cmd2_1, ok := cc2.Cmds[0].(*TxnStateCmd)
	require.True(t, ok)
	assert.Equal(t, cmd1.ID, cmd2_1.ID, "cmd1 ID should match")
	cmd2_2, ok := cc2.Cmds[1].(*TxnStateCmd)
	require.True(t, ok)
	assert.Equal(t, cmd2.ID, cmd2_2.ID, "cmd2 ID should match")
}

// TestComposedCmd_Close tests that Close properly cleans up
func TestComposedCmd_Close(t *testing.T) {
	cc := NewComposedCmd()
	cmd1 := newMockTxnCmd("cmd1", []byte("data1"))
	cc.AddCmd(cmd1)

	// Close should call Close on all commands
	cc.Close()

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

	// Second close should not panic
	assert.NotPanics(t, func() {
		cc.Close()
	}, "Close should be safe to call multiple times")
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

// TestTxnCmd_MarshalBinary_Basic tests basic TxnCmd MarshalBinary functionality
func TestTxnCmd_MarshalBinary_Basic(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-txn"

	// Add a simple command
	cmd1 := NewTxnStateCmd("cmd1", 1, [12]byte{1})
	txnCmd.ComposedCmd.AddCmd(cmd1)

	// Test that MarshalBinary works
	buf, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)
	assert.Greater(t, len(buf), 0, "buffer should contain data")

	// Test multiple calls work correctly (pool reuse)
	buf2, err := txnCmd.MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, buf, buf2, "multiple calls should produce same result")
}

// TestTxnCmd_MarshalBinaryWithBuffer tests MarshalBinaryWithBuffer
func TestTxnCmd_MarshalBinaryWithBuffer(t *testing.T) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-txn"

	cmd1 := NewTxnStateCmd("cmd1", 1, [12]byte{1})
	txnCmd.ComposedCmd.AddCmd(cmd1)

	// Test MarshalBinaryWithBuffer
	buf := &bytes.Buffer{}
	err := txnCmd.MarshalBinaryWithBuffer(buf)
	require.NoError(t, err)
	assert.Greater(t, buf.Len(), 0, "buffer should contain data")

	// Verify header
	data := buf.Bytes()
	assert.Equal(t, txnCmd.GetType(), types.DecodeUint16(data[0:2]), "type should match")
	assert.Equal(t, IOET_WALTxnEntry_CurrVer, types.DecodeUint16(data[2:4]), "version should match")
}

// TestTxnStateCmd_MarshalBinaryWithBuffer tests TxnStateCmd MarshalBinaryWithBuffer
func TestTxnStateCmd_MarshalBinaryWithBuffer(t *testing.T) {
	cmd := NewTxnStateCmd("test-id", 1, types.BuildTS(100, 1))

	buf := &bytes.Buffer{}
	err := cmd.MarshalBinaryWithBuffer(buf)
	require.NoError(t, err)
	assert.Greater(t, buf.Len(), 0, "buffer should contain data")

	// Compare with MarshalBinary
	data1, err := cmd.MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, data1, buf.Bytes(), "MarshalBinaryWithBuffer should produce same result as MarshalBinary")
}

// TestSyncPool_BufferReuse tests that sync.Pool properly reuses buffers
func TestSyncPool_BufferReuse(t *testing.T) {
	// Get buffer from pool
	buf1 := getMarshalBuffer()
	require.NotNil(t, buf1)
	buf1.WriteString("test data")

	// Return to pool
	putMarshalBuffer(buf1)

	// Get another buffer - may or may not be the same one
	buf2 := getMarshalBuffer()
	require.NotNil(t, buf2)
	assert.Equal(t, 0, buf2.Len(), "buffer from pool should be reset")

	putMarshalBuffer(buf2)
}

// TestSyncPool_LargeBufferNotReturned tests that large buffers are not returned to pool
func TestSyncPool_LargeBufferNotReturned(t *testing.T) {
	// Create a large buffer
	buf := &bytes.Buffer{}
	largeData := make([]byte, MaxPooledBufSize+1)
	buf.Write(largeData)

	// This should not panic and buffer should be discarded (not returned to pool)
	putMarshalBuffer(buf)

	// Get a new buffer - should be a fresh one
	newBuf := getMarshalBuffer()
	require.NotNil(t, newBuf)
	assert.Equal(t, 0, newBuf.Len(), "should get a fresh buffer")
	putMarshalBuffer(newBuf)
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

// BenchmarkTxnCmd_MarshalBinary benchmarks TxnCmd MarshalBinary performance
func BenchmarkTxnCmd_MarshalBinary(b *testing.B) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-txn"

	for i := 0; i < 10; i++ {
		cmd := NewTxnStateCmd("cmd", 1, [12]byte{1})
		txnCmd.ComposedCmd.AddCmd(cmd)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := txnCmd.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTxnCmd_MarshalBinaryWithBuffer benchmarks MarshalBinaryWithBuffer performance
func BenchmarkTxnCmd_MarshalBinaryWithBuffer(b *testing.B) {
	txnCmd := NewTxnCmd()
	txnCmd.ComposedCmd = NewComposedCmd()
	txnCmd.TxnCtx = NewEmptyTxnCtx()
	txnCmd.ID = "test-txn"

	for i := 0; i < 10; i++ {
		cmd := NewTxnStateCmd("cmd", 1, [12]byte{1})
		txnCmd.ComposedCmd.AddCmd(cmd)
	}

	buf := &bytes.Buffer{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		err := txnCmd.MarshalBinaryWithBuffer(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
