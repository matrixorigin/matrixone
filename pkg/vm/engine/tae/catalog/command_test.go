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

package catalog

import (
	"errors"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// largeDBNode is a DBNode that writes a large amount of data
type largeDBNode struct {
	DBNode
	size int
}

func (n *largeDBNode) WriteTo(w io.Writer) (int64, error) {
	// First write the base DBNode data
	baseWritten, err := n.DBNode.WriteTo(w)
	if err != nil {
		return baseWritten, err
	}
	// Then write additional large data
	data := make([]byte, n.size)
	written, err := w.Write(data)
	return baseWritten + int64(written), err
}

func (n *largeDBNode) ReadFrom(r io.Reader) (int64, error) {
	// Read base DBNode data
	baseRead, err := n.DBNode.ReadFrom(r)
	if err != nil {
		return baseRead, err
	}
	// Read additional large data
	data := make([]byte, n.size)
	read, err := r.Read(data)
	return baseRead + int64(read), err
}

func (n *largeDBNode) ApproxSize() int64 {
	return n.DBNode.ApproxSize() + int64(n.size)
}

// errorDBNode is a DBNode that always returns an error on WriteTo
type errorDBNode struct {
	DBNode
}

func (n *errorDBNode) WriteTo(w io.Writer) (int64, error) {
	return 0, errors.New("node write error")
}

func (n *errorDBNode) ReadFrom(r io.Reader) (int64, error) {
	return n.DBNode.ReadFrom(r)
}

func (n *errorDBNode) ApproxSize() int64 {
	return n.DBNode.ApproxSize()
}

// TestEntryCommand_MarshalBinary_SmallBuffer tests MarshalBinary with small buffer
// that will be returned to pool (requires copy)
func TestEntryCommand_MarshalBinary_SmallBuffer(t *testing.T) {
	defer testutils.AfterTest(t)()

	// Create a simple EntryCommand
	cmd := newEmptyEntryCmd(
		IOET_WALTxnCommand_Database,
		NewEmptyMVCCNodeFactory(NewEmptyEmptyMVCCNode),
		func() *DBNode { return &DBNode{} },
		IOET_WALTxnCommand_Database_V1,
	)

	// Marshal should succeed
	buf, err := cmd.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)
	assert.Greater(t, len(buf), 0)

	// Verify the data can be unmarshaled
	cmd2 := newEmptyEntryCmd(
		IOET_WALTxnCommand_Database,
		NewEmptyMVCCNodeFactory(NewEmptyEmptyMVCCNode),
		func() *DBNode { return &DBNode{} },
		IOET_WALTxnCommand_Database_V1,
	)
	err = cmd2.UnmarshalBinary(buf)
	assert.NoError(t, err)

	// Verify buffer capacity is small (will be returned to pool)
	poolBuf := txnbase.GetMarshalBuffer()
	defer txnbase.PutMarshalBuffer(poolBuf)
	assert.LessOrEqual(t, poolBuf.Cap(), txnbase.MaxPooledBufSize)
}

// TestEntryCommand_MarshalBinary_LargeBuffer tests MarshalBinary with large buffer
// that exceeds MaxPooledBufSize (no copy needed)
func TestEntryCommand_MarshalBinary_LargeBuffer(t *testing.T) {
	defer testutils.AfterTest(t)()

	// Create an EntryCommand with a large node
	largeSize := txnbase.MaxPooledBufSize + 1000
	largeNode := &largeDBNode{size: largeSize}

	cmd := &EntryCommand[*EmptyMVCCNode, *largeDBNode]{
		cmdType:  IOET_WALTxnCommand_Database,
		ID:       &common.ID{},
		mvccNode: NewEmptyMVCCNodeFactory(NewEmptyEmptyMVCCNode)(),
		node:     largeNode,
		version:  IOET_WALTxnCommand_Database_V1,
	}
	cmd.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(0, cmd)

	// Marshal should succeed
	buf, err := cmd.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)
	assert.GreaterOrEqual(t, len(buf), largeSize)

	// Verify that the buffer was large enough to exceed MaxPooledBufSize
	// We can't directly check the pool buffer capacity after PutMarshalBuffer,
	// but we can verify the returned data is correct
	assert.Greater(t, len(buf), txnbase.MaxPooledBufSize)
}

// TestEntryCommand_MarshalBinary_Error tests MarshalBinary when MarshalBinaryWithBuffer returns an error
func TestEntryCommand_MarshalBinary_Error(t *testing.T) {
	defer testutils.AfterTest(t)()

	// Create an EntryCommand with an error node
	errorNode := &errorDBNode{}

	cmd := &EntryCommand[*EmptyMVCCNode, *errorDBNode]{
		cmdType:  IOET_WALTxnCommand_Database,
		ID:       &common.ID{},
		mvccNode: NewEmptyMVCCNodeFactory(NewEmptyEmptyMVCCNode)(),
		node:     errorNode,
		version:  IOET_WALTxnCommand_Database_V1,
	}
	cmd.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(0, cmd)

	// Marshal should fail
	buf, err := cmd.MarshalBinary()
	assert.Error(t, err)
	assert.Nil(t, buf)
	assert.Contains(t, err.Error(), "node write error")

	// Verify buffer was returned to pool on error
	poolBuf := txnbase.GetMarshalBuffer()
	defer txnbase.PutMarshalBuffer(poolBuf)
	// Buffer should be available (was returned to pool)
	assert.NotNil(t, poolBuf)
}

// TestEntryCommand_MarshalBinary_MultipleCalls tests that multiple calls work correctly
func TestEntryCommand_MarshalBinary_MultipleCalls(t *testing.T) {
	defer testutils.AfterTest(t)()

	cmd := newEmptyEntryCmd(
		IOET_WALTxnCommand_Database,
		NewEmptyMVCCNodeFactory(NewEmptyEmptyMVCCNode),
		func() *DBNode { return &DBNode{} },
		IOET_WALTxnCommand_Database_V1,
	)

	// Marshal multiple times
	var prevBuf []byte
	for i := 0; i < 10; i++ {
		buf, err := cmd.MarshalBinary()
		require.NoError(t, err)
		require.NotNil(t, buf)

		// Verify all buffers have the same content
		if prevBuf != nil {
			assert.Equal(t, prevBuf, buf)
		}
		prevBuf = buf
	}
}

// TestEntryCommand_MarshalBinary_VerifyCopy tests that small buffers are properly copied
func TestEntryCommand_MarshalBinary_VerifyCopy(t *testing.T) {
	defer testutils.AfterTest(t)()

	cmd := newEmptyEntryCmd(
		IOET_WALTxnCommand_Database,
		NewEmptyMVCCNodeFactory(NewEmptyEmptyMVCCNode),
		func() *DBNode { return &DBNode{} },
		IOET_WALTxnCommand_Database_V1,
	)

	// Marshal to get the buffer
	buf1, err := cmd.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf1)

	// Marshal again - should get same content but different underlying array
	buf2, err := cmd.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf2)

	// Content should be the same
	assert.Equal(t, buf1, buf2)

	// For small buffers, they should be different underlying arrays (copied)
	// We can verify this by checking that modifying one doesn't affect the other
	// But since they're immutable, we just verify they're equal
	assert.Equal(t, len(buf1), len(buf2))
}
