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

package txnimpl

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComposedCmd(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	composed := txnbase.NewComposedCmd()
	defer composed.Close()

	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", "", "", nil)
	dbCmd, err := db.MakeCommand(1)
	assert.Nil(t, err)
	composed.AddCmd(dbCmd)

	table, _ := db.CreateTableEntry(schema, nil, nil)
	tblCmd, err := table.MakeCommand(1)
	assert.Nil(t, err)
	composed.AddCmd(tblCmd)

	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	objCmd, err := obj.MakeCommand(1)
	assert.Nil(t, err)
	composed.AddCmd(objCmd)

	// TODO

	// objMvcc := updates.NewObjectMVCCHandle(obj)

	// controller := updates.NewMVCCHandle(objMvcc, 0)
	// ts := types.NextGlobalTsForTest()

	// appenderMvcc := updates.NewAppendMVCCHandle(obj)

	// node := updates.MockAppendNode(ts, 0, 2515, appenderMvcc)
	// cmd := updates.NewAppendCmd(1, node)

	// composed.AddCmd(cmd)

	// del := updates.NewDeleteNode(nil, handle.DT_Normal,
	// 	updates.IOET_WALTxnCommand_DeleteNode_V2)
	// del.AttachTo(controller.GetDeleteChain())
	// cmd2, err := del.MakeCommand(1)
	// assert.Nil(t, err)
	// composed.AddCmd(cmd2)

	// buf, err := composed.MarshalBinary()
	// assert.Nil(t, err)
	// _, err = txnbase.BuildCommandFrom(buf)
	// assert.Nil(t, err)
}

// TestAppendCmd_MarshalBinary_BufferReuse tests that AppendCmd.MarshalBinary reuses marshalBuf
func TestAppendCmd_MarshalBinary_BufferReuse(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	cmd := NewEmptyAppendCmd()
	defer cmd.Close()

	// Set up minimal data for testing
	cmd.Data = containers.NewBatch()
	cmd.ID = 1
	cmd.Infos = []*appendInfo{}
	cmd.Ts = types.BuildTS(1, 1)
	cmd.IsTombstone = false

	// First marshal - should allocate buffer
	buf1, err := cmd.MarshalBinary()
	require.NoError(t, err)
	assert.NotNil(t, buf1)
	assert.NotNil(t, cmd.marshalBuf, "marshalBuf should be allocated")
	firstCap := cap(cmd.marshalBuf)

	// Second marshal - should reuse buffer
	buf2, err := cmd.MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, buf1, buf2, "marshaled data should be identical")
	assert.Equal(t, firstCap, cap(cmd.marshalBuf), "buffer capacity should be reused")
}

// TestAppendCmd_MarshalBinary_UsesApproxSize tests that AppendCmd.MarshalBinary uses ApproxSize for capacity estimation
func TestAppendCmd_MarshalBinary_UsesApproxSize(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	cmd := NewEmptyAppendCmd()
	defer cmd.Close()

	cmd.Data = containers.NewBatch()
	cmd.ID = 1
	cmd.Infos = []*appendInfo{}
	cmd.Ts = types.BuildTS(1, 1)
	cmd.IsTombstone = false

	// Get estimated size
	estimatedSize := int(cmd.ApproxSize())
	if estimatedSize < 256 {
		estimatedSize = 256
	}

	// Marshal
	_, err := cmd.MarshalBinary()
	require.NoError(t, err)

	// Buffer capacity should be at least the estimated size (or minimum 256)
	assert.GreaterOrEqual(t, cap(cmd.marshalBuf), estimatedSize,
		"buffer capacity should be at least the estimated size")
}

// TestAppendCmd_MarshalBinary_CapacityLimit tests that AppendCmd discards oversized buffers
func TestAppendCmd_MarshalBinary_CapacityLimit(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	cmd := NewEmptyAppendCmd()
	defer cmd.Close()

	cmd.Data = containers.NewBatch()
	cmd.ID = 1
	cmd.Infos = []*appendInfo{}
	cmd.Ts = types.BuildTS(1, 1)
	cmd.IsTombstone = false

	// Manually set a buffer exceeding MaxAppendCmdBufSize
	cmd.marshalBuf = make([]byte, 0, MaxAppendCmdBufSize+1)

	// Marshal should discard the oversized buffer
	buf, err := cmd.MarshalBinary()
	require.NoError(t, err)
	assert.NotNil(t, buf)
	assert.LessOrEqual(t, cap(cmd.marshalBuf), MaxAppendCmdBufSize,
		"oversized buffer should be discarded")
}

// TestAppendCmd_Close_ClearsMarshalBuf tests that AppendCmd.Close clears marshalBuf
func TestAppendCmd_Close_ClearsMarshalBuf(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	cmd := NewEmptyAppendCmd()

	cmd.Data = containers.NewBatch()
	cmd.ID = 1
	cmd.Infos = []*appendInfo{}
	cmd.Ts = types.BuildTS(1, 1)
	cmd.IsTombstone = false

	// Marshal to allocate buffer
	buf, err := cmd.MarshalBinary()
	require.NoError(t, err)
	assert.NotNil(t, cmd.marshalBuf, "marshalBuf should be allocated")

	// Close should clear buffer
	cmd.Close()
	assert.Nil(t, cmd.marshalBuf, "marshalBuf should be cleared after Close")
	assert.Nil(t, cmd.Data, "Data should be cleared after Close")

	// Verify buffer is not accessible after close
	assert.NotNil(t, buf, "buffer should have been created before close")
}

// TestAppendCmd_MarshalBinary_AfterClose tests MarshalBinary after Close
func TestAppendCmd_MarshalBinary_AfterClose(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	cmd := NewEmptyAppendCmd()
	defer cmd.Close()

	cmd.Data = containers.NewBatch()
	cmd.ID = 1
	cmd.Infos = []*appendInfo{}
	cmd.Ts = types.BuildTS(1, 1)
	cmd.IsTombstone = false

	// Marshal to allocate buffer
	buf1, err := cmd.MarshalBinary()
	require.NoError(t, err)
	assert.NotNil(t, cmd.marshalBuf, "marshalBuf should be allocated")

	// Close should clear buffer
	cmd.Close()
	assert.Nil(t, cmd.marshalBuf, "marshalBuf should be cleared after Close")

	// Re-create Data for next marshal
	cmd.Data = containers.NewBatch()

	// Marshal again - should allocate new buffer and work correctly
	buf2, err := cmd.MarshalBinary()
	require.NoError(t, err, "should marshal successfully after Close")
	assert.NotNil(t, buf2, "buffer should not be nil")
	assert.NotNil(t, cmd.marshalBuf, "marshalBuf should be allocated again")
	assert.Equal(t, buf1, buf2, "marshaled data should be identical")
}

// TestAppendCmd_MarshalBinary_WriteToConsistency tests consistency between MarshalBinary and WriteTo
func TestAppendCmd_MarshalBinary_WriteToConsistency(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	cmd := NewEmptyAppendCmd()
	defer cmd.Close()

	cmd.Data = containers.NewBatch()
	cmd.ID = 1
	cmd.Infos = []*appendInfo{}
	cmd.Ts = types.BuildTS(1, 1)
	cmd.IsTombstone = false

	// Marshal using MarshalBinary
	buf1, err := cmd.MarshalBinary()
	require.NoError(t, err)

	// Marshal using WriteTo
	var buf2 bytes.Buffer
	_, err = cmd.WriteTo(&buf2)
	require.NoError(t, err)

	// Results should be identical
	assert.Equal(t, buf1, buf2.Bytes(), "MarshalBinary and WriteTo should produce identical output")
}
