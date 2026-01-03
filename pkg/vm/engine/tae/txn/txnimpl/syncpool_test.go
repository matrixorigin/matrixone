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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAppendCmd_MarshalBinary_WithSyncPool tests AppendCmd.MarshalBinary uses sync.Pool
func TestAppendCmd_MarshalBinary_WithSyncPool(t *testing.T) {
	// Create a simple batch
	bat := containers.NewBatch()
	defer bat.Close()

	// Create AppendCmd
	cmd := NewEmptyAppendCmd()
	cmd.Data = bat
	cmd.Ts = types.BuildTS(1, 1)
	cmd.IsTombstone = false

	// Marshal multiple times
	for i := 0; i < 10; i++ {
		buf, err := cmd.MarshalBinary()
		require.NoError(t, err)
		require.NotNil(t, buf)
		assert.Greater(t, len(buf), 0)
	}
}

// TestAppendCmd_MarshalBinaryWithBuffer tests AppendCmd.MarshalBinaryWithBuffer
func TestAppendCmd_MarshalBinaryWithBuffer(t *testing.T) {
	// Create a simple batch
	bat := containers.NewBatch()
	defer bat.Close()

	// Create AppendCmd
	cmd := NewEmptyAppendCmd()
	cmd.Data = bat
	cmd.Ts = types.BuildTS(1, 1)
	cmd.IsTombstone = false

	// Get buffer from pool
	buf := txnbase.GetMarshalBuffer()
	defer txnbase.PutMarshalBuffer(buf)

	// Marshal using buffer
	data, err := cmd.MarshalBinaryWithBuffer(buf)
	require.NoError(t, err)
	require.NotNil(t, data)
	assert.Greater(t, len(data), 0)
}
