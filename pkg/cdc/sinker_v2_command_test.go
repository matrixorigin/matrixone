// Copyright 2024 Matrix Origin
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

package cdc

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommandType_String(t *testing.T) {
	tests := []struct {
		name     string
		cmdType  CommandType
		expected string
	}{
		{"Begin", CmdBegin, "BEGIN"},
		{"Commit", CmdCommit, "COMMIT"},
		{"Rollback", CmdRollback, "ROLLBACK"},
		{"InsertBatch", CmdInsertBatch, "INSERT_BATCH"},
		{"InsertDeleteBatch", CmdInsertDeleteBatch, "INSERT_DELETE_BATCH"},
		{"Flush", CmdFlush, "FLUSH"},
		{"Dummy", CmdDummy, "DUMMY"},
		{"Unknown", CommandType(999), "UNKNOWN(999)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.cmdType.String())
		})
	}
}

func TestNewBeginCommand(t *testing.T) {
	cmd := NewBeginCommand()

	require.NotNil(t, cmd)
	assert.Equal(t, CmdBegin, cmd.Type)
	assert.Nil(t, cmd.InsertBatch)
	assert.Nil(t, cmd.InsertAtmBatch)
	assert.Nil(t, cmd.DeleteAtmBatch)
}

func TestNewCommitCommand(t *testing.T) {
	cmd := NewCommitCommand()

	require.NotNil(t, cmd)
	assert.Equal(t, CmdCommit, cmd.Type)
}

func TestNewRollbackCommand(t *testing.T) {
	cmd := NewRollbackCommand()

	require.NotNil(t, cmd)
	assert.Equal(t, CmdRollback, cmd.Type)
}

func TestNewInsertBatchCommand(t *testing.T) {
	// Create a simple batch with 2 rows
	bat := &batch.Batch{
		Vecs: []*vector.Vector{
			vector.NewVec(types.T_int32.ToType()),
		},
	}
	fromTs := types.BuildTS(100, 0)
	toTs := types.BuildTS(200, 0)

	cmd := NewInsertBatchCommand(bat, fromTs, toTs)

	require.NotNil(t, cmd)
	assert.Equal(t, CmdInsertBatch, cmd.Type)
	assert.Equal(t, bat, cmd.InsertBatch)
	assert.Equal(t, fromTs, cmd.Meta.FromTs)
	assert.Equal(t, toTs, cmd.Meta.ToTs)
	assert.False(t, cmd.Meta.NoMoreData)
}

func TestNewInsertDeleteBatchCommand(t *testing.T) {
	// Create mock atomic batches
	insertBatch := NewAtomicBatch(nil)
	deleteBatch := NewAtomicBatch(nil)
	fromTs := types.BuildTS(100, 0)
	toTs := types.BuildTS(200, 0)

	cmd := NewInsertDeleteBatchCommand(insertBatch, deleteBatch, fromTs, toTs)

	require.NotNil(t, cmd)
	assert.Equal(t, CmdInsertDeleteBatch, cmd.Type)
	assert.Equal(t, insertBatch, cmd.InsertAtmBatch)
	assert.Equal(t, deleteBatch, cmd.DeleteAtmBatch)
	assert.Equal(t, fromTs, cmd.Meta.FromTs)
	assert.Equal(t, toTs, cmd.Meta.ToTs)
}

func TestNewFlushCommand(t *testing.T) {
	fromTs := types.BuildTS(100, 0)
	toTs := types.BuildTS(200, 0)

	t.Run("WithNoMoreData", func(t *testing.T) {
		cmd := NewFlushCommand(true, fromTs, toTs)

		require.NotNil(t, cmd)
		assert.Equal(t, CmdFlush, cmd.Type)
		assert.True(t, cmd.Meta.NoMoreData)
		assert.Equal(t, fromTs, cmd.Meta.FromTs)
		assert.Equal(t, toTs, cmd.Meta.ToTs)
	})

	t.Run("WithoutNoMoreData", func(t *testing.T) {
		cmd := NewFlushCommand(false, fromTs, toTs)

		require.NotNil(t, cmd)
		assert.Equal(t, CmdFlush, cmd.Type)
		assert.False(t, cmd.Meta.NoMoreData)
	})
}

func TestNewDummyCommand(t *testing.T) {
	cmd := NewDummyCommand()

	require.NotNil(t, cmd)
	assert.Equal(t, CmdDummy, cmd.Type)
}

func TestCommand_String(t *testing.T) {
	t.Run("NilCommand", func(t *testing.T) {
		var cmd *Command
		assert.Equal(t, "<nil>", cmd.String())
	})

	t.Run("BeginCommand", func(t *testing.T) {
		cmd := NewBeginCommand()
		assert.Equal(t, "BEGIN", cmd.String())
	})

	t.Run("CommitCommand", func(t *testing.T) {
		cmd := NewCommitCommand()
		assert.Equal(t, "COMMIT", cmd.String())
	})

	t.Run("RollbackCommand", func(t *testing.T) {
		cmd := NewRollbackCommand()
		assert.Equal(t, "ROLLBACK", cmd.String())
	})

	t.Run("DummyCommand", func(t *testing.T) {
		cmd := NewDummyCommand()
		assert.Equal(t, "DUMMY", cmd.String())
	})

	t.Run("InsertBatchCommand", func(t *testing.T) {
		// Create batch without actually appending data to avoid mpool requirement
		bat := &batch.Batch{
			Vecs: []*vector.Vector{
				vector.NewVec(types.T_int32.ToType()),
			},
		}
		bat.SetRowCount(2) // Simulate 2 rows

		fromTs := types.BuildTS(100, 0)
		toTs := types.BuildTS(200, 0)
		cmd := NewInsertBatchCommand(bat, fromTs, toTs)

		str := cmd.String()
		assert.Contains(t, str, "INSERT_BATCH")
		assert.Contains(t, str, "rows=2")
		assert.Contains(t, str, "100-0")
		assert.Contains(t, str, "200-0")
	})

	t.Run("FlushCommand", func(t *testing.T) {
		fromTs := types.BuildTS(100, 0)
		toTs := types.BuildTS(200, 0)
		cmd := NewFlushCommand(true, fromTs, toTs)

		str := cmd.String()
		assert.Contains(t, str, "FLUSH")
		assert.Contains(t, str, "noMoreData=true")
	})
}

func TestCommand_Validate(t *testing.T) {
	t.Run("NilCommand", func(t *testing.T) {
		var cmd *Command
		err := cmd.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})

	t.Run("ValidBeginCommand", func(t *testing.T) {
		cmd := NewBeginCommand()
		err := cmd.Validate()
		assert.NoError(t, err)
	})

	t.Run("ValidCommitCommand", func(t *testing.T) {
		cmd := NewCommitCommand()
		err := cmd.Validate()
		assert.NoError(t, err)
	})

	t.Run("ValidRollbackCommand", func(t *testing.T) {
		cmd := NewRollbackCommand()
		err := cmd.Validate()
		assert.NoError(t, err)
	})

	t.Run("ValidDummyCommand", func(t *testing.T) {
		cmd := NewDummyCommand()
		err := cmd.Validate()
		assert.NoError(t, err)
	})

	t.Run("InvalidInsertBatchCommand_NilBatch", func(t *testing.T) {
		cmd := &Command{
			Type:        CmdInsertBatch,
			InsertBatch: nil,
		}
		err := cmd.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "InsertBatch is required")
	})

	t.Run("InvalidInsertBatchCommand_EmptyBatch", func(t *testing.T) {
		bat := &batch.Batch{
			Vecs: []*vector.Vector{
				vector.NewVec(types.T_int32.ToType()),
			},
		}
		cmd := &Command{
			Type:        CmdInsertBatch,
			InsertBatch: bat,
		}
		err := cmd.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no rows")
	})

	t.Run("ValidInsertBatchCommand", func(t *testing.T) {
		bat := &batch.Batch{
			Vecs: []*vector.Vector{
				vector.NewVec(types.T_int32.ToType()),
			},
		}
		bat.SetRowCount(1) // Simulate 1 row

		cmd := &Command{
			Type:        CmdInsertBatch,
			InsertBatch: bat,
		}
		err := cmd.Validate()
		assert.NoError(t, err)
	})

	t.Run("InvalidInsertDeleteBatchCommand_NoBatches", func(t *testing.T) {
		cmd := &Command{
			Type:           CmdInsertDeleteBatch,
			InsertAtmBatch: nil,
			DeleteAtmBatch: nil,
		}
		err := cmd.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "at least one")
	})

	t.Run("ValidInsertDeleteBatchCommand_OnlyInsert", func(t *testing.T) {
		insertBatch := NewAtomicBatch(nil)
		cmd := &Command{
			Type:           CmdInsertDeleteBatch,
			InsertAtmBatch: insertBatch,
			DeleteAtmBatch: nil,
		}
		err := cmd.Validate()
		assert.NoError(t, err)
	})

	t.Run("ValidInsertDeleteBatchCommand_OnlyDelete", func(t *testing.T) {
		deleteBatch := NewAtomicBatch(nil)
		cmd := &Command{
			Type:           CmdInsertDeleteBatch,
			InsertAtmBatch: nil,
			DeleteAtmBatch: deleteBatch,
		}
		err := cmd.Validate()
		assert.NoError(t, err)
	})

	t.Run("ValidFlushCommand", func(t *testing.T) {
		cmd := NewFlushCommand(true, types.BuildTS(100, 0), types.BuildTS(200, 0))
		err := cmd.Validate()
		assert.NoError(t, err)
	})

	t.Run("UnknownCommandType", func(t *testing.T) {
		cmd := &Command{
			Type: CommandType(999),
		}
		err := cmd.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown command type")
	})
}
