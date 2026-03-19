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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// CommandType represents the type of command sent from producer to consumer
type CommandType int

const (
	// CmdBegin starts a new transaction
	CmdBegin CommandType = iota
	// CmdCommit commits the current transaction
	CmdCommit
	// CmdRollback rolls back the current transaction
	CmdRollback
	// CmdInsertBatch inserts a batch of rows (snapshot data)
	CmdInsertBatch
	// CmdInsertDeleteBatch inserts and deletes rows (tail data)
	CmdInsertDeleteBatch
	// CmdFlush flushes any buffered SQL statements
	CmdFlush
	// CmdDummy is a synchronization point (no-op)
	CmdDummy
)

// String returns the string representation of CommandType
func (ct CommandType) String() string {
	switch ct {
	case CmdBegin:
		return "BEGIN"
	case CmdCommit:
		return "COMMIT"
	case CmdRollback:
		return "ROLLBACK"
	case CmdInsertBatch:
		return "INSERT_BATCH"
	case CmdInsertDeleteBatch:
		return "INSERT_DELETE_BATCH"
	case CmdFlush:
		return "FLUSH"
	case CmdDummy:
		return "DUMMY"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", ct)
	}
}

// Command represents a command sent from producer to consumer
type Command struct {
	// Type of the command
	Type CommandType

	// Data for insert operations (snapshot)
	InsertBatch *batch.Batch

	// Data for insert/delete operations (tail)
	InsertAtmBatch *AtomicBatch
	DeleteAtmBatch *AtomicBatch

	// Metadata for the command
	Meta CommandMetadata
}

// CommandMetadata contains metadata for a command
type CommandMetadata struct {
	// Watermark range for this data
	FromTs types.TS
	ToTs   types.TS

	// Whether this is the last batch (no more data)
	NoMoreData bool
}

// NewBeginCommand creates a BEGIN transaction command
func NewBeginCommand() *Command {
	return &Command{
		Type: CmdBegin,
	}
}

// NewCommitCommand creates a COMMIT transaction command
func NewCommitCommand() *Command {
	return &Command{
		Type: CmdCommit,
	}
}

// NewRollbackCommand creates a ROLLBACK transaction command
func NewRollbackCommand() *Command {
	return &Command{
		Type: CmdRollback,
	}
}

// NewInsertBatchCommand creates a command to insert a batch of rows
func NewInsertBatchCommand(bat *batch.Batch, fromTs, toTs types.TS) *Command {
	return &Command{
		Type:        CmdInsertBatch,
		InsertBatch: bat,
		Meta: CommandMetadata{
			FromTs: fromTs,
			ToTs:   toTs,
		},
	}
}

// NewInsertDeleteBatchCommand creates a command to insert and delete rows
func NewInsertDeleteBatchCommand(insertBatch, deleteBatch *AtomicBatch, fromTs, toTs types.TS) *Command {
	return &Command{
		Type:           CmdInsertDeleteBatch,
		InsertAtmBatch: insertBatch,
		DeleteAtmBatch: deleteBatch,
		Meta: CommandMetadata{
			FromTs: fromTs,
			ToTs:   toTs,
		},
	}
}

// NewFlushCommand creates a command to flush buffered SQL
func NewFlushCommand(noMoreData bool, fromTs, toTs types.TS) *Command {
	return &Command{
		Type: CmdFlush,
		Meta: CommandMetadata{
			FromTs:     fromTs,
			ToTs:       toTs,
			NoMoreData: noMoreData,
		},
	}
}

// NewDummyCommand creates a dummy command for synchronization
func NewDummyCommand() *Command {
	return &Command{
		Type: CmdDummy,
	}
}

// String returns a string representation of the command (for debugging)
func (c *Command) String() string {
	if c == nil {
		return "<nil>"
	}

	switch c.Type {
	case CmdBegin, CmdCommit, CmdRollback, CmdDummy:
		return c.Type.String()
	case CmdInsertBatch:
		rows := 0
		if c.InsertBatch != nil {
			rows = c.InsertBatch.RowCount()
		}
		return fmt.Sprintf("%s(rows=%d, range=[%s, %s))",
			c.Type.String(), rows, c.Meta.FromTs.ToString(), c.Meta.ToTs.ToString())
	case CmdInsertDeleteBatch:
		insRows, delRows := 0, 0
		if c.InsertAtmBatch != nil {
			insRows = c.InsertAtmBatch.RowCount()
		}
		if c.DeleteAtmBatch != nil {
			delRows = c.DeleteAtmBatch.RowCount()
		}
		return fmt.Sprintf("%s(insert=%d, delete=%d, range=[%s, %s))",
			c.Type.String(), insRows, delRows, c.Meta.FromTs.ToString(), c.Meta.ToTs.ToString())
	case CmdFlush:
		return fmt.Sprintf("%s(noMoreData=%v, range=[%s, %s))",
			c.Type.String(), c.Meta.NoMoreData, c.Meta.FromTs.ToString(), c.Meta.ToTs.ToString())
	default:
		return c.Type.String()
	}
}

// Validate checks if the command is valid
func (c *Command) Validate() error {
	if c == nil {
		return moerr.NewInternalErrorNoCtx("command is nil")
	}

	switch c.Type {
	case CmdBegin, CmdCommit, CmdRollback, CmdDummy:
		// No data required for these commands
		return nil

	case CmdInsertBatch:
		if c.InsertBatch == nil {
			return moerr.NewInternalErrorNoCtx("InsertBatch is required for CmdInsertBatch")
		}
		if c.InsertBatch.RowCount() == 0 {
			return moerr.NewInternalErrorNoCtx("InsertBatch has no rows")
		}
		return nil

	case CmdInsertDeleteBatch:
		if c.InsertAtmBatch == nil && c.DeleteAtmBatch == nil {
			return moerr.NewInternalErrorNoCtx("at least one of InsertAtmBatch or DeleteAtmBatch is required")
		}
		return nil

	case CmdFlush:
		// Flush is always valid
		return nil

	default:
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("unknown command type: %v", c.Type))
	}
}
