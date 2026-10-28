// Copyright 2026 MatrixOne
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

package client

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

// The D-3 execution-read carrier changes ONLY readTS resolution: the wrapped
// operator's snapshot, its txn metadata, the named-snapshot cohort answer and
// the workspace identity stay untouched, so a relation bound to the carrier
// reads committed state at F while seeing the transaction's own writes
// through the same workspace.
func TestApplyExecutionReadTS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	op, closeFn := NewTestTxnOperator(ctx)
	defer closeFn()

	require.False(t, op.IsSnapOp())
	sessionTS := op.SnapshotTS()
	frontier := timestamp.Timestamp{
		PhysicalTime: sessionTS.PhysicalTime + 12345,
		LogicalTime:  1,
	}

	carrier := ApplyExecutionReadTS(op, frontier)
	require.False(t, carrier.IsSnapOp(), "the carrier never joins the snapshot cohort")

	// readTS resolution: the carrier returns the frontier.
	require.Equal(t, frontier, carrier.SnapshotTS())

	// The wrapped operator is unchanged: the transaction snapshot is never
	// advanced by a locking current read (design G2).
	require.Equal(t, sessionTS, op.SnapshotTS())
	// Raw txn metadata still reports the session snapshot: identity, commit
	// timestamps and lock ownership observe no mutation.
	require.Equal(t, sessionTS, carrier.Txn().SnapshotTS)
	// Workspace identity delegates: same workspace pointer, so own writes,
	// deletedBlocks and the flushed tombstone registry are the SAME objects.
	require.Equal(t, op.GetWorkspace(), carrier.GetWorkspace())

	// A later execution builds its own carrier; the earlier one keeps its
	// frontier (per-execution immutability, design D-2/D-8).
	frontier2 := timestamp.Timestamp{
		PhysicalTime: sessionTS.PhysicalTime + 54321,
		LogicalTime:  2,
	}
	carrier2 := ApplyExecutionReadTS(op, frontier2)
	require.Equal(t, frontier, carrier.SnapshotTS())
	require.Equal(t, frontier2, carrier2.SnapshotTS())
	require.NotSame(t, carrier, carrier2)
}
