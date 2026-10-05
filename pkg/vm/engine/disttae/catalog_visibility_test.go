// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
)

func TestSnapshotCheckpointPreservesSourceVersion(t *testing.T) {
	for _, version := range []uint32{logtail.CheckpointVersion12, logtail.CheckpointVersion13} {
		start := types.TS{}.ToTimestamp()
		end := types.BuildTS(10, 0).ToTimestamp()
		response := &cmd_util.SnapshotReadResp{
			Succeed: true,
			Entries: []*cmd_util.CheckpointEntryResp{{
				Start: &start, End: &end, EntryType: int32(checkpoint.ET_Global), Version: version,
			}},
		}
		entries, _, _, err := parseSnapshotCheckpointEntries(response)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, version, entries[0].GetVersion())
	}
}

func TestCatalogVisibilityRejectsActiveMutationAndABA(t *testing.T) {
	txn := &Transaction{}
	initial, stable := txn.CatalogVisibility()
	require.True(t, stable)
	txn.beginCatalogMutation()
	_, stable = txn.CatalogVisibility()
	require.False(t, stable)
	txn.beginCatalogMutation()
	txn.endCatalogMutation()
	_, stable = txn.CatalogVisibility()
	require.False(t, stable)
	txn.endCatalogMutation()
	next, stable := txn.CatalogVisibility()
	require.True(t, stable)
	require.Greater(t, next, initial)
	txn.UpdateSnapshotWriteOffset()
	after, _ := txn.CatalogVisibility()
	require.Greater(t, after, next)
}

func TestCatalogVisibilityIgnoresReadOnlyWorkspaceAdjustment(t *testing.T) {
	txn := &Transaction{statementID: 1, writes: []Entry{{typ: INSERT, tableId: 42}, {typ: DELETE, tableId: 42}}}
	before, _ := txn.CatalogVisibility()
	require.NoError(t, txn.adjustUpdateOrderLocked(0))
	changed, stable := txn.CatalogVisibility()
	require.True(t, stable)
	require.Greater(t, changed, before)
	require.Equal(t, DELETE, txn.writes[0].typ)
	require.NoError(t, txn.adjustUpdateOrderLocked(0))
	after, stable := txn.CatalogVisibility()
	require.True(t, stable)
	require.Equal(t, changed, after, "a derived read must not invalidate its own schema request")
	txn.writes = nil
	require.NoError(t, txn.adjustUpdateOrderLocked(0))
	empty, _ := txn.CatalogVisibility()
	require.Equal(t, after, empty)
}

func TestCatalogVisibilityIgnoresNoopWorkspaceDump(t *testing.T) {
	txn := &Transaction{writeWorkspaceThreshold: 1024, commitWorkspaceThreshold: 1024, engine: &Engine{}}
	txn.engine.config.insertEntryMaxCount = 100
	before, _ := txn.CatalogVisibility()
	for _, offset := range []int{0, -1} {
		require.NoError(t, txn.dumpBatchLocked(context.Background(), offset))
		after, stable := txn.CatalogVisibility()
		require.True(t, stable)
		require.Equal(t, before, after)
	}
}
