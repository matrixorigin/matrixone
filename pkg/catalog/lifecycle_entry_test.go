// Copyright 2026 Matrix Origin
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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func TestLifecycleRestoreChunkCatalogHasNoRedundantAutoIncrementState(t *testing.T) {
	require.NotContains(
		t,
		strings.ToLower(MoLifecycleRestoreChunksDDL),
		"auto_increment_maxima_blob",
	)
}

func TestLifecycleTerminalCleanupQueriesHaveCatalogIndexes(t *testing.T) {
	bindingDDL := strings.ToLower(MoLifecycleBindingsDDL)
	datasetDDL := strings.ToLower(MoLifecycleDatasetsDDL)
	restoreDDL := strings.ToLower(MoLifecycleRestoreAttemptsDDL)
	rootDDL := strings.ToLower(MoLifecycleCleanupRootsDDL)
	require.Contains(t, bindingDDL,
		"idx_lifecycle_binding_schedule (state, binding_id)")
	require.Contains(t, datasetDDL,
		"idx_lifecycle_dataset_terminal (state, updated_at, dataset_id)")
	require.Contains(t, restoreDDL,
		"idx_lifecycle_restore_terminal (state, updated_at, restore_id)")
	require.Contains(t, rootDDL,
		"idx_lifecycle_cleanup_temporary")
	require.Contains(t, rootDDL,
		"(state, temporary_cleanup_done, updated_at, root_id)")
	require.Contains(t, rootDDL,
		"idx_lifecycle_cleanup_terminal (state, updated_at, root_id)")
}

func TestLifecycleRestoreStagingTableNameIsReservedCaseInsensitively(t *testing.T) {
	require.True(t, IsLifecycleRestoreStagingTable(
		"__mo_lifecycle_restore_0123456789abcdef0123456789abcdef",
	))
	require.True(t, IsLifecycleRestoreStagingTable(
		"__MO_LIFECYCLE_RESTORE_0123456789ABCDEF0123456789ABCDEF",
	))
	require.False(t, IsLifecycleRestoreStagingTable("__mo_lifecycle_restore"))
	require.False(t, IsLifecycleRestoreStagingTable("__mo_lifecycle_restore_user"))
	require.False(t, IsLifecycleRestoreStagingTable(
		"__mo_lifecycle_restore_0123456789abcdef0123456789abcdeg",
	))
	require.False(t, IsLifecycleRestoreStagingTable("user_restore_1"))
}

func TestParseEntryListRejectsUnknownEntryBeforeBatch(t *testing.T) {
	require.NotPanics(t, func() {
		_, remaining, err := ParseEntryList([]*api.Entry{{
			EntryType: api.Entry_EntryType(99),
			Bat:       nil,
		}})
		require.Error(t, err)
		require.Empty(t, remaining)
	})
}

func TestParseEntryListReturnsLifecycleControlWithoutBatch(t *testing.T) {
	control := &api.LifecycleCommitEntry{
		ProtocolVersion: 1,
		RootId:          "root",
		AttemptId:       "attempt",
	}
	entry, remaining, err := ParseEntryList([]*api.Entry{{
		EntryType:       api.Entry_LifecycleCommit,
		LifecycleCommit: control,
	}})
	require.NoError(t, err)
	require.Empty(t, remaining)
	require.Same(t, control, entry)
}
