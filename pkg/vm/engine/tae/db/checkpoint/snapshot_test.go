// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoint

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
)

// TestFilterEntriesByTimestamp tests the filterEntriesByTimestamp function
func TestFilterEntriesByTimestamp(t *testing.T) {
	t.Run("BasicFiltering", func(t *testing.T) {
		// Create mock checkpoint entries with different timestamps
		entries := []*CheckpointEntry{
			createMockCheckpointEntry(100, 200),
			createMockCheckpointEntry(200, 300),
			createMockCheckpointEntry(300, 400),
			createMockCheckpointEntry(150, 250), // This should be filtered out
		}

		// Test filtering for file with timestamps 200-300
		fileStart := types.BuildTS(200, 0)
		fileEnd := types.BuildTS(300, 0)

		filteredEntries := filterEntriesByTimestamp(entries, &fileStart, &fileEnd)

		// Should only match the entry with timestamps 200-300
		assert.Equal(t, 1, len(filteredEntries))
		assert.True(t, filteredEntries[0].start.EQ(&fileStart))
		assert.True(t, filteredEntries[0].end.EQ(&fileEnd))
	})

	t.Run("EmptyEntries", func(t *testing.T) {
		entries := []*CheckpointEntry{}
		fileStart := types.BuildTS(100, 0)
		fileEnd := types.BuildTS(200, 0)

		filteredEntries := filterEntriesByTimestamp(entries, &fileStart, &fileEnd)
		assert.Equal(t, 0, len(filteredEntries))
	})

	t.Run("NoMatchingEntries", func(t *testing.T) {
		entries := []*CheckpointEntry{
			createMockCheckpointEntry(300, 400),
			createMockCheckpointEntry(500, 600),
		}
		fileStart := types.BuildTS(100, 0)
		fileEnd := types.BuildTS(200, 0)

		filteredEntries := filterEntriesByTimestamp(entries, &fileStart, &fileEnd)
		assert.Equal(t, 0, len(filteredEntries))
	})

	t.Run("MultipleMatchingEntries", func(t *testing.T) {
		entries := []*CheckpointEntry{
			createMockCheckpointEntry(200, 300),
			createMockCheckpointEntry(200, 300), // Duplicate
			createMockCheckpointEntry(100, 200), // Should be filtered out
		}
		fileStart := types.BuildTS(200, 0)
		fileEnd := types.BuildTS(300, 0)

		filteredEntries := filterEntriesByTimestamp(entries, &fileStart, &fileEnd)
		assert.Equal(t, 2, len(filteredEntries))
		for _, entry := range filteredEntries {
			assert.True(t, entry.start.EQ(&fileStart))
			assert.True(t, entry.end.EQ(&fileEnd))
		}
	})

	t.Run("PreventExpiredCheckpointReading", func(t *testing.T) {
		// This test verifies that the filtering prevents reading expired checkpoints
		// that would cause "is not found" errors
		allEntries := []*CheckpointEntry{
			createMockCheckpointEntry(100, 200), // Expired - should be filtered out
			createMockCheckpointEntry(200, 300), // Current - should be kept
			createMockCheckpointEntry(300, 400), // Future - should be filtered out
		}

		// File represents range 200-300
		fileStart := types.BuildTS(200, 0)
		fileEnd := types.BuildTS(300, 0)

		filteredEntries := filterEntriesByTimestamp(allEntries, &fileStart, &fileEnd)

		// Should only return the entry that matches the file's timestamp range
		assert.Equal(t, 1, len(filteredEntries))
		assert.Equal(t, int64(200), filteredEntries[0].start.Physical())
		assert.Equal(t, int64(300), filteredEntries[0].end.Physical())

		// Verify that expired entries are not included
		for _, entry := range filteredEntries {
			assert.NotEqual(t, int64(100), entry.start.Physical(), "Should not include expired checkpoint 100-200")
			assert.NotEqual(t, int64(300), entry.start.Physical(), "Should not include future checkpoint 300-400")
		}
	})
}

// TestFilterSnapshotEntries tests the filterSnapshotEntries function
func TestFilterSnapshotEntries(t *testing.T) {
	t.Run("BasicFiltering", func(t *testing.T) {
		// Create mock entries with different types and timestamps
		entries := []*CheckpointEntry{
			{
				start:     types.BuildTS(100, 0),
				end:       types.BuildTS(200, 0),
				entryType: ET_Incremental,
			},
			{
				start:     types.BuildTS(200, 0),
				end:       types.BuildTS(300, 0),
				entryType: ET_Global,
			},
			{
				start:     types.BuildTS(300, 0),
				end:       types.BuildTS(400, 0),
				entryType: ET_Incremental,
			},
		}

		result := filterSnapshotEntries(entries)

		// Should return entries from the global checkpoint onwards
		assert.Equal(t, 2, len(result))
		assert.Equal(t, ET_Global, result[0].entryType)
		assert.Equal(t, ET_Incremental, result[1].entryType)
	})

	t.Run("EmptyEntries", func(t *testing.T) {
		entries := []*CheckpointEntry{}
		result := filterSnapshotEntries(entries)
		assert.Equal(t, 0, len(result))
	})

	t.Run("NoGlobalCheckpoint", func(t *testing.T) {
		entries := []*CheckpointEntry{
			{
				start:     types.BuildTS(100, 0),
				end:       types.BuildTS(200, 0),
				entryType: ET_Incremental,
			},
			{
				start:     types.BuildTS(200, 0),
				end:       types.BuildTS(300, 0),
				entryType: ET_Incremental,
			},
		}

		result := filterSnapshotEntries(entries)
		// Should return all entries if no global checkpoint
		assert.Equal(t, 2, len(result))
	})

	t.Run("MultipleGlobalCheckpoints", func(t *testing.T) {
		entries := []*CheckpointEntry{
			{
				start:     types.BuildTS(100, 0),
				end:       types.BuildTS(200, 0),
				entryType: ET_Global,
			},
			{
				start:     types.BuildTS(200, 0),
				end:       types.BuildTS(300, 0),
				entryType: ET_Global,
			},
			{
				start:     types.BuildTS(300, 0),
				end:       types.BuildTS(400, 0),
				entryType: ET_Incremental,
			},
		}

		result := filterSnapshotEntries(entries)
		// Should return entries from the latest global checkpoint onwards
		assert.Equal(t, 2, len(result))
		assert.Equal(t, ET_Global, result[0].entryType)
		assert.Equal(t, ET_Incremental, result[1].entryType)
	})
}

func createMockCheckpointEntry(start, end int64) *CheckpointEntry {
	return &CheckpointEntry{
		start: types.BuildTS(start, 0),
		end:   types.BuildTS(end, 0),
		state: ST_Finished,
		doneC: make(chan struct{}),
	}
}
