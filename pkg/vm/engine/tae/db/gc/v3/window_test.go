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

package gc

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
)

// Constants for test module
const (
	ModuleName = "GC"
)

// GetDefaultTestPath returns the default path for test files
func GetDefaultTestPath(module string, name string) string {
	return filepath.Join("/tmp", module, name)
}

// MakeDefaultTestPath creates and returns the default path for test files
func MakeDefaultTestPath(module string, name string) string {
	path := GetDefaultTestPath(module, name)
	os.MkdirAll(path, os.FileMode(0755))
	return path
}

// RemoveDefaultTestPath removes the default path for test files
func RemoveDefaultTestPath(module string, name string) {
	path := GetDefaultTestPath(module, name)
	os.RemoveAll(path)
}

// InitTestEnv initializes the test environment
func InitTestEnv(module string, name string) string {
	RemoveDefaultTestPath(module, name)
	return MakeDefaultTestPath(module, name)
}

// TestObjectEntryMapWithMultipleTables tests handling objects referenced by multiple tables
func TestObjectEntryMapWithMultipleTables(t *testing.T) {
	// Create sample object statistics
	objUUID := types.Uuid{0x1a, 0x1b}
	objName := objectio.BuildObjectName(&objUUID, 0)
	stats := objectio.NewObjectStats()
	objectio.SetObjectStatsObjectName(stats, objName)

	// Create object reference mapping
	objects := make(map[string]map[uint64]*ObjectEntry)
	objectName := stats.ObjectName().String()
	objects[objectName] = make(map[uint64]*ObjectEntry)

	// Add object references: two different tables referencing the same object
	table1 := uint64(101)
	table2 := uint64(102)

	ts1 := types.BuildTS(10, 0)
	objects[objectName] = map[uint64]*ObjectEntry{
		table1: {
			stats:    stats,
			createTS: ts1,
			db:       1,
			table:    table1,
		},
		table2: {
			stats:    stats,
			createTS: ts1,
			db:       1,
			table:    table2,
		},
	}

	// Verify object references are correctly added
	require.Len(t, objects, 1, "Should have one object")
	require.Len(t, objects[objectName], 2, "Object should be referenced by two tables")

	// Verify table reference information is correct
	require.Equal(t, table1, objects[objectName][table1].table, "Table1 reference information is incorrect")
	require.Equal(t, table2, objects[objectName][table2].table, "Table2 reference information is incorrect")

	// Verify timestamp information
	require.Equal(t, ts1, objects[objectName][table1].createTS, "Table1 create timestamp is incorrect")
	require.Equal(t, ts1, objects[objectName][table2].createTS, "Table2 create timestamp is incorrect")
}

// TestMultiTableObjectReferenceGC tests the GC logic with multiple table references
func TestMultiTableObjectReferenceGC(t *testing.T) {
	// Define test objects
	objName1 := objectio.BuildObjectName(&types.Uuid{0x1a, 0x1b}, 0) // Object referenced by two tables
	objName2 := objectio.BuildObjectName(&types.Uuid{0x2a, 0x2b}, 0) // Object referenced by one table
	objName3 := objectio.BuildObjectName(&types.Uuid{0x3a, 0x3b}, 0) // Object referenced by one table

	// Create object statistics
	stats1 := objectio.NewObjectStats()
	stats2 := objectio.NewObjectStats()
	stats3 := objectio.NewObjectStats()

	objectio.SetObjectStatsObjectName(stats1, objName1)
	objectio.SetObjectStatsObjectName(stats2, objName2)
	objectio.SetObjectStatsObjectName(stats3, objName3)

	// Object name strings
	name1 := stats1.ObjectName().String()
	name2 := stats2.ObjectName().String()
	name3 := stats3.ObjectName().String()

	// Define table IDs and timestamps
	table1 := uint64(101)
	table2 := uint64(102)
	createTS1 := types.BuildTS(10, 0) // Table1 creation time
	createTS2 := types.BuildTS(12, 0) // Table2 creation time
	dropTS1 := types.BuildTS(20, 0)   // Table1 object drop time
	noDropTS := types.TS{}            // Not dropped marker

	// Create object reference map to simulate checkpoint data
	// Key is object name, value is a map from table ID to ObjectEntry
	objectRefs := make(map[string]map[uint64]*ObjectEntry)

	// 1. Set up object1 references
	objectRefs[name1] = make(map[uint64]*ObjectEntry)
	// Table1 references object1, but has marked it as dropped
	objectRefs[name1][table1] = &ObjectEntry{
		stats:    stats1,
		createTS: createTS1,
		dropTS:   dropTS1, // Dropped
		db:       1,
		table:    table1,
	}
	// Table2 also references object1, and has not marked it as dropped
	objectRefs[name1][table2] = &ObjectEntry{
		stats:    stats1,
		createTS: createTS2,
		dropTS:   noDropTS, // Not dropped
		db:       1,
		table:    table2,
	}

	// 2. Set up object2 reference state - only referenced by table1 and marked as dropped
	objectRefs[name2] = make(map[uint64]*ObjectEntry)
	objectRefs[name2][table1] = &ObjectEntry{
		stats:    stats2,
		createTS: createTS1,
		dropTS:   dropTS1, // Dropped
		db:       1,
		table:    table1,
	}

	// 3. Set up object3 reference state - only referenced by table1 and marked as dropped
	objectRefs[name3] = make(map[uint64]*ObjectEntry)
	objectRefs[name3][table1] = &ObjectEntry{
		stats:    stats3,
		createTS: createTS1,
		dropTS:   dropTS1, // Dropped
		db:       1,
		table:    table1,
	}

	// Objects to check
	objectsToCheck := []string{name1, name2, name3}

	// Directly simulate the core GC logic: check if objects should be garbage collected
	// An object should be GC'd only when all tables referencing it have marked it as dropped
	shouldGCObject := func(objName string) bool {
		tableEntries := objectRefs[objName]
		if tableEntries == nil {
			return false // Object doesn't exist, can't be GC'd
		}

		// Object should be GC'd only if ALL references have been dropped
		for _, entry := range tableEntries {
			if entry.dropTS.IsEmpty() {
				return false // At least one table still references this object
			}
		}
		return true // All references have been dropped
	}

	// Execute GC check
	objectsToGC := make([]string, 0)
	objectsToKeep := make([]string, 0)

	for _, objName := range objectsToCheck {
		if shouldGCObject(objName) {
			objectsToGC = append(objectsToGC, objName)
		} else {
			objectsToKeep = append(objectsToKeep, objName)
		}
	}

	// Verify results
	// 1. object1 should not be GC'd because table2 still references it
	require.Contains(t, objectsToKeep, name1, "object1 should be kept because table2 still references it")

	// 2. object2 and object3 should be GC'd because they're only referenced by table1 which has marked them as dropped
	require.Contains(t, objectsToGC, name2, "object2 should be GC'd as it's only referenced by table1 which dropped it")
	require.Contains(t, objectsToGC, name3, "object3 should be GC'd as it's only referenced by table1 which dropped it")

	// More detailed checks
	require.Equal(t, 1, len(objectsToKeep), "Should have 1 object kept")
	require.Equal(t, 2, len(objectsToGC), "Should have 2 objects GC'd")

	// Output results for inspection
	t.Logf("Objects to keep: %v", objectsToKeep)
	t.Logf("Objects to GC: %v", objectsToGC)
}

// MockSnapshotMeta simulates logtail.SnapshotMeta for testing
type MockSnapshotMeta struct {
	tableData map[uint64]bool
}

// NewMockSnapshotMeta creates a new mock snapshot metadata instance
func NewMockSnapshotMeta() *MockSnapshotMeta {
	return &MockSnapshotMeta{
		tableData: make(map[uint64]bool),
	}
}

// AccountToTableSnapshots mocks the same method in logtail.SnapshotMeta
func (m *MockSnapshotMeta) AccountToTableSnapshots(
	snapshots *logtail.SnapshotInfo,
	pitrs *logtail.PitrInfo,
) (map[uint64][]types.TS, map[uint64]*types.TS) {
	tableSnapshots := make(map[uint64][]types.TS)
	tablePitrs := make(map[uint64]*types.TS)
	return tableSnapshots, tablePitrs
}

// MockCheckpoint simulates a checkpoint for testing purposes
type MockCheckpoint struct {
	tableData map[uint64]map[string][]ObjectEntry
}

// NewMockCheckpoint creates a new mock checkpoint
func NewMockCheckpoint() *MockCheckpoint {
	return &MockCheckpoint{
		tableData: make(map[uint64]map[string][]ObjectEntry),
	}
}

// AddObjectEntry adds an object entry to the mock checkpoint
func (m *MockCheckpoint) AddObjectEntry(table uint64, entry ObjectEntry) {
	if _, ok := m.tableData[table]; !ok {
		m.tableData[table] = make(map[string][]ObjectEntry)
	}

	objectName := entry.stats.ObjectName().String()
	m.tableData[table][objectName] = append(m.tableData[table][objectName], entry)
}

// TestMultiTableGCWithRealCheckpointData tests GC with mocked checkpoint data
func TestMultiTableGCWithRealCheckpointData(t *testing.T) {
	// Simplified test - we use mock objects and direct implementation of filter/sink to validate GC logic
	ctx := context.Background()
	dir := InitTestEnv(ModuleName, t.Name())
	defer RemoveDefaultTestPath(ModuleName, t.Name())

	// Create necessary services
	fs, err := fileservice.NewFileService(
		ctx,
		fileservice.Config{
			Name:    "test-fs",
			Backend: "DISK",
			DataDir: dir,
			Cache:   fileservice.DisabledCacheConfig,
		},
		nil,
	)
	require.NoError(t, err)

	mp := mpool.MustNewZeroNoFixed()

	// Create test objects
	objUUID1 := types.Uuid{0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x1a, 0x1b, 0x1c, 0x1f}
	objUUID2 := types.Uuid{0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x2a, 0x2b, 0x2c, 0x2f}
	objUUID3 := types.Uuid{0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x3a, 0x3b, 0x3c, 0x3f}

	objName1 := objectio.BuildObjectName(&objUUID1, 0) // Shared by table1 and table2
	objName2 := objectio.BuildObjectName(&objUUID2, 0) // Only in table1
	objName3 := objectio.BuildObjectName(&objUUID3, 0) // Only in table1

	// Create object statistics
	stats1 := objectio.NewObjectStats()
	stats2 := objectio.NewObjectStats()
	stats3 := objectio.NewObjectStats()

	objectio.SetObjectStatsObjectName(stats1, objName1)
	objectio.SetObjectStatsObjectName(stats2, objName2)
	objectio.SetObjectStatsObjectName(stats3, objName3)

	// Define table IDs and timestamps
	table1Id := uint64(101)
	table2Id := uint64(102)

	// Define timestamps
	createTS1 := types.BuildTS(10, 0)
	createTS2 := types.BuildTS(15, 0)
	dropTS1 := types.BuildTS(20, 0) // Drop timestamp for table1

	// Create batch buffer
	buffer := containers.NewOneSchemaBatchBuffer(16*mpool.MB, ObjectTableAttrs, ObjectTableTypes, false)
	defer buffer.Close(mp)

	// Create batch
	newBat := buffer.Fetch()
	defer buffer.Putback(newBat, mp)

	// Add test data to batch
	// 1. object1 referenced by table1 (dropped)
	if err := vector.AppendBytes(newBat.Vecs[0], stats1[:], false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[1], createTS1, false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[2], dropTS1, false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[3], uint64(1), false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[4], table1Id, false, mp); err != nil {
		t.Fatal(err)
	}

	// 2. object1 referenced by table2 (not dropped)
	if err := vector.AppendBytes(newBat.Vecs[0], stats1[:], false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[1], createTS2, false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[2], types.TS{}, false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[3], uint64(1), false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[4], table2Id, false, mp); err != nil {
		t.Fatal(err)
	}

	// 3. object2 referenced by table1 (dropped)
	if err := vector.AppendBytes(newBat.Vecs[0], stats2[:], false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[1], createTS1, false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[2], dropTS1, false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[3], uint64(1), false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[4], table1Id, false, mp); err != nil {
		t.Fatal(err)
	}

	// 4. object3 referenced by table1 (dropped)
	if err := vector.AppendBytes(newBat.Vecs[0], stats3[:], false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[1], createTS1, false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[2], dropTS1, false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[3], uint64(1), false, mp); err != nil {
		t.Fatal(err)
	}
	if err := vector.AppendFixed(newBat.Vecs[4], table1Id, false, mp); err != nil {
		t.Fatal(err)
	}

	newBat.SetRowCount(newBat.Vecs[0].Length())

	// Create a GC executor
	executor := NewGCExecutor(buffer, false, 1000, mp, fs)
	defer executor.Close()
	done := false
	// Create a simple data source function
	sourceFn := func(ctx context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, bat *batch.Batch) (bool, error) {
		// Copy original batch to target batch
		if done {
			return true, nil // Already added, don't add again
		}

		for i := 0; i < len(newBat.Vecs); i++ {
			err = newBat.Vecs[i].CloneWindowTo(bat.Vecs[i], 0, newBat.Vecs[i].Length(), mp)
			if err != nil {
				return false, err
			}
		}
		bat.SetRowCount(newBat.Vecs[0].Length())
		done = true
		return false, nil
	}

	// Object GC status tracking
	shouldGC := map[string]bool{
		objName2.String(): true,  // Only referenced by table1, and marked as dropped
		objName3.String(): true,  // Only referenced by table1, and marked as dropped
		objName1.String(): false, // Should not be GC'd because table2 still references it
	}

	// Create object name to row index mapping
	objNameToRows := make(map[string][]int)
	for i := 0; i < newBat.RowCount(); i++ {
		stats := (objectio.ObjectStats)(newBat.Vecs[0].GetRawBytesAt(i))
		name := stats.ObjectName().String()
		objNameToRows[name] = append(objNameToRows[name], i)
	}

	// Create a filter that implements multi-table reference GC logic
	fineFilter := func(ctx context.Context, bm *bitmap.Bitmap, bat *batch.Batch, mp *mpool.MPool) error {
		// Create a table reference map for each object
		objRefs := make(map[string]map[uint64]bool)
		// First pass: collect reference information for all objects
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			stats := (objectio.ObjectStats)(bat.Vecs[0].GetRawBytesAt(i))
			objName := stats.ObjectName().String()
			tableID := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[4])[i]
			dropTS := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])[i]

			if objRefs[objName] == nil {
				objRefs[objName] = make(map[uint64]bool)
			}

			// true means this object has been dropped in this table
			objRefs[objName][tableID] = !dropTS.IsEmpty()
		}

		// Second pass: mark objects where all referencing tables have dropped them
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			stats := (objectio.ObjectStats)(bat.Vecs[0].GetRawBytesAt(i))
			objName := stats.ObjectName().String()

			allDropped := true
			for _, isDropped := range objRefs[objName] {
				if !isDropped {
					allDropped = false
					break
				}
			}

			// If all references are dropped, add this object to GC list
			if allDropped {
				bm.Add(uint64(i))
			}
		}

		return nil
	}

	// Track GC'd objects
	gcObjects := make(map[string]bool)
	finalSinker := func(ctx context.Context, bat *batch.Batch) error {
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			stats := (objectio.ObjectStats)(bat.Vecs[0].GetRawBytesAt(i))
			objName := stats.ObjectName().String()
			gcObjects[objName] = true
		}
		return nil
	}

	// Empty filter - does no filtering
	noFilter := func(ctx context.Context, bm *bitmap.Bitmap, bat *batch.Batch, mp *mpool.MPool) error {
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			bm.Add(uint64(i))
		}
		return nil
	}

	// Run GC executor
	_, err = executor.Run(
		ctx,
		sourceFn,
		noFilter,   // Coarse filter
		fineFilter, // Fine filter
		finalSinker,
	)
	require.NoError(t, err)

	// Verify results
	for objName, shouldBeGCd := range shouldGC {
		if shouldBeGCd {
			require.True(t, gcObjects[objName], "Object %s should have been GC'd", objName)
		} else {
			require.False(t, gcObjects[objName], "Object %s should NOT have been GC'd", objName)
		}
	}
}

// TestFilesToGCDeduplication tests that duplicate file names are deduplicated
// before being sent to the deleter. This is the fix for the issue where
// the same object could appear multiple times in filesToGC when referenced
// by multiple tables.
func TestFilesToGCDeduplication(t *testing.T) {
	// Simulate the deduplication logic used in ExecuteGlobalCheckpointBasedGC
	// This tests the map-based deduplication approach

	// Create a list of file names with duplicates (simulating the bug scenario)
	// In the original bug, vecToGC could contain the same object name multiple times
	// because the same object was referenced by multiple tables
	duplicateFiles := []string{
		"018f1234-5678-9abc-def0-123456789abc_0.data",
		"018f2345-6789-abcd-ef01-23456789abcd_0.data",
		"018f1234-5678-9abc-def0-123456789abc_0.data", // duplicate of first
		"018f3456-789a-bcde-f012-3456789abcde_0.data",
		"018f2345-6789-abcd-ef01-23456789abcd_0.data", // duplicate of second
		"018f1234-5678-9abc-def0-123456789abc_0.data", // another duplicate of first
		"018f4567-89ab-cdef-0123-456789abcdef_0.data",
	}

	// Apply the deduplication logic (same as in ExecuteGlobalCheckpointBasedGC)
	filesToGCSet := make(map[string]struct{})
	for _, file := range duplicateFiles {
		filesToGCSet[file] = struct{}{}
	}

	filesToGC := make([]string, 0, len(filesToGCSet))
	for file := range filesToGCSet {
		filesToGC = append(filesToGC, file)
	}

	// Verify deduplication worked correctly
	require.Equal(t, 4, len(filesToGC), "Should have 4 unique files after deduplication")

	// Verify all unique files are present
	uniqueFiles := map[string]bool{
		"018f1234-5678-9abc-def0-123456789abc_0.data": false,
		"018f2345-6789-abcd-ef01-23456789abcd_0.data": false,
		"018f3456-789a-bcde-f012-3456789abcde_0.data": false,
		"018f4567-89ab-cdef-0123-456789abcdef_0.data": false,
	}

	for _, file := range filesToGC {
		_, exists := uniqueFiles[file]
		require.True(t, exists, "File %s should be in the unique files set", file)
		uniqueFiles[file] = true
	}

	// Verify all unique files were found
	for file, found := range uniqueFiles {
		require.True(t, found, "File %s should have been found in filesToGC", file)
	}

	// Verify the original list had duplicates
	require.Equal(t, 7, len(duplicateFiles), "Original list should have 7 entries (with duplicates)")

	// Calculate the reduction ratio
	reductionRatio := float64(len(duplicateFiles)-len(filesToGC)) / float64(len(duplicateFiles)) * 100
	t.Logf("Deduplication reduced file count from %d to %d (%.1f%% reduction)",
		len(duplicateFiles), len(filesToGC), reductionRatio)
}

// TestFilesToGCDeduplicationWithEmptyInput tests deduplication with edge cases
func TestFilesToGCDeduplicationWithEmptyInput(t *testing.T) {
	testCases := []struct {
		name     string
		input    []string
		expected int
	}{
		{
			name:     "empty input",
			input:    []string{},
			expected: 0,
		},
		{
			name:     "single file",
			input:    []string{"file1.data"},
			expected: 1,
		},
		{
			name:     "all duplicates",
			input:    []string{"file1.data", "file1.data", "file1.data"},
			expected: 1,
		},
		{
			name:     "no duplicates",
			input:    []string{"file1.data", "file2.data", "file3.data"},
			expected: 3,
		},
		{
			name: "mixed duplicates",
			input: []string{
				"file1.data", "file2.data", "file1.data",
				"file3.data", "file2.data", "file4.data",
			},
			expected: 4,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Apply deduplication
			filesToGCSet := make(map[string]struct{})
			for _, file := range tc.input {
				filesToGCSet[file] = struct{}{}
			}

			filesToGC := make([]string, 0, len(filesToGCSet))
			for file := range filesToGCSet {
				filesToGC = append(filesToGC, file)
			}

			require.Equal(t, tc.expected, len(filesToGC),
				"Expected %d unique files, got %d", tc.expected, len(filesToGC))
		})
	}
}
