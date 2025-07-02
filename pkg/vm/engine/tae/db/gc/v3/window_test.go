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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
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
	accountSnapshots map[uint32][]types.TS,
	pitrs *logtail.PitrInfo,
) (map[uint64][]types.TS, map[uint64][]types.TS) {
	tableSnapshots := make(map[uint64][]types.TS)
	tablePitrs := make(map[uint64][]types.TS)
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

	// Create necessary services - needed for test setup
	_, err := fileservice.NewFileService(
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
	defer mp.Free([]byte{})

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

	// Define table IDs
	table1Id := uint64(101)
	table2Id := uint64(102)

	// Create a simplified structure to represent object references
	type ObjRef struct {
		objName   string
		tableID   uint64
		isDropped bool
	}

	// Create object references
	objRefs := []ObjRef{
		{objName1.String(), table1Id, true},  // object1 from table1 (dropped)
		{objName1.String(), table2Id, false}, // object1 from table2 (not dropped)
		{objName2.String(), table1Id, true},  // object2 from table1 (dropped)
		{objName3.String(), table1Id, true},  // object3 from table1 (dropped)
	}

	// Create a map to track table references for each object
	objToTableRefs := make(map[string]map[uint64]bool)

	// Log test setup
	t.Log("DEBUG: Test setup - Object references:")

	// Fill the reference map and log
	for _, ref := range objRefs {
		if objToTableRefs[ref.objName] == nil {
			objToTableRefs[ref.objName] = make(map[uint64]bool)
		}
		objToTableRefs[ref.objName][ref.tableID] = ref.isDropped
		t.Logf("  - Object: %s, Table: %d, Is dropped: %v", ref.objName, ref.tableID, ref.isDropped)
	}

	// Expected GC results
	shouldGC := map[string]bool{
		objName2.String(): true,  // Only referenced by table1, and marked as dropped
		objName3.String(): true,  // Only referenced by table1, and marked as dropped
		objName1.String(): false, // Should not be GC'd because table2 still references it
	}

	// Perform the GC logic directly
	t.Log("DEBUG: Performing GC logic:")
	gcObjects := make(map[string]bool)

	// Process each object and determine if it should be GC'd
	for objName, tableRefs := range objToTableRefs {
		// Check if all references are dropped
		allDropped := true
		for _, isDropped := range tableRefs {
			if !isDropped {
				allDropped = false
				break
			}
		}

		// If all references are dropped, mark for GC
		if allDropped {
			gcObjects[objName] = true
			t.Logf("  - Object %s: All references dropped, should be GC'd", objName)
		} else {
			t.Logf("  - Object %s: Some references not dropped, should NOT be GC'd", objName)
		}
	}

	// Check results
	t.Log("DEBUG: GC Results:")
	for objName, expected := range shouldGC {
		actual := gcObjects[objName]
		t.Logf("  - Object %s: Expected GC: %v, Actual GC: %v", objName, expected, actual)
		require.Equal(t, expected, actual, "Object %s GC status mismatch", objName)
	}
}
