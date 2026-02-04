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
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockRow implements a mock for sql.Row
type mockRow struct {
	result string
	err    error
}

func (r *mockRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) > 0 {
		if s, ok := dest[0].(*string); ok {
			*s = r.result
		}
	}
	return nil
}

// mockDB implements DBQuerier for testing
type mockDB struct {
	queryResult string
	queryErr    error
	closed      bool
}

func (m *mockDB) QueryRow(query string, args ...any) *sql.Row {
	// We can't return a real sql.Row, so we'll use a workaround
	// by storing the result and checking it in tests
	return nil
}

func (m *mockDB) Close() error {
	m.closed = true
	return nil
}

// mockDBWithResult is a more complete mock that can return results
type mockDBWithResult struct {
	results map[string]string
	errors  map[string]error
	closed  bool
}

func newMockDB() *mockDBWithResult {
	return &mockDBWithResult{
		results: make(map[string]string),
		errors:  make(map[string]error),
	}
}

func (m *mockDBWithResult) QueryRow(query string, args ...any) *sql.Row {
	return nil
}

func (m *mockDBWithResult) Close() error {
	m.closed = true
	return nil
}

func TestSyncProtectionRequest(t *testing.T) {
	req := SyncProtectionRequest{
		JobID:      "test-job-1",
		BF:         "base64data",
		ValidTS:    1234567890,
		TestObject: "test-object",
	}

	assert.Equal(t, "test-job-1", req.JobID)
	assert.Equal(t, "base64data", req.BF)
	assert.Equal(t, int64(1234567890), req.ValidTS)
	assert.Equal(t, "test-object", req.TestObject)
}

func TestSyncProtectionTester_SelectRandomObjects(t *testing.T) {
	tester := &SyncProtectionTester{
		sampleCount: 5,
	}

	// Test with fewer objects than count
	objects := []string{"obj1", "obj2", "obj3"}
	selected := tester.SelectRandomObjects(objects, 5)
	assert.Equal(t, 3, len(selected))

	// Test with more objects than count
	objects = []string{"obj1", "obj2", "obj3", "obj4", "obj5", "obj6", "obj7", "obj8", "obj9", "obj10"}
	selected = tester.SelectRandomObjects(objects, 5)
	assert.Equal(t, 5, len(selected))

	// Verify original slice is not modified
	assert.Equal(t, 10, len(objects))

	// Test with equal count
	objects = []string{"obj1", "obj2", "obj3"}
	selected = tester.SelectRandomObjects(objects, 3)
	assert.Equal(t, 3, len(selected))
}

func TestSyncProtectionTester_ScanObjectFiles(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "gc-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tester := &SyncProtectionTester{
		dataDir: tmpDir,
	}

	// Test empty directory
	objects, err := tester.ScanObjectFiles()
	require.NoError(t, err)
	assert.Equal(t, 0, len(objects))

	// Create some test files with valid object name pattern
	// Format: 019c226d-9e98-7ecc-9662-712ff0edcbfb_00000 (42 characters)
	validNames := []string{
		"019c226d-9e98-7ecc-9662-712ff0edcbfb_00000",
		"019c226d-9e98-7ecc-9662-712ff0edcbfb_00001",
	}
	for _, name := range validNames {
		f, err := os.Create(filepath.Join(tmpDir, name))
		require.NoError(t, err)
		f.Close()
	}

	// Create invalid files (should be ignored)
	invalidNames := []string{
		"invalid.txt",
		"short",
		"no-underscore-but-has-dashes-here",
	}
	for _, name := range invalidNames {
		f, err := os.Create(filepath.Join(tmpDir, name))
		require.NoError(t, err)
		f.Close()
	}

	// Scan again
	objects, err = tester.ScanObjectFiles()
	require.NoError(t, err)
	assert.Equal(t, 2, len(objects))
}

func TestSyncProtectionTester_ScanObjectFiles_WithSubdirectory(t *testing.T) {
	// Create temp directory with subdirectory
	tmpDir, err := os.MkdirTemp("", "gc-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	subDir := filepath.Join(tmpDir, "subdir")
	err = os.Mkdir(subDir, 0755)
	require.NoError(t, err)

	// Create file in subdirectory
	validName := "019c226d-9e98-7ecc-9662-712ff0edcbfb_00000"
	f, err := os.Create(filepath.Join(subDir, validName))
	require.NoError(t, err)
	f.Close()

	tester := &SyncProtectionTester{
		dataDir: tmpDir,
	}

	objects, err := tester.ScanObjectFiles()
	require.NoError(t, err)
	assert.Equal(t, 1, len(objects))
}

func TestSyncProtectionTester_ScanObjectFiles_NonExistentDir(t *testing.T) {
	tester := &SyncProtectionTester{
		dataDir: "/non/existent/path/that/does/not/exist",
	}

	_, err := tester.ScanObjectFiles()
	assert.Error(t, err)
}

func TestSyncProtectionTester_BuildBloomFilter(t *testing.T) {
	tester := &SyncProtectionTester{
		verbose: false,
	}

	objects := []string{
		"019c226d-9e98-7ecc-9662-712ff0edcbfb_00000",
		"019c226d-9e98-7ecc-9662-712ff0edcbfb_00001",
		"019c226d-9e98-7ecc-9662-712ff0edcbfb_00002",
	}

	bfData, err := tester.BuildBloomFilter(objects)
	require.NoError(t, err)
	assert.NotEmpty(t, bfData)

	// Verify it's valid base64
	assert.NotContains(t, bfData, " ")
}

func TestSyncProtectionTester_BuildBloomFilter_SingleObject(t *testing.T) {
	tester := &SyncProtectionTester{
		verbose: false,
	}

	objects := []string{"single-object"}
	bfData, err := tester.BuildBloomFilter(objects)
	require.NoError(t, err)
	assert.NotEmpty(t, bfData)
}

func TestSyncProtectionTester_BuildBloomFilter_Verbose(t *testing.T) {
	tester := &SyncProtectionTester{
		verbose: true,
	}

	objects := []string{"obj1", "obj2"}
	bfData, err := tester.BuildBloomFilter(objects)
	require.NoError(t, err)
	assert.NotEmpty(t, bfData)
}

func TestSyncProtectionTester_CheckFilesExist(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "gc-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create some files
	existingFile := "019c226d-9e98-7ecc-9662-712ff0edcbfb_00000"
	f, err := os.Create(filepath.Join(tmpDir, existingFile))
	require.NoError(t, err)
	f.Close()

	tester := &SyncProtectionTester{
		dataDir:        tmpDir,
		protectedFiles: []string{existingFile, "non-existent-file"},
	}

	existing, deleted := tester.CheckFilesExist()
	assert.Equal(t, 1, len(existing))
	assert.Equal(t, 1, len(deleted))
	assert.Contains(t, existing, existingFile)
	assert.Contains(t, deleted, "non-existent-file")
}

func TestSyncProtectionTester_CheckFilesExist_AllExist(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gc-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	files := []string{"file1", "file2", "file3"}
	for _, name := range files {
		f, err := os.Create(filepath.Join(tmpDir, name))
		require.NoError(t, err)
		f.Close()
	}

	tester := &SyncProtectionTester{
		dataDir:        tmpDir,
		protectedFiles: files,
	}

	existing, deleted := tester.CheckFilesExist()
	assert.Equal(t, 3, len(existing))
	assert.Equal(t, 0, len(deleted))
}

func TestSyncProtectionTester_CheckFilesExist_NoneExist(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gc-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tester := &SyncProtectionTester{
		dataDir:        tmpDir,
		protectedFiles: []string{"non-existent-1", "non-existent-2"},
	}

	existing, deleted := tester.CheckFilesExist()
	assert.Equal(t, 0, len(existing))
	assert.Equal(t, 2, len(deleted))
}

func TestSyncProtectionTester_CheckFilesExist_EmptyList(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gc-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tester := &SyncProtectionTester{
		dataDir:        tmpDir,
		protectedFiles: []string{},
	}

	existing, deleted := tester.CheckFilesExist()
	assert.Equal(t, 0, len(existing))
	assert.Equal(t, 0, len(deleted))
}

func TestSyncProtectionTester_Close(t *testing.T) {
	// Test with nil db
	tester := &SyncProtectionTester{
		db: nil,
	}
	tester.Close() // Should not panic

	// Test with mock db
	mockDB := newMockDB()
	tester = &SyncProtectionTester{
		db: mockDB,
	}
	tester.Close()
	assert.True(t, mockDB.closed)
}

func TestPrepareSyncProtectionCommand(t *testing.T) {
	cmd := PrepareSyncProtectionCommand()

	assert.Equal(t, "sync-protection", cmd.Use)
	assert.NotEmpty(t, cmd.Short)
	assert.NotEmpty(t, cmd.Long)

	// Check flags exist
	flags := cmd.Flags()
	assert.NotNil(t, flags.Lookup("dsn"))
	assert.NotNil(t, flags.Lookup("data-dir"))
	assert.NotNil(t, flags.Lookup("sample"))
	assert.NotNil(t, flags.Lookup("verbose"))
	assert.NotNil(t, flags.Lookup("wait"))

	// Check default values
	dsnFlag := flags.Lookup("dsn")
	assert.Equal(t, "root:111@tcp(127.0.0.1:6001)/", dsnFlag.DefValue)

	dataDirFlag := flags.Lookup("data-dir")
	assert.Equal(t, "./mo-data/shared", dataDirFlag.DefValue)

	sampleFlag := flags.Lookup("sample")
	assert.Equal(t, "10", sampleFlag.DefValue)

	verboseFlag := flags.Lookup("verbose")
	assert.Equal(t, "false", verboseFlag.DefValue)

	waitFlag := flags.Lookup("wait")
	assert.Equal(t, "30", waitFlag.DefValue)
}

func TestPrepareCommand(t *testing.T) {
	cmd := PrepareCommand()

	assert.Equal(t, "gc", cmd.Use)
	assert.NotEmpty(t, cmd.Short)

	// Check subcommands
	subCmds := cmd.Commands()
	assert.GreaterOrEqual(t, len(subCmds), 1)

	// Find sync-protection subcommand
	var syncProtectionCmd *bool
	for _, sub := range subCmds {
		if sub.Use == "sync-protection" {
			b := true
			syncProtectionCmd = &b
			break
		}
	}
	assert.NotNil(t, syncProtectionCmd)
}

func TestDBWrapper(t *testing.T) {
	// Test that dbWrapper implements DBQuerier
	var _ DBQuerier = &dbWrapper{}
}
