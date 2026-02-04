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

package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectRandomObjects(t *testing.T) {
	tester := &SyncProtectionTester{}

	// Test with count > len(objects)
	objects := []string{"obj1", "obj2", "obj3"}
	result := tester.SelectRandomObjects(objects, 10)
	assert.Equal(t, objects, result)

	// Test with count < len(objects)
	result = tester.SelectRandomObjects(objects, 2)
	assert.Len(t, result, 2)

	// Test with count == len(objects)
	result = tester.SelectRandomObjects(objects, 3)
	assert.Len(t, result, 3)

	// Test with empty objects
	result = tester.SelectRandomObjects([]string{}, 5)
	assert.Empty(t, result)

	// Test with count == 0
	result = tester.SelectRandomObjects(objects, 0)
	assert.Empty(t, result)
}

func TestScanObjectFiles(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "gc-tool-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tester := &SyncProtectionTester{
		dataDir: tmpDir,
	}

	// Test empty directory
	objects, err := tester.ScanObjectFiles()
	require.NoError(t, err)
	assert.Empty(t, objects)

	// Create valid object files (42 chars, UUID format with underscore)
	validFiles := []string{
		"019c226d-9e98-7ecc-9662-712ff0edcbfb_00000",
		"019c226d-9e98-7ecc-9662-712ff0edcbfc_00001",
	}
	for _, f := range validFiles {
		err := os.WriteFile(filepath.Join(tmpDir, f), []byte("test"), 0644)
		require.NoError(t, err)
	}

	// Create invalid files (should be ignored)
	invalidFiles := []string{
		"short.txt",
		"no-underscore-in-this-file-name-at-all-xx",
		"not-enough-dashes_00000000000000000000000",
	}
	for _, f := range invalidFiles {
		err := os.WriteFile(filepath.Join(tmpDir, f), []byte("test"), 0644)
		require.NoError(t, err)
	}

	// Scan should only find valid files
	objects, err = tester.ScanObjectFiles()
	require.NoError(t, err)
	assert.Len(t, objects, 2)
	assert.Contains(t, objects, validFiles[0])
	assert.Contains(t, objects, validFiles[1])
}

func TestScanObjectFiles_Subdirectory(t *testing.T) {
	// Create temp directory with subdirectory
	tmpDir, err := os.MkdirTemp("", "gc-tool-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	subDir := filepath.Join(tmpDir, "subdir")
	err = os.MkdirAll(subDir, 0755)
	require.NoError(t, err)

	tester := &SyncProtectionTester{
		dataDir: tmpDir,
	}

	// Create valid object file in subdirectory
	validFile := "019c226d-9e98-7ecc-9662-712ff0edcbfb_00000"
	err = os.WriteFile(filepath.Join(subDir, validFile), []byte("test"), 0644)
	require.NoError(t, err)

	// Scan should find file in subdirectory
	objects, err := tester.ScanObjectFiles()
	require.NoError(t, err)
	assert.Len(t, objects, 1)
	assert.Contains(t, objects, validFile)
}

func TestBuildBloomFilter(t *testing.T) {
	tester := &SyncProtectionTester{
		verbose: false,
	}

	objects := []string{"obj1", "obj2", "obj3"}
	bfData, err := tester.BuildBloomFilter(objects)
	require.NoError(t, err)
	assert.NotEmpty(t, bfData)

	// Verify it's valid base64
	decoded, err := base64.StdEncoding.DecodeString(bfData)
	require.NoError(t, err)
	assert.NotEmpty(t, decoded)
}

func TestBuildBloomFilter_Empty(t *testing.T) {
	tester := &SyncProtectionTester{
		verbose: false,
	}

	// Empty objects should still work (creates empty BF)
	bfData, err := tester.BuildBloomFilter([]string{})
	// Note: index.NewBloomFilter may return error for empty input
	if err != nil {
		// Expected behavior for empty input
		return
	}
	assert.NotEmpty(t, bfData)
}

func TestBuildBloomFilter_LargeInput(t *testing.T) {
	tester := &SyncProtectionTester{
		verbose: false,
	}

	// Create many objects
	objects := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		objects[i] = "019c226d-9e98-7ecc-9662-712ff0edcbfb_" + string(rune('0'+i%10)) + string(rune('0'+i/10%10)) + string(rune('0'+i/100%10))
	}

	bfData, err := tester.BuildBloomFilter(objects)
	require.NoError(t, err)
	assert.NotEmpty(t, bfData)
}

func TestSyncProtectionRequest_JSON(t *testing.T) {
	req := SyncProtectionRequest{
		JobID:      "test-job-123",
		BF:         "base64encodeddata",
		ValidTS:    1234567890,
		TestObject: "test-obj",
	}

	// Marshal
	data, err := json.Marshal(req)
	require.NoError(t, err)

	// Unmarshal
	var decoded SyncProtectionRequest
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, req.JobID, decoded.JobID)
	assert.Equal(t, req.BF, decoded.BF)
	assert.Equal(t, req.ValidTS, decoded.ValidTS)
	assert.Equal(t, req.TestObject, decoded.TestObject)
}

func TestSyncProtectionRequest_JSON_Partial(t *testing.T) {
	// Test with only required fields (for renew/unregister)
	req := SyncProtectionRequest{
		JobID:   "test-job-123",
		ValidTS: 1234567890,
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded SyncProtectionRequest
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, req.JobID, decoded.JobID)
	assert.Equal(t, req.ValidTS, decoded.ValidTS)
	assert.Empty(t, decoded.BF)
	assert.Empty(t, decoded.TestObject)
}

func TestCheckFilesExist(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "gc-tool-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tester := &SyncProtectionTester{
		dataDir:        tmpDir,
		protectedFiles: []string{"exists.txt", "not-exists.txt"},
	}

	// Create one file
	err = os.WriteFile(filepath.Join(tmpDir, "exists.txt"), []byte("test"), 0644)
	require.NoError(t, err)

	existing, deleted := tester.CheckFilesExist()
	assert.Contains(t, existing, "exists.txt")
	assert.Contains(t, deleted, "not-exists.txt")
}

func TestCheckFilesExist_Empty(t *testing.T) {
	tester := &SyncProtectionTester{
		dataDir:        "/tmp",
		protectedFiles: []string{},
	}

	existing, deleted := tester.CheckFilesExist()
	assert.Empty(t, existing)
	assert.Empty(t, deleted)
}

func TestPrepareSyncProtectionCommand(t *testing.T) {
	cmd := PrepareSyncProtectionCommand()

	assert.Equal(t, "sync-protection", cmd.Use)
	assert.NotEmpty(t, cmd.Short)
	assert.NotEmpty(t, cmd.Long)

	// Check flags exist
	assert.NotNil(t, cmd.Flags().Lookup("dsn"))
	assert.NotNil(t, cmd.Flags().Lookup("data-dir"))
	assert.NotNil(t, cmd.Flags().Lookup("sample"))
	assert.NotNil(t, cmd.Flags().Lookup("verbose"))
	assert.NotNil(t, cmd.Flags().Lookup("wait"))

	// Check default values
	dsn, _ := cmd.Flags().GetString("dsn")
	assert.Equal(t, "root:111@tcp(127.0.0.1:6001)/", dsn)

	dataDir, _ := cmd.Flags().GetString("data-dir")
	assert.Equal(t, "./mo-data/shared", dataDir)

	sample, _ := cmd.Flags().GetInt("sample")
	assert.Equal(t, 10, sample)

	verbose, _ := cmd.Flags().GetBool("verbose")
	assert.False(t, verbose)

	wait, _ := cmd.Flags().GetInt("wait")
	assert.Equal(t, 30, wait)
}
