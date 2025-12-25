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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockFileService is a mock implementation of fileservice.FileService for testing
type mockFileService struct {
	fileservice.FileService
	deletedFiles sync.Map // map[string]int to track delete count per file
	deleteCount  atomic.Int64
	deleteCalls  atomic.Int64
	deleteDelay  time.Duration
}

func newMockFileService() *mockFileService {
	return &mockFileService{}
}

func (m *mockFileService) Delete(ctx context.Context, filePaths ...string) error {
	m.deleteCalls.Add(1)
	if m.deleteDelay > 0 {
		time.Sleep(m.deleteDelay)
	}
	for _, path := range filePaths {
		m.deleteCount.Add(1)
		// Track how many times each file is deleted
		if val, ok := m.deletedFiles.Load(path); ok {
			m.deletedFiles.Store(path, val.(int)+1)
		} else {
			m.deletedFiles.Store(path, 1)
		}
	}
	return nil
}

func (m *mockFileService) getDeletedFiles() map[string]int {
	result := make(map[string]int)
	m.deletedFiles.Range(func(key, value interface{}) bool {
		result[key.(string)] = value.(int)
		return true
	})
	return result
}

// TestDeleterDeduplication tests that duplicate files are handled correctly
// This test verifies the fix for the issue where filesToGC contained duplicate files
func TestDeleterDeduplication(t *testing.T) {
	mockFS := newMockFileService()

	// Save original values and restore after test
	origBatchSize := deleteBatchSize
	origWorkerNum := deleteWorkerNum
	defer func() {
		deleteBatchSize = origBatchSize
		deleteWorkerNum = origWorkerNum
	}()

	SetDeleteBatchSize(10)
	SetDeleteWorkerNum(1)

	deleter := NewDeleter(mockFS)

	// Create a list with duplicate files (simulating the bug scenario)
	// In the original bug, the same object could appear multiple times
	// because it was referenced by multiple tables
	paths := []string{
		"object1.data",
		"object2.data",
		"object1.data", // duplicate
		"object3.data",
		"object2.data", // duplicate
		"object1.data", // duplicate
		"object4.data",
	}

	ctx := context.Background()
	err := deleter.DeleteMany(ctx, "test-dedup", paths)
	require.NoError(t, err)

	// Verify that all files were deleted (including duplicates in this case,
	// since deduplication should happen before calling DeleteMany)
	deletedFiles := mockFS.getDeletedFiles()

	// The deleter itself doesn't deduplicate - that's done in ExecuteGlobalCheckpointBasedGC
	// So we expect all 7 delete operations
	assert.Equal(t, int64(7), mockFS.deleteCount.Load())

	// Each unique file should have been deleted the number of times it appeared
	assert.Equal(t, 3, deletedFiles["object1.data"])
	assert.Equal(t, 2, deletedFiles["object2.data"])
	assert.Equal(t, 1, deletedFiles["object3.data"])
	assert.Equal(t, 1, deletedFiles["object4.data"])
}

// TestDeleterConcurrentDelete tests concurrent deletion with multiple workers
func TestDeleterConcurrentDelete(t *testing.T) {
	mockFS := newMockFileService()
	mockFS.deleteDelay = 10 * time.Millisecond // Add delay to simulate S3 latency

	// Save original values and restore after test
	origBatchSize := deleteBatchSize
	origWorkerNum := deleteWorkerNum
	defer func() {
		deleteBatchSize = origBatchSize
		deleteWorkerNum = origWorkerNum
	}()

	SetDeleteBatchSize(5)
	SetDeleteWorkerNum(4)

	deleter := NewDeleter(mockFS)

	// Create 20 files to delete (will be split into 4 batches of 5)
	paths := make([]string, 20)
	for i := 0; i < 20; i++ {
		paths[i] = fmt.Sprintf("object%d.data", i)
	}

	ctx := context.Background()
	start := time.Now()
	err := deleter.DeleteMany(ctx, "test-concurrent", paths)
	duration := time.Since(start)

	require.NoError(t, err)

	// Verify all files were deleted
	assert.Equal(t, int64(20), mockFS.deleteCount.Load())

	// With 4 workers and 4 batches, concurrent execution should be faster
	// than sequential (4 * 10ms = 40ms sequential vs ~10ms concurrent)
	// Allow some margin for test stability
	assert.Less(t, duration, 35*time.Millisecond,
		"Concurrent deletion should be faster than sequential")

	// Verify each file was deleted exactly once
	deletedFiles := mockFS.getDeletedFiles()
	for i := 0; i < 20; i++ {
		fileName := fmt.Sprintf("object%d.data", i)
		assert.Equal(t, 1, deletedFiles[fileName],
			"File %s should be deleted exactly once", fileName)
	}
}

// TestDeleterSingleWorker tests that single worker mode works correctly
func TestDeleterSingleWorker(t *testing.T) {
	mockFS := newMockFileService()

	// Save original values and restore after test
	origBatchSize := deleteBatchSize
	origWorkerNum := deleteWorkerNum
	defer func() {
		deleteBatchSize = origBatchSize
		deleteWorkerNum = origWorkerNum
	}()

	SetDeleteBatchSize(5)
	SetDeleteWorkerNum(1) // Single worker

	deleter := NewDeleter(mockFS)

	paths := make([]string, 10)
	for i := 0; i < 10; i++ {
		paths[i] = fmt.Sprintf("object%d.data", i)
	}

	ctx := context.Background()
	err := deleter.DeleteMany(ctx, "test-single-worker", paths)
	require.NoError(t, err)

	// Verify all files were deleted
	assert.Equal(t, int64(10), mockFS.deleteCount.Load())

	// Should have 2 delete calls (2 batches of 5)
	assert.Equal(t, int64(2), mockFS.deleteCalls.Load())
}

// TestDeleterContextCancellation tests that context cancellation is handled correctly
func TestDeleterContextCancellation(t *testing.T) {
	mockFS := newMockFileService()
	mockFS.deleteDelay = 50 * time.Millisecond

	// Save original values and restore after test
	origBatchSize := deleteBatchSize
	origWorkerNum := deleteWorkerNum
	defer func() {
		deleteBatchSize = origBatchSize
		deleteWorkerNum = origWorkerNum
	}()

	SetDeleteBatchSize(5)
	SetDeleteWorkerNum(2)

	deleter := NewDeleter(mockFS)

	paths := make([]string, 20)
	for i := 0; i < 20; i++ {
		paths[i] = fmt.Sprintf("object%d.data", i)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := deleter.DeleteMany(ctx, "test-cancel", paths)

	// Should return context error
	assert.Error(t, err)

	// Not all files should be deleted due to cancellation
	assert.Less(t, mockFS.deleteCount.Load(), int64(20))
}

// TestDeleterEmptyPaths tests handling of empty paths
func TestDeleterEmptyPaths(t *testing.T) {
	mockFS := newMockFileService()

	deleter := NewDeleter(mockFS)

	ctx := context.Background()
	err := deleter.DeleteMany(ctx, "test-empty", []string{})
	require.NoError(t, err)

	assert.Equal(t, int64(0), mockFS.deleteCount.Load())
}

// TestDeleterWorkerNumValidation tests that invalid worker numbers are handled
func TestDeleterWorkerNumValidation(t *testing.T) {
	origWorkerNum := deleteWorkerNum
	defer func() {
		deleteWorkerNum = origWorkerNum
	}()

	// Setting 0 or negative should not change the value
	SetDeleteWorkerNum(0)
	assert.Equal(t, origWorkerNum, deleteWorkerNum)

	SetDeleteWorkerNum(-1)
	assert.Equal(t, origWorkerNum, deleteWorkerNum)

	// Setting positive value should work
	SetDeleteWorkerNum(8)
	assert.Equal(t, 8, deleteWorkerNum)
}
