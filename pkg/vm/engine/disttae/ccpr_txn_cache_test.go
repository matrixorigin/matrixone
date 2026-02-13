// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockIteration represents a single transaction iteration for testing
type mockIteration struct {
	txnID       []byte
	objectNames []string
	commit      bool // true for commit, false for rollback
}

// newCleanFS creates a new clean memory file service for testing
func newCleanFS(t *testing.T) fileservice.FileService {
	fs, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)
	return fs
}

// writeObjectToFS writes dummy data to the file service
func writeObjectToFS(ctx context.Context, fs fileservice.FileService, objectName string) error {
	ioVec := fileservice.IOVector{
		FilePath: objectName,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   int64(len("test data")),
				Data:   []byte("test data"),
			},
		},
	}
	return fs.Write(ctx, ioVec)
}

// objectExistsInFS checks if an object exists in the file service
func objectExistsInFS(ctx context.Context, fs fileservice.FileService, objectName string) bool {
	_, err := fs.StatFile(ctx, objectName)
	return err == nil
}

// TestCCPRTxnCache_Iterations tests the CCPRTxnCache with mock iterations
func TestCCPRTxnCache_Iterations(t *testing.T) {
	ctx := context.Background()

	// Create a clean memory file service
	fs := newCleanFS(t)

	// Create a gc pool for async GC operations
	gcPool, err := ants.NewPool(10)
	require.NoError(t, err)
	defer gcPool.Release()

	// Create the cache
	cache := NewCCPRTxnCache(gcPool, fs)

	// Define test iterations
	// Test 1:
	// - Iteration 1: txn1, obj1, rollback → isNew should be true, fs should NOT have obj1 after GC
	// - Iteration 2: txn2, obj1, commit → isNew should be true, fs should have obj1
	// - Iteration 3: txn3, obj1, commit → isNew should be false (file already exists), fs should have obj1

	iterations := []mockIteration{
		{
			txnID:       []byte("txn1"),
			objectNames: []string{"obj1"},
			commit:      false, // rollback
		},
		{
			txnID:       []byte("txn2"),
			objectNames: []string{"obj1"},
			commit:      true, // commit
		},
		{
			txnID:       []byte("txn3"),
			objectNames: []string{"obj1"},
			commit:      true, // commit
		},
	}

	// Iteration 1: txn1, obj1, rollback
	t.Run("Iteration1_Rollback", func(t *testing.T) {
		iter := iterations[0]
		for _, objName := range iter.objectNames {
			isNew, err := cache.WriteObject(ctx, objName, iter.txnID)
			require.NoError(t, err)
			assert.True(t, isNew, "Iteration 1: obj1 should be new")

			if isNew {
				// Write to fs
				err = writeObjectToFS(ctx, fs, objName)
				require.NoError(t, err)
				cache.OnFileWritten(objName)
			}
		}

		// Rollback
		cache.OnTxnRollback(iter.txnID)

		// Wait for async GC to complete
		time.Sleep(100 * time.Millisecond)

		// Verify obj1 is NOT in fs (GC'd after rollback)
		assert.False(t, objectExistsInFS(ctx, fs, "obj1"), "Iteration 1: obj1 should be GC'd after rollback")
	})

	// Iteration 2: txn2, obj1, commit
	t.Run("Iteration2_Commit", func(t *testing.T) {
		iter := iterations[1]
		for _, objName := range iter.objectNames {
			isNew, err := cache.WriteObject(ctx, objName, iter.txnID)
			require.NoError(t, err)
			assert.True(t, isNew, "Iteration 2: obj1 should be new (was GC'd in iter 1)")

			if isNew {
				// Write to fs
				err = writeObjectToFS(ctx, fs, objName)
				require.NoError(t, err)
				cache.OnFileWritten(objName)
			}
		}

		// Commit
		cache.OnTxnCommit(iter.txnID)

		// Verify obj1 IS in fs (committed, not GC'd)
		assert.True(t, objectExistsInFS(ctx, fs, "obj1"), "Iteration 2: obj1 should exist in fs after commit")
	})

	// Iteration 3: txn3, obj1, commit
	t.Run("Iteration3_ExistingFile", func(t *testing.T) {
		iter := iterations[2]
		for _, objName := range iter.objectNames {
			isNew, err := cache.WriteObject(ctx, objName, iter.txnID)
			require.NoError(t, err)
			assert.False(t, isNew, "Iteration 3: obj1 should NOT be new (already exists in fs)")
		}

		// Commit (no-op since isNew was false, but call it anyway)
		cache.OnTxnCommit(iter.txnID)

		// Verify obj1 IS still in fs
		assert.True(t, objectExistsInFS(ctx, fs, "obj1"), "Iteration 3: obj1 should still exist in fs")
	})
}

// TestCCPRTxnCache_ConcurrentAllCommit tests 5 concurrent iterations with different txns,
// same object, all commit. Only one isNew should be true, fs should have the object.
func TestCCPRTxnCache_ConcurrentAllCommit(t *testing.T) {
	ctx := context.Background()

	// Create a clean memory file service
	fs := newCleanFS(t)

	// Create a gc pool for async GC operations
	gcPool, err := ants.NewPool(10)
	require.NoError(t, err)
	defer gcPool.Release()

	// Create the cache
	cache := NewCCPRTxnCache(gcPool, fs)

	const numIterations = 5
	const objectName = "concurrent_obj1"

	var isNewCount atomic.Int32
	var wg sync.WaitGroup
	var mu sync.Mutex // protect fs write

	// Launch 5 concurrent iterations
	for i := 0; i < numIterations; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			txnID := []byte(fmt.Sprintf("txn_%d", idx))

			isNew, err := cache.WriteObject(ctx, objectName, txnID)
			assert.NoError(t, err)

			if isNew {
				isNewCount.Add(1)
				// Write to fs (only one goroutine should do this)
				mu.Lock()
				if !objectExistsInFS(ctx, fs, objectName) {
					err = writeObjectToFS(ctx, fs, objectName)
					assert.NoError(t, err)
				}
				mu.Unlock()
				cache.OnFileWritten(objectName)
			}

			// Commit
			cache.OnTxnCommit(txnID)
		}(i)
	}

	wg.Wait()

	// Verify only one isNew was true
	assert.Equal(t, int32(1), isNewCount.Load(), "Only one iteration should have isNew=true")

	// Verify object exists in fs
	assert.True(t, objectExistsInFS(ctx, fs, objectName), "Object should exist in fs after all commits")
}

// TestCCPRTxnCache_ConcurrentAllRollback tests 5 concurrent iterations with different txns,
// same object, all rollback. fs should NOT have the object.
func TestCCPRTxnCache_ConcurrentAllRollback(t *testing.T) {
	ctx := context.Background()

	// Create a clean memory file service
	fs := newCleanFS(t)

	// Create a gc pool for async GC operations
	gcPool, err := ants.NewPool(10)
	require.NoError(t, err)
	defer gcPool.Release()

	// Create the cache
	cache := NewCCPRTxnCache(gcPool, fs)

	const numIterations = 5
	const objectName = "concurrent_obj2"

	var wg sync.WaitGroup
	var mu sync.Mutex // protect fs write

	// Launch 5 concurrent iterations
	for i := 0; i < numIterations; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			txnID := []byte(fmt.Sprintf("txn_%d", idx))

			isNew, err := cache.WriteObject(ctx, objectName, txnID)
			assert.NoError(t, err)

			if isNew {
				// Write to fs
				mu.Lock()
				if !objectExistsInFS(ctx, fs, objectName) {
					err = writeObjectToFS(ctx, fs, objectName)
					assert.NoError(t, err)
				}
				mu.Unlock()
				cache.OnFileWritten(objectName)
			}

			// Rollback
			cache.OnTxnRollback(txnID)
		}(i)
	}

	wg.Wait()

	// Wait for async GC to complete
	time.Sleep(200 * time.Millisecond)

	// Verify object does NOT exist in fs (all rolled back, GC'd)
	assert.False(t, objectExistsInFS(ctx, fs, objectName), "Object should NOT exist in fs after all rollbacks")
}

// TestCCPRTxnCache_ConcurrentOneCommitOthersRollback tests 5 concurrent iterations with different txns,
// same object, one commit and others rollback. fs should have the object.
func TestCCPRTxnCache_ConcurrentOneCommitOthersRollback(t *testing.T) {
	ctx := context.Background()

	// Create a clean memory file service
	fs := newCleanFS(t)

	// Create a gc pool for async GC operations
	gcPool, err := ants.NewPool(10)
	require.NoError(t, err)
	defer gcPool.Release()

	// Create the cache
	cache := NewCCPRTxnCache(gcPool, fs)

	const numIterations = 5
	const objectName = "concurrent_obj3"

	var wg sync.WaitGroup
	var mu sync.Mutex // protect fs write

	// Launch 5 concurrent iterations, only txn_0 will commit
	for i := 0; i < numIterations; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			txnID := []byte(fmt.Sprintf("txn_%d", idx))
			shouldCommit := (idx == 0) // Only txn_0 commits

			isNew, err := cache.WriteObject(ctx, objectName, txnID)
			assert.NoError(t, err)

			if isNew {
				// Write to fs
				mu.Lock()
				if !objectExistsInFS(ctx, fs, objectName) {
					err = writeObjectToFS(ctx, fs, objectName)
					assert.NoError(t, err)
				}
				mu.Unlock()
				cache.OnFileWritten(objectName)
			}

			if shouldCommit {
				cache.OnTxnCommit(txnID)
			} else {
				cache.OnTxnRollback(txnID)
			}
		}(i)
	}

	wg.Wait()

	// Wait for async GC to complete
	time.Sleep(200 * time.Millisecond)

	// Verify object exists in fs (one txn committed, so file should persist)
	assert.True(t, objectExistsInFS(ctx, fs, objectName), "Object should exist in fs (one commit keeps the file)")
}
