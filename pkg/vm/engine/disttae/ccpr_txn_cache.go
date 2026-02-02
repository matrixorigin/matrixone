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
	"bytes"
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// CCPRTxnCache is a cache for tracking CCPR (Cross-Cluster Publication Replication) objects
// and their associated transactions. It maintains a mapping from object names to transaction IDs.
//
// Thread-safety: All methods are thread-safe and use mutex for synchronization.
//
// Usage:
//  1. When writing an object in CCPR filter, call WriteObject to register the object with txnID
//  2. When a transaction commits successfully, call OnTxnCommit to remove the entry
//  3. When a transaction rolls back, call OnTxnRollback to remove the txnID;
//     if no more txnIDs are associated with the object, the object file will be GC'd
type CCPRTxnCache struct {
	mu sync.Mutex
	// items maps object_name to a list of txnIDs that reference this object
	// key: object name (string)
	// value: list of transaction IDs ([][]byte)
	items map[string][][]byte

	// writingObjects tracks objects that are currently being written to fileservice
	// key: object name (string)
	// value: txnID that is writing this object
	writingObjects map[string][]byte

	// gcPool is the pool for async GC operations
	gcPool *ants.Pool
	// fs is the file service for deleting object files
	fs fileservice.FileService
}

// NewCCPRTxnCache creates a new CCPRTxnCache instance
func NewCCPRTxnCache(gcPool *ants.Pool, fs fileservice.FileService) *CCPRTxnCache {
	return &CCPRTxnCache{
		items:          make(map[string][][]byte),
		writingObjects: make(map[string][]byte),
		gcPool:         gcPool,
		fs:             fs,
	}
}

// WriteObject checks if an object needs to be written and registers it with the given transaction ID.
// This method DOES NOT write the file - it only checks and registers in the cache.
// The caller is responsible for writing the file when isNewFile is true.
//
// If the object already exists in fileservice or is currently being written, returns isNewFile=false.
// If the object needs to be written, registers it and returns isNewFile=true.
// After the caller writes the file, it should call OnFileWritten to complete the registration.
//
// Parameters:
//   - ctx: context for the operation
//   - objectName: the name of the object being written
//   - txnID: the ID of the transaction writing this object
//
// Returns:
//   - isNewFile: true if file needs to be written, false if file already exists or is being written
//   - error: error if operation failed
func (c *CCPRTxnCache) WriteObject(ctx context.Context, objectName string, txnID []byte) (isNewFile bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.fs == nil {
		return false, moerr.NewInternalError(ctx, "fileservice is nil in CCPRTxnCache")
	}

	// Check if object is currently being written
	if _, isWriting := c.writingObjects[objectName]; isWriting {
		return false, nil
	}

	// Check if object already exists in cache (already written by uncommitted txn)
	if existingTxnIDs, exists := c.items[objectName]; exists {
		// Object exists in cache, add txnID if not already present
		txnIDCopy := make([]byte, len(txnID))
		copy(txnIDCopy, txnID)
		for _, id := range existingTxnIDs {
			if bytes.Equal(id, txnIDCopy) {
				return false, nil
			}
		}
		c.items[objectName] = append(existingTxnIDs, txnIDCopy)
		return false, nil
	}

	// Check if file already exists in fileservice
	_, err = c.fs.StatFile(ctx, objectName)
	if err == nil {
		// File exists in fileservice, no need to write
		return false, nil
	}
	if !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		// Other error occurred
		return false, moerr.NewInternalErrorf(ctx, "failed to stat object in fileservice: %v", err)
	}

	// File does not exist, mark as writing and register txnID
	txnIDCopy := make([]byte, len(txnID))
	copy(txnIDCopy, txnID)

	c.writingObjects[objectName] = txnIDCopy
	c.items[objectName] = [][]byte{txnIDCopy}

	return true, nil
}

// OnFileWritten is called after the file has been successfully written to fileservice.
// It removes the object from the writingObjects set.
//
// Parameters:
//   - objectName: the name of the object that was written
func (c *CCPRTxnCache) OnFileWritten(objectName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.writingObjects, objectName)
}

// OnTxnCommit is called when a transaction commits successfully.
// It removes the entire entry for all objects associated with this transaction.
//
// This method ensures atomicity by removing all entries for the given txnID.
//
// Parameters:
//   - txnID: the ID of the committed transaction
func (c *CCPRTxnCache) OnTxnCommit(txnID []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find and remove all entries where this txnID is the only one
	// For entries with multiple txnIDs, just remove this txnID
	toDelete := make([]string, 0)

	for objectName, txnIDs := range c.items {
		for i, id := range txnIDs {
			if bytes.Equal(id, txnID) {
				if len(txnIDs) == 1 {
					// This txnID is the only one, mark for deletion
					toDelete = append(toDelete, objectName)
				} else {
					// Remove this txnID from the list
					c.items[objectName] = append(txnIDs[:i], txnIDs[i+1:]...)
				}
				break
			}
		}
	}

	// Delete marked entries
	for _, objectName := range toDelete {
		delete(c.items, objectName)
	}
}

// OnTxnRollback is called when a transaction rolls back.
// It removes the txnID from all associated object entries.
// If an object entry has no more txnIDs after removal, the object file is GC'd.
//
// This method ensures atomicity by:
// 1. Removing txnID from all relevant entries
// 2. GC'ing objects that have no more associated txnIDs
//
// Parameters:
//   - txnID: the ID of the rolled back transaction
func (c *CCPRTxnCache) OnTxnRollback(txnID []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find objects to GC (those with no txnIDs left after removing this one)
	toGC := make([]string, 0)

	for objectName, txnIDs := range c.items {
		for i, id := range txnIDs {
			if bytes.Equal(id, txnID) {
				if len(txnIDs) == 1 {
					// This was the only txnID, need to GC the object
					toGC = append(toGC, objectName)
					delete(c.items, objectName)
				} else {
					// Remove this txnID from the list
					c.items[objectName] = append(txnIDs[:i], txnIDs[i+1:]...)
				}
				break
			}
		}
	}

	// GC objects asynchronously
	if len(toGC) > 0 {
		c.gcObjectsAsync(toGC)
	}
}

// gcObjectsAsync asynchronously deletes object files from the file service
func (c *CCPRTxnCache) gcObjectsAsync(objectNames []string) {
	if c.gcPool == nil || c.fs == nil || len(objectNames) == 0 {
		return
	}

	logutil.Info("CCPR-TXN-CACHE-GC",
		zap.Strings("objects", objectNames),
	)

	// Submit GC job to pool
	names := make([]string, len(objectNames))
	copy(names, objectNames)

	err := c.gcPool.Submit(func() {
		if err := c.fs.Delete(context.Background(), names...); err != nil {
			logutil.Warn("failed to delete CCPR objects",
				zap.Strings("objects", names),
				zap.Error(err),
			)
		}
	})
	if err != nil {
		logutil.Warn("failed to submit CCPR GC job",
			zap.Strings("objects", names),
			zap.Error(err),
		)
	}
}

// HasObject checks if an object exists in the cache or is currently being written
//
// Parameters:
//   - objectName: the name of the object to check
//
// Returns:
//   - bool: true if the object exists in the cache or is being written
func (c *CCPRTxnCache) HasObject(objectName string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.items[objectName]; exists {
		return true
	}
	if _, isWriting := c.writingObjects[objectName]; isWriting {
		return true
	}
	return false
}

// GetTxnIDs returns the list of txnIDs associated with an object
//
// Parameters:
//   - objectName: the name of the object
//
// Returns:
//   - [][]byte: the list of txnIDs, or nil if object doesn't exist
func (c *CCPRTxnCache) GetTxnIDs(objectName string) [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	txnIDs, exists := c.items[objectName]
	if !exists {
		return nil
	}
	// Return a copy to avoid data race
	result := make([][]byte, len(txnIDs))
	for i, id := range txnIDs {
		result[i] = make([]byte, len(id))
		copy(result[i], id)
	}
	return result
}

// Size returns the number of objects in the cache
func (c *CCPRTxnCache) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// Clear clears all entries in the cache without GC'ing objects
func (c *CCPRTxnCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string][][]byte)
	c.writingObjects = make(map[string][]byte)
}
