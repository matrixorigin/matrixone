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
	"github.com/tidwall/btree"
	"go.uber.org/zap"
)

// ItemEntry represents an entry in the items BTree, sorted by objectName
type ItemEntry struct {
	objectName string
	txnIDs     [][]byte
	isWriting  bool // true if the object is currently being written to fileservice
}

// Less compares two ItemEntry by objectName for BTree ordering
func (e ItemEntry) Less(other ItemEntry) bool {
	return e.objectName < other.objectName
}

// TxnIndexEntry represents an entry in the txnIndex BTree, sorted by txnID
// It stores all objectNames associated with a transaction for fast lookup
type TxnIndexEntry struct {
	txnID       []byte
	objectNames []string
}

// Less compares two TxnIndexEntry by txnID for BTree ordering
func (e TxnIndexEntry) Less(other TxnIndexEntry) bool {
	return bytes.Compare(e.txnID, other.txnID) < 0
}

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
	// items is a BTree mapping object_name to a list of txnIDs that reference this object
	// sorted by objectName, also tracks isWriting state
	items *btree.BTreeG[ItemEntry]

	// txnIndex is a BTree mapping txnID to a list of objectNames
	// sorted by txnID, for fast lookup of objects by transaction
	txnIndex *btree.BTreeG[TxnIndexEntry]

	// gcPool is the pool for async GC operations
	gcPool *ants.Pool
	// fs is the file service for deleting object files
	fs fileservice.FileService
}

// NewCCPRTxnCache creates a new CCPRTxnCache instance
func NewCCPRTxnCache(gcPool *ants.Pool, fs fileservice.FileService) *CCPRTxnCache {
	return &CCPRTxnCache{
		items:    btree.NewBTreeG(ItemEntry.Less),
		txnIndex: btree.NewBTreeG(TxnIndexEntry.Less),
		gcPool:   gcPool,
		fs:       fs,
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

	txnIDCopy := make([]byte, len(txnID))
	copy(txnIDCopy, txnID)

	// Check if object already exists in cache
	if entry, exists := c.items.Get(ItemEntry{objectName: objectName}); exists {
		// Object exists in cache, add txnID if not already present
		for _, id := range entry.txnIDs {
			if bytes.Equal(id, txnIDCopy) {
				return false, nil
			}
		}
		entry.txnIDs = append(entry.txnIDs, txnIDCopy)
		c.items.Set(entry)
		// Update txnIndex
		c.addObjectToTxnIndex(txnIDCopy, objectName)
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
	c.items.Set(ItemEntry{objectName: objectName, txnIDs: [][]byte{txnIDCopy}, isWriting: true})
	// Update txnIndex
	c.addObjectToTxnIndex(txnIDCopy, objectName)

	return true, nil
}

// addObjectToTxnIndex adds an objectName to the txnIndex for the given txnID
func (c *CCPRTxnCache) addObjectToTxnIndex(txnID []byte, objectName string) {
	if entry, exists := c.txnIndex.Get(TxnIndexEntry{txnID: txnID}); exists {
		// Check if objectName already exists
		for _, name := range entry.objectNames {
			if name == objectName {
				return
			}
		}
		entry.objectNames = append(entry.objectNames, objectName)
		c.txnIndex.Set(entry)
	} else {
		c.txnIndex.Set(TxnIndexEntry{txnID: txnID, objectNames: []string{objectName}})
	}
}

// OnFileWritten is called after the file has been successfully written to fileservice.
// It clears the isWriting flag for the object.
//
// Parameters:
//   - objectName: the name of the object that was written
func (c *CCPRTxnCache) OnFileWritten(objectName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, exists := c.items.Get(ItemEntry{objectName: objectName}); exists {
		entry.isWriting = false
		c.items.Set(entry)
	}
}

// OnTxnCommit is called when a transaction commits successfully.
// It removes all object entries associated with this transaction.
// Since the transaction committed successfully, the objects are persisted and
// no longer need cache tracking.
//
// Parameters:
//   - txnID: the ID of the committed transaction
func (c *CCPRTxnCache) OnTxnCommit(txnID []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First, find all objectNames for this txnID using txnIndex
	txnEntry, exists := c.txnIndex.Get(TxnIndexEntry{txnID: txnID})
	if !exists {
		return
	}

	// Delete all object entries for this transaction
	for _, objectName := range txnEntry.objectNames {
		c.items.Delete(ItemEntry{objectName: objectName})
	}

	// Remove the txnIndex entry
	c.txnIndex.Delete(txnEntry)
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

	// First, find all objectNames for this txnID using txnIndex
	txnEntry, exists := c.txnIndex.Get(TxnIndexEntry{txnID: txnID})
	if !exists {
		return
	}

	// Find objects to GC (those with no txnIDs left after removing this one)
	toGC := make([]string, 0)

	// Process each object by direct lookup
	for _, objectName := range txnEntry.objectNames {
		entry, found := c.items.Get(ItemEntry{objectName: objectName})
		if !found {
			// objectentry is deleted when the transaction commits
			continue
		}

		// Find and remove this txnID from the entry
		for i, id := range entry.txnIDs {
			if bytes.Equal(id, txnID) {
				if len(entry.txnIDs) == 1 {
					// This was the only txnID, need to GC the object
					toGC = append(toGC, objectName)
					c.items.Delete(entry)
				} else {
					// Remove this txnID from the list
					entry.txnIDs = append(entry.txnIDs[:i], entry.txnIDs[i+1:]...)
					c.items.Set(entry)
				}
				break
			}
		}
	}

	// Remove the txnIndex entry
	c.txnIndex.Delete(txnEntry)

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

	if err := c.fs.Delete(context.Background(), names...); err != nil {
		logutil.Warn("failed to delete CCPR objects",
			zap.Strings("objects", names),
			zap.Error(err),
		)
	}
}
