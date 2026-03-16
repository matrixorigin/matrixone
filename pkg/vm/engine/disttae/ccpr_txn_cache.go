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
	committed  bool // true if at least one txn has committed this object
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
// Lifecycle of an entry:
//  1. WriteObject: creates entry with isWriting=true (if new file) or appends txnID
//  2. OnFileWritten: clears isWriting; if already committed, deletes the entry
//  3. OnTxnCommit: marks committed=true; if not isWriting, deletes the entry
//  4. OnTxnRollback: removes txnID; if last txnID and not committed, GCs the file
type CCPRTxnCache struct {
	mu sync.Mutex
	// items is a BTree mapping object_name to a list of txnIDs that reference this object
	// sorted by objectName, also tracks isWriting and committed state
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
// After the caller writes the file, it should call OnFileWritten to complete the registration.
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
		return false, moerr.NewInternalErrorf(ctx, "failed to stat object in fileservice: %v", err)
	}

	// File does not exist, mark as writing and register txnID
	c.items.Set(ItemEntry{objectName: objectName, txnIDs: [][]byte{txnIDCopy}, isWriting: true})
	c.addObjectToTxnIndex(txnIDCopy, objectName)

	return true, nil
}

// addObjectToTxnIndex adds an objectName to the txnIndex for the given txnID
func (c *CCPRTxnCache) addObjectToTxnIndex(txnID []byte, objectName string) {
	if entry, exists := c.txnIndex.Get(TxnIndexEntry{txnID: txnID}); exists {
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
// It clears the isWriting flag. If the entry is already committed, deletes the entry
// since the file is now safely persisted and no longer needs cache tracking.
func (c *CCPRTxnCache) OnFileWritten(objectName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, exists := c.items.Get(ItemEntry{objectName: objectName})
	if !exists {
		return
	}
	entry.isWriting = false
	if entry.committed {
		// File written and committed — safe to remove the entire entry
		c.items.Delete(entry)
	} else {
		c.items.Set(entry)
	}
}

// OnTxnCommit is called when a transaction commits successfully.
// It marks the entry as committed and removes the entire entry if the file
// has already been written (!isWriting). If still writing, the entry stays
// until OnFileWritten cleans it up.
func (c *CCPRTxnCache) OnTxnCommit(txnID []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	txnEntry, exists := c.txnIndex.Get(TxnIndexEntry{txnID: txnID})
	if !exists {
		return
	}

	for _, objectName := range txnEntry.objectNames {
		entry, found := c.items.Get(ItemEntry{objectName: objectName})
		if !found {
			continue
		}
		entry.committed = true
		if !entry.isWriting {
			// File already written and now committed — remove the entire entry
			c.items.Delete(entry)
		} else {
			// Still writing — keep entry, OnFileWritten will clean up
			c.items.Set(entry)
		}
	}

	c.txnIndex.Delete(txnEntry)
}

// OnTxnRollback is called when a transaction rolls back.
// It removes the txnID from all associated object entries.
// If an object entry has no more txnIDs and was never committed, the file is GC'd.
// If the entry was committed (by another txn), the file is kept.
func (c *CCPRTxnCache) OnTxnRollback(txnID []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	txnEntry, exists := c.txnIndex.Get(TxnIndexEntry{txnID: txnID})
	if !exists {
		return
	}

	toGC := make([]string, 0)

	for _, objectName := range txnEntry.objectNames {
		entry, found := c.items.Get(ItemEntry{objectName: objectName})
		if !found {
			// Entry already deleted by a commit
			continue
		}

		for i, id := range entry.txnIDs {
			if bytes.Equal(id, txnID) {
				if len(entry.txnIDs) == 1 {
					// Last txnID — decide whether to GC
					if !entry.committed {
						toGC = append(toGC, objectName)
					}
					c.items.Delete(entry)
				} else {
					entry.txnIDs = append(entry.txnIDs[:i], entry.txnIDs[i+1:]...)
					c.items.Set(entry)
				}
				break
			}
		}
	}

	c.txnIndex.Delete(txnEntry)

	if len(toGC) > 0 {
		c.gcObjectsAsync(toGC)
	}
}

// gcObjectsAsync deletes object files from the file service
func (c *CCPRTxnCache) gcObjectsAsync(objectNames []string) {
	if c.gcPool == nil || c.fs == nil || len(objectNames) == 0 {
		return
	}

	logutil.Info("CCPR-TXN-CACHE-GC",
		zap.Strings("objects", objectNames),
	)

	names := make([]string, len(objectNames))
	copy(names, objectNames)

	if err := c.fs.Delete(context.Background(), names...); err != nil {
		logutil.Warn("failed to delete CCPR objects",
			zap.Strings("objects", names),
			zap.Error(err),
		)
	}
}
