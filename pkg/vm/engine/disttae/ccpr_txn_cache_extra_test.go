// Copyright 2024 Matrix Origin
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
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCCPRTxnCache_WriteObject_DuplicateTxnID tests that calling WriteObject
// with the same txnID for the same object returns isNew=false
func TestCCPRTxnCache_WriteObject_DuplicateTxnID(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()

	cache := NewCCPRTxnCache(gcPool, fs)

	txnID := []byte("txn-dup")
	isNew, err := cache.WriteObject(ctx, "obj_dup", txnID)
	require.NoError(t, err)
	assert.True(t, isNew)

	// Write the file
	require.NoError(t, writeObjectToFS(ctx, fs, "obj_dup"))
	cache.OnFileWritten("obj_dup")

	// Same txnID, same object → should be false (already in cache, committed path)
	cache.OnTxnCommit(txnID)

	// Now the entry is removed. A new txn writing the same object should see it in FS.
	isNew2, err := cache.WriteObject(ctx, "obj_dup", []byte("txn-dup2"))
	require.NoError(t, err)
	assert.False(t, isNew2) // file exists in FS
}

// TestCCPRTxnCache_OnFileWritten_NonExistent tests OnFileWritten for an object not in cache
func TestCCPRTxnCache_OnFileWritten_NonExistent(t *testing.T) {
	fs := newCleanFS(t)
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()

	cache := NewCCPRTxnCache(gcPool, fs)
	// Should not panic
	cache.OnFileWritten("nonexistent")
}

// TestCCPRTxnCache_OnTxnCommit_NonExistent tests OnTxnCommit for a txn not in cache
func TestCCPRTxnCache_OnTxnCommit_NonExistent(t *testing.T) {
	fs := newCleanFS(t)
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()

	cache := NewCCPRTxnCache(gcPool, fs)
	// Should not panic
	cache.OnTxnCommit([]byte("nonexistent-txn"))
}

// TestCCPRTxnCache_OnTxnRollback_NonExistent tests OnTxnRollback for a txn not in cache
func TestCCPRTxnCache_OnTxnRollback_NonExistent(t *testing.T) {
	fs := newCleanFS(t)
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()

	cache := NewCCPRTxnCache(gcPool, fs)
	// Should not panic
	cache.OnTxnRollback([]byte("nonexistent-txn"))
}

// TestCCPRTxnCache_WriteObject_NilFS tests WriteObject with nil fileservice
func TestCCPRTxnCache_WriteObject_NilFS(t *testing.T) {
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()

	cache := NewCCPRTxnCache(gcPool, nil)
	_, err = cache.WriteObject(context.Background(), "obj", []byte("txn"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fileservice is nil")
}

// TestCCPRTxnCache_WriteObject_SameTxnIDTwice tests duplicate txnID for same object in cache
func TestCCPRTxnCache_WriteObject_SameTxnIDTwice(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()

	cache := NewCCPRTxnCache(gcPool, fs)

	txnID := []byte("txn-same")
	isNew, err := cache.WriteObject(ctx, "obj_same", txnID)
	require.NoError(t, err)
	assert.True(t, isNew)

	// Same txnID again for same object (entry exists in cache, txnID already present)
	isNew2, err := cache.WriteObject(ctx, "obj_same", txnID)
	require.NoError(t, err)
	assert.False(t, isNew2)
}

// TestCCPRTxnCache_Rollback_MultiTxn tests rollback when multiple txns reference same object
func TestCCPRTxnCache_Rollback_MultiTxn(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()

	cache := NewCCPRTxnCache(gcPool, fs)

	// txn1 writes obj
	isNew, err := cache.WriteObject(ctx, "obj_multi", []byte("txn1"))
	require.NoError(t, err)
	assert.True(t, isNew)
	require.NoError(t, writeObjectToFS(ctx, fs, "obj_multi"))
	cache.OnFileWritten("obj_multi")

	// txn2 also references obj (entry exists, different txnID)
	isNew2, err := cache.WriteObject(ctx, "obj_multi", []byte("txn2"))
	require.NoError(t, err)
	assert.False(t, isNew2)

	// Rollback txn1 → should NOT GC because txn2 still references it
	cache.OnTxnRollback([]byte("txn1"))
	time.Sleep(50 * time.Millisecond)
	assert.True(t, objectExistsInFS(ctx, fs, "obj_multi"))

	// Rollback txn2 → last txn, should GC
	cache.OnTxnRollback([]byte("txn2"))
	time.Sleep(100 * time.Millisecond)
	assert.False(t, objectExistsInFS(ctx, fs, "obj_multi"))
}

// TestIsCCPRTxn tests the IsCCPRTxn method
func TestIsCCPRTxn(t *testing.T) {
	txn := &Transaction{}
	assert.False(t, txn.IsCCPRTxn())
	txn.SetCCPRTxn()
	assert.True(t, txn.IsCCPRTxn())
}

// TestSetGetCCPRTaskID tests SetCCPRTaskID and GetCCPRTaskID
func TestSetGetCCPRTaskID(t *testing.T) {
	txn := &Transaction{}
	assert.Empty(t, txn.GetCCPRTaskID())
	txn.SetCCPRTaskID("task-123")
	assert.Equal(t, "task-123", txn.GetCCPRTaskID())
}
