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
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type rejectingStatFileService struct {
	fileservice.FileService
	stats int
}

type deadlineDeleteFileService struct {
	fileservice.FileService
	hasDeadline atomic.Bool
}

func TestEngineGetPrimaryTNShardIDUsesWorkspaceSelection(t *testing.T) {
	eng := &Engine{}
	require.Zero(t, eng.GetPrimaryTNShardID(nil))
	require.Zero(t, eng.GetPrimaryTNShardID(&Transaction{}))

	workspace := &Transaction{tnStores: []DNStore{{
		Shards: []metadata.TNShard{{TNShardRecord: metadata.TNShardRecord{
			ShardID: 7,
		}}},
	}}}
	require.Equal(t, uint64(7), eng.GetPrimaryTNShardID(workspace))
}

type blockingDeleteFileService struct {
	fileservice.FileService
	entered chan struct{}
	release chan struct{}
}

type blockingMarkerWriteFileService struct {
	fileservice.FileService
	entered chan struct{}
	release chan struct{}
}

type failingObjectDeleteFileService struct {
	fileservice.FileService
	fail bool
}

type observingMarkerDeleteFileService struct {
	fileservice.FileService
	markerDeleted chan struct{}
}

func (fs *observingMarkerDeleteFileService) Delete(
	ctx context.Context,
	paths ...string,
) error {
	marker := false
	for _, path := range paths {
		if strings.HasPrefix(path, "gc/ccpr-unpublished/") {
			marker = true
		}
	}
	err := fs.FileService.Delete(ctx, paths...)
	if err == nil && marker {
		select {
		case fs.markerDeleted <- struct{}{}:
		default:
		}
	}
	return err
}

func (fs *failingObjectDeleteFileService) Delete(
	ctx context.Context,
	paths ...string,
) error {
	if fs.fail {
		for _, path := range paths {
			if !strings.HasPrefix(path, "gc/ccpr-unpublished/") {
				return errors.New("injected object delete failure")
			}
		}
	}
	return fs.FileService.Delete(ctx, paths...)
}

func (fs *blockingDeleteFileService) Delete(
	ctx context.Context,
	paths ...string,
) error {
	select {
	case fs.entered <- struct{}{}:
	default:
	}
	select {
	case <-fs.release:
		return fs.FileService.Delete(ctx, paths...)
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (fs *blockingMarkerWriteFileService) Write(
	ctx context.Context,
	vector fileservice.IOVector,
) error {
	if !strings.HasPrefix(vector.FilePath, "gc/ccpr-unpublished/") {
		return fs.FileService.Write(ctx, vector)
	}
	select {
	case fs.entered <- struct{}{}:
	default:
	}
	select {
	case <-fs.release:
		return fs.FileService.Write(ctx, vector)
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (fs *deadlineDeleteFileService) Delete(
	ctx context.Context,
	paths ...string,
) error {
	_, hasDeadline := ctx.Deadline()
	fs.hasDeadline.Store(hasDeadline)
	return fs.FileService.Delete(ctx, paths...)
}

func (fs *rejectingStatFileService) StatFile(
	context.Context,
	string,
) (*fileservice.DirEntry, error) {
	fs.stats++
	return nil, errors.New("unexpected StatFile")
}

func TestCCPRTxnCacheWriteNewObjectSkipsRemoteStat(t *testing.T) {
	ctx := context.Background()
	fs := &rejectingStatFileService{FileService: newCleanFS(t)}
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()
	cache := NewCCPRTxnCache(gcPool, fs)

	object := testCCPRUnpublishedObject()
	require.NoError(t, cache.WriteNewObject(ctx, object, []byte("txn")))
	require.Zero(t, fs.stats)
	require.Error(t, cache.WriteNewObject(ctx, object, []byte("other")))
	require.Zero(t, fs.stats)
}

func TestCCPRTxnCacheUniqueObjectOwnershipSurvivesCacheRestart(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()

	name := objectio.MockObjectName().String()
	cache := NewCCPRTxnCache(gcPool, fs)
	require.NoError(t, cache.WriteNewObject(ctx, ioutil.UnpublishedObject{
		File:                  name,
		DBID:                  1,
		TableID:               2,
		IsTombstone:           true,
		TNShardID:             3,
		SyncProtectionJobID:   "publication-job",
		SyncProtectionValidTS: time.Now().Add(-time.Hour).UnixNano(),
	}, []byte("txn")))
	require.NoError(t, writeObjectToFS(ctx, fs, name))
	cache.OnFileWritten(name)

	// Model a CN restart before the object stats reach the transaction. The
	// process-local cache is gone, so only the write-ahead marker can retain
	// the exact object identity for restart replay.
	cache = NewCCPRTxnCache(gcPool, fs)
	_ = cache
	replayed, _, _, remaining, err :=
		ioutil.ReplayCCPRUnpublishedObjectCleanupPageFrom(
			ctx,
			fs,
			func(ioutil.UnpublishedObject) (
				ioutil.UnpublishedObjectCleanupDecision, error,
			) {
				return ioutil.DeleteUnpublishedObject, nil
			},
			"",
			10,
		)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)
	require.False(t, remaining)
	require.False(t, objectExistsInFS(ctx, fs, name))
}

func TestCCPRTxnCacheRollbackDeleteFailureRetainsDurableOwner(t *testing.T) {
	ctx := context.Background()
	base := newCleanFS(t)
	fs := &failingObjectDeleteFileService{FileService: base, fail: true}
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()

	object := testCCPRUnpublishedObject()
	cache := NewCCPRTxnCache(gcPool, fs)
	require.NoError(t, cache.WriteNewObject(ctx, object, []byte("txn")))
	require.NoError(t, writeObjectToFS(ctx, fs, object.File))
	cache.OnFileWritten(object.File)
	cache.OnTxnRollback([]byte("txn"))
	require.True(t, objectExistsInFS(ctx, fs, object.File))

	fs.fail = false
	replayed, inspected, _, remaining, err :=
		ioutil.ReplayCCPRUnpublishedObjectCleanupPageFrom(
			ctx,
			fs,
			func(ioutil.UnpublishedObject) (
				ioutil.UnpublishedObjectCleanupDecision, error,
			) {
				return ioutil.DeleteUnpublishedObject, nil
			},
			"",
			10,
		)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)
	require.Equal(t, 1, inspected,
		"failed rollback must retain the exact durable cleanup owner")
	require.False(t, remaining)
	require.False(t, objectExistsInFS(ctx, fs, object.File))
}

func TestCCPRTxnCacheRollbackOfUncertainWriteRetainsDurableOwner(t *testing.T) {
	ctx := context.Background()
	fs := &observingMarkerDeleteFileService{
		FileService:   newCleanFS(t),
		markerDeleted: make(chan struct{}, 1),
	}
	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()

	object := testCCPRUnpublishedObject()
	cache := NewCCPRTxnCache(gcPool, fs)
	require.NoError(t, cache.WriteNewObject(ctx, object, []byte("txn")))
	// No OnFileWritten models both an in-flight Sync and an ambiguous Sync
	// error. In either case rollback cannot prove that a preceding delete wins.
	cache.OnTxnRollback([]byte("txn"))
	barrier := make(chan struct{})
	require.NoError(t, gcPool.Submit(func() { close(barrier) }))
	select {
	case <-barrier:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup pool barrier did not finish")
	}
	select {
	case <-fs.markerDeleted:
		t.Fatal("uncertain write released its durable cleanup marker")
	default:
	}

	replayed, inspected, _, _, err :=
		ioutil.ReplayCCPRUnpublishedObjectCleanupPageFrom(
			ctx,
			fs,
			func(ioutil.UnpublishedObject) (
				ioutil.UnpublishedObjectCleanupDecision, error,
			) {
				return ioutil.ReleaseUnpublishedObjectCleanup, nil
			},
			"",
			10,
		)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)
	require.Equal(t, 1, inspected)
}

func TestCCPRTxnCacheRollbackDuringMarkerWriteRetainsDurableOwner(t *testing.T) {
	ctx := context.Background()
	fs := &blockingMarkerWriteFileService{
		FileService: newCleanFS(t),
		entered:     make(chan struct{}, 1),
		release:     make(chan struct{}),
	}
	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()
	cache := NewCCPRTxnCache(gcPool, fs)
	object := testCCPRUnpublishedObject()

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- cache.WriteNewObject(ctx, object, []byte("txn"))
	}()
	select {
	case <-fs.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("marker write did not start")
	}
	cache.OnTxnRollback([]byte("txn"))
	close(fs.release)
	select {
	case err = <-writeDone:
		require.ErrorContains(t, err, "lost its cache reservation")
	case <-time.After(5 * time.Second):
		t.Fatal("marker writer did not observe the terminal reservation")
	}

	replayed, inspected, _, _, err :=
		ioutil.ReplayCCPRUnpublishedObjectCleanupPageFrom(
			ctx,
			fs,
			func(ioutil.UnpublishedObject) (
				ioutil.UnpublishedObjectCleanupDecision, error,
			) {
				return ioutil.ReleaseUnpublishedObjectCleanup, nil
			},
			"",
			10,
		)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)
	require.Equal(t, 1, inspected,
		"rollback must not lose a marker that finishes concurrently")
}

func TestCCPRTxnCacheCommitReleasesDurableOwner(t *testing.T) {
	ctx := context.Background()
	fs := &observingMarkerDeleteFileService{
		FileService:   newCleanFS(t),
		markerDeleted: make(chan struct{}, 1),
	}
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()

	object := testCCPRUnpublishedObject()
	cache := NewCCPRTxnCache(gcPool, fs)
	require.NoError(t, cache.WriteNewObject(ctx, object, []byte("txn")))
	require.NoError(t, writeObjectToFS(ctx, fs, object.File))
	cache.OnFileWritten(object.File)
	cache.OnTxnCommit([]byte("txn"))
	select {
	case <-fs.markerDeleted:
	case <-time.After(5 * time.Second):
		t.Fatal("committed CCPR marker was not released")
	}
	require.True(t, objectExistsInFS(ctx, fs, object.File),
		"commit must release only the marker, not the catalog-owned object")
	_, inspected, _, _, err := ioutil.ReplayCCPRUnpublishedObjectCleanupPageFrom(
		ctx, fs, nil, "", 10)
	require.NoError(t, err)
	require.Zero(t, inspected)
}

func TestCCPRTxnCacheCommitMarkerCleanupDoesNotBlock(t *testing.T) {
	ctx := context.Background()
	fs := &blockingDeleteFileService{
		FileService: newCleanFS(t),
		entered:     make(chan struct{}, 1),
		release:     make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(fs.release) }) }
	t.Cleanup(release)
	gcPool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer gcPool.Release()
	cache := NewCCPRTxnCache(gcPool, fs)
	object := testCCPRUnpublishedObject()
	require.NoError(t, cache.WriteNewObject(ctx, object, []byte("txn")))
	require.NoError(t, writeObjectToFS(ctx, fs, object.File))
	cache.OnFileWritten(object.File)

	commitDone := make(chan struct{})
	go func() {
		cache.OnTxnCommit([]byte("txn"))
		close(commitDone)
	}()
	select {
	case <-commitDone:
	case <-time.After(time.Second):
		t.Fatal("marker deletion blocked committed transaction completion")
	}
	select {
	case <-fs.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("marker deletion did not start")
	}
	release()
	require.Eventually(t, func() bool {
		return !cache.markerCleanupRunning.Load()
	}, 5*time.Second, time.Millisecond)
}

func TestCCPRTxnCacheUnknownResultRetainsDurableOwner(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()

	object := testCCPRUnpublishedObject()
	cache := NewCCPRTxnCache(gcPool, fs)
	require.NoError(t, cache.WriteNewObject(ctx, object, []byte("txn")))
	require.NoError(t, writeObjectToFS(ctx, fs, object.File))
	cache.OnFileWritten(object.File)
	cache.OnTxnUnknownResult([]byte("txn"))

	replayed, inspected, _, _, err :=
		ioutil.ReplayCCPRUnpublishedObjectCleanupPageFrom(
			ctx,
			fs,
			func(ioutil.UnpublishedObject) (
				ioutil.UnpublishedObjectCleanupDecision, error,
			) {
				return ioutil.ReleaseUnpublishedObjectCleanup, nil
			},
			"",
			10,
		)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)
	require.Equal(t, 1, inspected,
		"unknown commit results must leave catalog-aware replay ownership")
	require.True(t, objectExistsInFS(ctx, fs, object.File))
}

func TestCCPRTxnCacheRollbackCleanupIsBounded(t *testing.T) {
	ctx := context.Background()
	fs := &deadlineDeleteFileService{FileService: newCleanFS(t)}
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()
	cache := NewCCPRTxnCache(gcPool, fs)

	object := testCCPRUnpublishedObject()
	require.NoError(t, cache.WriteNewObject(ctx, object, []byte("txn")))
	require.NoError(t, writeObjectToFS(ctx, fs, object.File))
	cache.OnFileWritten(object.File)
	cache.OnTxnRollback([]byte("txn"))
	require.True(t, fs.hasDeadline.Load())
}

func TestCCPRTxnCacheRollbackDeleteDoesNotHoldCacheMutex(t *testing.T) {
	ctx := context.Background()
	fs := &blockingDeleteFileService{
		FileService: newCleanFS(t),
		entered:     make(chan struct{}, 1),
		release:     make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(fs.release) }) }
	t.Cleanup(release)
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()
	cache := NewCCPRTxnCache(gcPool, fs)

	rollbackObject := testCCPRUnpublishedObject()
	require.NoError(t, cache.WriteNewObject(ctx, rollbackObject, []byte("txn")))
	require.NoError(t, writeObjectToFS(ctx, fs, rollbackObject.File))
	cache.OnFileWritten(rollbackObject.File)
	rollbackDone := make(chan struct{})
	go func() {
		cache.OnTxnRollback([]byte("txn"))
		close(rollbackDone)
	}()
	select {
	case <-fs.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("rollback cleanup did not enter object deletion")
	}

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- cache.WriteNewObject(
			ctx, testCCPRUnpublishedObject(), []byte("other"))
	}()
	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("remote rollback delete held the global CCPR cache mutex")
	}
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	isNew, err := cache.WriteObject(
		canceledCtx, rollbackObject.File, []byte("same-name"))
	require.ErrorIs(t, err, context.Canceled,
		"the deleting generation must not be adopted by a same-name writer")
	require.False(t, isNew)

	release()
	select {
	case <-rollbackDone:
	case <-time.After(5 * time.Second):
		t.Fatal("rollback cleanup did not finish after delete was released")
	}
	isNew, err = cache.WriteObject(
		ctx, rollbackObject.File, []byte("same-name"))
	require.NoError(t, err)
	require.True(t, isNew,
		"the same name may be rewritten only after the delete fence closes")
}

func testCCPRUnpublishedObject() ioutil.UnpublishedObject {
	return ioutil.UnpublishedObject{
		File:                  objectio.MockObjectName().String(),
		DBID:                  1,
		TableID:               2,
		IsTombstone:           true,
		TNShardID:             3,
		SyncProtectionJobID:   "publication-job",
		SyncProtectionValidTS: time.Now().UnixNano(),
	}
}

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

func TestCCPRTxnCache_OnTxnUnknownResultRemovesTrackingWithoutGC(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()

	cache := NewCCPRTxnCache(gcPool, fs)
	txnID := []byte("txn-unknown")
	otherTxnID := []byte("txn-other")
	objectName := "obj_unknown"

	isNew, err := cache.WriteObject(ctx, objectName, txnID)
	require.NoError(t, err)
	require.True(t, isNew)
	require.NoError(t, writeObjectToFS(ctx, fs, objectName))
	cache.OnFileWritten(objectName)

	isNew, err = cache.WriteObject(ctx, objectName, otherTxnID)
	require.NoError(t, err)
	require.False(t, isNew)

	cache.OnTxnUnknownResult(txnID)

	cache.mu.Lock()
	_, hasObject := cache.items.Get(ItemEntry{objectName: objectName})
	_, hasUnknownTxn := cache.txnIndex.Get(TxnIndexEntry{txnID: txnID})
	_, hasOtherTxn := cache.txnIndex.Get(TxnIndexEntry{txnID: otherTxnID})
	cache.mu.Unlock()

	assert.False(t, hasObject)
	assert.False(t, hasUnknownTxn)
	assert.False(t, hasOtherTxn)
	assert.True(t, objectExistsInFS(ctx, fs, objectName))

	cache.OnTxnRollback(otherTxnID)
	time.Sleep(50 * time.Millisecond)
	assert.True(t, objectExistsInFS(ctx, fs, objectName))
}

func TestTransactionFinalizeCommitUnknownCleansCCPRCache(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	defer gcPool.Release()

	cache := NewCCPRTxnCache(gcPool, fs)
	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()
	colexec.NewServer("")

	objectName := "obj_unknown_finalize"
	txnID := txnOp.Txn().ID
	isNew, err := cache.WriteObject(ctx, objectName, txnID)
	require.NoError(t, err)
	require.True(t, isNew)
	require.NoError(t, writeObjectToFS(ctx, fs, objectName))
	cache.OnFileWritten(objectName)

	txn := &Transaction{
		engine:    &Engine{ccprTxnCache: cache},
		op:        txnOp,
		isCCPRTxn: true,
	}
	txn.FinalizeCommitWithUnknownResult(ctx)

	cache.mu.Lock()
	_, hasObject := cache.items.Get(ItemEntry{objectName: objectName})
	_, hasTxn := cache.txnIndex.Get(TxnIndexEntry{txnID: txnID})
	cache.mu.Unlock()

	assert.False(t, hasObject)
	assert.False(t, hasTxn)
	assert.True(t, objectExistsInFS(ctx, fs, objectName))
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
