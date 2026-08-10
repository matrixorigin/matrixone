// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/stretchr/testify/require"
)

type controllableUnpublishedCleanupFS struct {
	fileservice.FileService
	rejectMarkerWrites    atomic.Bool
	ambiguousMarkerWrites atomic.Bool
	rejectObjectDelete    atomic.Bool
}

func (fs *controllableUnpublishedCleanupFS) Write(
	ctx context.Context,
	vector fileservice.IOVector,
) error {
	if strings.HasPrefix(vector.FilePath, "gc/unpublished/") &&
		fs.rejectMarkerWrites.Load() {
		return errors.New("injected marker write failure")
	}
	err := fs.FileService.Write(ctx, vector)
	if err == nil && strings.HasPrefix(vector.FilePath, "gc/unpublished/") &&
		fs.ambiguousMarkerWrites.Load() {
		return errors.New("injected post-persist marker write failure")
	}
	return err
}

func (fs *controllableUnpublishedCleanupFS) Delete(
	ctx context.Context,
	paths ...string,
) error {
	if fs.rejectObjectDelete.Load() {
		for _, path := range paths {
			if !strings.HasPrefix(path, "gc/unpublished/") {
				return errors.New("injected object delete failure")
			}
		}
	}
	return fs.FileService.Delete(ctx, paths...)
}

func TestCheckpointCleanerWriteAheadOwnership(t *testing.T) {
	ctx := context.Background()
	objectBase, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerBase, err := fileservice.NewMemoryFS(
		"local", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	objectFS := &controllableUnpublishedCleanupFS{FileService: objectBase}
	markerFS := &controllableUnpublishedCleanupFS{FileService: markerBase}
	cleaner := NewCheckpointCleaner(
		ctx, "", objectFS, nil, nil, WithUnpublishedCleanupFS(markerFS),
	).(*checkpointCleaner)
	cleaner.DisableGC()

	name := objectio.BuildObjectNameWithObjectID(
		func() *objectio.ObjectId { id := objectio.NewObjectid(); return &id }()).String()
	marker, err := cleaner.PrepareUnpublishedObject(ctx, 1, 2, true, name)
	require.NoError(t, err)
	require.NotEmpty(t, marker)
	require.NoError(t, objectBase.Write(ctx, fileservice.IOVector{
		FilePath: name,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte{1}}},
	}))

	// Active writers are fenced from a scheduled cleaner cycle.
	cleaner.unpublishedCleanupGeneration.Add(1)
	require.NoError(t, cleaner.Process(ctx, nil))
	_, err = objectBase.StatFile(ctx, name)
	require.NoError(t, err)

	cleaner.AbandonUnpublishedObject(name)
	require.NoError(t, cleaner.Process(ctx, nil))
	_, err = objectBase.StatFile(ctx, name)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	_, err = markerBase.StatFile(ctx, marker)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
}

func TestCheckpointCleanerRejectsBeforeObjectWriteWhenMarkerAdmissionFails(t *testing.T) {
	ctx := context.Background()
	objectFS, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerBase, err := fileservice.NewMemoryFS(
		"local", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerFS := &controllableUnpublishedCleanupFS{FileService: markerBase}
	markerFS.rejectMarkerWrites.Store(true)
	cleaner := NewCheckpointCleaner(
		ctx, "", objectFS, nil, nil, WithUnpublishedCleanupFS(markerFS),
	).(*checkpointCleaner)

	_, err = cleaner.PrepareUnpublishedObject(
		ctx, 1, 2, true, objectio.MockObjectName().String())
	require.ErrorContains(t, err, "injected marker write failure")
	cleaner.unpublishedCleanupOwnership.Lock()
	require.Zero(t, cleaner.unpublishedCleanupOwnership.pending)
	require.Empty(t, cleaner.unpublishedCleanupOwnership.active)
	cleaner.unpublishedCleanupOwnership.Unlock()
}

func TestCheckpointCleanerAcceptsMarkerOnlyAfterAmbiguousWriteIsVisible(t *testing.T) {
	ctx := context.Background()
	objectFS, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerBase, err := fileservice.NewMemoryFS(
		"local", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerFS := &controllableUnpublishedCleanupFS{FileService: markerBase}
	markerFS.ambiguousMarkerWrites.Store(true)
	cleaner := NewCheckpointCleaner(
		ctx, "", objectFS, nil, nil, WithUnpublishedCleanupFS(markerFS),
	).(*checkpointCleaner)

	marker, err := cleaner.PrepareUnpublishedObject(
		ctx, 1, 2, true, objectio.MockObjectName().String())
	require.NoError(t, err)
	_, err = markerBase.StatFile(ctx, marker)
	require.NoError(t, err)
	cleaner.unpublishedCleanupOwnership.Lock()
	require.Equal(t, 1, cleaner.unpublishedCleanupOwnership.pending)
	require.Len(t, cleaner.unpublishedCleanupOwnership.active, 1)
	require.Contains(t, cleaner.unpublishedCleanupOwnership.markers, marker)
	cleaner.unpublishedCleanupOwnership.Unlock()
}

func TestCheckpointCleanerCleanupSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	objectFS, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerFS, err := fileservice.NewMemoryFS(
		"local", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	first := NewCheckpointCleaner(
		ctx, "", objectFS, nil, nil, WithUnpublishedCleanupFS(markerFS),
	).(*checkpointCleaner)
	name := objectio.MockObjectName().String()
	_, err = first.PrepareUnpublishedObject(ctx, 1, 2, true, name)
	require.NoError(t, err)
	require.NoError(t, objectFS.Write(ctx, fileservice.IOVector{
		FilePath: name,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte{1}}},
	}))

	second := NewCheckpointCleaner(
		ctx, "", objectFS, nil, nil, WithUnpublishedCleanupFS(markerFS),
	).(*checkpointCleaner)
	second.unpublishedCleanupGeneration.Add(1)
	require.NoError(t, second.replayUnpublishedObjectCleanup(ctx))
	_, err = objectFS.StatFile(ctx, name)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
}

func TestCheckpointCleanerCleanupFailureDoesNotFailStopProcess(t *testing.T) {
	ctx := context.Background()
	objectBase, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	objectFS := &controllableUnpublishedCleanupFS{FileService: objectBase}
	markerFS, err := fileservice.NewMemoryFS(
		"local", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	cleaner := NewCheckpointCleaner(
		ctx, "", objectFS, nil, nil, WithUnpublishedCleanupFS(markerFS),
	).(*checkpointCleaner)
	cleaner.DisableGC()
	name := objectio.MockObjectName().String()
	_, err = cleaner.PrepareUnpublishedObject(ctx, 1, 2, true, name)
	require.NoError(t, err)
	require.NoError(t, objectBase.Write(ctx, fileservice.IOVector{
		FilePath: name,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte{1}}},
	}))
	cleaner.AbandonUnpublishedObject(name)
	objectFS.rejectObjectDelete.Store(true)

	// The cleanup error is logged and retained, but Process returns the ordinary
	// GC result instead of failing before the GCEnabled gate.
	require.NoError(t, cleaner.Process(ctx, nil))
	_, err = objectBase.StatFile(ctx, name)
	require.NoError(t, err)
}

func TestCheckpointCleanerCleanupAdmissionIsBounded(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	cleaner := NewCheckpointCleaner(ctx, "", fs, nil, nil).(*checkpointCleaner)
	cleaner.unpublishedCleanupOwnership.pending = unpublishedCleanupMaxPending

	_, err = cleaner.PrepareUnpublishedObject(
		ctx, 1, 2, true, objectio.MockObjectName().String())
	require.ErrorContains(t, err, "capacity")
}

func TestCheckpointCleanerMarkerAccountingIsIdentityBased(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	cleaner := NewCheckpointCleaner(ctx, "", fs, nil, nil).(*checkpointCleaner)
	nameA := objectio.MockObjectName().String()
	markerA, err := cleaner.PrepareUnpublishedObject(
		ctx, 1, 2, true, nameA)
	require.NoError(t, err)
	_, err = cleaner.PrepareUnpublishedObject(
		ctx, 1, 2, true, objectio.MockObjectName().String())
	require.NoError(t, err)
	require.NoError(t, cleaner.FinishUnpublishedObject(ctx, markerA, nameA))

	// A replay may have listed markerA before Finish removed it. Its later
	// completion must not decrement the independent marker's admission slot.
	cleaner.releaseUnpublishedObjectMarker(markerA)
	cleaner.unpublishedCleanupOwnership.Lock()
	require.Equal(t, 1, cleaner.unpublishedCleanupOwnership.pending)
	require.Len(t, cleaner.unpublishedCleanupOwnership.markers, 1)
	cleaner.unpublishedCleanupOwnership.Unlock()
}

func TestCheckpointCleanerOverflowStaysClosedUntilCompleteReplay(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	cleaner := NewCheckpointCleaner(ctx, "", fs, nil, nil).(*checkpointCleaner)
	cleaner.unpublishedCleanupOwnership.initialized = true
	cleaner.unpublishedCleanupOwnership.pending = unpublishedCleanupMaxPending + 1
	cleaner.unpublishedCleanupOwnership.overflow = true

	_, err = cleaner.PrepareUnpublishedObject(
		ctx, 1, 2, true, objectio.MockObjectName().String())
	require.ErrorContains(t, err, "capacity")
	cleaner.unpublishedCleanupGeneration.Add(1)
	require.NoError(t, cleaner.replayUnpublishedObjectCleanup(ctx))
	cleaner.unpublishedCleanupOwnership.Lock()
	require.False(t, cleaner.unpublishedCleanupOwnership.overflow)
	require.Zero(t, cleaner.unpublishedCleanupOwnership.pending)
	cleaner.unpublishedCleanupOwnership.Unlock()
}

func TestCheckpointCleanerReconstructsPendingBeyondOneReplayBatch(t *testing.T) {
	ctx := context.Background()
	objectFS, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerFS, err := fileservice.NewMemoryFS(
		"local", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	for i := 0; i < 1001; i++ {
		_, err = ioutil.RecordUnpublishedObjectCleanup(
			ctx,
			markerFS,
			ioutil.UnpublishedObject{File: fmt.Sprintf("object-%04d", i)},
		)
		require.NoError(t, err)
	}

	cleaner := NewCheckpointCleaner(
		ctx, "", objectFS, nil, nil, WithUnpublishedCleanupFS(markerFS),
	).(*checkpointCleaner)
	require.NoError(t, cleaner.initializeUnpublishedObjectOwnership(ctx))
	cleaner.unpublishedCleanupOwnership.Lock()
	require.Equal(t, 1001, cleaner.unpublishedCleanupOwnership.pending)
	cleaner.unpublishedCleanupOwnership.Unlock()

	cleaner.unpublishedCleanupGeneration.Add(1)
	require.NoError(t, cleaner.replayUnpublishedObjectCleanup(ctx))
	cleaner.unpublishedCleanupOwnership.Lock()
	require.Equal(t, 1, cleaner.unpublishedCleanupOwnership.pending)
	cleaner.unpublishedCleanupOwnership.Unlock()
	require.NoError(t, cleaner.replayUnpublishedObjectCleanup(ctx))
	cleaner.unpublishedCleanupOwnership.Lock()
	require.Zero(t, cleaner.unpublishedCleanupOwnership.pending)
	cleaner.unpublishedCleanupOwnership.Unlock()
}
