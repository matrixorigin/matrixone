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
	rejectMarkerStats     atomic.Bool
	rejectMarkerDeletes   atomic.Bool
	ambiguousMarkerWrites atomic.Bool
	rejectObjectDelete    atomic.Bool
	markerDeleteCalls     atomic.Int64
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
	for _, path := range paths {
		if strings.HasPrefix(path, "gc/unpublished/") {
			fs.markerDeleteCalls.Add(1)
			if fs.rejectMarkerDeletes.Load() {
				return errors.New("injected marker delete failure")
			}
		}
	}
	if fs.rejectObjectDelete.Load() {
		for _, path := range paths {
			if !strings.HasPrefix(path, "gc/unpublished/") {
				return errors.New("injected object delete failure")
			}
		}
	}
	return fs.FileService.Delete(ctx, paths...)
}

func (fs *controllableUnpublishedCleanupFS) StatFile(
	ctx context.Context,
	path string,
) (*fileservice.DirEntry, error) {
	if strings.HasPrefix(path, "gc/unpublished/") &&
		fs.rejectMarkerStats.Load() {
		return nil, errors.New("injected marker stat failure")
	}
	return fs.FileService.StatFile(ctx, path)
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

	name := objectio.MockObjectName().String()
	_, err = cleaner.PrepareUnpublishedObject(ctx, 1, 2, true, name)
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

func TestCheckpointCleanerReconcilesAmbiguousMarkerWithoutObjectWrite(t *testing.T) {
	ctx := context.Background()
	objectFS, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerBase, err := fileservice.NewMemoryFS(
		"local", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerFS := &controllableUnpublishedCleanupFS{FileService: markerBase}
	markerFS.rejectMarkerWrites.Store(true)
	markerFS.rejectMarkerStats.Store(true)
	cleaner := NewCheckpointCleaner(
		ctx, "", objectFS, nil, nil, WithUnpublishedCleanupFS(markerFS),
	).(*checkpointCleaner)

	name := objectio.MockObjectName().String()
	_, err = cleaner.PrepareUnpublishedObject(ctx, 1, 2, true, name)
	require.ErrorContains(t, err, "injected marker write failure")
	require.ErrorContains(t, err, "injected marker stat failure")
	cleaner.unpublishedCleanupOwnership.Lock()
	require.Equal(t, 1, cleaner.unpublishedCleanupOwnership.pending)
	require.Contains(t, cleaner.unpublishedCleanupOwnership.active, name)
	require.Len(t, cleaner.unpublishedCleanupOwnership.markers, 1)
	require.Len(t, cleaner.unpublishedCleanupOwnership.uncertain, 1)
	cleaner.unpublishedCleanupOwnership.Unlock()
	_, err = cleaner.PrepareUnpublishedObject(ctx, 1, 2, true, name)
	require.ErrorContains(t, err, "is already active")

	markerFS.rejectMarkerWrites.Store(false)
	markerFS.rejectMarkerDeletes.Store(true)
	require.ErrorContains(
		t, cleaner.replayUnpublishedObjectCleanup(ctx),
		"injected marker delete failure",
	)
	cleaner.unpublishedCleanupOwnership.Lock()
	require.Equal(t, 1, cleaner.unpublishedCleanupOwnership.pending)
	require.Len(t, cleaner.unpublishedCleanupOwnership.uncertain, 1)
	cleaner.unpublishedCleanupOwnership.Unlock()

	markerFS.rejectMarkerDeletes.Store(false)
	markerFS.rejectMarkerStats.Store(false)
	require.NoError(t, cleaner.replayUnpublishedObjectCleanup(ctx))
	cleaner.unpublishedCleanupOwnership.Lock()
	require.Zero(t, cleaner.unpublishedCleanupOwnership.pending)
	require.Empty(t, cleaner.unpublishedCleanupOwnership.markers)
	require.Empty(t, cleaner.unpublishedCleanupOwnership.uncertain)
	require.Empty(t, cleaner.unpublishedCleanupOwnership.active)
	cleaner.unpublishedCleanupOwnership.Unlock()

	marker, err := cleaner.PrepareUnpublishedObject(ctx, 1, 2, true, name)
	require.NoError(t, err)
	require.NoError(t, cleaner.FinishUnpublishedObject(ctx, marker, name))
}

func TestCheckpointCleanerReconcilesPersistedAmbiguousMarker(t *testing.T) {
	ctx := context.Background()
	objectFS, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerBase, err := fileservice.NewMemoryFS(
		"local", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerFS := &controllableUnpublishedCleanupFS{FileService: markerBase}
	markerFS.ambiguousMarkerWrites.Store(true)
	markerFS.rejectMarkerStats.Store(true)
	cleaner := NewCheckpointCleaner(
		ctx, "", objectFS, nil, nil, WithUnpublishedCleanupFS(markerFS),
	).(*checkpointCleaner)

	name := objectio.MockObjectName().String()
	marker, err := cleaner.PrepareUnpublishedObject(ctx, 1, 2, true, name)
	require.ErrorContains(t, err, "post-persist marker write failure")
	require.ErrorContains(t, err, "marker stat failure")
	_, err = markerBase.StatFile(ctx, marker)
	require.NoError(t, err)

	require.NoError(t, cleaner.replayUnpublishedObjectCleanup(ctx))
	_, err = markerBase.StatFile(ctx, marker)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	cleaner.unpublishedCleanupOwnership.Lock()
	require.Zero(t, cleaner.unpublishedCleanupOwnership.pending)
	require.Empty(t, cleaner.unpublishedCleanupOwnership.active)
	require.Empty(t, cleaner.unpublishedCleanupOwnership.markers)
	require.Empty(t, cleaner.unpublishedCleanupOwnership.uncertain)
	cleaner.unpublishedCleanupOwnership.Unlock()
}

func TestCheckpointCleanerBoundsAmbiguousMarkerReconciliation(t *testing.T) {
	ctx := context.Background()
	objectFS, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerBase, err := fileservice.NewMemoryFS(
		"local", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerFS := &controllableUnpublishedCleanupFS{FileService: markerBase}
	cleaner := NewCheckpointCleaner(
		ctx, "", objectFS, nil, nil, WithUnpublishedCleanupFS(markerFS),
	).(*checkpointCleaner)
	cleaner.unpublishedCleanupOwnership.initialized = true
	cleaner.unpublishedCleanupOwnership.markers = make(map[string]struct{}, 1001)
	cleaner.unpublishedCleanupOwnership.active = make(map[string]struct{}, 1001)
	cleaner.unpublishedCleanupOwnership.uncertain = make(map[string]string, 1001)
	for i := 0; i < 1001; i++ {
		marker := fmt.Sprintf("gc/unpublished/ambiguous-%04d.json", i)
		file := fmt.Sprintf("ambiguous-%04d", i)
		cleaner.unpublishedCleanupOwnership.markers[marker] = struct{}{}
		cleaner.unpublishedCleanupOwnership.active[file] = struct{}{}
		cleaner.unpublishedCleanupOwnership.uncertain[marker] = file
	}
	cleaner.unpublishedCleanupOwnership.pending = 1001
	cleaner.unpublishedCleanupGeneration.Add(1)

	require.NoError(t, cleaner.replayUnpublishedObjectCleanup(ctx))
	require.Equal(t, int64(1000), markerFS.markerDeleteCalls.Load())
	cleaner.unpublishedCleanupOwnership.Lock()
	require.Equal(t, 1, cleaner.unpublishedCleanupOwnership.pending)
	require.Len(t, cleaner.unpublishedCleanupOwnership.uncertain, 1)
	require.Len(t, cleaner.unpublishedCleanupOwnership.active, 1)
	cleaner.unpublishedCleanupOwnership.Unlock()

	require.NoError(t, cleaner.replayUnpublishedObjectCleanup(ctx))
	require.Equal(t, int64(1001), markerFS.markerDeleteCalls.Load())
	cleaner.unpublishedCleanupOwnership.Lock()
	require.Zero(t, cleaner.unpublishedCleanupOwnership.pending)
	require.Empty(t, cleaner.unpublishedCleanupOwnership.markers)
	require.Empty(t, cleaner.unpublishedCleanupOwnership.uncertain)
	require.Empty(t, cleaner.unpublishedCleanupOwnership.active)
	cleaner.unpublishedCleanupOwnership.Unlock()
}

func TestCheckpointCleanerReconcilesAmbiguousMarkerBesideActiveWriter(t *testing.T) {
	ctx := context.Background()
	objectFS, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerBase, err := fileservice.NewMemoryFS(
		"local", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	markerFS := &controllableUnpublishedCleanupFS{FileService: markerBase}
	cleaner := NewCheckpointCleaner(
		ctx, "", objectFS, nil, nil, WithUnpublishedCleanupFS(markerFS),
	).(*checkpointCleaner)

	activeName := objectio.MockObjectName().String()
	activeMarker, err := cleaner.PrepareUnpublishedObject(
		ctx, 1, 2, true, activeName)
	require.NoError(t, err)
	phantom := "gc/unpublished/ambiguous-beside-active.json"
	phantomName := "ambiguous-beside-active"
	cleaner.unpublishedCleanupOwnership.Lock()
	cleaner.unpublishedCleanupOwnership.markers[phantom] = struct{}{}
	cleaner.unpublishedCleanupOwnership.active[phantomName] = struct{}{}
	cleaner.unpublishedCleanupOwnership.uncertain = map[string]string{
		phantom: phantomName,
	}
	cleaner.unpublishedCleanupOwnership.pending++
	cleaner.unpublishedCleanupOwnership.Unlock()
	cleaner.unpublishedCleanupGeneration.Add(1)

	require.NoError(t, cleaner.replayUnpublishedObjectCleanup(ctx))
	cleaner.unpublishedCleanupOwnership.Lock()
	require.Equal(t, 1, cleaner.unpublishedCleanupOwnership.pending)
	require.Empty(t, cleaner.unpublishedCleanupOwnership.uncertain)
	require.Len(t, cleaner.unpublishedCleanupOwnership.markers, 1)
	require.Contains(t, cleaner.unpublishedCleanupOwnership.active, activeName)
	cleaner.unpublishedCleanupOwnership.Unlock()
	require.NoError(t, cleaner.FinishUnpublishedObject(
		ctx, activeMarker, activeName))
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
