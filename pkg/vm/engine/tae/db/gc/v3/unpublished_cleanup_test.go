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
	"iter"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

type rejectSharedCleanupMarkerWriteFS struct {
	fileservice.FileService
}

func (fs *rejectSharedCleanupMarkerWriteFS) Write(
	ctx context.Context,
	vector fileservice.IOVector,
) error {
	if strings.HasPrefix(vector.FilePath, "gc/unpublished/") {
		return errors.New("injected shared marker write failure")
	}
	return fs.FileService.Write(ctx, vector)
}

type blockingUnpublishedCleanupListFS struct {
	fileservice.FileService
	listed  chan struct{}
	release chan struct{}
	once    sync.Once
}

func (fs *blockingUnpublishedCleanupListFS) List(
	ctx context.Context,
	dir string,
) iter.Seq2[*fileservice.DirEntry, error] {
	if dir != "gc/unpublished/" {
		return fs.FileService.List(ctx, dir)
	}
	return func(yield func(*fileservice.DirEntry, error) bool) {
		fs.once.Do(func() { close(fs.listed) })
		select {
		case <-ctx.Done():
			yield(nil, context.Cause(ctx))
			return
		case <-fs.release:
		}
		for entry, err := range fs.FileService.List(ctx, dir) {
			if !yield(entry, err) {
				return
			}
		}
	}
}

func TestCheckpointCleanerReplaysUnpublishedCleanupAfterRestart(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	sharedFS := &rejectSharedCleanupMarkerWriteFS{FileService: fs}
	localDir := t.TempDir()
	firstLocalFS, err := fileservice.NewLocalFS(
		ctx, "local", localDir, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	object := objectio.MockObjectName().String()
	writeCheckpointCleanerCleanupTestObject(t, fs, object)

	firstProcess := &checkpointCleaner{
		fs:                   sharedFS,
		unpublishedCleanupFS: firstLocalFS,
	}
	require.NoError(t, firstProcess.HandoffUnpublishedObjects(ctx, object))
	require.NotEqual(t,
		firstProcess.unpublishedCleanupProcessed.Load(),
		firstProcess.unpublishedCleanupGeneration.Load(),
		"a runtime handoff must wake the next cleaner cycle",
	)

	secondLocalFS, err := fileservice.NewLocalFS(
		ctx, "local", localDir, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	secondProcess := &checkpointCleaner{
		fs:                   sharedFS,
		unpublishedCleanupFS: secondLocalFS,
	}
	require.NoError(t, secondProcess.replayUnpublishedObjectCleanup(ctx))
	_, err = fs.StatFile(ctx, object)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	require.Equal(t,
		secondProcess.unpublishedCleanupProcessed.Load(),
		secondProcess.unpublishedCleanupGeneration.Load(),
		"successful startup replay must leave the steady state scan-free",
	)
}

func TestCheckpointCleanerDoesNotLoseConcurrentHandoff(t *testing.T) {
	ctx := context.Background()
	baseFS, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	fs := &blockingUnpublishedCleanupListFS{
		FileService: baseFS,
		listed:      make(chan struct{}),
		release:     make(chan struct{}),
	}
	cleaner := &checkpointCleaner{fs: fs}

	first := objectio.MockObjectName().String()
	second := objectio.MockObjectName().String()
	writeCheckpointCleanerCleanupTestObject(t, baseFS, first)
	writeCheckpointCleanerCleanupTestObject(t, baseFS, second)
	require.NoError(t, cleaner.HandoffUnpublishedObjects(ctx, first))

	replayDone := make(chan error, 1)
	go func() {
		replayDone <- cleaner.replayUnpublishedObjectCleanup(ctx)
	}()
	<-fs.listed
	require.NoError(t, cleaner.HandoffUnpublishedObjects(ctx, second))
	close(fs.release)
	require.NoError(t, <-replayDone)
	require.NotEqual(t,
		cleaner.unpublishedCleanupProcessed.Load(),
		cleaner.unpublishedCleanupGeneration.Load(),
		"a handoff concurrent with replay must leave a visible next generation",
	)
	require.NoError(t, cleaner.replayUnpublishedObjectCleanup(ctx))
	require.Equal(t,
		cleaner.unpublishedCleanupProcessed.Load(),
		cleaner.unpublishedCleanupGeneration.Load(),
	)
}

func writeCheckpointCleanerCleanupTestObject(
	t *testing.T,
	fs fileservice.FileService,
	path string,
) {
	t.Helper()
	require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   1,
			Data:   []byte{1},
		}},
	}))
}
