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

package ioutil

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

type failingUnpublishedCleanupFS struct {
	fileservice.FileService
	failObjectDelete bool
	failMarkerDelete bool
}

func (fs *failingUnpublishedCleanupFS) Delete(ctx context.Context, paths ...string) error {
	for _, path := range paths {
		if fs.failObjectDelete && !strings.HasPrefix(path, unpublishedObjectCleanupDir) {
			return errors.New("injected object delete failure")
		}
		if fs.failMarkerDelete && strings.HasPrefix(path, unpublishedObjectCleanupDir) {
			return errors.New("injected marker delete failure")
		}
	}
	return fs.FileService.Delete(ctx, paths...)
}

func TestReplayUnpublishedObjectCleanupRetainsDurableOwner(t *testing.T) {
	ctx := context.Background()
	baseFS, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	fs := &failingUnpublishedCleanupFS{FileService: baseFS}

	first := objectio.MockObjectName().String()
	second := objectio.MockObjectName().String()
	writeUnpublishedCleanupTestFile(t, baseFS, first)
	writeUnpublishedCleanupTestFile(t, baseFS, second)

	marker, err := RecordUnpublishedObjectCleanup(ctx, fs, second, first, first)
	require.NoError(t, err)
	require.NotEmpty(t, marker)
	retryMarker, err := RecordUnpublishedObjectCleanup(ctx, fs, first, second)
	require.NoError(t, err)
	require.Equal(t, marker, retryMarker, "the same ownership set must be idempotent")

	fs.failObjectDelete = true
	_, _, err = ReplayUnpublishedObjectCleanup(ctx, fs)
	require.ErrorContains(t, err, "injected object delete failure")
	require.True(t, unpublishedCleanupTestFileExists(t, baseFS, marker))
	require.True(t, unpublishedCleanupTestFileExists(t, baseFS, first))
	require.True(t, unpublishedCleanupTestFileExists(t, baseFS, second))

	fs.failObjectDelete = false
	fs.failMarkerDelete = true
	_, _, err = ReplayUnpublishedObjectCleanup(ctx, fs)
	require.ErrorContains(t, err, "injected marker delete failure")
	require.True(t, unpublishedCleanupTestFileExists(t, baseFS, marker),
		"the marker remains the retry owner after object deletion")
	require.False(t, unpublishedCleanupTestFileExists(t, baseFS, first))
	require.False(t, unpublishedCleanupTestFileExists(t, baseFS, second))

	fs.failMarkerDelete = false
	replayed, remaining, err := ReplayUnpublishedObjectCleanup(ctx, fs)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)
	require.False(t, remaining)
	require.False(t, unpublishedCleanupTestFileExists(t, baseFS, marker))
}

func TestReplayUnpublishedObjectCleanupIsBatchBounded(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		"shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	for i := 0; i < unpublishedCleanupReplayBatch+1; i++ {
		_, err = RecordUnpublishedObjectCleanup(
			ctx, fs, fmt.Sprintf("unpublished-%04d", i))
		require.NoError(t, err)
	}

	replayed, remaining, err := ReplayUnpublishedObjectCleanup(ctx, fs)
	require.NoError(t, err)
	require.Equal(t, unpublishedCleanupReplayBatch, replayed)
	require.True(t, remaining)

	replayed, remaining, err = ReplayUnpublishedObjectCleanup(ctx, fs)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)
	require.False(t, remaining)
}

func writeUnpublishedCleanupTestFile(
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

func unpublishedCleanupTestFileExists(
	t *testing.T,
	fs fileservice.FileService,
	path string,
) bool {
	t.Helper()
	_, err := fs.StatFile(context.Background(), path)
	if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return false
	}
	require.NoError(t, err)
	return true
}
