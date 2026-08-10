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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

type cleanupTestFS struct {
	fileservice.FileService
	deleteErr     error
	postDeleteErr error
}

func (fs *cleanupTestFS) Delete(ctx context.Context, paths ...string) error {
	if fs.deleteErr != nil {
		return fs.deleteErr
	}
	err := fs.FileService.Delete(ctx, paths...)
	if err == nil && fs.postDeleteErr != nil {
		return fs.postDeleteErr
	}
	return err
}

func TestDeleteUnpublishedObjectCleanupAcceptsAmbiguousDeleteAbsence(t *testing.T) {
	ctx := context.Background()
	base, err := fileservice.NewMemoryFS(
		"cleanup", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	fs := &cleanupTestFS{FileService: base}
	marker, err := RecordUnpublishedObjectCleanup(
		ctx, fs, UnpublishedObject{File: "ambiguous-marker-delete"})
	require.NoError(t, err)
	fs.postDeleteErr = errors.New("injected post-delete failure")

	require.NoError(t, DeleteUnpublishedObjectCleanup(ctx, fs, marker))
	require.True(t, moerr.IsMoErrCode(
		statCleanupTestFile(ctx, base, marker), moerr.ErrFileNotFound))
}

func TestUnpublishedObjectCleanupOwnershipDecisions(t *testing.T) {
	ctx := context.Background()
	base, err := fileservice.NewMemoryFS(
		"cleanup", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	fs := &cleanupTestFS{FileService: base}
	object := UnpublishedObject{
		File: "019c0000-0000-7000-8000-000000000001_00000",
		DBID: 1, TableID: 2, IsTombstone: true,
	}
	writeUnpublishedCleanupTestFile(t, base, object.File)
	marker, err := RecordUnpublishedObjectCleanup(ctx, fs, object)
	require.NoError(t, err)

	replayed, remaining, err := ReplayUnpublishedObjectCleanupFrom(
		ctx, fs, fs,
		func(UnpublishedObject) (UnpublishedObjectCleanupDecision, error) {
			return RetryUnpublishedObjectCleanup, nil
		},
		nil,
	)
	require.NoError(t, err)
	require.Zero(t, replayed)
	require.True(t, remaining)
	require.NoError(t, statCleanupTestFile(ctx, base, object.File))
	require.NoError(t, statCleanupTestFile(ctx, base, marker))

	replayed, remaining, err = ReplayUnpublishedObjectCleanupFrom(
		ctx, fs, fs,
		func(UnpublishedObject) (UnpublishedObjectCleanupDecision, error) {
			return ReleaseUnpublishedObjectCleanup, nil
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)
	require.False(t, remaining)
	require.NoError(t, statCleanupTestFile(ctx, base, object.File),
		"catalog ownership keeps the object")
	require.True(t, moerr.IsMoErrCode(
		statCleanupTestFile(ctx, base, marker), moerr.ErrFileNotFound))

	marker, err = RecordUnpublishedObjectCleanup(ctx, fs, object)
	require.NoError(t, err)
	replayed, remaining, err = ReplayUnpublishedObjectCleanupFrom(
		ctx, fs, fs, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)
	require.False(t, remaining)
	require.True(t, moerr.IsMoErrCode(
		statCleanupTestFile(ctx, base, object.File), moerr.ErrFileNotFound))
	require.True(t, moerr.IsMoErrCode(
		statCleanupTestFile(ctx, base, marker), moerr.ErrFileNotFound))
}

func TestUnpublishedObjectCleanupRetainsMarkerOnDeleteFailure(t *testing.T) {
	ctx := context.Background()
	base, err := fileservice.NewMemoryFS(
		"cleanup", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	fs := &cleanupTestFS{FileService: base}
	object := UnpublishedObject{File: "delete-failure"}
	writeUnpublishedCleanupTestFile(t, base, object.File)
	marker, err := RecordUnpublishedObjectCleanup(ctx, fs, object)
	require.NoError(t, err)

	fs.deleteErr = errors.New("injected delete failure")
	_, remaining, err := ReplayUnpublishedObjectCleanupFrom(ctx, fs, fs, nil, nil)
	require.ErrorContains(t, err, "injected delete failure")
	require.True(t, remaining)
	require.NoError(t, statCleanupTestFile(ctx, base, marker))
}

func TestUnpublishedObjectCleanupReplayIsBounded(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		"cleanup", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	for i := 0; i < unpublishedCleanupReplayBatch+1; i++ {
		object := UnpublishedObject{File: fmt.Sprintf("unpublished-%04d", i)}
		writeUnpublishedCleanupTestFile(t, fs, object.File)
		_, err = RecordUnpublishedObjectCleanup(ctx, fs, object)
		require.NoError(t, err)
	}

	replayed, remaining, err := ReplayUnpublishedObjectCleanupFrom(ctx, fs, fs, nil, nil)
	require.NoError(t, err)
	require.Equal(t, unpublishedCleanupReplayBatch, replayed)
	require.True(t, remaining)
	replayed, remaining, err = ReplayUnpublishedObjectCleanupFrom(ctx, fs, fs, nil, nil)
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
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte{1}}},
	}))
}

func statCleanupTestFile(
	ctx context.Context,
	fs fileservice.FileService,
	path string,
) error {
	_, err := fs.StatFile(ctx, path)
	return err
}
