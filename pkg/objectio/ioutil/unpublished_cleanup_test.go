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
	"container/heap"
	"context"
	"errors"
	"fmt"
	"iter"
	"sort"
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

type reverseCleanupListFS struct {
	fileservice.FileService
}

type failingCleanupListFS struct {
	fileservice.FileService
	err error
}

func (fs *failingCleanupListFS) List(
	context.Context,
	string,
) iter.Seq2[*fileservice.DirEntry, error] {
	return func(yield func(*fileservice.DirEntry, error) bool) {
		yield(nil, fs.err)
	}
}

func (fs *reverseCleanupListFS) List(
	ctx context.Context,
	dir string,
) iter.Seq2[*fileservice.DirEntry, error] {
	return func(yield func(*fileservice.DirEntry, error) bool) {
		var entries []*fileservice.DirEntry
		for entry, err := range fs.FileService.List(ctx, dir) {
			if err != nil {
				yield(nil, err)
				return
			}
			entries = append(entries, entry)
		}
		for i := len(entries) - 1; i >= 0; i-- {
			if !yield(entries[i], nil) {
				return
			}
		}
	}
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

func TestCCPRUnpublishedCleanupIsRestartSafeAndNamespaceIsolated(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		"cleanup", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	_, err = RecordUnpublishedObjectCleanup(
		ctx, fs, UnpublishedObject{File: "tn-local"})
	require.NoError(t, err)
	object := UnpublishedObject{
		File:                  "ccpr-object",
		DBID:                  1,
		TableID:               2,
		IsTombstone:           true,
		TNShardID:             3,
		SyncProtectionJobID:   "job",
		SyncProtectionValidTS: 3,
	}
	marker, err := RecordCCPRUnpublishedObjectCleanup(ctx, fs, object)
	require.NoError(t, err)
	require.Contains(t, marker, ccprUnpublishedObjectCleanupDir)

	localMarkers, _, err := ListUnpublishedObjectCleanup(ctx, fs, 10)
	require.NoError(t, err)
	require.Len(t, localMarkers, 1,
		"TN-local admission must not count cross-CN markers")

	var replayedObject UnpublishedObject
	replayed, inspected, next, remaining, err :=
		ReplayCCPRUnpublishedObjectCleanupPageFrom(
			ctx,
			fs,
			func(got UnpublishedObject) (
				UnpublishedObjectCleanupDecision, error,
			) {
				replayedObject = got
				return ReleaseUnpublishedObjectCleanup, nil
			},
			"",
			10,
		)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)
	require.Equal(t, 1, inspected)
	require.Empty(t, next)
	require.False(t, remaining)
	require.Equal(t, object, replayedObject)
	require.True(t, moerr.IsMoErrCode(
		statCleanupTestFile(ctx, fs, marker), moerr.ErrFileNotFound))
}

func TestRecordCCPRUnpublishedCleanupRequiresProtection(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		"cleanup", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	_, err = RecordCCPRUnpublishedObjectCleanup(
		ctx, fs, UnpublishedObject{File: "missing-protection"})
	require.ErrorContains(t, err, "requires catalog and sync protection ownership")
	_, err = RecordCCPRUnpublishedObjectCleanup(ctx, fs, UnpublishedObject{
		File:                  "missing-catalog-owner",
		SyncProtectionJobID:   "job",
		SyncProtectionValidTS: 1,
	})
	require.ErrorContains(t, err, "requires catalog and sync protection ownership")
	_, err = RecordCCPRUnpublishedObjectCleanup(ctx, fs, UnpublishedObject{
		File:                  "missing-shard-owner",
		DBID:                  1,
		TableID:               2,
		SyncProtectionJobID:   "job",
		SyncProtectionValidTS: 1,
	})
	require.ErrorContains(t, err, "requires catalog and sync protection ownership")
}

func TestUnpublishedObjectCleanupRejectsInvalidInputsAndListFailures(t *testing.T) {
	ctx := context.Background()
	base, err := fileservice.NewMemoryFS(
		"cleanup", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	_, err = RecordUnpublishedObjectCleanup(ctx, base, UnpublishedObject{})
	require.ErrorContains(t, err, "empty unpublished object")
	require.NoError(t, DeleteUnpublishedObjectCleanup(ctx, base, ""))

	markers, remaining, err := ListUnpublishedObjectCleanup(ctx, base, 0)
	require.NoError(t, err)
	require.Empty(t, markers)
	require.False(t, remaining)
	markers, remaining, err = ListUnpublishedObjectCleanupAfter(
		ctx, base, "", 0)
	require.NoError(t, err)
	require.Empty(t, markers)
	require.False(t, remaining)

	listErr := errors.New("injected marker list failure")
	failing := &failingCleanupListFS{FileService: base, err: listErr}
	_, remaining, err = ListUnpublishedObjectCleanup(ctx, failing, 1)
	require.ErrorIs(t, err, listErr)
	require.True(t, remaining)
	_, remaining, err = ListUnpublishedObjectCleanupAfter(
		ctx, failing, "", 1)
	require.ErrorIs(t, err, listErr)
	require.True(t, remaining)
	_, _, cursor, remaining, err := ReplayUnpublishedObjectCleanupPageFrom(
		ctx, failing, base, nil, nil, nil, "cursor", 1)
	require.ErrorIs(t, err, listErr)
	require.Equal(t, "cursor", cursor)
	require.True(t, remaining)
}

func TestUnpublishedObjectCleanupRejectsMalformedMarker(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		"cleanup", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	marker := unpublishedObjectCleanupDir + "malformed.json"
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: marker,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len("not-json")),
			Data:   []byte("not-json"),
		}},
	}))

	_, inspected, remaining, err := ReplayUnpublishedObjectCleanupFrom(
		ctx, fs, fs, nil, nil, 1)
	require.Error(t, err)
	require.Equal(t, 1, inspected)
	require.True(t, remaining)
	require.NoError(t, statCleanupTestFile(ctx, fs, marker))
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

	replayed, _, remaining, err := ReplayUnpublishedObjectCleanupFrom(
		ctx, fs, fs,
		func(UnpublishedObject) (UnpublishedObjectCleanupDecision, error) {
			return RetryUnpublishedObjectCleanup, nil
		},
		nil,
		10,
	)
	require.NoError(t, err)
	require.Zero(t, replayed)
	require.True(t, remaining)
	require.NoError(t, statCleanupTestFile(ctx, base, object.File))
	require.NoError(t, statCleanupTestFile(ctx, base, marker))

	replayed, _, remaining, err = ReplayUnpublishedObjectCleanupFrom(
		ctx, fs, fs,
		func(UnpublishedObject) (UnpublishedObjectCleanupDecision, error) {
			return ReleaseUnpublishedObjectCleanup, nil
		},
		nil,
		10,
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
	replayed, _, remaining, err = ReplayUnpublishedObjectCleanupFrom(
		ctx, fs, fs, nil, nil, 10)
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
	_, _, remaining, err := ReplayUnpublishedObjectCleanupFrom(
		ctx, fs, fs, nil, nil, 10)
	require.ErrorContains(t, err, "injected delete failure")
	require.True(t, remaining)
	require.NoError(t, statCleanupTestFile(ctx, base, marker))
}

func TestUnpublishedObjectCleanupReplayIsBounded(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		"cleanup", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	const limit = 7
	for i := 0; i < limit+1; i++ {
		object := UnpublishedObject{File: fmt.Sprintf("unpublished-%04d", i)}
		writeUnpublishedCleanupTestFile(t, fs, object.File)
		_, err = RecordUnpublishedObjectCleanup(ctx, fs, object)
		require.NoError(t, err)
	}

	replayed, inspected, remaining, err := ReplayUnpublishedObjectCleanupFrom(
		ctx, fs, fs, nil, nil, limit)
	require.NoError(t, err)
	require.Equal(t, limit, replayed)
	require.Equal(t, limit, inspected)
	require.True(t, remaining)
	replayed, inspected, remaining, err = ReplayUnpublishedObjectCleanupFrom(
		ctx, fs, fs, nil, nil, limit)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)
	require.Equal(t, 1, inspected)
	require.False(t, remaining)
}

func TestUnpublishedObjectCleanupPagedReplayPassesRetryingPrefix(t *testing.T) {
	ctx := context.Background()
	base, err := fileservice.NewMemoryFS(
		"cleanup", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	fs := &reverseCleanupListFS{FileService: base}
	type markedObject struct {
		marker string
		object UnpublishedObject
	}
	objects := make([]markedObject, 0, 4)
	for i := 0; i < 4; i++ {
		object := UnpublishedObject{File: fmt.Sprintf("paged-%d", i)}
		writeUnpublishedCleanupTestFile(t, fs, object.File)
		marker, err := RecordUnpublishedObjectCleanup(ctx, fs, object)
		require.NoError(t, err)
		objects = append(objects, markedObject{marker: marker, object: object})
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].marker < objects[j].marker
	})
	retrying := map[string]struct{}{
		objects[0].object.File: {},
		objects[1].object.File: {},
	}
	decide := func(
		object UnpublishedObject,
	) (UnpublishedObjectCleanupDecision, error) {
		if _, ok := retrying[object.File]; ok {
			return RetryUnpublishedObjectCleanup, nil
		}
		return DeleteUnpublishedObject, nil
	}

	_, inspected, cursor, remaining, err :=
		ReplayUnpublishedObjectCleanupPageFrom(
			ctx, fs, fs, decide, nil, nil, "", 2)
	require.NoError(t, err)
	require.Equal(t, 2, inspected)
	require.NotEmpty(t, cursor)
	require.True(t, remaining)

	replayed, inspected, cursor, remaining, err :=
		ReplayUnpublishedObjectCleanupPageFrom(
			ctx, fs, fs, decide, nil, nil, cursor, 2)
	require.NoError(t, err)
	require.Equal(t, 2, replayed)
	require.Equal(t, 2, inspected)
	require.Empty(t, cursor)
	require.True(t, remaining, "the completed cursor pass must wrap")
	require.True(t, moerr.IsMoErrCode(
		statCleanupTestFile(ctx, fs, objects[2].object.File),
		moerr.ErrFileNotFound,
	))
	require.True(t, moerr.IsMoErrCode(
		statCleanupTestFile(ctx, fs, objects[2].marker),
		moerr.ErrFileNotFound,
	))
	require.True(t, moerr.IsMoErrCode(
		statCleanupTestFile(ctx, fs, objects[3].marker),
		moerr.ErrFileNotFound,
	))
}

func TestMaxMarkerPathHeap(t *testing.T) {
	h := &maxMarkerPathHeap{}
	heap.Push(h, "a")
	heap.Push(h, "c")
	heap.Push(h, "b")
	require.Equal(t, "c", heap.Pop(h))
	require.Equal(t, "b", heap.Pop(h))
	require.Equal(t, "a", heap.Pop(h))
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
