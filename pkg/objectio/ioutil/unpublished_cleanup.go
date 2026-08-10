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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const (
	unpublishedObjectCleanupDir     = "gc/unpublished/"
	unpublishedObjectCleanupVersion = 1
	unpublishedCleanupReplayBatch   = 1000
)

type unpublishedObjectCleanupIntent struct {
	Version uint8    `json:"version"`
	Files   []string `json:"files"`
}

// RecordUnpublishedObjectCleanup durably transfers cleanup ownership for
// exact, unpublished object names. The content-derived path makes retries
// idempotent, including an ambiguous write that actually reached storage.
func RecordUnpublishedObjectCleanup(
	ctx context.Context,
	fs fileservice.FileService,
	files ...string,
) (string, error) {
	files = normalizeUnpublishedObjectNames(files)
	if len(files) == 0 {
		return "", nil
	}
	payload, err := json.Marshal(unpublishedObjectCleanupIntent{
		Version: unpublishedObjectCleanupVersion,
		Files:   files,
	})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	path := fmt.Sprintf("%s%x.json", unpublishedObjectCleanupDir, digest)
	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(payload)),
			Data:   payload,
		}},
		Policy: fileservice.SkipAllCache,
	})
	if err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
		return "", err
	}
	return path, nil
}

// ReplayUnpublishedObjectCleanup deletes objects before their marker. A
// failed object or marker delete leaves durable evidence for the next replay.
// It is safe for multiple cleaners to replay the same marker concurrently.
func ReplayUnpublishedObjectCleanup(
	ctx context.Context,
	fs fileservice.FileService,
) (replayed int, remaining bool, err error) {
	return ReplayUnpublishedObjectCleanupFrom(ctx, fs, fs)
}

// ReplayUnpublishedObjectCleanupFrom reads durable intents from markerFS and
// applies them to objectFS. Keeping the marker on local durable storage lets a
// TN retain cleanup ownership while shared object storage is unavailable.
func ReplayUnpublishedObjectCleanupFrom(
	ctx context.Context,
	markerFS fileservice.FileService,
	objectFS fileservice.FileService,
) (replayed int, remaining bool, err error) {
	markers := make([]string, 0, unpublishedCleanupReplayBatch)
	for entry, listErr := range markerFS.List(ctx, unpublishedObjectCleanupDir) {
		if listErr != nil {
			return 0, true, listErr
		}
		if entry.IsDir {
			continue
		}
		if len(markers) == unpublishedCleanupReplayBatch {
			remaining = true
			break
		}
		markers = append(markers, unpublishedObjectCleanupDir+entry.Name)
	}

	failed := 0
	var firstErr error
	recordFailure := func(failure error) {
		failed++
		if firstErr == nil {
			firstErr = failure
		}
	}
	for _, marker := range markers {
		if cause := context.Cause(ctx); cause != nil {
			return replayed, true, errors.Join(firstErr, cause)
		}
		intent, readErr := readUnpublishedObjectCleanup(ctx, markerFS, marker)
		if readErr != nil {
			remaining = true
			recordFailure(readErr)
			continue
		}
		if _, deleteErr := DeleteUnpublishedObjects(ctx, objectFS, intent.Files...); deleteErr != nil {
			remaining = true
			recordFailure(deleteErr)
			continue
		}
		if deleteErr := markerFS.Delete(ctx, marker); deleteErr != nil &&
			!moerr.IsMoErrCode(deleteErr, moerr.ErrFileNotFound) {
			remaining = true
			recordFailure(deleteErr)
			continue
		}
		replayed++
	}
	if failed != 0 {
		return replayed, remaining, errors.Join(
			moerr.NewInternalErrorf(
				ctx, "replay %d unpublished object cleanup intents", failed),
			firstErr,
		)
	}
	return replayed, remaining, nil
}

func readUnpublishedObjectCleanup(
	ctx context.Context,
	fs fileservice.FileService,
	path string,
) (unpublishedObjectCleanupIntent, error) {
	vector := &fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   -1,
		}},
		Policy: fileservice.SkipAllCache,
	}
	defer vector.Release()
	if err := fs.Read(ctx, vector); err != nil {
		return unpublishedObjectCleanupIntent{}, err
	}
	var intent unpublishedObjectCleanupIntent
	if err := json.Unmarshal(vector.Entries[0].Data, &intent); err != nil {
		return unpublishedObjectCleanupIntent{}, err
	}
	if intent.Version != unpublishedObjectCleanupVersion || len(intent.Files) == 0 {
		return unpublishedObjectCleanupIntent{}, moerr.NewInternalErrorf(
			ctx,
			"invalid unpublished object cleanup intent %s version %d with %d files",
			path,
			intent.Version,
			len(intent.Files),
		)
	}
	return intent, nil
}

func normalizeUnpublishedObjectNames(files []string) []string {
	seen := make(map[string]struct{}, len(files))
	unique := make([]string, 0, len(files))
	for _, file := range files {
		if file == "" {
			continue
		}
		if _, ok := seen[file]; ok {
			continue
		}
		seen[file] = struct{}{}
		unique = append(unique, file)
	}
	sort.Strings(unique)
	return unique
}
