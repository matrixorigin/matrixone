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
	unpublishedObjectCleanupVersion = 2
	unpublishedCleanupReplayBatch   = 1000
)

// UnpublishedObject identifies one object and the catalog owner that will
// make it reachable after transaction commit. Cleanup intents are written
// before the object itself, so a persisted object always has a restart-safe
// owner even when the object write returns an ambiguous error.
type UnpublishedObject struct {
	File        string `json:"file"`
	DBID        uint64 `json:"db_id"`
	TableID     uint64 `json:"table_id"`
	IsTombstone bool   `json:"is_tombstone"`
}

type unpublishedObjectCleanupIntent struct {
	Version uint8             `json:"version"`
	Object  UnpublishedObject `json:"object"`
}

// UnpublishedObjectCleanupDecision tells replay how ownership moved since the
// write-ahead intent was created.
type UnpublishedObjectCleanupDecision uint8

const (
	// RetryUnpublishedObjectCleanup leaves both object and marker untouched.
	// It is used while the creating transaction is still active.
	RetryUnpublishedObjectCleanup UnpublishedObjectCleanupDecision = iota
	// DeleteUnpublishedObject removes the object and then its marker.
	DeleteUnpublishedObject
	// ReleaseUnpublishedObjectCleanup removes only the marker because the
	// catalog (and therefore ordinary GC) now owns the object.
	ReleaseUnpublishedObjectCleanup
)

// RecordUnpublishedObjectCleanup durably records cleanup ownership before the
// corresponding object is written. The content-derived path makes an
// ambiguous marker write idempotent.
func RecordUnpublishedObjectCleanup(
	ctx context.Context,
	fs fileservice.FileService,
	object UnpublishedObject,
) (string, error) {
	if object.File == "" {
		return "", moerr.NewInternalErrorNoCtx(
			"cannot record cleanup for an empty unpublished object")
	}
	payload, err := json.Marshal(unpublishedObjectCleanupIntent{
		Version: unpublishedObjectCleanupVersion,
		Object:  object,
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
		return path, err
	}
	return path, nil
}

// DeleteUnpublishedObjectCleanup releases a durable cleanup intent after the
// object either became catalog-owned or was physically deleted.
func DeleteUnpublishedObjectCleanup(
	ctx context.Context,
	fs fileservice.FileService,
	marker string,
) error {
	if marker == "" {
		return nil
	}
	err := fs.Delete(ctx, marker)
	if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return nil
	}
	if err != nil {
		// Delete may report an error after removing the marker. Absence is a
		// sufficient release proof and prevents a phantom admission slot from
		// surviving forever.
		if _, statErr := fs.StatFile(ctx, marker); moerr.IsMoErrCode(
			statErr, moerr.ErrFileNotFound) {
			return nil
		} else if statErr != nil {
			return errors.Join(err, statErr)
		}
	}
	return err
}

// ListUnpublishedObjectCleanup lists durable intents up to limit. remaining
// reports that more entries exist, allowing callers to reconstruct bounded
// admission state without materializing an unbounded directory listing.
func ListUnpublishedObjectCleanup(
	ctx context.Context,
	fs fileservice.FileService,
	limit int,
) (markers []string, remaining bool, err error) {
	if limit <= 0 {
		return nil, false, nil
	}
	markers = make([]string, 0, limit)
	for entry, listErr := range fs.List(ctx, unpublishedObjectCleanupDir) {
		if listErr != nil {
			return markers, true, listErr
		}
		if entry.IsDir {
			continue
		}
		if len(markers) == limit {
			return markers, true, nil
		}
		markers = append(markers, unpublishedObjectCleanupDir+entry.Name)
	}
	return markers, false, nil
}

// ReplayUnpublishedObjectCleanupFrom replays at most one bounded marker batch.
// decide must fence active writers and catalog-owned objects; replay never
// infers ownership from a bare file name.
func ReplayUnpublishedObjectCleanupFrom(
	ctx context.Context,
	markerFS fileservice.FileService,
	objectFS fileservice.FileService,
	decide func(UnpublishedObject) (UnpublishedObjectCleanupDecision, error),
	onReplayed func(marker string),
) (replayed int, remaining bool, err error) {
	markers, remaining, err := ListUnpublishedObjectCleanup(
		ctx, markerFS, unpublishedCleanupReplayBatch)
	if err != nil {
		return 0, true, err
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
		decision := DeleteUnpublishedObject
		if decide != nil {
			decision, readErr = decide(intent.Object)
			if readErr != nil {
				remaining = true
				recordFailure(readErr)
				continue
			}
		}
		if decision == RetryUnpublishedObjectCleanup {
			remaining = true
			continue
		}
		if decision == DeleteUnpublishedObject {
			if _, deleteErr := DeleteUnpublishedObjects(
				ctx, objectFS, intent.Object.File); deleteErr != nil {
				remaining = true
				recordFailure(deleteErr)
				continue
			}
		}
		if deleteErr := DeleteUnpublishedObjectCleanup(
			ctx, markerFS, marker); deleteErr != nil {
			remaining = true
			recordFailure(deleteErr)
			continue
		}
		replayed++
		if onReplayed != nil {
			onReplayed(marker)
		}
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
	if intent.Version != unpublishedObjectCleanupVersion || intent.Object.File == "" {
		return unpublishedObjectCleanupIntent{}, moerr.NewInternalErrorf(
			ctx,
			"invalid unpublished object cleanup intent %s version %d",
			path,
			intent.Version,
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
