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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const (
	unpublishedObjectCleanupDir     = "gc/unpublished/"
	ccprUnpublishedObjectCleanupDir = "gc/ccpr-unpublished/"
	unpublishedObjectCleanupVersion = 2
)

// UnpublishedObject identifies one object and the catalog owner that will
// make it reachable after transaction commit. Cleanup intents are written
// before the object itself, so a persisted object always has a restart-safe
// owner even when the object write returns an ambiguous error.
type UnpublishedObject struct {
	File                  string `json:"file"`
	DBID                  uint64 `json:"db_id"`
	TableID               uint64 `json:"table_id"`
	IsTombstone           bool   `json:"is_tombstone"`
	TNShardID             uint64 `json:"tn_shard_id,omitempty"`
	SyncProtectionJobID   string `json:"sync_protection_job_id,omitempty"`
	SyncProtectionValidTS int64  `json:"sync_protection_valid_ts,omitempty"`
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
	return recordUnpublishedObjectCleanup(
		ctx, fs, unpublishedObjectCleanupDir, object)
}

// RecordCCPRUnpublishedObjectCleanup records a publication-owned object in
// shared storage. Keeping publication intents in a separate namespace prevents
// a TN-local active-writer fence from being mistaken for a cross-CN fence.
func RecordCCPRUnpublishedObjectCleanup(
	ctx context.Context,
	fs fileservice.FileService,
	object UnpublishedObject,
) (string, error) {
	if object.DBID == 0 || object.TableID == 0 || object.TNShardID == 0 ||
		object.SyncProtectionJobID == "" || object.SyncProtectionValidTS <= 0 {
		return "", moerr.NewInternalErrorNoCtx(
			"CCPR unpublished object requires catalog and sync protection ownership")
	}
	return recordUnpublishedObjectCleanup(
		ctx, fs, ccprUnpublishedObjectCleanupDir, object)
}

func recordUnpublishedObjectCleanup(
	ctx context.Context,
	fs fileservice.FileService,
	dir string,
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
	path := fmt.Sprintf("%s%x.json", dir, digest)
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

// ListUnpublishedObjectCleanupAfter lists one lexicographic page after marker.
// FileService does not promise directory order, so the bounded max-heap finds
// the next smallest paths without materializing the complete marker set. A
// caller-held cursor can therefore advance past intents that must remain
// durable instead of retrying the same prefix forever.
func ListUnpublishedObjectCleanupAfter(
	ctx context.Context,
	fs fileservice.FileService,
	after string,
	limit int,
) (markers []string, remaining bool, err error) {
	return listUnpublishedObjectCleanupAfter(
		ctx, fs, unpublishedObjectCleanupDir, after, limit)
}

func listUnpublishedObjectCleanupAfter(
	ctx context.Context,
	fs fileservice.FileService,
	dir string,
	after string,
	limit int,
) (markers []string, remaining bool, err error) {
	if limit <= 0 {
		return nil, false, nil
	}
	candidates := make(maxMarkerPathHeap, 0, limit+1)
	for entry, listErr := range fs.List(ctx, dir) {
		if listErr != nil {
			return nil, true, listErr
		}
		if entry.IsDir {
			continue
		}
		marker := dir + entry.Name
		if marker <= after {
			continue
		}
		if len(candidates) < limit+1 {
			heap.Push(&candidates, marker)
			continue
		}
		if marker < candidates[0] {
			candidates[0] = marker
			heap.Fix(&candidates, 0)
		}
	}
	markers = append(markers, candidates...)
	sort.Strings(markers)
	if len(markers) > limit {
		return markers[:limit], true, nil
	}
	return markers, false, nil
}

type maxMarkerPathHeap []string

func (h maxMarkerPathHeap) Len() int           { return len(h) }
func (h maxMarkerPathHeap) Less(i, j int) bool { return h[i] > h[j] }
func (h maxMarkerPathHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *maxMarkerPathHeap) Push(value any)    { *h = append(*h, value.(string)) }
func (h *maxMarkerPathHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = ""
	*h = old[:last]
	return value
}

// ReplayUnpublishedObjectCleanupFrom replays at most limit markers.
// decide must fence active writers and catalog-owned objects; replay never
// infers ownership from a bare file name.
func ReplayUnpublishedObjectCleanupFrom(
	ctx context.Context,
	markerFS fileservice.FileService,
	objectFS fileservice.FileService,
	decide func(UnpublishedObject) (UnpublishedObjectCleanupDecision, error),
	onReplayed func(marker string),
	limit int,
) (replayed int, inspected int, remaining bool, err error) {
	markers, remaining, err := ListUnpublishedObjectCleanup(
		ctx, markerFS, limit)
	if err != nil {
		return 0, 0, true, err
	}
	return replayUnpublishedObjectCleanupMarkers(
		ctx, markerFS, objectFS, decide, onReplayed, nil, markers, remaining)
}

// ReplayUnpublishedObjectCleanupPageFrom replays one cursor page. The
// onReleaseDeferred callback receives exact marker identities whose object
// ownership is already terminal but whose marker deletion was ambiguous. The
// caller must retain those identities for exact-name reconciliation because a
// successfully deleted marker will not appear in a later directory listing.
func ReplayUnpublishedObjectCleanupPageFrom(
	ctx context.Context,
	markerFS fileservice.FileService,
	objectFS fileservice.FileService,
	decide func(UnpublishedObject) (UnpublishedObjectCleanupDecision, error),
	onReplayed func(marker string),
	onReleaseDeferred func(marker string),
	after string,
	limit int,
) (
	replayed int,
	inspected int,
	next string,
	remaining bool,
	err error,
) {
	return replayUnpublishedObjectCleanupPageFrom(
		ctx,
		markerFS,
		objectFS,
		unpublishedObjectCleanupDir,
		decide,
		onReplayed,
		onReleaseDeferred,
		after,
		limit,
	)
}

// ReplayCCPRUnpublishedObjectCleanupPageFrom replays one bounded page of
// cross-CN publication intents from shared storage.
func ReplayCCPRUnpublishedObjectCleanupPageFrom(
	ctx context.Context,
	fs fileservice.FileService,
	decide func(UnpublishedObject) (UnpublishedObjectCleanupDecision, error),
	after string,
	limit int,
) (
	replayed int,
	inspected int,
	next string,
	remaining bool,
	err error,
) {
	return replayUnpublishedObjectCleanupPageFrom(
		ctx,
		fs,
		fs,
		ccprUnpublishedObjectCleanupDir,
		decide,
		nil,
		nil,
		after,
		limit,
	)
}

func replayUnpublishedObjectCleanupPageFrom(
	ctx context.Context,
	markerFS fileservice.FileService,
	objectFS fileservice.FileService,
	dir string,
	decide func(UnpublishedObject) (UnpublishedObjectCleanupDecision, error),
	onReplayed func(marker string),
	onReleaseDeferred func(marker string),
	after string,
	limit int,
) (
	replayed int,
	inspected int,
	next string,
	remaining bool,
	err error,
) {
	markers, more, err := listUnpublishedObjectCleanupAfter(
		ctx, markerFS, dir, after, limit)
	if err != nil {
		return 0, 0, after, true, err
	}
	if more && len(markers) != 0 {
		next = markers[len(markers)-1]
	}
	replayed, inspected, remaining, err = replayUnpublishedObjectCleanupMarkers(
		ctx,
		markerFS,
		objectFS,
		decide,
		onReplayed,
		onReleaseDeferred,
		markers,
		more,
	)
	if after != "" && !more {
		// Reaching the end of a cursor pass does not prove that markers before
		// the cursor disappeared. Wrap once so a later empty first page can
		// establish quiescence without starving retained prefix entries.
		remaining = true
	}
	return
}

func replayUnpublishedObjectCleanupMarkers(
	ctx context.Context,
	markerFS fileservice.FileService,
	objectFS fileservice.FileService,
	decide func(UnpublishedObject) (UnpublishedObjectCleanupDecision, error),
	onReplayed func(marker string),
	onReleaseDeferred func(marker string),
	markers []string,
	remaining bool,
) (replayed int, inspected int, hasRemaining bool, err error) {
	hasRemaining = remaining

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
			return replayed, inspected, true, errors.Join(firstErr, cause)
		}
		inspected++
		intent, readErr := readUnpublishedObjectCleanup(ctx, markerFS, marker)
		if readErr != nil {
			hasRemaining = true
			recordFailure(readErr)
			continue
		}
		decision := DeleteUnpublishedObject
		if decide != nil {
			decision, readErr = decide(intent.Object)
			if readErr != nil {
				hasRemaining = true
				recordFailure(readErr)
				continue
			}
		}
		if decision == RetryUnpublishedObjectCleanup {
			hasRemaining = true
			continue
		}
		if decision == DeleteUnpublishedObject {
			if _, deleteErr := DeleteUnpublishedObjects(
				ctx, objectFS, intent.Object.File); deleteErr != nil {
				hasRemaining = true
				recordFailure(deleteErr)
				continue
			}
		}
		if deleteErr := DeleteUnpublishedObjectCleanup(
			ctx, markerFS, marker); deleteErr != nil {
			hasRemaining = true
			if onReleaseDeferred != nil {
				onReleaseDeferred(marker)
			}
			recordFailure(deleteErr)
			continue
		}
		replayed++
		if onReplayed != nil {
			onReplayed(marker)
		}
	}
	if failed != 0 {
		return replayed, inspected, hasRemaining, errors.Join(
			moerr.NewInternalErrorf(
				ctx, "replay %d unpublished object cleanup intents", failed),
			firstErr,
		)
	}
	return replayed, inspected, hasRemaining, nil
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
	invalidProtection := (intent.Object.SyncProtectionJobID == "") !=
		(intent.Object.SyncProtectionValidTS == 0) ||
		intent.Object.SyncProtectionValidTS < 0
	invalidCCPROwner := strings.HasPrefix(
		path, ccprUnpublishedObjectCleanupDir) &&
		(intent.Object.DBID == 0 || intent.Object.TableID == 0 ||
			intent.Object.TNShardID == 0 ||
			intent.Object.SyncProtectionJobID == "")
	if intent.Version != unpublishedObjectCleanupVersion ||
		intent.Object.File == "" || invalidProtection || invalidCCPROwner {
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
