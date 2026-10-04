// Copyright 2026 Matrix Origin
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

package write

import (
	"context"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

type AppendManifestRequest struct {
	Append             api.AppendRequest
	SnapshotID         int64
	SequenceNumber     int64
	TimestampMS        int64
	ManifestPath       string
	ManifestListPath   string
	PreservedManifests []api.ManifestFile
	MaxMemoryBytes     int64
	InitialMemoryBytes int64
}

type AppendManifestResult struct {
	Entries           []api.ManifestEntry
	ManifestFile      api.ManifestFile
	ManifestBytes     []byte
	ManifestListBytes []byte
	Attempt           *api.CommitAttempt
}

func BuildAppendManifests(ctx context.Context, req AppendManifestRequest) (*AppendManifestResult, error) {
	return buildAppendManifests(ctx, req, AppendBuilder{})
}

func buildAppendManifests(ctx context.Context, req AppendManifestRequest, builder api.WriteBuilder) (*AppendManifestResult, error) {
	if strings.TrimSpace(req.ManifestPath) == "" || strings.TrimSpace(req.ManifestListPath) == "" {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg append manifest paths are required", nil)
	}
	if builder == nil {
		builder = AppendBuilder{}
	}
	attempt, err := builder.BuildAppend(ctx, req.Append)
	if err != nil {
		return nil, err
	}
	validated := req.Append
	validated.DataFiles = attempt.DataFiles
	if err := ValidateAppendRequest(validated); err != nil {
		return nil, err
	}
	budget, err := newAppendManifestBudget(req.MaxMemoryBytes, req.InitialMemoryBytes)
	if err != nil {
		return nil, err
	}
	entryMemory := appendManifestEntriesMemoryWeight(attempt.DataFiles)
	if err := budget.reserve(entryMemory); err != nil {
		return nil, err
	}
	entries := make([]api.ManifestEntry, 0, len(attempt.DataFiles))
	for _, file := range attempt.DataFiles {
		entries = append(entries, api.ManifestEntry{
			Status:         api.ManifestEntryAdded,
			SnapshotID:     req.SnapshotID,
			SequenceNumber: req.SequenceNumber,
			FileSequence:   req.SequenceNumber,
			DataFile:       file,
		})
	}
	manifestBytes, err := budget.encodeManifest(entries, metadata.ManifestWriteOptions{
		FormatVersion: req.Append.FormatVersion,
		Schema:        req.Append.BaseSchema,
		PartitionSpec: req.Append.BaseSpec,
		Content:       api.ManifestContentData,
	})
	if err != nil {
		return nil, err
	}
	manifest := api.ManifestFile{
		Path:                  req.ManifestPath,
		Length:                int64(len(manifestBytes)),
		PartitionSpecID:       manifestPartitionSpecID(validated),
		Content:               api.ManifestContentData,
		SequenceNumber:        req.SequenceNumber,
		MinSequenceNumber:     req.SequenceNumber,
		AddedSnapshotID:       req.SnapshotID,
		AddedFilesCount:       len(attempt.DataFiles),
		AddedRowsCount:        totalRecords(attempt.DataFiles),
		AddedFilesSizeInBytes: totalFileSize(attempt.DataFiles),
		ManifestPathRedacted:  api.RedactPath(req.ManifestPath),
		ManifestPathHash:      api.PathHash(req.ManifestPath),
	}
	manifestList := append(cloneManifestFiles(req.PreservedManifests), manifest)
	if err := budget.reserve(metadata.ManifestListMemoryWeight(0, manifestList)); err != nil {
		return nil, err
	}
	var parentSnapshotID *int64
	if req.Append.BaseSnapshotID > 0 {
		parentSnapshotID = &req.Append.BaseSnapshotID
	}
	manifestListBytes, err := budget.encodeManifestList(manifestList, metadata.ManifestListWriteOptions{
		FormatVersion:    req.Append.FormatVersion,
		SnapshotID:       req.SnapshotID,
		ParentSnapshotID: parentSnapshotID,
		SequenceNumber:   req.SequenceNumber,
	})
	if err != nil {
		return nil, err
	}
	attempt.ManifestFiles = []api.ManifestFile{manifest}
	attempt.Updates = []api.CommitUpdate{
		api.NewAddSnapshotUpdate(api.NewCommitSnapshot(
			req.SnapshotID,
			req.Append.BaseSnapshotID,
			req.SequenceNumber,
			req.Append.BaseSchemaID,
			appendTimestampMS(req.TimestampMS),
			req.ManifestListPath,
			attempt.Summary,
		)),
		api.NewSetSnapshotRefUpdatePreservingRetention(attempt.TargetRef, attempt.TargetRefType, req.SnapshotID, req.Append.TargetRefRetention),
	}
	return &AppendManifestResult{
		Entries:           entries,
		ManifestFile:      manifest,
		ManifestBytes:     manifestBytes,
		ManifestListBytes: manifestListBytes,
		Attempt:           attempt,
	}, nil
}

func appendTimestampMS(timestampMS int64) int64 {
	if timestampMS > 0 {
		return timestampMS
	}
	return time.Now().UnixMilli()
}

func cloneManifestFiles(in []api.ManifestFile) []api.ManifestFile {
	if len(in) == 0 {
		return nil
	}
	out := make([]api.ManifestFile, len(in))
	copy(out, in)
	return out
}

func manifestPartitionSpecID(req api.AppendRequest) int {
	if len(req.DataFiles) == 0 {
		return req.BaseSpecID
	}
	specID := req.DataFiles[0].SpecID
	if specID == 0 {
		return req.BaseSpecID
	}
	for _, file := range req.DataFiles[1:] {
		if file.SpecID != 0 && file.SpecID != specID {
			return req.BaseSpecID
		}
	}
	return specID
}

func totalFileSize(files []api.DataFile) int64 {
	var total int64
	for _, file := range files {
		total += file.FileSizeInBytes
	}
	return total
}

func appendManifestEntriesMemoryWeight(files []api.DataFile) int64 {
	var total int64
	for idx := range files {
		weight := metadata.ManifestEntriesMemoryWeight(0, []api.ManifestEntry{{DataFile: files[idx]}})
		if total > math.MaxInt64-weight {
			return math.MaxInt64
		}
		total += weight
	}
	return total
}

type appendManifestBudget struct {
	limit int64
	used  int64
}

func newAppendManifestBudget(limit, initial int64) (*appendManifestBudget, error) {
	if initial < 0 {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg append initial memory usage cannot be negative", nil)
	}
	budget := &appendManifestBudget{limit: limit, used: initial}
	if limit > 0 && initial > limit {
		return nil, appendManifestMemoryExceeded(initial, 0, limit)
	}
	return budget, nil
}

func (b *appendManifestBudget) reserve(bytes int64) error {
	if b == nil || bytes <= 0 {
		return nil
	}
	if b.limit > 0 && (b.used > b.limit || bytes > b.limit-b.used) {
		return appendManifestMemoryExceeded(b.used, bytes, b.limit)
	}
	if b.used > math.MaxInt64-bytes {
		b.used = math.MaxInt64
	} else {
		b.used += bytes
	}
	return nil
}

func (b *appendManifestBudget) encodeManifest(entries []api.ManifestEntry, opts metadata.ManifestWriteOptions) ([]byte, error) {
	if b == nil || b.limit <= 0 {
		return metadata.EncodeManifest(entries, opts)
	}
	remaining, err := b.remainingAfterScratch(metadata.ManifestEntriesMemoryWeight(0, entries))
	if err != nil {
		return nil, err
	}
	payload, err := metadata.EncodeManifestBounded(entries, opts, remaining)
	if err != nil {
		return nil, err
	}
	if err := b.reserve(int64(cap(payload))); err != nil {
		return nil, err
	}
	return payload, nil
}

func (b *appendManifestBudget) encodeManifestList(manifests []api.ManifestFile, opts metadata.ManifestListWriteOptions) ([]byte, error) {
	if b == nil || b.limit <= 0 {
		return metadata.EncodeManifestList(manifests, opts)
	}
	remaining, err := b.remainingAfterScratch(metadata.ManifestListMemoryWeight(0, manifests))
	if err != nil {
		return nil, err
	}
	payload, err := metadata.EncodeManifestListBounded(manifests, opts, remaining)
	if err != nil {
		return nil, err
	}
	if err := b.reserve(int64(cap(payload))); err != nil {
		return nil, err
	}
	return payload, nil
}

func (b *appendManifestBudget) remainingAfterScratch(scratch int64) (int64, error) {
	if scratch < 0 || b.used > b.limit || scratch >= b.limit-b.used {
		return 0, appendManifestMemoryExceeded(b.used, scratch, b.limit)
	}
	return b.limit - b.used - scratch, nil
}

func appendManifestMemoryExceeded(used, requested, limit int64) error {
	return api.NewError(api.ErrPlanningLimitExceeded, "Iceberg append manifest materialization memory limit exceeded", map[string]string{
		"used_bytes":      strconv.FormatInt(used, 10),
		"requested_bytes": strconv.FormatInt(requested, 10),
		"limit_bytes":     strconv.FormatInt(limit, 10),
	})
}
