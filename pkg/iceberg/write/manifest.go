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
	manifestBytes, err := metadata.EncodeManifest(entries, metadata.ManifestWriteOptions{
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
		PartitionSpecID:       manifestPartitionSpecID(req.Append),
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
	var parentSnapshotID *int64
	if req.Append.BaseSnapshotID > 0 {
		parentSnapshotID = &req.Append.BaseSnapshotID
	}
	manifestListBytes, err := metadata.EncodeManifestList(manifestList, metadata.ManifestListWriteOptions{
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
