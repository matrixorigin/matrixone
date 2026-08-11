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

package iceberg

import (
	"context"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	icebergwrite "github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

type PublishJobInserter interface {
	InsertPublishJob(ctx context.Context, job model.PublishJob) error
}

type OrphanFileInserter interface {
	InsertOrphanFile(ctx context.Context, file model.OrphanFile) error
}

type PublishAuditRecorder struct {
	DAO PublishJobInserter
}

func (r PublishAuditRecorder) RecordPublish(ctx context.Context, audit icebergwrite.PublishAudit) error {
	if r.DAO == nil {
		return moerr.NewInvalidInput(ctx, "iceberg publish audit recorder requires a DAO")
	}
	return r.DAO.InsertPublishJob(ctx, model.PublishJob{
		AccountID:       audit.AccountID,
		JobID:           audit.JobID,
		SourceDB:        audit.SourceDB,
		SourceTable:     audit.SourceTable,
		TargetCatalogID: audit.TargetCatalogID,
		TargetNamespace: audit.TargetNamespace,
		TargetTable:     audit.TargetTable,
		SourceBatch:     audit.SourceBatch,
		WatermarkStart:  audit.WatermarkStart,
		WatermarkEnd:    audit.WatermarkEnd,
		BusinessWindow:  audit.BusinessWindow,
		SnapshotID:      optionalInt64String(audit.SnapshotID),
		CommitID:        audit.CommitID,
		RowCount:        nonNegativeUint64(audit.RowCount),
		FileCount:       uint64(audit.FileCount),
		Status:          audit.Status,
		ErrorCategory:   audit.ErrorCategory,
		Version:         1,
	})
}

type OrphanFileRecorder struct {
	DAO OrphanFileInserter
}

func (r OrphanFileRecorder) RecordOrphans(ctx context.Context, candidates []icebergwrite.OrphanCandidate) error {
	if r.DAO == nil {
		return moerr.NewInvalidInput(ctx, "iceberg orphan recorder requires a DAO")
	}
	for _, candidate := range candidates {
		if err := r.DAO.InsertOrphanFile(ctx, model.OrphanFile{
			AccountID:         candidate.AccountID,
			JobID:             candidate.JobID,
			CatalogID:         candidate.CatalogID,
			Namespace:         candidate.Namespace,
			TableName:         candidate.TableName,
			TableLocationHash: candidate.TableLocationHash,
			FilePath:          candidate.FilePath,
			FilePathHash:      candidate.FilePathHash,
			FilePathRedacted:  candidate.FilePathRedacted,
			WrittenAt:         candidate.WrittenAt,
			ExpireAt:          candidate.ExpireAt,
			CleanupStatus:     candidate.CleanupStatus,
			Version:           1,
		}); err != nil {
			return err
		}
	}
	return nil
}

func optionalInt64String(value int64) string {
	if value == 0 {
		return ""
	}
	return strconv.FormatInt(value, 10)
}

func nonNegativeUint64(value int64) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}
