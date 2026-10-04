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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	icebergwrite "github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

func TestPublishAuditRecorderMapsAuditFields(t *testing.T) {
	store := &recordingPublishStore{}
	err := (PublishAuditRecorder{DAO: store}).RecordPublish(context.Background(), icebergwrite.PublishAudit{
		AccountID:       7,
		JobID:           "job-1",
		SourceDB:        "src",
		SourceTable:     "orders",
		TargetCatalogID: 42,
		TargetNamespace: "sales",
		TargetTable:     "orders_iceberg",
		SourceBatch:     "batch-1",
		WatermarkStart:  "2026-01-01",
		WatermarkEnd:    "2026-01-02",
		BusinessWindow:  "daily",
		SnapshotID:      100,
		CommitID:        "commit-100",
		RowCount:        -1,
		FileCount:       3,
		Status:          "failed",
		ErrorCategory:   string(api.ErrObjectIO),
	})
	if err != nil {
		t.Fatalf("record publish: %v", err)
	}
	if len(store.jobs) != 1 {
		t.Fatalf("expected one publish job, got %+v", store.jobs)
	}
	job := store.jobs[0]
	if job.SnapshotID != "100" || job.RowCount != 0 || job.FileCount != 3 || job.Version != 1 {
		t.Fatalf("unexpected publish job mapping: %+v", job)
	}
	if err := (PublishAuditRecorder{}).RecordPublish(context.Background(), icebergwrite.PublishAudit{}); err == nil {
		t.Fatalf("expected missing DAO error")
	}
}

func TestOrphanFileRecorderMapsCandidatesAndStopsOnError(t *testing.T) {
	now := time.Date(2026, 7, 8, 10, 0, 0, 0, time.UTC)
	store := &recordingOrphanStore{}
	err := (OrphanFileRecorder{DAO: store}).RecordOrphans(context.Background(), []icebergwrite.OrphanCandidate{{
		AccountID:         7,
		JobID:             "job-1",
		CatalogID:         42,
		Namespace:         "sales",
		TableName:         "orders",
		TableLocationHash: "table-hash",
		FilePath:          "s3://warehouse/orders/data/part-1.parquet",
		FilePathHash:      "file-hash",
		FilePathRedacted:  "<redacted:path:file-hash>",
		WrittenAt:         now,
		ExpireAt:          now.Add(time.Hour),
		CleanupStatus:     "pending",
	}})
	if err != nil {
		t.Fatalf("record orphan: %v", err)
	}
	if len(store.files) != 1 || store.files[0].Version != 1 || store.files[0].FilePathHash != "file-hash" {
		t.Fatalf("unexpected orphan mapping: %+v", store.files)
	}
	if err := (OrphanFileRecorder{}).RecordOrphans(context.Background(), nil); err == nil {
		t.Fatalf("expected missing DAO error")
	}
	sentinel := api.NewError(api.ErrObjectIO, "orphan store down", nil)
	store = &recordingOrphanStore{err: sentinel}
	err = (OrphanFileRecorder{DAO: store}).RecordOrphans(context.Background(), []icebergwrite.OrphanCandidate{{JobID: "job-1"}})
	if err != sentinel {
		t.Fatalf("expected store error, got %v", err)
	}
}

func TestWriteAdapterScalarHelpers(t *testing.T) {
	if optionalInt64String(0) != "" || optionalInt64String(12) != "12" {
		t.Fatalf("unexpected optional int64 formatting")
	}
	if nonNegativeUint64(-1) != 0 || nonNegativeUint64(0) != 0 || nonNegativeUint64(9) != 9 {
		t.Fatalf("unexpected non-negative conversion")
	}
}

type recordingPublishStore struct {
	jobs []model.PublishJob
	err  error
}

func (s *recordingPublishStore) InsertPublishJob(ctx context.Context, job model.PublishJob) error {
	if s.err != nil {
		return s.err
	}
	s.jobs = append(s.jobs, job)
	return nil
}

type recordingOrphanStore struct {
	files []model.OrphanFile
	err   error
}

func (s *recordingOrphanStore) InsertOrphanFile(ctx context.Context, file model.OrphanFile) error {
	if s.err != nil {
		return s.err
	}
	s.files = append(s.files, file)
	return nil
}
