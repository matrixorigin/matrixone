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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergcatalog "github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestOrphanSweeperDeletesUnreferencedCandidate(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-orphan-sweep-delete", fileservice.DisabledCacheConfig, nil)
	requireNoErr(t, err)
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	candidatePath := "s3://warehouse/orders/data/orphan.parquet"
	manifestListPath := "s3://warehouse/orders/metadata/snap-4.avro"
	manifestPath := "s3://warehouse/orders/metadata/manifest-4.avro"
	writeMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(candidatePath), []byte("candidate"))
	writeOrphanSweeperManifestFiles(t, ctx, fs, manifestListPath, manifestPath, "s3://warehouse/orders/data/live.parquet")

	store := newFakeOrphanSweeperStore(candidatePath, now)
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     orphanSweeperMetadataJSON(manifestListPath),
			}, nil
		},
	}
	sweeper := NewOrphanSweeper(OrphanSweeperOptions{
		Store:            store,
		AccountID:        7,
		CatalogFactory:   &fakeMaintenanceCatalogFactory{client: client},
		ObjectIOProvider: icebergio.ScopedProvider{FileService: fs},
		ScopeForLocation: orphanSweeperScopeForLocation,
		Now:              func() time.Time { return now },
		Limit:            5,
	})

	result, err := sweeper.Sweep(ctx)
	requireNoErr(t, err)
	if result.Scanned != 1 || result.Deleted != 1 || result.Failed != 0 {
		t.Fatalf("unexpected sweep result: %+v", result)
	}
	if store.statusByHash[api.PathHash(candidatePath)] != OrphanCleanupStatusDeleted || store.limit != 5 {
		t.Fatalf("unexpected cleanup status: status=%s limit=%d", store.statusByHash[api.PathHash(candidatePath)], store.limit)
	}
	vec := fileservice.IOVector{FilePath: maintenanceMemoryPath(candidatePath), Entries: []fileservice.IOEntry{{Offset: 0, Size: -1}}}
	if err := fs.Read(ctx, &vec); err == nil {
		t.Fatalf("expected candidate object to be deleted")
	}
}

func TestOrphanSweeperRefusesReferencedCandidate(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-orphan-sweep-referenced", fileservice.DisabledCacheConfig, nil)
	requireNoErr(t, err)
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	candidatePath := "s3://warehouse/orders/data/referenced.parquet"
	manifestListPath := "s3://warehouse/orders/metadata/snap-4.avro"
	manifestPath := "s3://warehouse/orders/metadata/manifest-4.avro"
	writeMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(candidatePath), []byte("candidate"))
	writeOrphanSweeperManifestFiles(t, ctx, fs, manifestListPath, manifestPath, candidatePath)

	store := newFakeOrphanSweeperStore(candidatePath, now)
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     orphanSweeperMetadataJSON(manifestListPath),
			}, nil
		},
	}
	sweeper := NewOrphanSweeper(OrphanSweeperOptions{
		Store:            store,
		AccountID:        7,
		CatalogFactory:   &fakeMaintenanceCatalogFactory{client: client},
		ObjectIOProvider: icebergio.ScopedProvider{FileService: fs},
		ScopeForLocation: orphanSweeperScopeForLocation,
		Now:              func() time.Time { return now },
		Limit:            5,
	})

	result, err := sweeper.Sweep(ctx)
	requireNoErr(t, err)
	if result.Scanned != 1 || result.Deleted != 0 || result.Failed != 1 {
		t.Fatalf("unexpected sweep result: %+v", result)
	}
	if store.statusByHash[api.PathHash(candidatePath)] != OrphanCleanupStatusFailed {
		t.Fatalf("referenced candidate must be marked failed, got %s", store.statusByHash[api.PathHash(candidatePath)])
	}
	_ = readMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(candidatePath))
}

func writeOrphanSweeperManifestFiles(t *testing.T, ctx context.Context, fs fileservice.ETLFileService, manifestListPath, manifestPath, dataPath string) {
	t.Helper()
	manifestBytes, err := metadata.EncodeManifest([]api.ManifestEntry{{
		Status:         api.ManifestEntryExisting,
		SnapshotID:     4,
		SequenceNumber: 4,
		DataFile: api.DataFile{
			Content:          api.DataFileContentData,
			FilePath:         dataPath,
			FileFormat:       "parquet",
			RecordCount:      1,
			FileSizeInBytes:  9,
			SpecID:           0,
			FilePathRedacted: api.RedactPath(dataPath),
			FilePathHash:     api.PathHash(dataPath),
		},
	}})
	requireNoErr(t, err)
	manifestListBytes, err := metadata.EncodeManifestList([]api.ManifestFile{{
		Path:    manifestPath,
		Content: api.ManifestContentData,
		Length:  int64(len(manifestBytes)),
	}})
	requireNoErr(t, err)
	writeMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(manifestPath), manifestBytes)
	writeMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(manifestListPath), manifestListBytes)
}

func orphanSweeperScopeForLocation(location string) icebergio.ObjectScope {
	return icebergio.ObjectScope{
		StorageLocation: maintenanceMemoryPath(location),
		Endpoint:        "https://s3.us-east-1.amazonaws.com",
		Region:          "us-east-1",
		Bucket:          "warehouse",
	}
}

func orphanSweeperMetadataJSON(manifestListPath string) []byte {
	return []byte(fmt.Sprintf(`{
		"format-version": 2,
		"table-uuid": "uuid-1",
		"location": "s3://warehouse/orders",
		"current-schema-id": 1,
		"schemas": [{"schema-id": 1, "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"current-snapshot-id": 4,
		"refs": {"main": {"snapshot-id": 4, "type": "branch"}},
		"snapshots": [{"snapshot-id": 4, "timestamp-ms": 1767484800000, "manifest-list": %q}]
	}`, manifestListPath))
}

func newFakeOrphanSweeperStore(candidatePath string, now time.Time) *fakeOrphanSweeperStore {
	return &fakeOrphanSweeperStore{
		catalog: model.Catalog{
			AccountID:        7,
			CatalogID:        42,
			Name:             "ksa_gold",
			Type:             "rest",
			URI:              "https://catalog.example.com",
			CapabilitiesJSON: `{"commit":true}`,
		},
		files: []model.OrphanFile{{
			AccountID:         7,
			JobID:             "job-1",
			CatalogID:         42,
			Namespace:         "sales",
			TableName:         "orders",
			TableLocationHash: api.PathHash("s3://warehouse/orders"),
			FilePath:          candidatePath,
			FilePathHash:      api.PathHash(candidatePath),
			FilePathRedacted:  api.RedactPath(candidatePath),
			WrittenAt:         now.Add(-2 * time.Hour),
			ExpireAt:          now.Add(-time.Hour),
			CleanupStatus:     "pending",
			Version:           3,
		}},
		statusByHash: make(map[string]string),
	}
}

type fakeOrphanSweeperStore struct {
	catalog      model.Catalog
	files        []model.OrphanFile
	limit        int
	statusByHash map[string]string
}

func (s *fakeOrphanSweeperStore) GetCatalogByID(ctx context.Context, accountID uint32, catalogID uint64) (model.Catalog, error) {
	return s.catalog, nil
}

func (s *fakeOrphanSweeperStore) ListOrphanCleanupCandidates(ctx context.Context, accountID uint32, limit int) ([]model.OrphanFile, error) {
	s.limit = limit
	return append([]model.OrphanFile(nil), s.files...), nil
}

func (s *fakeOrphanSweeperStore) UpdateOrphanFileCleanupStatus(ctx context.Context, accountID uint32, jobID, filePathHash, cleanupStatus string, expectedVersion uint64) error {
	s.statusByHash[filePathHash] = cleanupStatus
	return nil
}
