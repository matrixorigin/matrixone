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

package maintenance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

func TestMetadataReferenceCheckerFindsCommittedReferences(t *testing.T) {
	manifestEntries := []api.ManifestEntry{{
		Status:         api.ManifestEntryAdded,
		SnapshotID:     10,
		SequenceNumber: 10,
		FileSequence:   10,
		DataFile: api.DataFile{
			Content:         api.DataFileContentData,
			FilePath:        "s3://warehouse/orders/data/part-1.parquet",
			FileFormat:      "parquet",
			RecordCount:     1,
			FileSizeInBytes: 1,
		},
	}}
	manifestBytes, err := encodeMaintenanceTestManifest(manifestEntries)
	require.NoError(t, err)
	manifestListBytes, err := encodeMaintenanceTestManifestList([]api.ManifestFile{{
		Path:            "s3://warehouse/orders/metadata/manifest-1.avro",
		Length:          int64(len(manifestBytes)),
		Content:         api.ManifestContentData,
		SequenceNumber:  10,
		AddedSnapshotID: 10,
	}})
	require.NoError(t, err)

	checker := MetadataReferenceChecker{
		Loader: MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) {
			require.Equal(t, "sales", req.Namespace)
			require.Equal(t, "orders", req.Table)
			currentSnapshotID := int64(10)
			return &api.TableMetadata{
				CurrentSnapshotID: &currentSnapshotID,
				Snapshots: []api.Snapshot{{
					SnapshotID:   10,
					ManifestList: "s3://warehouse/orders/metadata/snap-10.avro",
				}},
			}, nil
		}),
		ObjectReader: fakeOrphanReferenceReader{
			"s3://warehouse/orders/metadata/snap-10.avro":    manifestListBytes,
			"s3://warehouse/orders/metadata/manifest-1.avro": manifestBytes,
		},
	}

	referenced, err := checker.IsReferenced(context.Background(), write.OrphanCandidate{
		AccountID: 1,
		CatalogID: 2,
		Namespace: "sales",
		TableName: "orders",
		FilePath:  "s3://warehouse/orders/data/part-1.parquet",
	})
	require.NoError(t, err)
	require.True(t, referenced)

	referenced, err = checker.IsReferenced(context.Background(), write.OrphanCandidate{
		AccountID: 1,
		CatalogID: 2,
		Namespace: "sales",
		TableName: "orders",
		FilePath:  "s3://warehouse/orders/data/orphan.parquet",
	})
	require.NoError(t, err)
	require.False(t, referenced)

	referenced, err = checker.IsReferenced(context.Background(), write.OrphanCandidate{
		AccountID: 1,
		CatalogID: 2,
		Namespace: "sales",
		TableName: "orders",
		FilePath:  "s3://warehouse/orders/metadata/manifest-1.avro",
	})
	require.NoError(t, err)
	require.True(t, referenced)
}

type fakeOrphanReferenceReader map[string][]byte

func (r fakeOrphanReferenceReader) Read(ctx context.Context, location string, offset, length int64) ([]byte, error) {
	data := r[location]
	return append([]byte(nil), data...), nil
}
