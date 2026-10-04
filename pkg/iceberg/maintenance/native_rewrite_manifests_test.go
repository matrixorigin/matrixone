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
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

func TestRewriteManifestsMaterializerRewritesManifestObjects(t *testing.T) {
	oldManifestPath := "s3://warehouse/orders/metadata/old-manifest.avro"
	oldManifestListPath := "s3://warehouse/orders/metadata/snap-10.avro"
	entries := []api.ManifestEntry{{
		Status:         api.ManifestEntryAdded,
		SnapshotID:     10,
		SequenceNumber: 10,
		FileSequence:   10,
		DataFile: api.DataFile{
			Content:          api.DataFileContentData,
			FilePath:         "s3://warehouse/orders/data/part-1.parquet",
			FileFormat:       "parquet",
			RecordCount:      9,
			FileSizeInBytes:  123,
			ValueCounts:      map[int]int64{1: 9},
			NullValueCounts:  map[int]int64{1: 0},
			FilePathHash:     api.PathHash("s3://warehouse/orders/data/part-1.parquet"),
			FilePathRedacted: api.RedactPath("s3://warehouse/orders/data/part-1.parquet"),
		},
	}}
	manifestBytes, err := encodeMaintenanceTestManifest(entries)
	require.NoError(t, err)
	manifestListBytes, err := encodeMaintenanceTestManifestList([]api.ManifestFile{{
		Path:                 oldManifestPath,
		Length:               int64(len(manifestBytes)),
		PartitionSpecID:      0,
		Content:              api.ManifestContentData,
		SequenceNumber:       10,
		MinSequenceNumber:    10,
		AddedSnapshotID:      10,
		AddedFilesCount:      1,
		AddedRowsCount:       9,
		ManifestPathHash:     api.PathHash(oldManifestPath),
		ManifestPathRedacted: api.RedactPath(oldManifestPath),
	}})
	require.NoError(t, err)

	meta := maintenanceTestTableMetadata("s3://warehouse/orders")
	result, err := (RewriteManifestsMaterializer{
		ObjectReader: fakeRewriteManifestObjectReader{
			oldManifestListPath: manifestListBytes,
			oldManifestPath:     manifestBytes,
		},
	}).Materialize(context.Background(), RewriteManifestsMaterializeRequest{
		Metadata:       meta,
		Snapshot:       api.Snapshot{SnapshotID: 10, ManifestList: oldManifestListPath},
		SnapshotID:     11,
		SequenceNumber: 11,
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
	})
	require.NoError(t, err)
	require.Len(t, result.Manifests, 1)
	require.Len(t, result.Objects, 2)
	require.Equal(t, uint64(1), result.RewrittenManifestCount)
	require.ElementsMatch(t, []string{oldManifestListPath, oldManifestPath}, result.PostCommitOrphanPaths)

	rewritePrefix := "s3://warehouse/orders/metadata/mo-rewrite-manifests/rw-" + api.PathHash("idem-1")
	require.True(t, strings.HasPrefix(result.Manifests[0].Path, rewritePrefix), result.Manifests[0].Path)
	require.Equal(t, result.Manifests[0].Path, result.Objects[0].Location)
	require.Equal(t, int64(len(result.Objects[0].Payload)), result.Manifests[0].Length)
	require.Equal(t, api.PathHash(result.Manifests[0].Path), result.Manifests[0].ManifestPathHash)
	require.NotContains(t, result.Manifests[0].ManifestPathRedacted, "warehouse")
	require.Equal(t, rewritePrefix+"/manifest-list.avro", result.ManifestListPath)
	require.Equal(t, result.ManifestListPath, result.Objects[1].Location)

	rewrittenEntries, err := metadata.ReadManifest(result.Objects[0].Payload)
	require.NoError(t, err)
	require.Len(t, rewrittenEntries, 1)
	require.Equal(t, "s3://warehouse/orders/data/part-1.parquet", rewrittenEntries[0].DataFile.FilePath)
	require.Equal(t, int64(9), rewrittenEntries[0].DataFile.RecordCount)
	require.Equal(t, api.ManifestEntryExisting, rewrittenEntries[0].Status)
	require.Equal(t, int64(10), rewrittenEntries[0].SequenceNumber)
	require.Equal(t, int64(10), rewrittenEntries[0].FileSequence)

	rewrittenManifestList, err := metadata.ReadManifestList(result.Objects[1].Payload)
	require.NoError(t, err)
	require.Len(t, rewrittenManifestList, 1)
	require.Equal(t, result.Manifests[0].Path, rewrittenManifestList[0].Path)
	require.Equal(t, int64(len(result.Objects[0].Payload)), rewrittenManifestList[0].Length)
	require.NotEqual(t, oldManifestPath, rewrittenManifestList[0].Path)
	require.Equal(t, int64(11), rewrittenManifestList[0].SequenceNumber)
	require.Equal(t, int64(10), rewrittenManifestList[0].MinSequenceNumber)
	require.Zero(t, rewrittenManifestList[0].AddedFilesCount)
	require.Equal(t, 1, rewrittenManifestList[0].ExistingFilesCount)
}

func TestRewriteManifestsMaterializerUsesCustomPathPrefixAndManifestListDirFallback(t *testing.T) {
	oldManifestPath := "s3://warehouse/orders/metadata/old-manifest.avro"
	oldManifestListPath := "s3://warehouse/orders/metadata/snap-10.avro"
	manifestBytes, err := encodeMaintenanceTestManifest([]api.ManifestEntry{{
		Status:     api.ManifestEntryExisting,
		SnapshotID: 10,
		DataFile:   api.DataFile{Content: api.DataFileContentData, FilePath: "s3://warehouse/orders/data/part-1.parquet", FileFormat: "parquet"},
	}})
	require.NoError(t, err)
	manifestListBytes, err := encodeMaintenanceTestManifestList([]api.ManifestFile{{Path: oldManifestPath, Length: int64(len(manifestBytes))}})
	require.NoError(t, err)
	reader := fakeRewriteManifestObjectReader{
		oldManifestListPath: manifestListBytes,
		oldManifestPath:     manifestBytes,
	}

	result, err := (RewriteManifestsMaterializer{
		ObjectReader: reader,
		PathPrefix:   "s3://tmp/rewrite-prefix/",
	}).Materialize(context.Background(), RewriteManifestsMaterializeRequest{
		Metadata:       maintenanceTestTableMetadata("s3://warehouse/orders"),
		Snapshot:       api.Snapshot{SnapshotID: 10, ManifestList: oldManifestListPath},
		SnapshotID:     11,
		SequenceNumber: 11,
		JobID:          "job-1",
	})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(result.ManifestListPath, "s3://tmp/rewrite-prefix/mo-rewrite-manifests/rw-"+api.PathHash("job-1")))

	result, err = (RewriteManifestsMaterializer{ObjectReader: reader}).Materialize(context.Background(), RewriteManifestsMaterializeRequest{
		Metadata:       maintenanceTestTableMetadata("s3://warehouse/orders"),
		Snapshot:       api.Snapshot{SnapshotID: 10, ManifestList: oldManifestListPath},
		SnapshotID:     11,
		SequenceNumber: 11,
	})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(result.ManifestListPath, "s3://warehouse/orders/metadata/mo-rewrite-manifests/rw-"+api.PathHash("10")))
}

func TestRewriteManifestsMaterializerRequiresObjectReader(t *testing.T) {
	_, err := (RewriteManifestsMaterializer{}).Materialize(context.Background(), RewriteManifestsMaterializeRequest{
		Snapshot: api.Snapshot{SnapshotID: 10, ManifestList: "s3://warehouse/orders/metadata/snap-10.avro"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires an object reader")
}

func TestRewrittenMaintenanceManifestPreservesUnknownPositionReferenceCount(t *testing.T) {
	manifest, err := rewrittenMaintenanceManifest(RewriteManifestsMaterializeRequest{
		SnapshotID:     11,
		SequenceNumber: 11,
	}, api.ManifestFile{
		Content:                  api.ManifestContentDeletes,
		ReferencedDataFilesCount: 7,
	}, []api.ManifestEntry{{
		Status:         api.ManifestEntryExisting,
		SequenceNumber: 10,
		DataFile: api.DataFile{
			Content:     api.DataFileContentPositionDelete,
			RecordCount: 1,
		},
	}})
	require.NoError(t, err)
	require.Equal(t, 7, manifest.ReferencedDataFilesCount)
}

func TestRewrittenMaintenanceManifestRejectsAggregateOverflow(t *testing.T) {
	_, err := rewrittenMaintenanceManifest(RewriteManifestsMaterializeRequest{
		SnapshotID: 11, SequenceNumber: 11,
	}, api.ManifestFile{}, []api.ManifestEntry{
		{SequenceNumber: 1, DataFile: api.DataFile{FilePath: "a.parquet", RecordCount: math.MaxInt64}},
		{SequenceNumber: 1, DataFile: api.DataFile{FilePath: "b.parquet", RecordCount: 1}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrMetadataInvalid))
}

func TestRewriteManifestsMaterializerBoundsMetadataReads(t *testing.T) {
	const manifestList = "s3://warehouse/orders/metadata/snap-10.avro"
	_, err := (RewriteManifestsMaterializer{
		ObjectReader:   fakeRewriteManifestObjectReader{manifestList: []byte("oversized")},
		MaxMemoryBytes: 1,
	}).Materialize(context.Background(), RewriteManifestsMaterializeRequest{
		Metadata:   maintenanceTestTableMetadata("s3://warehouse/orders"),
		Snapshot:   api.Snapshot{SnapshotID: 10, ManifestList: manifestList},
		SnapshotID: 11,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrPlanningLimitExceeded))
}

type fakeRewriteManifestObjectReader map[string][]byte

func (r fakeRewriteManifestObjectReader) Read(ctx context.Context, location string, offset, length int64) ([]byte, error) {
	data := r[location]
	return append([]byte(nil), data...), nil
}
