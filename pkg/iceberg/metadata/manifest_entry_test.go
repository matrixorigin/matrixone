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

package metadata

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

const sequenceManifestSchema = `{"type":"record","name":"entry","fields":[
 {"name":"status","type":"int"},
 {"name":"sequence_number","type":["null","long"]},
 {"name":"file_sequence_number","type":["null","long"]},
 {"name":"data_file","type":{"type":"record","name":"file","fields":[
 {"name":"file_path","type":"string"},{"name":"file_format","type":"string"},
 {"name":"record_count","type":"long"},{"name":"file_size_in_bytes","type":"long"}]}}]}`

func TestManifestEntrySequenceInheritance(t *testing.T) {
	for _, tc := range []struct {
		name               string
		status             api.ManifestEntryStatus
		data, file         any
		v1                 bool
		wantData, wantFile int64
		bad                bool
	}{
		{"added_null", api.ManifestEntryAdded, nil, nil, false, 10, 10, false},
		{"added_explicit_zero", api.ManifestEntryAdded, int64(0), int64(0), false, 0, 0, false},
		{"added_explicit_older", api.ManifestEntryAdded, int64(3), nil, false, 3, 10, false},
		{"independent_file_sequence", api.ManifestEntryAdded, nil, int64(4), false, 10, 4, false},
		{"existing_zero", api.ManifestEntryExisting, int64(0), int64(0), false, 0, 0, false},
		{"v1_absent", api.ManifestEntryAdded, nil, nil, true, 0, 0, false},
		{"existing_null", api.ManifestEntryExisting, nil, int64(0), false, 0, 0, true},
		{"deleted_null", api.ManifestEntryDeleted, int64(0), nil, false, 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := sequenceManifestSchema
			record := map[string]any{"status": int32(tc.status), "sequence_number": tc.data, "file_sequence_number": tc.file, "data_file": map[string]any{"file_path": "s3://warehouse/t/data.parquet", "file_format": "PARQUET", "record_count": int64(1), "file_size_in_bytes": int64(10)}}
			if tc.v1 {
				schema = strings.ReplaceAll(schema, ` {"name":"sequence_number","type":["null","long"]},`, "")
				schema = strings.ReplaceAll(schema, ` {"name":"file_sequence_number","type":["null","long"]},`, "")
				delete(record, "sequence_number")
				delete(record, "file_sequence_number")
			}
			entries, err := ReadManifest(encodeOCF(t, schema, []map[string]any{record}))
			if tc.bad {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, entries, 1)
			normalized, err := NormalizeManifestEntry(entries[0], api.ManifestFile{SequenceNumber: 10, PartitionSpecID: 7})
			require.NoError(t, err)
			require.Equal(t, tc.wantData, normalized.SequenceNumber)
			require.Equal(t, tc.wantFile, normalized.FileSequence)
			require.Equal(t, tc.wantData, normalized.DataFile.SequenceNumber)
			require.Equal(t, 7, normalized.DataFile.SpecID)
			again, err := NormalizeManifestEntry(normalized, api.ManifestFile{SequenceNumber: 20, PartitionSpecID: 7})
			require.NoError(t, err)
			require.Equal(t, normalized, again)
			payload, err := EncodeManifest([]api.ManifestEntry{normalized}, manifestTestOptions(api.ManifestContentData, api.SchemaField{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}}))
			require.NoError(t, err)
			decoded, err := ReadManifest(payload)
			require.NoError(t, err)
			durable, err := NormalizeManifestEntry(decoded[0], api.ManifestFile{SequenceNumber: 99})
			require.NoError(t, err)
			require.Equal(t, tc.wantData, durable.SequenceNumber)
			require.Equal(t, tc.wantFile, durable.FileSequence)
		})
	}
}

func TestLocalScanPlannerPreservesExplicitZeroSequence(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		fixture := newPlannerFixture(t, 1)
		manifest := &fixture.facade.manifests[0]
		manifest.SequenceNumber = 10
		entries := fixture.facade.entries[manifest.Path]
		entries[0].SequenceNumberSet = explicit
		entries[0].FileSequenceSet = explicit
		fixture.facade.entries[manifest.Path] = entries
		scan, err := fixture.planner().PlanScan(context.Background(), api.ScanPlanRequest{Namespace: api.Namespace{"sales"}, Table: "orders", Ref: "main"})
		require.NoError(t, err)
		require.Len(t, scan.DataTasks, 1)
		want := int64(10)
		if explicit {
			want = 0
		}
		require.Equal(t, want, scan.DataTasks[0].DataFile.SequenceNumber)
		require.Equal(t, want, scan.DataTasks[0].DataFile.FileSequenceNumber)
	}
}
