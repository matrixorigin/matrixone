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

package compile

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type currentSchemaManifestListReader []byte

func (r currentSchemaManifestListReader) Read(_ context.Context, _ string, offset, length int64) ([]byte, error) {
	if offset >= int64(len(r)) {
		return nil, nil
	}
	end := int64(len(r))
	if length >= 0 && length < end-offset {
		end = offset + length
	}
	return r[offset:end], nil
}

// A schema-only commit does not create a new data snapshot. Current reads must
// still use current-schema-id instead of the schema at snapshot creation.
func TestIcebergCurrentSchemaAfterRename(t *testing.T) {
	for _, tc := range []struct {
		name             string
		currentName      string
		snapshotSchemaID int
		ref              string
		selector         api.SnapshotSelector
		want             string
	}{
		{"unchanged_control", "id", 0, "main", api.SnapshotSelector{}, "id"},
		{"rename_without_new_snapshot", "renamed_id", 0, "main", api.SnapshotSelector{}, "renamed_id"},
		{"new_snapshot_after_rename_control", "renamed_id", 1, "main", api.SnapshotSelector{}, "renamed_id"},
		{"unnamed_current", "renamed_id", 0, "", api.SnapshotSelector{}, "renamed_id"},
		{"named_branch", "renamed_id", 0, "dev", api.SnapshotSelector{}, "renamed_id"},
		{"selector_ref_precedence", "renamed_id", 0, "audit", api.SnapshotSelector{RefName: "dev"}, "renamed_id"},
		{"tag", "renamed_id", 0, "audit", api.SnapshotSelector{}, "id"},
		{"explicit_snapshot", "renamed_id", 0, "main", api.SnapshotSelector{HasSnapshotID: true, SnapshotID: 10}, "id"},
		{"timestamp", "renamed_id", 0, "main", api.SnapshotSelector{HasTimestampMS: true, TimestampMS: 1710000000000}, "id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			current := int64(10)
			meta := api.TableMetadata{
				FormatVersion: 2, TableUUID: "58991825-2910-4bfb-bf9e-14629a2f4db1", Location: "s3://warehouse/orders", CurrentSchemaID: 1,
				LastSequenceNumber: 1, LastUpdatedMS: 1710001000000,
				Schemas: []api.Schema{
					{SchemaID: 0, Fields: []api.SchemaField{{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}}}},
					{SchemaID: 1, Fields: []api.SchemaField{{ID: 1, Name: tc.currentName, Type: api.IcebergType{Kind: api.TypeLong}}}},
				},
				PartitionSpecs: []api.PartitionSpec{{SpecID: 0}}, CurrentSnapshotID: &current,
				Snapshots: []api.Snapshot{{SnapshotID: 10, SequenceNumber: 1, TimestampMS: 1710000000000, ManifestList: "s3://warehouse/orders/metadata/list.avro", SchemaID: &tc.snapshotSchemaID}},
				Refs:      map[string]api.SnapshotRef{"main": {SnapshotID: 10, Type: "branch"}, "dev": {SnapshotID: 10, Type: "branch"}, "audit": {SnapshotID: 10, Type: "tag"}},
			}
			metadataJSON, err := json.Marshal(meta)
			require.NoError(t, err)
			metadataJSON = bytes.Replace(metadataJSON, []byte(`"Refs":`), []byte(`"refs":`), 1)
			list, err := metadata.EncodeManifestList(nil, metadata.ManifestListWriteOptions{FormatVersion: 2, SnapshotID: 10, SequenceNumber: 1})
			require.NoError(t, err)
			planner := metadata.LocalScanPlanner{
				Metadata: metadata.NativeFacade{}, ObjectReader: currentSchemaManifestListReader(list),
				Catalog: &catalog.MockClient{LoadTableFunc: func(context.Context, api.LoadTableRequest) (*api.LoadTableResponse, error) {
					return &api.LoadTableResponse{MetadataLocation: "s3://warehouse/orders/metadata/v2.json", MetadataJSON: metadataJSON}, nil
				}},
			}
			scan, err := planner.PlanScan(ctx, api.ScanPlanRequest{Namespace: api.Namespace{"sales"}, Table: "orders", Ref: tc.ref, Snapshot: tc.selector})
			require.NoError(t, err)
			require.Len(t, scan.ColumnMapping, 1)
			_, err = icebergScanPlanToRuntimeForTable(ctx, scan, "", &plan.TableDef{Cols: []*plan.ColDef{{Name: tc.want, Typ: plan.Type{Id: int32(types.T_int64)}}}})
			t.Logf("current schema column=%s; snapshot schema=%d; planned column=%s; compile error=%v", tc.currentName, tc.snapshotSchemaID, scan.ColumnMapping[0].ColumnName, err)
			require.NoError(t, err, "a current read after a schema-only rename must remain compilable")
			require.Equal(t, tc.want, scan.ColumnMapping[0].ColumnName)
			require.Equal(t, 1, scan.ColumnMapping[0].FieldID)
			if tc.want != tc.currentName {
				require.Equal(t, 0, scan.Snapshot.SchemaID)
			} else {
				require.Equal(t, 1, scan.Snapshot.SchemaID)
			}
		})
	}
}
