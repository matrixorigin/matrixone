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
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestIcebergScanPlanToRuntime(t *testing.T) {
	ctx := context.Background()
	runtime, err := icebergScanPlanToRuntime(ctx, &api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{
			SnapshotID:           22,
			SchemaID:             1,
			PartitionSpecIDs:     []int{0, 2},
			MetadataLocationHash: "meta-hash",
			ManifestListHash:     "manifest-hash",
			RefName:              "main",
			PlanningMode:         "client-side",
		},
		DataTasks: []api.DataFileTask{{
			DataFile: api.DataFile{
				FilePath:           "s3://warehouse/sales/orders/data/file-0.parquet",
				FileFormat:         "PARQUET",
				FileSizeInBytes:    100,
				RecordCount:        10,
				SpecID:             2,
				SequenceNumber:     9,
				FileSequenceNumber: 8,
				Partition:          map[string]any{"created_day": int32(19000), "region": "me-central-1"},
				SplitOffsets:       []int64{4, 8},
			},
			CredentialScope: "scope-hmac",
			ResidualFilter:  api.ResidualFilter{ExpressionSQL: "id > 10 and secret_token = 'raw'", AlwaysTrue: false},
			RowGroups:       []api.RowGroupSplit{{Ordinal: 2, StartRowOrdinal: 20, RowCount: 5, Bytes: 50}},
		}},
		DeleteTasks: []api.DeleteFileTask{{
			DataFile: api.DataFile{
				Content:        api.DataFileContentPositionDelete,
				FilePath:       "s3://warehouse/sales/orders/delete/pos.parquet",
				FileFormat:     "parquet",
				SequenceNumber: 11,
			},
			AppliesToPath:   "s3://warehouse/sales/orders/data/file-0.parquet",
			CredentialScope: "scope-hmac",
		}},
		ColumnMapping: []api.IcebergColumnMapping{
			{FieldID: 1, ColumnName: "id", MOType: api.MOType{Name: "BIGINT"}, Required: true, Projected: true, ParquetFieldID: 1},
			{FieldID: 2, ColumnName: "name", MOType: api.MOType{Name: "TEXT"}, Projected: false, ParquetFieldID: 2},
			{FieldID: 3, ColumnName: "price", MOType: api.MOType{Name: "DECIMAL", Width: 12, Scale: 2}, Projected: true, ParquetFieldID: 3},
			{FieldID: 4, ColumnName: "created_at", MOType: api.MOType{Name: "TIMESTAMP(6)"}, Projected: true, ParquetFieldID: 4, Hidden: true},
		},
		Profile: api.PlanningProfile{
			MetadataBytes:         1,
			ManifestListBytes:     2,
			ManifestBytes:         3,
			ManifestsSelected:     4,
			ManifestsPruned:       5,
			DataFilesSelected:     6,
			DataFilesPruned:       7,
			DataFileBytesSelected: 8,
			DataFileBytesPruned:   9,
			PlanningCacheHits:     10,
			PlanningCacheMiss:     11,
		},
		DeleteMaxMemoryBytes: 2048,
		EnableDeleteSpill:    true,
	}, "object-scope-ref")
	require.NoError(t, err)
	require.Equal(t, "object-scope-ref", runtime.objectIORef)
	require.NotNil(t, runtime.snapshot)
	require.Equal(t, int64(22), runtime.snapshot.SnapshotId)
	require.Equal(t, []int32{0, 2}, runtime.snapshot.PartitionSpecIds)
	require.Equal(t, "meta-hash", runtime.snapshot.MetadataLocationHash)
	require.Len(t, runtime.dataTasks, 1)
	require.Equal(t, "s3://warehouse/sales/orders/data/file-0.parquet", runtime.dataTasks[0].FilePath)
	require.Equal(t, "parquet", runtime.dataTasks[0].FileFormat)
	require.Equal(t, int64(50), runtime.dataTasks[0].FileSize)
	require.Equal(t, int64(5), runtime.dataTasks[0].RecordCount)
	require.Equal(t, int32(2), runtime.dataTasks[0].RowGroupStart)
	require.Equal(t, int32(3), runtime.dataTasks[0].RowGroupEnd)
	require.Equal(t, int64(9), runtime.dataTasks[0].ContentSequenceNumber)
	require.Equal(t, int64(8), runtime.dataTasks[0].FileSequenceNumber)
	require.Equal(t, "scope-hmac", runtime.dataTasks[0].CredentialScope)
	require.Equal(t, "19000", runtime.dataTasks[0].PartitionValues["created_day"])
	require.Equal(t, []int64{4, 8}, runtime.dataTasks[0].SplitOffsets)
	require.True(t, runtime.dataTasks[0].HasResidualFilter)
	require.NotEmpty(t, runtime.dataTasks[0].ResidualFilterHash)
	require.False(t, strings.Contains(runtime.dataTasks[0].String(), "raw"))
	require.False(t, strings.Contains(runtime.dataTasks[0].String(), "secret_token"))
	require.Len(t, runtime.columns, 4)
	require.Equal(t, int32(1), runtime.columns[0].IcebergFieldId)
	require.Equal(t, int32(types.T_int64), runtime.columns[0].MoType.Id)
	require.False(t, runtime.columns[0].DefaultNullFill)
	require.True(t, runtime.columns[1].DefaultNullFill)
	require.Equal(t, int32(types.T_text), runtime.columns[1].MoType.Id)
	require.Equal(t, int32(types.T_decimal64), runtime.columns[2].MoType.Id)
	require.Equal(t, int32(12), runtime.columns[2].MoType.Width)
	require.Equal(t, int32(2), runtime.columns[2].MoType.Scale)
	require.Equal(t, int32(types.T_timestamp), runtime.columns[3].MoType.Id)
	require.Equal(t, int32(6), runtime.columns[3].MoType.Scale)
	require.Equal(t, []int32{3}, runtime.hiddenReadCols)
	require.True(t, runtime.needRowOrdinal)
	require.Len(t, runtime.deleteTasks, 1)
	require.Equal(t, "position", runtime.deleteTasks[0].DeleteType)
	require.Equal(t, int64(11), runtime.deleteTasks[0].SequenceNumber)
	require.Equal(t, int64(2048), runtime.deleteMaxMemoryBytes)
	require.True(t, runtime.deleteSpillEnabled)
	require.Equal(t, int64(1), runtime.planningStats.IcebergMetadataBytes)
	require.Equal(t, int64(2), runtime.planningStats.IcebergManifestListBytes)
	require.Equal(t, int64(3), runtime.planningStats.IcebergManifestBytes)
	require.Equal(t, int64(4), runtime.planningStats.IcebergManifestsSelected)
	require.Equal(t, int64(5), runtime.planningStats.IcebergManifestsPruned)
	require.Equal(t, int64(6), runtime.planningStats.IcebergDataFilesSelected)
	require.Equal(t, int64(7), runtime.planningStats.IcebergDataFilesPruned)
	require.Equal(t, int64(8), runtime.planningStats.IcebergDataFileBytesSelected)
	require.Equal(t, int64(9), runtime.planningStats.IcebergDataFileBytesPruned)
	require.Equal(t, int64(10), runtime.planningStats.IcebergPlanningCacheHits)
	require.Equal(t, int64(11), runtime.planningStats.IcebergPlanningCacheMiss)
}

func TestIcebergDataTaskRowGroupShards(t *testing.T) {
	tasks := []*pipeline.IcebergDataFileTask{
		{FilePath: "a.parquet", RowGroupStart: 2, RowGroupEnd: 3, RecordCount: 10, FileSize: 100},
		{FilePath: "b.parquet"},
		{FilePath: "c.parquet", RowGroupStart: 0, RowGroupEnd: 1, RecordCount: 5, FileSize: 50},
	}
	shards := icebergDataTaskRowGroupShards(tasks)
	require.Len(t, shards, 2)
	require.Equal(t, int32(0), shards[0].FileIndex)
	require.Equal(t, int32(2), shards[0].RowGroupStart)
	require.Equal(t, int32(3), shards[0].RowGroupEnd)
	require.Equal(t, int64(10), shards[0].NumRows)
	require.Equal(t, int32(2), shards[1].FileIndex)
	require.Equal(t, int32(0), shards[1].RowGroupStart)
	require.Equal(t, int32(1), shards[1].RowGroupEnd)
}

func TestIcebergMOTypeToPlanType(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		typ  api.MOType
		oid  types.T
	}{
		{name: "bool", typ: api.MOType{Name: "BOOL"}, oid: types.T_bool},
		{name: "int", typ: api.MOType{Name: "INT"}, oid: types.T_int32},
		{name: "bigint", typ: api.MOType{Name: "BIGINT"}, oid: types.T_int64},
		{name: "float", typ: api.MOType{Name: "FLOAT"}, oid: types.T_float32},
		{name: "double", typ: api.MOType{Name: "DOUBLE"}, oid: types.T_float64},
		{name: "date", typ: api.MOType{Name: "DATE"}, oid: types.T_date},
		{name: "datetime", typ: api.MOType{Name: "DATETIME(6)"}, oid: types.T_datetime},
		{name: "timestamp", typ: api.MOType{Name: "TIMESTAMP(6)"}, oid: types.T_timestamp},
		{name: "text", typ: api.MOType{Name: "TEXT"}, oid: types.T_text},
		{name: "varbinary", typ: api.MOType{Name: "VARBINARY"}, oid: types.T_varbinary},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			typ, err := icebergMOTypeToPlanType(ctx, tc.typ)
			require.NoError(t, err)
			require.Equal(t, int32(tc.oid), typ.Id)
		})
	}
	typ, err := icebergMOTypeToPlanType(ctx, api.MOType{Name: "DECIMAL", Width: 38, Scale: 6})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal128), typ.Id)
	_, err = icebergMOTypeToPlanType(ctx, api.MOType{Name: "UNSUPPORTED"})
	require.Error(t, err)
}

func TestIcebergScanPlanToRuntimeRejectsNilPlan(t *testing.T) {
	_, err := icebergScanPlanToRuntime(context.Background(), nil, "")
	require.Error(t, err)
}

func TestIcebergScanPlanToRuntimeAlignsColumnsWithTableDef(t *testing.T) {
	ctx := context.Background()
	runtime, err := icebergScanPlanToRuntimeForTable(ctx, &api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{SnapshotID: 22},
		ColumnMapping: []api.IcebergColumnMapping{
			{FieldID: 1, ColumnName: "id", MOType: api.MOType{Name: "BIGINT"}, Required: true, Projected: true, ParquetFieldID: 1},
			{FieldID: 2, ColumnName: "name", MOType: api.MOType{Name: "TEXT"}, Projected: true, ParquetFieldID: 2},
			{FieldID: 3, ColumnName: "dropped_col", MOType: api.MOType{Name: "INT"}, Projected: false, ParquetFieldID: 3},
		},
	}, "", &plan.TableDef{Cols: []*plan.ColDef{
		{Name: "name", Typ: plan.Type{Id: int32(types.T_text)}},
		{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
	}})
	require.NoError(t, err)
	require.Len(t, runtime.columns, 2)
	require.Equal(t, int32(1), runtime.columns[0].MoColIndex)
	require.Equal(t, int32(1), runtime.columns[0].IcebergFieldId)
	require.Equal(t, "id", runtime.columns[0].CurrentFieldName)
	require.Equal(t, int32(0), runtime.columns[1].MoColIndex)
	require.Equal(t, int32(2), runtime.columns[1].IcebergFieldId)
	require.Equal(t, "name", runtime.columns[1].CurrentFieldName)
}

func TestIcebergScanPlanToRuntimeSkipsDMLMetadataColumns(t *testing.T) {
	ctx := context.Background()
	runtime, err := icebergScanPlanToRuntimeForTable(ctx, &api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{SnapshotID: 22},
		ColumnMapping: []api.IcebergColumnMapping{
			{FieldID: 1, ColumnName: "id", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 1},
			{FieldID: 2, ColumnName: "amount", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 2},
		},
	}, "", &plan.TableDef{Cols: []*plan.ColDef{
		{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: api.DMLDataFilePathColumnName, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		{Name: api.DMLRowOrdinalColumnName, Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
		{Name: "amount", Typ: plan.Type{Id: int32(types.T_int64)}},
	}})
	require.NoError(t, err)
	require.Len(t, runtime.columns, 2)
	require.Equal(t, int32(0), runtime.columns[0].MoColIndex)
	require.Equal(t, int32(1), runtime.columns[0].IcebergFieldId)
	require.Equal(t, int32(3), runtime.columns[1].MoColIndex)
	require.Equal(t, int32(2), runtime.columns[1].IcebergFieldId)
	require.True(t, runtime.needRowOrdinal)
}

func TestIcebergScanPlanToRuntimeBindsDMLMetadataColumnsInPlace(t *testing.T) {
	ctx := context.Background()
	runtime, err := icebergScanPlanToRuntimeForTable(ctx, &api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{SnapshotID: 22},
		ColumnMapping: []api.IcebergColumnMapping{
			{FieldID: 1, ColumnName: "id", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 1},
			{FieldID: 2, ColumnName: "amount", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 2},
			{FieldID: -1001, ColumnName: api.DMLDataFilePathColumnName, MOType: api.MOType{Name: "TEXT", Width: types.MaxVarcharLen}, Projected: true, Hidden: true},
			{FieldID: -1002, ColumnName: api.DMLRowOrdinalColumnName, MOType: api.MOType{Name: "BIGINT"}, Projected: true, Hidden: true},
		},
	}, "", &plan.TableDef{Cols: []*plan.ColDef{
		{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: api.DMLDataFilePathColumnName, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		{Name: api.DMLRowOrdinalColumnName, Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
		{Name: "amount", Typ: plan.Type{Id: int32(types.T_int64)}},
	}})
	require.NoError(t, err)
	require.Len(t, runtime.columns, 4)
	require.Equal(t, int32(0), runtime.columns[0].MoColIndex)
	require.Equal(t, int32(3), runtime.columns[1].MoColIndex)
	require.Equal(t, int32(1), runtime.columns[2].MoColIndex)
	require.Equal(t, int32(2), runtime.columns[3].MoColIndex)
	require.Empty(t, runtime.hiddenReadCols)
	require.True(t, runtime.needRowOrdinal)
}

func TestIcebergScanPlanToRuntimeKeepsRowOrdinalOffWithoutDMLPositionMetadata(t *testing.T) {
	ctx := context.Background()
	runtime, err := icebergScanPlanToRuntimeForTable(ctx, &api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{SnapshotID: 22},
		ColumnMapping: []api.IcebergColumnMapping{
			{FieldID: 1, ColumnName: "id", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 1},
		},
	}, "", &plan.TableDef{Cols: []*plan.ColDef{
		{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
	}})
	require.NoError(t, err)
	require.False(t, runtime.needRowOrdinal)
}

func TestIcebergScanPlanToRuntimeKeepsHiddenDeleteKeysMissingFromProjection(t *testing.T) {
	ctx := context.Background()
	runtime, err := icebergScanPlanToRuntimeForTable(ctx, &api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{SnapshotID: 22},
		ColumnMapping: []api.IcebergColumnMapping{
			{FieldID: 1, ColumnName: "id", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 1},
			{FieldID: 2, ColumnName: "hidden_key", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 2, Hidden: true},
			{FieldID: 4, ColumnName: "amount", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 4},
		},
	}, "", &plan.TableDef{Cols: []*plan.ColDef{
		{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: "amount", Typ: plan.Type{Id: int32(types.T_int64)}},
	}})
	require.NoError(t, err)
	require.Len(t, runtime.columns, 3)
	require.Equal(t, int32(0), runtime.columns[0].MoColIndex)
	require.Equal(t, int32(2), runtime.columns[1].MoColIndex)
	require.Equal(t, int32(1), runtime.columns[2].MoColIndex)
	require.Equal(t, []int32{2}, runtime.hiddenReadCols)
	require.True(t, runtime.columns[1].IsHidden)
	require.Equal(t, int32(2), runtime.columns[1].IcebergFieldId)
}

func TestIcebergScanPlanToRuntimePlacesHiddenKeysAfterDMLMetadataColumns(t *testing.T) {
	ctx := context.Background()
	runtime, err := icebergScanPlanToRuntimeForTable(ctx, &api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{SnapshotID: 22},
		ColumnMapping: []api.IcebergColumnMapping{
			{FieldID: 1, ColumnName: "id", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 1},
			{FieldID: 2, ColumnName: "hidden_key", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 2, Hidden: true},
			{FieldID: 3, ColumnName: "amount", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 3},
		},
	}, "", &plan.TableDef{Cols: []*plan.ColDef{
		{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: api.DMLDataFilePathColumnName, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		{Name: api.DMLRowOrdinalColumnName, Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
		{Name: "amount", Typ: plan.Type{Id: int32(types.T_int64)}},
	}})
	require.NoError(t, err)
	require.Len(t, runtime.columns, 3)
	require.Equal(t, int32(0), runtime.columns[0].MoColIndex)
	require.Equal(t, int32(4), runtime.columns[1].MoColIndex)
	require.Equal(t, int32(3), runtime.columns[2].MoColIndex)
	require.Equal(t, []int32{4}, runtime.hiddenReadCols)
	require.True(t, runtime.columns[1].IsHidden)
	require.Equal(t, int32(2), runtime.columns[1].IcebergFieldId)
}

func TestIcebergScanPlanToRuntimeRejectsAmbiguousTableColumns(t *testing.T) {
	_, err := icebergScanPlanToRuntimeForTable(context.Background(), &api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{SnapshotID: 22},
		ColumnMapping: []api.IcebergColumnMapping{
			{FieldID: 1, ColumnName: "name", MOType: api.MOType{Name: "TEXT"}, Projected: true, ParquetFieldID: 1},
		},
	}, "", &plan.TableDef{Cols: []*plan.ColDef{
		{Name: "Name"},
		{Name: "name"},
	}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ambiguous iceberg MO column name")
}

func TestIcebergScanPlanToRuntimeRejectsMissingTableColumnMapping(t *testing.T) {
	_, err := icebergScanPlanToRuntimeForTable(context.Background(), &api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{SnapshotID: 22},
		ColumnMapping: []api.IcebergColumnMapping{
			{FieldID: 1, ColumnName: "id", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 1},
		},
	}, "", &plan.TableDef{Cols: []*plan.ColDef{
		{Name: "id"},
		{Name: "amount"},
	}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "iceberg column mapping not found for MO column amount")
}
