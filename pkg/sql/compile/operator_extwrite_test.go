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
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func extWriteCreatesql(t *testing.T, pattern string) string {
	opt := []string{"format", "csv"}
	if pattern != "" {
		opt = append(opt, "write_file_pattern", pattern)
	}
	raw, err := json.Marshal(&tree.ExternParam{ExParamConst: tree.ExParamConst{Option: opt}})
	require.NoError(t, err)
	return string(raw)
}

func icebergInsertCreatesql() string {
	return sqliceberg.BuildCreateSQLEnvelope(model.TableMapping{
		Namespace:  "sales",
		TableName:  "orders",
		DefaultRef: model.DefaultRefMain,
		ReadMode:   model.ReadModeAppendOnly,
		WriteMode:  "append_only",
	}, "ksa_gold")
}

func TestIsExternalWriteInsert(t *testing.T) {
	// nil InsertCtx
	require.False(t, isExternalWriteInsert(&plan.Node{}))

	// nil TableDef
	require.False(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{}}))

	// regular (non-external) table
	require.False(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{TableType: catalog.SystemOrdinaryRel},
	}}))

	// external table but read-only (no write_file_pattern)
	require.False(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{
			TableType: catalog.SystemExternalRel,
			Createsql: extWriteCreatesql(t, ""),
		},
	}}))

	// external table with malformed Createsql
	require.False(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{
			TableType: catalog.SystemExternalRel,
			Createsql: "{not json",
		},
	}}))

	// writable external table
	require.True(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{
			TableType: catalog.SystemExternalRel,
			Createsql: extWriteCreatesql(t, "stage://s/p-%U.csv"),
		},
	}}))
}

func TestIcebergAppendInsertDispatchesBeforeExternalWrite(t *testing.T) {
	restoreRuntimeVariableForTest(t, "", IcebergAppendCoordinatorFactoryRuntimeKey, icebergwrite.CoordinatorFactoryFunc(func(ctx context.Context, req icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
		return nil, nil
	}))
	node := &plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{
			Name:      "gold_orders",
			TableType: catalog.SystemExternalRel,
			Createsql: icebergInsertCreatesql(),
		},
	}}
	ok, err := isIcebergAppendInsert(context.Background(), node)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, isExternalWriteInsert(node))

	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = defines.AttachAccountId(context.Background(), 42)
	op, err := constructIcebergInsert(proc, node)
	require.NoError(t, err)
	writer, ok := op.(*icebergwrite.IcebergWrite)
	require.True(t, ok)
	require.Equal(t, "gold_orders", writer.Request.TableDef.Name)
	require.Equal(t, uint32(42), writer.Request.AccountID)
	require.Equal(t, "ksa_gold", writer.Request.CatalogName)
	require.Equal(t, "sales", writer.Request.Namespace)
	require.Equal(t, "orders", writer.Request.Table)
	require.Equal(t, model.DefaultRefMain, writer.Request.DefaultRef)
	require.Equal(t, icebergwrite.OperationAppend, writer.Request.Operation)
	require.NotNil(t, writer.Factory)
}

func TestIcebergDeleteIntentBuildsDeleteWriteRequest(t *testing.T) {
	restoreRuntimeVariableForTest(t, "", IcebergAppendCoordinatorFactoryRuntimeKey, icebergwrite.CoordinatorFactoryFunc(func(ctx context.Context, req icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
		return nil, nil
	}))
	node := &plan.Node{
		ExtraOptions: icebergapi.DMLDeletePlanExtraOptions,
		InsertCtx: &plan.InsertCtx{
			TableDef: &plan.TableDef{
				Name:      "gold_orders",
				TableType: catalog.SystemExternalRel,
				Createsql: icebergInsertCreatesql(),
				Cols: []*plan.ColDef{
					{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
					{Name: icebergapi.DMLDataFilePathColumnName, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
					{Name: icebergapi.DMLRowOrdinalColumnName, Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
				},
			},
		},
	}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = defines.AttachAccountId(context.Background(), 42)
	op, err := constructIcebergInsert(proc, node)
	require.NoError(t, err)
	writer, ok := op.(*icebergwrite.IcebergWrite)
	require.True(t, ok)
	require.Equal(t, uint32(42), writer.Request.AccountID)
	require.Equal(t, icebergwrite.OperationDelete, writer.Request.Operation)
	require.Equal(t, int32(1), writer.Request.DataFilePathColumnIndex)
	require.Equal(t, int32(2), writer.Request.RowOrdinalColumnIndex)
	require.Equal(t, []string{"id", icebergapi.DMLDataFilePathColumnName, icebergapi.DMLRowOrdinalColumnName}, writer.Request.Attrs)
}

func TestIcebergUpdateIntentBuildsUpdateWriteRequest(t *testing.T) {
	restoreRuntimeVariableForTest(t, "", IcebergAppendCoordinatorFactoryRuntimeKey, icebergwrite.CoordinatorFactoryFunc(func(ctx context.Context, req icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
		return nil, nil
	}))
	node := &plan.Node{
		ExtraOptions: icebergapi.DMLUpdatePlanExtraOptions,
		InsertCtx: &plan.InsertCtx{
			TableDef: &plan.TableDef{
				Name:      "gold_orders",
				TableType: catalog.SystemExternalRel,
				Createsql: icebergInsertCreatesql(),
				Cols: []*plan.ColDef{
					{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
					{Name: icebergapi.DMLDataFilePathColumnName, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
					{Name: icebergapi.DMLRowOrdinalColumnName, Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
				},
			},
		},
	}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = defines.AttachAccountId(context.Background(), 42)
	op, err := constructIcebergInsert(proc, node)
	require.NoError(t, err)
	writer, ok := op.(*icebergwrite.IcebergWrite)
	require.True(t, ok)
	require.Equal(t, uint32(42), writer.Request.AccountID)
	require.Equal(t, icebergwrite.OperationUpdate, writer.Request.Operation)
	require.Equal(t, int32(1), writer.Request.DataFilePathColumnIndex)
	require.Equal(t, int32(2), writer.Request.RowOrdinalColumnIndex)
	require.Equal(t, []string{"id", icebergapi.DMLDataFilePathColumnName, icebergapi.DMLRowOrdinalColumnName}, writer.Request.Attrs)
}

func TestIcebergMergeIntentBuildsMergeWriteRequest(t *testing.T) {
	restoreRuntimeVariableForTest(t, "", IcebergAppendCoordinatorFactoryRuntimeKey, icebergwrite.CoordinatorFactoryFunc(func(ctx context.Context, req icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
		return nil, nil
	}))
	node := &plan.Node{
		ExtraOptions: icebergapi.DMLMergePlanExtraOptions,
		InsertCtx: &plan.InsertCtx{
			TableDef: &plan.TableDef{
				Name:      "gold_orders",
				TableType: catalog.SystemExternalRel,
				Createsql: icebergInsertCreatesql(),
				Cols: []*plan.ColDef{
					{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
					{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
					{Name: icebergapi.DMLDataFilePathColumnName, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
					{Name: icebergapi.DMLRowOrdinalColumnName, Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
					{Name: icebergapi.DMLMergeActionColumnName, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
				},
			},
		},
	}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = defines.AttachAccountId(context.Background(), 42)
	op, err := constructIcebergInsert(proc, node)
	require.NoError(t, err)
	writer, ok := op.(*icebergwrite.IcebergWrite)
	require.True(t, ok)
	require.Equal(t, uint32(42), writer.Request.AccountID)
	require.Equal(t, icebergwrite.OperationMerge, writer.Request.Operation)
	require.Equal(t, int32(2), writer.Request.DataFilePathColumnIndex)
	require.Equal(t, int32(3), writer.Request.RowOrdinalColumnIndex)
	require.Equal(t, int32(4), writer.Request.MergeActionColumnIndex)
	require.Equal(t, []string{"id", "name", icebergapi.DMLDataFilePathColumnName, icebergapi.DMLRowOrdinalColumnName, icebergapi.DMLMergeActionColumnName}, writer.Request.Attrs)
}

func TestIcebergOverwriteIntentBuildsOverwriteWriteRequest(t *testing.T) {
	restoreRuntimeVariableForTest(t, "", IcebergAppendCoordinatorFactoryRuntimeKey, icebergwrite.CoordinatorFactoryFunc(func(ctx context.Context, req icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
		return nil, nil
	}))
	node := &plan.Node{
		ExtraOptions: icebergapi.DMLOverwritePlanExtraOptions,
		InsertCtx: &plan.InsertCtx{
			TableDef: &plan.TableDef{
				Name:      "gold_orders",
				TableType: catalog.SystemExternalRel,
				Createsql: icebergInsertCreatesql(),
				Cols: []*plan.ColDef{
					{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
				},
			},
		},
	}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = defines.AttachAccountId(context.Background(), 42)
	op, err := constructIcebergInsert(proc, node)
	require.NoError(t, err)
	writer, ok := op.(*icebergwrite.IcebergWrite)
	require.True(t, ok)
	require.Equal(t, uint32(42), writer.Request.AccountID)
	require.Equal(t, icebergwrite.OperationOverwrite, writer.Request.Operation)
	require.Equal(t, int32(-1), writer.Request.DataFilePathColumnIndex)
	require.Equal(t, int32(-1), writer.Request.RowOrdinalColumnIndex)
	require.Equal(t, []string{"id"}, writer.Request.Attrs)
}

func TestIcebergPartitionOverwriteIntentBuildsOverwriteWriteRequest(t *testing.T) {
	restoreRuntimeVariableForTest(t, "", IcebergAppendCoordinatorFactoryRuntimeKey, icebergwrite.CoordinatorFactoryFunc(func(ctx context.Context, req icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
		return nil, nil
	}))
	extraOptions, err := icebergapi.EncodeDMLOverwritePartitionPlanExtraOptions(map[string]any{
		"region": "ksa",
		"day":    int64(20260624),
	})
	require.NoError(t, err)
	node := &plan.Node{
		ExtraOptions: extraOptions,
		InsertCtx: &plan.InsertCtx{
			TableDef: &plan.TableDef{
				Name:      "gold_orders",
				TableType: catalog.SystemExternalRel,
				Createsql: icebergInsertCreatesql(),
				Cols: []*plan.ColDef{
					{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
				},
			},
		},
	}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = defines.AttachAccountId(context.Background(), 42)
	op, err := constructIcebergInsert(proc, node)
	require.NoError(t, err)
	writer, ok := op.(*icebergwrite.IcebergWrite)
	require.True(t, ok)
	require.Equal(t, icebergwrite.OperationOverwrite, writer.Request.Operation)
	require.Equal(t, "partition", writer.Request.DMLScan.OverwriteScope)
	require.Equal(t, "ksa", writer.Request.DMLScan.OverwritePartition["region"])
	require.Equal(t, int64(20260624), writer.Request.DMLScan.OverwritePartition["day"])
}

func TestIcebergWriteNeedsDMLScanMetadata(t *testing.T) {
	require.True(t, icebergWriteNeedsDMLScanMetadata(icebergwrite.OperationDelete))
	require.True(t, icebergWriteNeedsDMLScanMetadata(icebergwrite.OperationUpdate))
	require.True(t, icebergWriteNeedsDMLScanMetadata(icebergwrite.OperationMerge))
	require.False(t, icebergWriteNeedsDMLScanMetadata(icebergwrite.OperationOverwrite))
	require.False(t, icebergWriteNeedsDMLScanMetadata(icebergwrite.OperationAppend))
}

func TestIcebergDMLScanMetadataFromPlanIgnoresDeleteTasks(t *testing.T) {
	metadata := icebergDMLScanMetadataFromPlan(&icebergapi.IcebergScanPlan{
		Snapshot:    icebergapi.SnapshotPlan{SnapshotID: 30, SchemaID: 9, RefName: "main"},
		ObjectIORef: "iceberg-object-io://test-ref",
		DataTasks: []icebergapi.DataFileTask{
			{DataFile: icebergapi.DataFile{FilePath: "s3://warehouse/gold/orders/data/a.parquet", SpecID: 3}},
			{DataFile: icebergapi.DataFile{FilePath: "s3://warehouse/gold/orders/data/a.parquet", SpecID: 3}},
			{DataFile: icebergapi.DataFile{FilePath: "s3://warehouse/gold/orders/data/b.parquet", SpecID: 4}},
		},
		DeleteTasks: []icebergapi.DeleteFileTask{{
			DataFile:      icebergapi.DataFile{FilePath: "s3://warehouse/gold/orders/delete/pos-a.parquet"},
			AppliesToPath: "s3://warehouse/gold/orders/data/a.parquet",
		}},
	})
	require.Equal(t, int64(30), metadata.BaseSnapshotID)
	require.Equal(t, 9, metadata.BaseSchemaID)
	require.Equal(t, "main", metadata.Ref)
	require.Equal(t, "iceberg-object-io://test-ref", metadata.ObjectIORef)
	require.Len(t, metadata.DataFiles, 2)
	require.Equal(t, "s3://warehouse/gold/orders/data/a.parquet", metadata.DataFiles[0].FilePath)
	require.Equal(t, "s3://warehouse/gold/orders/data/b.parquet", metadata.DataFiles[1].FilePath)
}

func TestIcebergDeleteIntentCarriesCompiledScanMetadata(t *testing.T) {
	scanNode := &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		ExternScan: &plan.ExternScan{
			Type:        int32(plan.ExternType_ICEBERG_TB),
			IcebergScan: &plan.IcebergScan{Namespace: "sales", Table: "orders"},
		},
	}
	insertNode := &plan.Node{
		NodeType:     plan.Node_INSERT,
		Children:     []int32{0},
		ExtraOptions: icebergapi.DMLDeletePlanExtraOptions,
		InsertCtx: &plan.InsertCtx{
			TableDef: &plan.TableDef{
				Name:      "gold_orders",
				TableType: catalog.SystemExternalRel,
				Createsql: icebergInsertCreatesql(),
				Cols: []*plan.ColDef{
					{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
					{Name: icebergapi.DMLDataFilePathColumnName, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
					{Name: icebergapi.DMLRowOrdinalColumnName, Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
				},
			},
		},
	}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = defines.AttachAccountId(context.Background(), 42)
	testCompile := &Compile{
		proc: proc,
		icebergScanPlans: map[int32]*icebergapi.IcebergScanPlan{
			0: {
				Snapshot:    icebergapi.SnapshotPlan{SnapshotID: 30, SchemaID: 9, RefName: "audit"},
				ObjectIORef: "iceberg-object-io://test-ref",
				DataTasks: []icebergapi.DataFileTask{
					{DataFile: icebergapi.DataFile{FilePath: "s3://warehouse/gold/orders/data/a.parquet", SpecID: 3}},
					{DataFile: icebergapi.DataFile{FilePath: "s3://warehouse/gold/orders/data/a.parquet", SpecID: 3}},
					{DataFile: icebergapi.DataFile{FilePath: "s3://warehouse/gold/orders/data/b.parquet", SpecID: 4}},
				},
			},
		},
	}
	op, err := testCompile.constructIcebergInsert([]*plan.Node{scanNode, insertNode}, insertNode)
	require.NoError(t, err)
	writer, ok := op.(*icebergwrite.IcebergWrite)
	require.True(t, ok)
	require.Equal(t, uint32(42), writer.Request.AccountID)
	require.Equal(t, int64(30), writer.Request.DMLScan.BaseSnapshotID)
	require.Equal(t, 9, writer.Request.DMLScan.BaseSchemaID)
	require.Equal(t, "audit", writer.Request.DMLScan.Ref)
	require.Equal(t, "iceberg-object-io://test-ref", writer.Request.DMLScan.ObjectIORef)
	require.Len(t, writer.Request.DMLScan.DataFiles, 2)
	require.Equal(t, "s3://warehouse/gold/orders/data/a.parquet", writer.Request.DMLScan.DataFiles[0].FilePath)
	require.Equal(t, 4, writer.Request.DMLScan.DataFiles[1].SpecID)
}

func TestIcebergMergeIntentCarriesCompiledScanMetadata(t *testing.T) {
	scanNode := &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		ExternScan: &plan.ExternScan{
			Type:        int32(plan.ExternType_ICEBERG_TB),
			IcebergScan: &plan.IcebergScan{Namespace: "sales", Table: "orders"},
		},
	}
	insertNode := &plan.Node{
		NodeType:     plan.Node_INSERT,
		Children:     []int32{0},
		ExtraOptions: icebergapi.DMLMergePlanExtraOptions,
		InsertCtx: &plan.InsertCtx{
			TableDef: &plan.TableDef{
				Name:      "gold_orders",
				TableType: catalog.SystemExternalRel,
				Createsql: icebergInsertCreatesql(),
				Cols: []*plan.ColDef{
					{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
					{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
					{Name: icebergapi.DMLDataFilePathColumnName, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
					{Name: icebergapi.DMLRowOrdinalColumnName, Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
					{Name: icebergapi.DMLMergeActionColumnName, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
				},
			},
		},
	}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = defines.AttachAccountId(context.Background(), 42)
	testCompile := &Compile{
		proc: proc,
		icebergScanPlans: map[int32]*icebergapi.IcebergScanPlan{
			0: {
				Snapshot:    icebergapi.SnapshotPlan{SnapshotID: 30, SchemaID: 9, RefName: "audit"},
				ObjectIORef: "iceberg-object-io://test-ref",
				DataTasks: []icebergapi.DataFileTask{
					{DataFile: icebergapi.DataFile{FilePath: "s3://warehouse/gold/orders/data/a.parquet", SpecID: 3}},
					{DataFile: icebergapi.DataFile{FilePath: "s3://warehouse/gold/orders/data/b.parquet", SpecID: 4}},
				},
			},
		},
	}
	op, err := testCompile.constructIcebergInsert([]*plan.Node{scanNode, insertNode}, insertNode)
	require.NoError(t, err)
	writer, ok := op.(*icebergwrite.IcebergWrite)
	require.True(t, ok)
	require.Equal(t, icebergwrite.OperationMerge, writer.Request.Operation)
	require.Equal(t, int64(30), writer.Request.DMLScan.BaseSnapshotID)
	require.Equal(t, 9, writer.Request.DMLScan.BaseSchemaID)
	require.Equal(t, "audit", writer.Request.DMLScan.Ref)
	require.Equal(t, "iceberg-object-io://test-ref", writer.Request.DMLScan.ObjectIORef)
	require.Len(t, writer.Request.DMLScan.DataFiles, 2)
}

func TestIcebergMergeIntentSelectsTargetScanWhenSourceIsIceberg(t *testing.T) {
	targetRef := &plan.ObjectRef{DbName: "iceberg_tier_a", ObjName: "dml_accounts", Obj: 1001}
	sourceRef := &plan.ObjectRef{DbName: "iceberg_tier_a", ObjName: "dml_accounts_updates", Obj: 1002}
	targetScanNode := &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		ObjRef:   targetRef,
		ExternScan: &plan.ExternScan{
			Type:        int32(plan.ExternType_ICEBERG_TB),
			IcebergScan: &plan.IcebergScan{Namespace: "dml", Table: "accounts"},
		},
	}
	sourceScanNode := &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		ObjRef:   sourceRef,
		ExternScan: &plan.ExternScan{
			Type:        int32(plan.ExternType_ICEBERG_TB),
			IcebergScan: &plan.IcebergScan{Namespace: "dml", Table: "accounts_updates"},
		},
	}
	joinNode := &plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{0, 1},
	}
	insertNode := &plan.Node{
		NodeType:     plan.Node_INSERT,
		ObjRef:       targetRef,
		Children:     []int32{2},
		ExtraOptions: icebergapi.DMLMergePlanExtraOptions,
		InsertCtx: &plan.InsertCtx{
			Ref: targetRef,
			TableDef: &plan.TableDef{
				Name:      "dml_accounts",
				TableType: catalog.SystemExternalRel,
				Createsql: icebergInsertCreatesql(),
				Cols: []*plan.ColDef{
					{Name: "account_id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
					{Name: "balance", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
					{Name: icebergapi.DMLDataFilePathColumnName, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
					{Name: icebergapi.DMLRowOrdinalColumnName, Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
					{Name: icebergapi.DMLMergeActionColumnName, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
				},
			},
		},
	}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = defines.AttachAccountId(context.Background(), 42)
	testCompile := &Compile{
		proc: proc,
		icebergScanPlans: map[int32]*icebergapi.IcebergScanPlan{
			0: {
				Snapshot:    icebergapi.SnapshotPlan{SnapshotID: 30, SchemaID: 9, RefName: "main"},
				ObjectIORef: "iceberg-object-io://target",
				DataTasks: []icebergapi.DataFileTask{
					{DataFile: icebergapi.DataFile{FilePath: "s3://warehouse/dml/accounts/data/a.parquet", SpecID: 3}},
				},
			},
			1: {
				Snapshot:    icebergapi.SnapshotPlan{SnapshotID: 40, SchemaID: 9, RefName: "main"},
				ObjectIORef: "iceberg-object-io://source",
				DataTasks: []icebergapi.DataFileTask{
					{DataFile: icebergapi.DataFile{FilePath: "s3://warehouse/dml/accounts_updates/data/b.parquet", SpecID: 3}},
				},
			},
		},
	}

	op, err := testCompile.constructIcebergInsert([]*plan.Node{targetScanNode, sourceScanNode, joinNode, insertNode}, insertNode)
	require.NoError(t, err)
	writer, ok := op.(*icebergwrite.IcebergWrite)
	require.True(t, ok)
	require.Equal(t, icebergwrite.OperationMerge, writer.Request.Operation)
	require.Equal(t, int64(30), writer.Request.DMLScan.BaseSnapshotID)
	require.Equal(t, "iceberg-object-io://target", writer.Request.DMLScan.ObjectIORef)
	require.Len(t, writer.Request.DMLScan.DataFiles, 1)
	require.Equal(t, "s3://warehouse/dml/accounts/data/a.parquet", writer.Request.DMLScan.DataFiles[0].FilePath)
}

func TestIcebergAppendInsertRejectsInvalidCoordinatorFactoryRuntime(t *testing.T) {
	restoreRuntimeVariableForTest(t, "", IcebergAppendCoordinatorFactoryRuntimeKey, "not-a-factory")
	node := &plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{
			Name:      "gold_orders",
			TableType: catalog.SystemExternalRel,
			Createsql: icebergInsertCreatesql(),
		},
	}}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = context.Background()
	_, err := constructIcebergInsert(proc, node)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ICEBERG_CONFIG_INVALID")
}

// TestExternalInsertStmtTime ensures the writer evaluates WRITE_FILE_PATTERN
// against one statement-start timestamp shared by all scopes: the frontend's
// defines.StartTS when present, else the Compile's startAt (set on every
// construction path including the internal SQL executor), else the wall clock.
func TestExternalInsertStmtTime(t *testing.T) {
	want := time.Unix(1718000000, 0).UTC()
	startAt := time.Unix(1718000100, 0).UTC()

	// defines.StartTS wins.
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = context.WithValue(context.Background(), defines.StartTS{}, want)
	require.True(t, externalInsertStmtTime(proc, startAt).Equal(want))

	// No StartTS on the context: the Compile's startAt.
	proc2 := &process.Process{}
	proc2.Base = &process.BaseProcess{}
	proc2.Ctx = context.Background()
	require.True(t, externalInsertStmtTime(proc2, startAt).Equal(startAt))

	// Neither: wall clock.
	before := time.Now()
	got := externalInsertStmtTime(proc2, time.Time{})
	require.False(t, got.Before(before))
	require.False(t, got.After(time.Now()))

	// The resolved timestamp lands in the writer config.
	node := &plan.Node{InsertCtx: &plan.InsertCtx{
		Ref: &plan.ObjectRef{ObjName: "wext"},
		TableDef: &plan.TableDef{
			Name:      "wext",
			TableType: catalog.SystemExternalRel,
			Createsql: extWriteCreatesql(t, "stage://s/p-%U.csv"),
			Cols:      []*plan.ColDef{{Name: "a"}},
		},
	}}
	op, err := constructExternalInsert(proc, node, nil, want)
	require.NoError(t, err)
	arg := op.(*insert.Insert)
	defer arg.Release()
	require.True(t, arg.InsertCtx.ExternalConfig.Stmt.Equal(want))
}

func TestExternalInsertTargetIsLocalFile(t *testing.T) {
	proc := process.NewTopProcess(context.Background(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	fileURL, err := url.Parse("file:///tmp/wext")
	require.NoError(t, err)
	s3URL, err := url.Parse("s3://bucket/wext")
	require.NoError(t, err)
	proc.GetStageCache().Set("local_stage", stage.StageDef{Name: "local_stage", Url: fileURL})
	proc.GetStageCache().Set("s3_stage", stage.StageDef{Name: "s3_stage", Url: s3URL})

	node := &plan.Node{InsertCtx: &plan.InsertCtx{TableDef: &plan.TableDef{
		TableType: catalog.SystemExternalRel,
		Createsql: extWriteCreatesql(t, "stage://local_stage/p-%U.csv"),
	}}}
	isLocal, err := externalInsertTargetIsLocalFile(proc, node, time.Unix(1, 0).UTC())
	require.NoError(t, err)
	require.True(t, isLocal)

	node.InsertCtx.TableDef.Createsql = extWriteCreatesql(t, "stage://s3_stage/p-%U.csv")
	isLocal, err = externalInsertTargetIsLocalFile(proc, node, time.Unix(1, 0).UTC())
	require.NoError(t, err)
	require.False(t, isLocal)

	isLocal, err = externalInsertTargetIsLocalFile(proc, nil, time.Unix(1, 0).UTC())
	require.NoError(t, err)
	require.False(t, isLocal)

	node.InsertCtx.TableDef.Createsql = "{not json"
	isLocal, err = externalInsertTargetIsLocalFile(proc, node, time.Unix(1, 0).UTC())
	require.Error(t, err)
	require.False(t, isLocal)

	node.InsertCtx.TableDef.Createsql = extWriteCreatesql(t, "")
	isLocal, err = externalInsertTargetIsLocalFile(proc, node, time.Unix(1, 0).UTC())
	require.NoError(t, err)
	require.False(t, isLocal)

	node.InsertCtx.TableDef.Createsql = extWriteCreatesql(t, "stage://local_stage/p-%.csv")
	isLocal, err = externalInsertTargetIsLocalFile(proc, node, time.Unix(1, 0).UTC())
	require.Error(t, err)
	require.False(t, isLocal)

	node.InsertCtx.TableDef.Createsql = extWriteCreatesql(t, "file:///tmp/wext/p-%U.csv")
	isLocal, err = externalInsertTargetIsLocalFile(proc, node, time.Unix(1, 0).UTC())
	require.Error(t, err)
	require.False(t, isLocal)
}

func TestCompileExternalInsertFileStageMergesToCurrentCN(t *testing.T) {
	proc := process.NewTopProcess(context.Background(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	fileURL, err := url.Parse("file:///tmp/wext")
	require.NoError(t, err)
	proc.GetStageCache().Set("local_stage", stage.StageDef{Name: "local_stage", Url: fileURL})

	c := NewCompile("127.0.0.1:6001", "db", "insert into wext select * from src", "tenant", "uid", nil, proc, nil, false, nil, time.Unix(1, 0).UTC())
	c.anal = &AnalyzeModule{isFirst: true}
	node := &plan.Node{InsertCtx: &plan.InsertCtx{
		Ref:             &plan.ObjectRef{ObjName: "wext"},
		AddAffectedRows: true,
		TableDef: &plan.TableDef{
			Name:      "wext",
			TableType: catalog.SystemExternalRel,
			Createsql: extWriteCreatesql(t, "stage://local_stage/p-%U.csv"),
			Cols:      []*plan.ColDef{{Name: "a"}},
		},
	}}
	input := []*Scope{
		{Magic: Remote, NodeInfo: engine.Node{Addr: "127.0.0.2:6001", Mcpu: 1}, Proc: proc.NewNoContextChildProc(1)},
		{Magic: Remote, NodeInfo: engine.Node{Addr: "127.0.0.3:6001", Mcpu: 1}, Proc: proc.NewNoContextChildProc(1)},
	}

	out, err := c.compileInsert(nil, node, input)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, "127.0.0.1:6001", out[0].NodeInfo.Addr)
	require.Len(t, out[0].PreScopes, 2)
	require.IsType(t, &insert.Insert{}, out[0].RootOp)
	require.Equal(t, vm.Insert, out[0].RootOp.OpType())

	c.anal = &AnalyzeModule{isFirst: true}
	input = []*Scope{
		{Magic: Remote, NodeInfo: engine.Node{Addr: "127.0.0.2:6001", Mcpu: 1}, Proc: proc.NewNoContextChildProc(1)},
	}
	out, err = c.compileInsert(nil, node, input)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, "127.0.0.1:6001", out[0].NodeInfo.Addr)
	require.Len(t, out[0].PreScopes, 1)
}

func TestCompileExternalInsertReturnsFileStageResolveError(t *testing.T) {
	proc := process.NewTopProcess(context.Background(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	c := NewCompile("127.0.0.1:6001", "db", "insert into wext select * from src", "tenant", "uid", nil, proc, nil, false, nil, time.Unix(1, 0).UTC())
	c.anal = &AnalyzeModule{isFirst: true}
	node := &plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{
			Name:      "wext",
			TableType: catalog.SystemExternalRel,
			Createsql: extWriteCreatesql(t, "file:///tmp/wext/p-%U.csv"),
			Cols:      []*plan.ColDef{{Name: "a"}},
		},
	}}
	input := []*Scope{
		{Magic: Remote, NodeInfo: engine.Node{Addr: "127.0.0.2:6001", Mcpu: 1}, Proc: proc.NewNoContextChildProc(1)},
	}

	out, err := c.compileInsert(nil, node, input)
	require.Error(t, err)
	require.Nil(t, out)
}

func TestCompileExternalInsertS3StageKeepsRemoteScopes(t *testing.T) {
	proc := process.NewTopProcess(context.Background(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	s3URL, err := url.Parse("s3://bucket/wext")
	require.NoError(t, err)
	proc.GetStageCache().Set("s3_stage", stage.StageDef{Name: "s3_stage", Url: s3URL})

	c := NewCompile("127.0.0.1:6001", "db", "insert into wext select * from src", "tenant", "uid", nil, proc, nil, false, nil, time.Unix(1, 0).UTC())
	c.anal = &AnalyzeModule{isFirst: true}
	node := &plan.Node{InsertCtx: &plan.InsertCtx{
		Ref:             &plan.ObjectRef{ObjName: "wext"},
		AddAffectedRows: true,
		TableDef: &plan.TableDef{
			Name:      "wext",
			TableType: catalog.SystemExternalRel,
			Createsql: extWriteCreatesql(t, "stage://s3_stage/p-%U.csv"),
			Cols:      []*plan.ColDef{{Name: "a"}},
		},
	}}
	input := []*Scope{
		{Magic: Remote, NodeInfo: engine.Node{Addr: "127.0.0.2:6001", Mcpu: 1}, Proc: proc.NewNoContextChildProc(1)},
		{Magic: Remote, NodeInfo: engine.Node{Addr: "127.0.0.3:6001", Mcpu: 1}, Proc: proc.NewNoContextChildProc(1)},
	}

	out, err := c.compileInsert(nil, node, input)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, "127.0.0.2:6001", out[0].NodeInfo.Addr)
	require.Equal(t, "127.0.0.3:6001", out[1].NodeInfo.Addr)
	require.Empty(t, out[0].PreScopes)
	require.Empty(t, out[1].PreScopes)
	require.IsType(t, &insert.Insert{}, out[0].RootOp)
	require.Equal(t, vm.Insert, out[0].RootOp.OpType())
	require.IsType(t, &insert.Insert{}, out[1].RootOp)
	require.Equal(t, vm.Insert, out[1].RootOp.OpType())
}

// TestExternalInsertDupOperator ensures parallelizing a scope keeps the
// duplicated insert in external-write mode; losing the flag silently turned
// the parallel instances into engine-relation inserts.
func TestExternalInsertDupOperator(t *testing.T) {
	src := insert.NewArgument()
	defer src.Release()
	src.ToExternal = true
	src.InsertCtx = &insert.InsertCtx{Attrs: []string{"a"}}

	dup := dupOperator(src, 1, 2).(*insert.Insert)
	defer dup.Release()
	require.True(t, dup.ToExternal)
	require.Equal(t, src.InsertCtx, dup.InsertCtx)
}

// TestExternalInsertRemoteRunRoundtrip ensures the external-write insert
// survives pipeline encode/decode: a remote CN must rebuild the operator with
// ToExternal set and the same writer config (pattern, format, statement
// timestamp) instead of a plain engine-relation insert.
func TestExternalInsertRemoteRunRoundtrip(t *testing.T) {
	stmtAt := time.Unix(1718000000, 12345).UTC()
	tableDef := &plan.TableDef{
		Name:      "wext",
		TableType: catalog.SystemExternalRel,
		Createsql: extWriteCreatesql(t, "stage://s/p-%U.csv"),
		Cols: []*plan.ColDef{
			{Name: "a"},
			{Name: catalog.Row_ID, Hidden: true},
		},
	}
	arg, err := buildExternalInsertArg(t.Context(), &plan.ObjectRef{ObjName: "wext"},
		tableDef, true, nil, stmtAt)
	require.NoError(t, err)
	defer arg.Release()
	require.True(t, arg.ToExternal)
	require.Equal(t, []string{"a"}, arg.InsertCtx.Attrs)

	ctx := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = context.Background()
	proc.Base.SessionInfo.TimeZone = time.UTC

	_, pipeInstr, err := convertToPipelineInstruction(arg, proc, ctx, 1)
	require.NoError(t, err)
	require.True(t, pipeInstr.Insert.ToExternal)
	require.Equal(t, stmtAt.UnixNano(), pipeInstr.Insert.ExternalStmtUnixNano)

	restored, err := convertToVmOperator(pipeInstr, ctx, nil)
	require.NoError(t, err)
	restoredOp := restored.(*insert.Insert)
	defer restoredOp.Release()
	require.True(t, restoredOp.ToExternal)
	cfg := restoredOp.InsertCtx.ExternalConfig
	require.Equal(t, "stage://s/p-%U.csv", cfg.Pattern)
	require.Equal(t, "csv", cfg.Format)
	require.True(t, cfg.Stmt.Equal(stmtAt))
	require.Equal(t, []string{"a"}, restoredOp.InsertCtx.Attrs)
}

// TestExternalInsertRemoteRunTailParity: remote CNs RE-DERIVE the writer
// config from the serialized TableDef instead of receiving it, so every
// FIELDS/LINES option the local path resolves must come out identical on
// decode — and the session time zone must survive via the explicit proto
// fields (the generic session codec round-trips zones lossily).
func TestExternalInsertRemoteRunTailParity(t *testing.T) {
	raw, err := json.Marshal(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Option: []string{"format", "csv", "write_file_pattern", "stage://s/p-%U.csv"},
			Tail: &tree.TailParameter{
				Fields: &tree.Fields{
					Terminated: &tree.Terminated{Value: "||"},
					EnclosedBy: &tree.EnclosedBy{Value: '\''},
					EscapedBy:  &tree.EscapedBy{Value: '!'},
				},
				Lines: &tree.Lines{
					StartingBy:   "R>",
					TerminatedBy: &tree.Terminated{Value: "\r\n"},
				},
			},
		},
	})
	require.NoError(t, err)
	tableDef := &plan.TableDef{
		Name:      "wext",
		TableType: catalog.SystemExternalRel,
		Createsql: string(raw),
		Cols:      []*plan.ColDef{{Name: "a"}},
	}

	stmtAt := time.Unix(1718000000, 0).UTC()
	local, err := buildExternalInsertArg(t.Context(), &plan.ObjectRef{ObjName: "wext"},
		tableDef, true, nil, stmtAt)
	require.NoError(t, err)
	defer local.Release()

	ctx := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = context.Background()
	proc.Base.SessionInfo.TimeZone = time.UTC

	_, pipeInstr, err := convertToPipelineInstruction(local, proc, ctx, 1)
	require.NoError(t, err)
	require.Equal(t, "UTC", pipeInstr.Insert.ExternalTzName)

	restored, err := convertToVmOperator(pipeInstr, ctx, nil)
	require.NoError(t, err)
	remote := restored.(*insert.Insert)
	defer remote.Release()

	lc, rc := local.InsertCtx.ExternalConfig, remote.InsertCtx.ExternalConfig
	require.Equal(t, lc.Pattern, rc.Pattern)
	require.Equal(t, lc.Format, rc.Format)
	require.Equal(t, lc.FieldTerminator, rc.FieldTerminator)
	require.Equal(t, lc.LineTerminator, rc.LineTerminator)
	require.Equal(t, lc.LineStartingBy, rc.LineStartingBy)
	require.Equal(t, lc.EnclosedBy, rc.EnclosedBy)
	require.Equal(t, lc.EscapedBy, rc.EscapedBy)
	require.Equal(t, lc.NoEscape, rc.NoEscape)
	require.Equal(t, time.UTC, rc.TimeZone)

	// ESCAPED BY '' (NoEscape) survives the round-trip too.
	raw2, err := json.Marshal(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Option: []string{"format", "csv", "write_file_pattern", "stage://s/p-%U.csv"},
			Tail: &tree.TailParameter{
				Fields: &tree.Fields{EscapedBy: &tree.EscapedBy{Value: 0}},
			},
		},
	})
	require.NoError(t, err)
	tableDef2 := &plan.TableDef{
		Name:      "wext2",
		TableType: catalog.SystemExternalRel,
		Createsql: string(raw2),
		Cols:      []*plan.ColDef{{Name: "a"}},
	}
	local2, err := buildExternalInsertArg(t.Context(), &plan.ObjectRef{ObjName: "wext2"},
		tableDef2, true, nil, stmtAt)
	require.NoError(t, err)
	defer local2.Release()
	require.True(t, local2.InsertCtx.ExternalConfig.NoEscape)

	_, pipeInstr2, err := convertToPipelineInstruction(local2, proc, ctx, 1)
	require.NoError(t, err)
	restored2, err := convertToVmOperator(pipeInstr2, ctx, nil)
	require.NoError(t, err)
	remote2 := restored2.(*insert.Insert)
	defer remote2.Release()
	require.True(t, remote2.InsertCtx.ExternalConfig.NoEscape)
}
