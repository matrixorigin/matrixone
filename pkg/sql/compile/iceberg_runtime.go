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
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func icebergScanPlanToRuntime(ctx context.Context, scanPlan *api.IcebergScanPlan, objectIORef string) (icebergExternalScanRuntime, error) {
	return icebergScanPlanToRuntimeForTable(ctx, scanPlan, objectIORef, nil)
}

func icebergScanPlanToRuntimeForTable(
	ctx context.Context,
	scanPlan *api.IcebergScanPlan,
	objectIORef string,
	tableDef *plan.TableDef,
) (icebergExternalScanRuntime, error) {
	if scanPlan == nil {
		return icebergExternalScanRuntime{}, moerr.NewInvalidInput(ctx, "iceberg scan plan is nil")
	}
	columns, err := icebergColumnMappingsToPipelineForTable(ctx, scanPlan.ColumnMapping, tableDef)
	if err != nil {
		return icebergExternalScanRuntime{}, err
	}
	if objectIORef == "" {
		objectIORef = scanPlan.ObjectIORef
	}
	runtimeColumns := columns
	return icebergExternalScanRuntime{
		dataTasks:   icebergDataTasksToPipeline(scanPlan.DataTasks),
		deleteTasks: icebergDeleteTasksToPipeline(scanPlan.DeleteTasks),
		columns:     runtimeColumns,
		snapshot: &pipeline.IcebergSnapshotRuntime{
			SnapshotId:           scanPlan.Snapshot.SnapshotID,
			SchemaId:             int32(scanPlan.Snapshot.SchemaID),
			PartitionSpecIds:     intSliceToInt32(scanPlan.Snapshot.PartitionSpecIDs),
			MetadataLocationHash: scanPlan.Snapshot.MetadataLocationHash,
			ManifestListHash:     scanPlan.Snapshot.ManifestListHash,
			RefName:              scanPlan.Snapshot.RefName,
			PlanningMode:         scanPlan.Snapshot.PlanningMode,
		},
		objectIORef:          objectIORef,
		hiddenReadCols:       icebergHiddenReadColumns(runtimeColumns),
		planningStats:        icebergPlanningProfileToParquetStats(scanPlan.Profile),
		needRowOrdinal:       icebergNeedsRowOrdinal(scanPlan.DeleteTasks),
		deleteMaxMemoryBytes: scanPlan.DeleteMaxMemoryBytes,
		deleteSpillEnabled:   scanPlan.EnableDeleteSpill,
	}, nil
}

func icebergPlanningProfileToParquetStats(profile api.PlanningProfile) process.ParquetProfileStats {
	return process.ParquetProfileStats{
		IcebergMetadataBytes:         profile.MetadataBytes,
		IcebergManifestListBytes:     profile.ManifestListBytes,
		IcebergManifestBytes:         profile.ManifestBytes,
		IcebergManifestsSelected:     int64(profile.ManifestsSelected),
		IcebergManifestsPruned:       int64(profile.ManifestsPruned),
		IcebergDataFilesSelected:     int64(profile.DataFilesSelected),
		IcebergDataFilesPruned:       int64(profile.DataFilesPruned),
		IcebergDataFileBytesSelected: profile.DataFileBytesSelected,
		IcebergDataFileBytesPruned:   profile.DataFileBytesPruned,
		IcebergPlanningCacheHits:     int64(profile.PlanningCacheHits),
		IcebergPlanningCacheMiss:     int64(profile.PlanningCacheMiss),
	}
}

func icebergDataTasksToPipeline(tasks []api.DataFileTask) []*pipeline.IcebergDataFileTask {
	if len(tasks) == 0 {
		return nil
	}
	out := make([]*pipeline.IcebergDataFileTask, 0, len(tasks))
	for _, task := range tasks {
		file := task.DataFile
		hasResidual, residualHash := icebergResidualFilterRuntime(task.ResidualFilter)
		runtimeTask := &pipeline.IcebergDataFileTask{
			FilePath:              file.FilePath,
			FileFormat:            strings.ToLower(strings.TrimSpace(file.FileFormat)),
			FileSize:              file.FileSizeInBytes,
			RecordCount:           file.RecordCount,
			PartitionSpecId:       int32(file.SpecID),
			PartitionValues:       icebergPartitionValues(file.Partition),
			SplitOffsets:          append([]int64(nil), file.SplitOffsets...),
			CredentialScope:       task.CredentialScope,
			ContentSequenceNumber: file.SequenceNumber,
			FileSequenceNumber:    file.FileSequenceNumber,
			HasResidualFilter:     hasResidual,
			ResidualFilterHash:    residualHash,
		}
		if len(task.RowGroups) == 1 {
			rg := task.RowGroups[0]
			runtimeTask.RowGroupStart = rg.Ordinal
			runtimeTask.RowGroupEnd = rg.Ordinal + 1
			if rg.RowCount > 0 {
				runtimeTask.RecordCount = rg.RowCount
			}
			if rg.Bytes > 0 {
				runtimeTask.FileSize = rg.Bytes
			}
		}
		out = append(out, runtimeTask)
	}
	return out
}

func icebergResidualFilterRuntime(filter api.ResidualFilter) (bool, string) {
	expr := strings.TrimSpace(filter.ExpressionSQL)
	if filter.AlwaysTrue || expr == "" {
		return false, ""
	}
	return true, api.PathHash(expr)
}

func icebergDeleteTasksToPipeline(tasks []api.DeleteFileTask) []*pipeline.IcebergDeleteFileTask {
	if len(tasks) == 0 {
		return nil
	}
	out := make([]*pipeline.IcebergDeleteFileTask, 0, len(tasks))
	for _, task := range tasks {
		file := task.DataFile
		out = append(out, &pipeline.IcebergDeleteFileTask{
			DeleteType:         icebergDeleteType(file.Content),
			DeleteFilePath:     file.FilePath,
			ReferencedDataFile: task.AppliesToPath,
			EqualityFieldIds:   intSliceToInt32(file.EqualityIDs),
			DeleteSchemaId:     int32(firstNonZeroInt(task.DeleteSchemaID, file.DeleteSchemaID)),
			PartitionSpecId:    int32(file.SpecID),
			SequenceNumber:     firstNonZeroInt64(task.SequenceNumber, file.SequenceNumber),
			CredentialScope:    task.CredentialScope,
		})
	}
	return out
}

func icebergHiddenReadColumns(columns []*pipeline.IcebergColumnMapping) []int32 {
	if len(columns) == 0 {
		return nil
	}
	out := make([]int32, 0)
	seen := make(map[int32]struct{})
	for _, column := range columns {
		if column == nil || !column.IsHidden {
			continue
		}
		if isIcebergDMLMetadataColumnName(column.CurrentFieldName) ||
			isIcebergDMLMetadataColumnName(column.SnapshotFieldName) {
			continue
		}
		if _, ok := seen[column.MoColIndex]; ok {
			continue
		}
		seen[column.MoColIndex] = struct{}{}
		out = append(out, column.MoColIndex)
	}
	return out
}

func icebergNeedsRowOrdinal(tasks []api.DeleteFileTask) bool {
	for _, task := range tasks {
		if task.DataFile.Content == api.DataFileContentPositionDelete {
			return true
		}
	}
	return false
}

func icebergColumnMappingsToPipeline(ctx context.Context, mappings []api.IcebergColumnMapping) ([]*pipeline.IcebergColumnMapping, error) {
	return icebergColumnMappingsToPipelineForTable(ctx, mappings, nil)
}

func icebergColumnMappingsToPipelineForTable(
	ctx context.Context,
	mappings []api.IcebergColumnMapping,
	tableDef *plan.TableDef,
) ([]*pipeline.IcebergColumnMapping, error) {
	if len(mappings) == 0 {
		return nil, nil
	}
	if tableDef == nil || len(tableDef.Cols) == 0 {
		return icebergColumnMappingsToPipelineByMappingOrder(ctx, mappings)
	}
	tableIndex, err := newIcebergTableColumnIndex(ctx, tableDef)
	if err != nil {
		return nil, err
	}

	out := make([]*pipeline.IcebergColumnMapping, 0, len(mappings))
	seen := make(map[int32]int, len(tableIndex.exact))
	nextSyntheticIndex := tableIndex.nextSyntheticIndex
	for _, mapping := range mappings {
		moColIndex, ok := tableIndex.find(mapping.ColumnName)
		if !ok && isIcebergDMLMetadataColumnName(mapping.ColumnName) {
			moColIndex, ok = tableIndex.findDMLMetadata(mapping.ColumnName)
		}
		if !ok {
			if !mapping.Hidden {
				continue
			}
			moColIndex = nextSyntheticIndex
			nextSyntheticIndex++
		}
		if prevFieldID, exists := seen[moColIndex]; exists {
			return nil, moerr.NewInvalidInputf(ctx,
				"duplicate iceberg column mapping for MO column index %d: field_id=%d and field_id=%d",
				moColIndex, prevFieldID, mapping.FieldID)
		}
		seen[moColIndex] = mapping.FieldID
		moType, err := icebergMOTypeToPlanType(ctx, mapping.MOType)
		if err != nil {
			return nil, err
		}
		out = append(out, &pipeline.IcebergColumnMapping{
			MoColIndex:        moColIndex,
			IcebergFieldId:    int32(mapping.FieldID),
			SnapshotFieldName: mapping.ColumnName,
			CurrentFieldName:  mapping.ColumnName,
			MoType:            moType,
			Required:          mapping.Required,
			IsHidden:          mapping.Hidden,
			DefaultNullFill:   !mapping.Projected,
			ParquetPathHint:   strconv.Itoa(mapping.ParquetFieldID),
		})
	}
	for _, col := range tableIndex.columns {
		if _, ok := seen[col.index]; !ok {
			return nil, moerr.NewInvalidInputf(ctx,
				"iceberg column mapping not found for MO column %s index %d",
				col.name, col.index)
		}
	}
	return out, nil
}

func icebergColumnMappingsToPipelineByMappingOrder(ctx context.Context, mappings []api.IcebergColumnMapping) ([]*pipeline.IcebergColumnMapping, error) {
	out := make([]*pipeline.IcebergColumnMapping, 0, len(mappings))
	for idx, mapping := range mappings {
		moType, err := icebergMOTypeToPlanType(ctx, mapping.MOType)
		if err != nil {
			return nil, err
		}
		out = append(out, &pipeline.IcebergColumnMapping{
			MoColIndex:        int32(idx),
			IcebergFieldId:    int32(mapping.FieldID),
			SnapshotFieldName: mapping.ColumnName,
			CurrentFieldName:  mapping.ColumnName,
			MoType:            moType,
			Required:          mapping.Required,
			IsHidden:          mapping.Hidden,
			DefaultNullFill:   !mapping.Projected,
			ParquetPathHint:   strconv.Itoa(mapping.ParquetFieldID),
		})
	}
	return out, nil
}

type icebergTableColumnIndex struct {
	exact              map[string]int32
	folded             map[string]int32
	dmlMetadata        map[string]int32
	columns            []icebergTableColumn
	nextSyntheticIndex int32
}

type icebergTableColumn struct {
	name  string
	index int32
}

func newIcebergTableColumnIndex(ctx context.Context, tableDef *plan.TableDef) (icebergTableColumnIndex, error) {
	index := icebergTableColumnIndex{
		exact:              make(map[string]int32, len(tableDef.Cols)),
		folded:             make(map[string]int32, len(tableDef.Cols)),
		dmlMetadata:        make(map[string]int32),
		nextSyntheticIndex: int32(len(tableDef.Cols)),
	}
	ambiguousFolded := make(map[string]string)
	for i, col := range tableDef.Cols {
		if col == nil || col.Hidden {
			continue
		}
		name := strings.TrimSpace(col.Name)
		if name == "" {
			return icebergTableColumnIndex{}, moerr.NewInvalidInputf(ctx,
				"iceberg MO column at index %d has empty name", i)
		}
		if isIcebergDMLMetadataColumnName(name) {
			index.dmlMetadata[strings.ToLower(name)] = int32(i)
			continue
		}
		if _, exists := index.exact[name]; exists {
			return icebergTableColumnIndex{}, moerr.NewInvalidInputf(ctx,
				"duplicate iceberg MO column name %s", name)
		}
		folded := strings.ToLower(name)
		if prev, exists := index.folded[folded]; exists {
			ambiguousFolded[folded] = tableDef.Cols[int(prev)].Name
		}
		moColIndex := int32(i)
		index.exact[name] = moColIndex
		index.folded[folded] = moColIndex
		index.columns = append(index.columns, icebergTableColumn{name: name, index: moColIndex})
	}
	for folded, first := range ambiguousFolded {
		return icebergTableColumnIndex{}, moerr.NewInvalidInputf(ctx,
			"ambiguous iceberg MO column name %s: %s conflicts case-insensitively",
			folded, first)
	}
	return index, nil
}

func (index icebergTableColumnIndex) find(name string) (int32, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, false
	}
	if idx, ok := index.exact[name]; ok {
		return idx, true
	}
	if idx, ok := index.folded[strings.ToLower(name)]; ok {
		return idx, true
	}
	return 0, false
}

func (index icebergTableColumnIndex) findDMLMetadata(name string) (int32, bool) {
	name = strings.TrimSpace(name)
	if name == "" || len(index.dmlMetadata) == 0 {
		return 0, false
	}
	idx, ok := index.dmlMetadata[strings.ToLower(name)]
	return idx, ok
}

func isIcebergDMLMetadataColumnName(name string) bool {
	name = strings.TrimSpace(name)
	return strings.EqualFold(name, api.DMLDataFilePathColumnName) ||
		strings.EqualFold(name, api.DMLRowOrdinalColumnName)
}

func icebergMOTypeToPlanType(ctx context.Context, moType api.MOType) (*plan.Type, error) {
	name := strings.ToUpper(strings.TrimSpace(moType.Name))
	switch name {
	case "BOOL":
		return &plan.Type{Id: int32(types.T_bool)}, nil
	case "INT":
		return &plan.Type{Id: int32(types.T_int32)}, nil
	case "BIGINT":
		return &plan.Type{Id: int32(types.T_int64)}, nil
	case "FLOAT":
		return &plan.Type{Id: int32(types.T_float32)}, nil
	case "DOUBLE":
		return &plan.Type{Id: int32(types.T_float64)}, nil
	case "DECIMAL":
		id := types.T_decimal128
		if moType.Width > 0 && moType.Width <= 18 {
			id = types.T_decimal64
		}
		return &plan.Type{Id: int32(id), Width: int32(moType.Width), Scale: int32(moType.Scale)}, nil
	case "DATE":
		return &plan.Type{Id: int32(types.T_date)}, nil
	case "DATETIME", "DATETIME(6)":
		return &plan.Type{Id: int32(types.T_datetime), Scale: 6}, nil
	case "TIMESTAMP", "TIMESTAMP(6)":
		return &plan.Type{Id: int32(types.T_timestamp), Scale: 6}, nil
	case "TEXT":
		return &plan.Type{Id: int32(types.T_text), Width: types.MaxVarcharLen}, nil
	case "VARBINARY":
		width := int32(moType.Width)
		if width <= 0 {
			width = types.MaxVarBinaryLen
		}
		return &plan.Type{Id: int32(types.T_varbinary), Width: width}, nil
	default:
		return nil, moerr.NewInvalidInput(ctx, fmt.Sprintf("unsupported iceberg MO type for scan runtime: %s", moType.String()))
	}
}

func icebergPartitionValues(partition map[string]any) map[string]string {
	if len(partition) == 0 {
		return nil
	}
	out := make(map[string]string, len(partition))
	for key, value := range partition {
		out[key] = icebergPartitionValueString(value)
	}
	return out
}

func icebergPartitionValueString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}

func icebergDeleteType(content api.DataFileContent) string {
	switch content {
	case api.DataFileContentPositionDelete:
		return "position"
	case api.DataFileContentEqualityDelete:
		return "equality"
	default:
		return "data"
	}
}

func firstNonZeroInt(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func firstNonZeroInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func intSliceToInt32(in []int) []int32 {
	if len(in) == 0 {
		return nil
	}
	out := make([]int32, 0, len(in))
	for _, value := range in {
		out = append(out, int32(value))
	}
	return out
}
