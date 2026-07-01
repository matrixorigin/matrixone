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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
)

type DMLMatchedRowsBatchRequest struct {
	DataFile              api.DataFile
	Batch                 *batch.Batch
	EqualityFieldIDs      []int
	EqualityColumnIndexes []int32
	PredicateStable       bool
	IncludePositionRows   bool
	StartRowOrdinal       int64
}

type DMLMatchedScanBatchRequest struct {
	Batch                   *batch.Batch
	DataFiles               []api.DataFile
	DataFilePathColumnIndex int32
	RowOrdinalColumnIndex   int32
	EqualityFieldIDs        []int
	EqualityColumnIndexes   []int32
	PredicateStable         bool
	IncludePositionRows     bool
}

func BuildDMLMatchedDeleteTargetsFromScanBatch(ctx context.Context, req DMLMatchedScanBatchRequest) ([]DMLMatchedDeleteTarget, error) {
	if req.Batch == nil || req.Batch.RowCount() == 0 {
		return nil, nil
	}
	if req.DataFilePathColumnIndex < 0 || int(req.DataFilePathColumnIndex) >= len(req.Batch.Vecs) {
		return nil, moerr.NewInvalidInputf(ctx, "Iceberg DML scan batch data file column index out of range: column=%d columns=%d", req.DataFilePathColumnIndex, len(req.Batch.Vecs))
	}
	if req.IncludePositionRows && (req.RowOrdinalColumnIndex < 0 || int(req.RowOrdinalColumnIndex) >= len(req.Batch.Vecs)) {
		return nil, moerr.NewInvalidInputf(ctx, "Iceberg DML scan batch row ordinal column index out of range: column=%d columns=%d", req.RowOrdinalColumnIndex, len(req.Batch.Vecs))
	}
	dataFiles := make(map[string]api.DataFile, len(req.DataFiles))
	for _, file := range req.DataFiles {
		path := strings.TrimSpace(file.FilePath)
		if path != "" {
			dataFiles[path] = file
		}
	}
	if len(dataFiles) == 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML scan batch requires data file metadata", nil))
	}
	targetsByPath := make(map[string]*DMLMatchedDeleteTarget)
	order := make([]string, 0)
	for row := 0; row < req.Batch.RowCount(); row++ {
		path, err := dmlStringValue(ctx, req.Batch.Vecs[req.DataFilePathColumnIndex], row)
		if err != nil {
			return nil, err
		}
		path = strings.TrimSpace(path)
		if path == "" {
			return nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML scan batch row is missing data file path", map[string]string{
				"row": strconv.Itoa(row),
			}))
		}
		dataFile, ok := dataFiles[path]
		if !ok {
			return nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML scan batch references an unknown data file", map[string]string{
				"path": api.RedactPath(path),
			}))
		}
		target := targetsByPath[path]
		if target == nil {
			target = &DMLMatchedDeleteTarget{
				DataFile:        dataFile,
				EqualityIDs:     append([]int(nil), req.EqualityFieldIDs...),
				PredicateStable: req.PredicateStable,
			}
			targetsByPath[path] = target
			order = append(order, path)
		}
		if len(req.EqualityFieldIDs) > 0 || len(req.EqualityColumnIndexes) > 0 {
			values, err := dmlEqualityRowFromBatch(ctx, req.Batch, row, req.EqualityFieldIDs, req.EqualityColumnIndexes)
			if err != nil {
				return nil, err
			}
			target.EqualityRows = append(target.EqualityRows, dml.EqualityDeleteRow{Values: values})
		}
		if req.IncludePositionRows {
			ordinal, err := dmlInt64Value(ctx, req.Batch.Vecs[req.RowOrdinalColumnIndex], row)
			if err != nil {
				return nil, err
			}
			if ordinal < 0 {
				return nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML scan batch row ordinal must be non-negative", map[string]string{
					"path": api.RedactPath(path),
					"row":  strconv.Itoa(row),
				}))
			}
			target.PositionRows = append(target.PositionRows, dml.PositionDeleteRow{FilePath: path, Pos: ordinal})
			target.HasRowOrdinal = true
		}
	}
	out := make([]DMLMatchedDeleteTarget, 0, len(order))
	for _, path := range order {
		target := targetsByPath[path]
		if len(target.EqualityRows) == 0 && len(target.PositionRows) == 0 {
			continue
		}
		out = append(out, *target)
	}
	return out, nil
}

func BuildDMLMatchedDeleteTargetFromBatch(ctx context.Context, req DMLMatchedRowsBatchRequest) (DMLMatchedDeleteTarget, error) {
	dataPath := strings.TrimSpace(req.DataFile.FilePath)
	if dataPath == "" {
		return DMLMatchedDeleteTarget{}, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML matched batch requires data file path", nil))
	}
	target := DMLMatchedDeleteTarget{DataFile: req.DataFile}
	if req.Batch == nil || req.Batch.RowCount() == 0 {
		return target, nil
	}
	if len(req.EqualityFieldIDs) > 0 || len(req.EqualityColumnIndexes) > 0 {
		rows, err := dmlEqualityRowsFromBatch(ctx, req.Batch, req.EqualityFieldIDs, req.EqualityColumnIndexes)
		if err != nil {
			return DMLMatchedDeleteTarget{}, err
		}
		target.EqualityIDs = append([]int(nil), req.EqualityFieldIDs...)
		target.EqualityRows = rows
		target.PredicateStable = req.PredicateStable
	}
	if req.IncludePositionRows {
		if req.StartRowOrdinal < 0 {
			return DMLMatchedDeleteTarget{}, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML matched batch row ordinal must be non-negative", map[string]string{
				"path": api.RedactPath(dataPath),
			}))
		}
		rows := make([]dml.PositionDeleteRow, req.Batch.RowCount())
		for row := range rows {
			rows[row] = dml.PositionDeleteRow{FilePath: dataPath, Pos: req.StartRowOrdinal + int64(row)}
		}
		target.PositionRows = rows
		target.HasRowOrdinal = true
	}
	return target, nil
}

func dmlEqualityRowsFromBatch(ctx context.Context, bat *batch.Batch, fieldIDs []int, columnIndexes []int32) ([]dml.EqualityDeleteRow, error) {
	if len(fieldIDs) == 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML equality rows require field ids", nil))
	}
	if len(fieldIDs) != len(columnIndexes) {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML equality field ids and column indexes must have the same length", map[string]string{
			"field_ids": strconv.Itoa(len(fieldIDs)),
			"columns":   strconv.Itoa(len(columnIndexes)),
		}))
	}
	rows := make([]dml.EqualityDeleteRow, bat.RowCount())
	for row := 0; row < bat.RowCount(); row++ {
		values, err := dmlEqualityRowFromBatch(ctx, bat, row, fieldIDs, columnIndexes)
		if err != nil {
			return nil, err
		}
		rows[row] = dml.EqualityDeleteRow{Values: values}
	}
	return rows, nil
}

func dmlEqualityRowFromBatch(ctx context.Context, bat *batch.Batch, row int, fieldIDs []int, columnIndexes []int32) (map[int]any, error) {
	if len(fieldIDs) == 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML equality rows require field ids", nil))
	}
	if len(fieldIDs) != len(columnIndexes) {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML equality field ids and column indexes must have the same length", map[string]string{
			"field_ids": strconv.Itoa(len(fieldIDs)),
			"columns":   strconv.Itoa(len(columnIndexes)),
		}))
	}
	values := make(map[int]any, len(fieldIDs))
	for idx, fieldID := range fieldIDs {
		if fieldID <= 0 {
			return nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML equality field id must be positive", map[string]string{
				"field_id": strconv.Itoa(fieldID),
			}))
		}
		colIdx := int(columnIndexes[idx])
		if colIdx < 0 || colIdx >= len(bat.Vecs) {
			return nil, moerr.NewInvalidInputf(ctx, "Iceberg DML equality column index out of range: column=%d columns=%d", colIdx, len(bat.Vecs))
		}
		value, err := dmlVectorValue(ctx, bat.Vecs[colIdx], row)
		if err != nil {
			return nil, err
		}
		values[fieldID] = value
	}
	return values, nil
}

func dmlStringValue(ctx context.Context, vec *vector.Vector, row int) (string, error) {
	if vec == nil || vec.IsNull(uint64(row)) {
		return "", nil
	}
	switch vec.GetType().Oid {
	case types.T_varchar, types.T_text, types.T_json:
		return vec.GetStringAt(row), nil
	default:
		return "", api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg DML metadata path column type is unsupported", map[string]string{
			"type": vec.GetType().String(),
		}))
	}
}

func dmlInt64Value(ctx context.Context, vec *vector.Vector, row int) (int64, error) {
	if vec == nil || vec.IsNull(uint64(row)) {
		return 0, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML row ordinal is NULL", map[string]string{
			"row": strconv.Itoa(row),
		}))
	}
	switch vec.GetType().Oid {
	case types.T_int64:
		return vector.GetFixedAtWithTypeCheck[int64](vec, row), nil
	case types.T_int32:
		return int64(vector.GetFixedAtWithTypeCheck[int32](vec, row)), nil
	default:
		return 0, api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg DML row ordinal column type is unsupported", map[string]string{
			"type": vec.GetType().String(),
		}))
	}
}

func dmlVectorValue(ctx context.Context, vec *vector.Vector, row int) (any, error) {
	if vec == nil || vec.IsNull(uint64(row)) {
		return nil, nil
	}
	switch vec.GetType().Oid {
	case types.T_bool:
		return vector.GetFixedAtWithTypeCheck[bool](vec, row), nil
	case types.T_int8:
		return vector.GetFixedAtWithTypeCheck[int8](vec, row), nil
	case types.T_int16:
		return vector.GetFixedAtWithTypeCheck[int16](vec, row), nil
	case types.T_int32:
		return vector.GetFixedAtWithTypeCheck[int32](vec, row), nil
	case types.T_int64:
		return vector.GetFixedAtWithTypeCheck[int64](vec, row), nil
	case types.T_uint8:
		return vector.GetFixedAtWithTypeCheck[uint8](vec, row), nil
	case types.T_uint16:
		return vector.GetFixedAtWithTypeCheck[uint16](vec, row), nil
	case types.T_uint32:
		return vector.GetFixedAtWithTypeCheck[uint32](vec, row), nil
	case types.T_uint64:
		return vector.GetFixedAtWithTypeCheck[uint64](vec, row), nil
	case types.T_float32:
		return vector.GetFixedAtWithTypeCheck[float32](vec, row), nil
	case types.T_float64:
		return vector.GetFixedAtWithTypeCheck[float64](vec, row), nil
	case types.T_date:
		return int32(vector.GetFixedAtWithTypeCheck[types.Date](vec, row)), nil
	case types.T_datetime:
		return int64(vector.GetFixedAtWithTypeCheck[types.Datetime](vec, row)), nil
	case types.T_timestamp:
		return int64(vector.GetFixedAtWithTypeCheck[types.Timestamp](vec, row)), nil
	case types.T_varchar, types.T_text, types.T_json:
		return vec.GetStringAt(row), nil
	default:
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg DML equality column type is unsupported", map[string]string{
			"type": vec.GetType().String(),
		}))
	}
}
