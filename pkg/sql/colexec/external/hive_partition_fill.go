// Copyright 2024 Matrix Origin
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

package external

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// isHivePartitionCol returns true if colName is a declared Hive partition column.
func (param *ExternalParam) isHivePartitionCol(colName string) bool {
	if param.Extern == nil || !param.Extern.HivePartitioning {
		return false
	}
	lower := strings.ToLower(colName)
	for _, pc := range param.Extern.HivePartitionCols {
		if pc == lower {
			return true
		}
	}
	return false
}

// refreshPartitionValues extracts and validates partition values from the current file path.
func (param *ExternalParam) refreshPartitionValues(proc *process.Process) error {
	if param.Extern == nil || !param.Extern.HivePartitioning {
		return nil
	}
	values, err := ExtractPartitionValues(
		param.Fileparam.Filepath,
		param.Extern.Filepath,
		param.Extern.HivePartitionCols,
	)
	if err != nil {
		return err
	}
	param.currentPartValues = values
	return param.validatePartitionValues(proc)
}

func (param *ExternalParam) validatePartitionValues(proc *process.Process) error {
	partCols := param.Extern.HivePartitionCols
	colTypes := param.Extern.HivePartitionColTypes
	if len(colTypes) == 0 {
		return nil
	}
	if len(colTypes) != len(partCols) {
		return moerr.NewInternalErrorf(proc.Ctx,
			"hive partition column/type mismatch: cols=%d types=%d", len(partCols), len(colTypes))
	}

	relPath := param.Fileparam.Filepath
	if param.Extern.Filepath != "" {
		relPath = relPartitionPath(param.Fileparam.Filepath, param.Extern.Filepath)
	}

	for i, colName := range partCols {
		colName = strings.ToLower(colName)
		strVal, present := param.currentPartValues[colName]
		if !present {
			return moerr.NewInternalErrorf(proc.Ctx,
				"partition column '%s' not found in path '%s'", colName, relPath)
		}
		if strVal == HiveDefaultPartition {
			if !colTypes[i].NullAbility {
				return moerr.NewConstraintViolationf(proc.Ctx,
					"partition column '%s' is NOT NULL but directory has __HIVE_DEFAULT_PARTITION__ in path '%s'; allow NULL on the partition column or remove/rename the default partition directory",
					colName, relPath)
			}
			continue
		}
		if err := validatePartitionValue(strVal, colName, colTypes[i], proc, relPath); err != nil {
			return err
		}
	}
	return nil
}

func validatePartitionValue(strVal, colName string, colType tree.HivePartColType, proc *process.Process, filePath string) error {
	typ := types.T(colType.Id)
	if typ == types.T_any {
		return nil
	}
	vec := vector.NewVec(types.NewWithCharset(typ, colType.Width, colType.Scale, uint8(colType.Charset)), proc.Mp())
	col := &plan.ColDef{
		Name: colName,
		Typ: plan.Type{
			Id:          colType.Id,
			Width:       colType.Width,
			Scale:       colType.Scale,
			Enumvalues:  colType.Enumvalues,
			Charset:     colType.Charset,
			NotNullable: !colType.NullAbility,
		},
		Default: &plan.Default{NullAbility: colType.NullAbility},
	}
	err := fillConstantVector(vec, strVal, col, 1, proc, filePath)
	vec.Free(proc.Mp())
	return err
}

// fillVirtualColumns fills partition columns and __mo_filepath for a batch.
func (h *ParquetHandler) fillVirtualColumns(bat *batch.Batch, param *ExternalParam, proc *process.Process) error {
	rowCount := bat.RowCount()
	mp := param.outputPool(proc)

	if h.filepathColIndex >= 0 {
		vec := bat.Vecs[h.filepathColIndex]
		if err := vector.SetConstBytes(vec, []byte(param.Fileparam.Filepath), rowCount, mp); err != nil {
			return err
		}
	}

	if len(h.partitionColIndices) > 0 {
		return h.fillPartitionColumns(bat, param, proc)
	}
	return nil
}

// fillPartitionColumns fills partition column vectors with constant values from the path.
func (h *ParquetHandler) fillPartitionColumns(bat *batch.Batch, param *ExternalParam, proc *process.Process) error {
	partValues := param.currentPartValues
	rowCount := bat.RowCount()
	mp := param.outputPool(proc)

	// Error messages use the path relative to the DDL base so that BVT output
	// is portable across machines (absolute filesystem paths would embed
	// /Users/... or /tmp/... in .result files).
	relPath := param.Fileparam.Filepath
	if param.Extern != nil && param.Extern.Filepath != "" {
		relPath = relPartitionPath(param.Fileparam.Filepath, param.Extern.Filepath)
	}

	for _, idx := range h.partitionColIndices {
		col := param.Cols[idx]
		colName := strings.ToLower(col.Name)
		strVal, present := partValues[colName]
		vec := bat.Vecs[idx]

		if !present {
			return moerr.NewInternalErrorf(param.Ctx,
				"partition column '%s' not found in path '%s'", colName, relPath)
		}

		if strVal == HiveDefaultPartition {
			notNullable := col.Default != nil && !col.Default.NullAbility
			if notNullable {
				return moerr.NewConstraintViolationf(param.Ctx,
					"partition column '%s' is NOT NULL but directory has __HIVE_DEFAULT_PARTITION__ in path '%s'; allow NULL on the partition column or remove/rename the default partition directory",
					colName, relPath)
			}
			if err := vector.SetConstNull(vec, rowCount, mp); err != nil {
				return err
			}
			continue
		}

		if err := fillConstantVectorWithPool(vec, strVal, col, rowCount, proc, mp, relPath); err != nil {
			return err
		}
	}
	return nil
}

// fillConstantVector converts a string partition value to the column's typed vector.
// Follows external loader semantics (getColData path), not SQL CAST.
func fillConstantVector(
	vec *vector.Vector, strVal string, col *plan.ColDef,
	rowCount int, proc *process.Process, filePath string,
) error {
	return fillConstantVectorWithPool(vec, strVal, col, rowCount, proc, proc.Mp(), filePath)
}

// fillConstantVectorWithPool is fillConstantVector allocating through mp.
func fillConstantVectorWithPool(
	vec *vector.Vector, strVal string, col *plan.ColDef,
	rowCount int, proc *process.Process, mp *mpool.MPool, filePath string,
) error {
	typ := types.T(col.Typ.Id)

	wrapErr := func(err error) error {
		return moerr.NewInternalErrorf(proc.Ctx,
			"partition value type conversion failed: col=%s, value='%s', path=%s: %v",
			col.Name, strVal, filePath, err)
	}

	switch {
	case typ == types.T_uint64 && col.Typ.Enumvalues != "":
		// SET type stored as uint64 with Enumvalues
		v, err := types.ParseSet(col.Typ.Enumvalues, strVal)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)
	case typ == types.T_enum:
		v, err := types.ParseEnum(col.Typ.Enumvalues, strVal)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)
	case typ.IsArrayRelate() || typ.IsBlockScaledArray():
		return moerr.NewNotSupportedf(proc.Ctx,
			"unsupported partition column type VECTOR for col=%s, path=%s", col.Name, filePath)
	}
	v, err := types.ParseValue(*vec.GetType(), strVal, proc.GetSessionInfo().TimeZone)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNotSupported) {
			return moerr.NewNotSupportedf(proc.Ctx,
				"unsupported partition column type %v for col=%s, path=%s", typ, col.Name, filePath)
		}
		return wrapErr(err)
	}
	return vector.SetConstValue(vec, v, rowCount, mp)
}
