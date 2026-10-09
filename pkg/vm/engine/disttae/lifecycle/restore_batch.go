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

package lifecycle

import (
	"context"
	"crypto/sha256"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// VerifyRestoreBatch re-encodes the final MO vectors, after all Parquet to MO
// type conversions, before Relation.Write. This prevents a conversion drift
// from being hidden by validating only the intermediate Parquet values.
func VerifyRestoreBatch(
	ctx context.Context,
	schemaDigest [sha256.Size]byte,
	value *batch.Batch,
	expectedRows uint64,
	expectedLogicalBytes uint64,
	expectedHash [sha256.Size]byte,
) error {
	if value == nil {
		return moerr.NewInternalErrorNoCtxf("Lifecycle Restore verification Batch is nil")
	}
	encoder := NewCanonicalBatchEncoder(schemaDigest)
	if err := encoder.WriteBatch(ctx, value, nil); err != nil {
		return err
	}
	if encoder.RowCount() != expectedRows ||
		encoder.LogicalBytes() != expectedLogicalBytes ||
		encoder.Sum() != expectedHash {
		return moerr.NewInternalErrorNoCtxf(
			"Lifecycle Restore final MO vectors do not match the Archive Chunk",
		)
	}
	return nil
}

// CanonicalRowsToBatch converts verified Archive values into the ordinary MO
// vectors accepted by Relation.Write. Callers own and must Clean the Batch.
func CanonicalRowsToBatch(
	ctx context.Context,
	schema SchemaDescriptor,
	rows [][]CanonicalCell,
	mp *mpool.MPool,
) (*batch.Batch, error) {
	if mp == nil || len(schema.Columns) == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("Lifecycle Restore Batch input is incomplete")
	}
	value := batch.NewWithSize(len(schema.Columns))
	attributes := make([]string, len(schema.Columns))
	for ordinal, column := range schema.Columns {
		if column.Ordinal != uint32(ordinal) {
			return nil, moerr.NewInternalErrorNoCtxf("Lifecycle Restore schema ordinals are corrupt")
		}
		attributes[ordinal] = column.Name
		value.Vecs[ordinal] = vector.NewVec(types.New(
			types.T(column.TypeID),
			column.Width,
			column.Scale,
		))
	}
	value.SetAttributes(attributes)
	cleanup := true
	defer func() {
		if cleanup {
			value.Clean(mp)
		}
	}()
	for rowOrdinal, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(row) != len(schema.Columns) {
			return nil, moerr.NewInternalErrorNoCtxf(
				"Lifecycle Restore row %d has %d columns, expected %d",
				rowOrdinal,
				len(row),
				len(schema.Columns),
			)
		}
		for columnOrdinal, cell := range row {
			expected := value.Vecs[columnOrdinal].GetType()
			if cell.Type.Oid != expected.Oid ||
				cell.Type.Width != expected.Width ||
				cell.Type.Scale != expected.Scale {
				return nil, moerr.NewInternalErrorNoCtxf(
					"Lifecycle Restore row %d column %d type changed",
					rowOrdinal,
					columnOrdinal,
				)
			}
			converted, err := restoreVectorValue(cell)
			if err != nil {
				return nil, err
			}
			if err := vector.AppendAny(
				value.Vecs[columnOrdinal],
				converted,
				cell.Null,
				mp,
			); err != nil {
				return nil, err
			}
		}
	}
	value.SetRowCount(len(rows))
	cleanup = false
	return value, nil
}

func restoreVectorValue(cell CanonicalCell) (any, error) {
	if cell.Null || cell.Type.Oid != types.T_json {
		return cell.Value, nil
	}
	var encoded []byte
	switch value := cell.Value.(type) {
	case []byte:
		encoded = value
	case string:
		encoded = []byte(value)
	default:
		return nil, moerr.NewInternalErrorNoCtxf(
			"Lifecycle Restore JSON has unexpected type %T",
			cell.Value,
		)
	}
	jsonValue, err := types.ParseStringToByteJson(string(encoded))
	if err != nil {
		return nil, err
	}
	return types.EncodeJson(jsonValue)
}
