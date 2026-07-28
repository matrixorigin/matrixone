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

package plan

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type IcebergDMLMetadataProjection struct {
	DataFilePathColumnIndex int32
	RowOrdinalColumnIndex   int32
	Attrs                   []plan.ExternAttr
	Cols                    []*plan.ColDef
}

func BuildIcebergDMLMetadataProjection(
	ctx context.Context,
	tableDef *plan.TableDef,
	startIndex int32,
	includePositionRows bool,
) (IcebergDMLMetadataProjection, error) {
	if tableDef == nil {
		return IcebergDMLMetadataProjection{}, moerr.NewInvalidInput(ctx, "Iceberg DML metadata projection requires table definition")
	}
	if startIndex < 0 {
		return IcebergDMLMetadataProjection{}, moerr.NewInvalidInputf(ctx, "Iceberg DML metadata projection start index must be non-negative: %d", startIndex)
	}
	if err := rejectIcebergDMLMetadataColumnConflicts(ctx, tableDef); err != nil {
		return IcebergDMLMetadataProjection{}, err
	}
	out := IcebergDMLMetadataProjection{DataFilePathColumnIndex: startIndex, RowOrdinalColumnIndex: -1}
	out.Attrs = append(out.Attrs, plan.ExternAttr{
		ColName:  icebergapi.DMLDataFilePathColumnName,
		ColIndex: startIndex,
	})
	out.Cols = append(out.Cols, &plan.ColDef{
		Name: icebergapi.DMLDataFilePathColumnName,
		Typ:  plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen, Table: tableDef.Name},
	})
	if includePositionRows {
		out.RowOrdinalColumnIndex = startIndex + 1
		out.Attrs = append(out.Attrs, plan.ExternAttr{
			ColName:  icebergapi.DMLRowOrdinalColumnName,
			ColIndex: out.RowOrdinalColumnIndex,
		})
		out.Cols = append(out.Cols, &plan.ColDef{
			Name: icebergapi.DMLRowOrdinalColumnName,
			Typ:  plan.Type{Id: int32(types.T_int64), Width: 64, Table: tableDef.Name},
		})
	}
	return out, nil
}

func rejectIcebergDMLMetadataColumnConflicts(ctx context.Context, tableDef *plan.TableDef) error {
	for _, col := range tableDef.GetCols() {
		if col == nil {
			continue
		}
		name := strings.TrimSpace(col.Name)
		if strings.EqualFold(name, icebergapi.DMLDataFilePathColumnName) ||
			strings.EqualFold(name, icebergapi.DMLRowOrdinalColumnName) ||
			strings.EqualFold(name, icebergapi.DMLMergeActionColumnName) {
			return moerr.NewInvalidInputf(ctx,
				"Iceberg table column %s conflicts with an internal row-level DML metadata column",
				col.Name)
		}
	}
	return nil
}
