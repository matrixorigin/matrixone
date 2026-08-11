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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestBuildIcebergDMLMetadataProjectionAppendsStableColumns(t *testing.T) {
	projection, err := BuildIcebergDMLMetadataProjection(context.Background(), icebergDMLProjectionTableDef(), 3, true)
	if err != nil {
		t.Fatalf("build metadata projection: %v", err)
	}
	if projection.DataFilePathColumnIndex != 3 || projection.RowOrdinalColumnIndex != 4 {
		t.Fatalf("unexpected metadata indexes: %+v", projection)
	}
	if len(projection.Attrs) != 2 || len(projection.Cols) != 2 {
		t.Fatalf("expected two metadata columns, got %+v", projection)
	}
	if projection.Attrs[0].ColName != icebergapi.DMLDataFilePathColumnName ||
		projection.Attrs[0].ColIndex != 3 ||
		projection.Cols[0].Typ.Id != int32(types.T_varchar) {
		t.Fatalf("unexpected data-file metadata column: %+v %+v", projection.Attrs[0], projection.Cols[0])
	}
	if projection.Attrs[1].ColName != icebergapi.DMLRowOrdinalColumnName ||
		projection.Attrs[1].ColIndex != 4 ||
		projection.Cols[1].Typ.Id != int32(types.T_int64) {
		t.Fatalf("unexpected row-ordinal metadata column: %+v %+v", projection.Attrs[1], projection.Cols[1])
	}
}

func TestBuildIcebergDMLMetadataProjectionCanOmitRowOrdinal(t *testing.T) {
	projection, err := BuildIcebergDMLMetadataProjection(context.Background(), icebergDMLProjectionTableDef(), 2, false)
	if err != nil {
		t.Fatalf("build metadata projection: %v", err)
	}
	if projection.DataFilePathColumnIndex != 2 || projection.RowOrdinalColumnIndex != -1 {
		t.Fatalf("unexpected metadata indexes: %+v", projection)
	}
	if len(projection.Attrs) != 1 || projection.Attrs[0].ColName != icebergapi.DMLDataFilePathColumnName {
		t.Fatalf("expected only data-file metadata column, got %+v", projection.Attrs)
	}
}

func TestBuildIcebergDMLMetadataProjectionRejectsColumnNameConflict(t *testing.T) {
	for _, name := range []string{
		icebergapi.DMLDataFilePathColumnName,
		icebergapi.DMLRowOrdinalColumnName,
		icebergapi.DMLMergeActionColumnName,
	} {
		tableDef := icebergDMLProjectionTableDef()
		tableDef.Cols = append(tableDef.Cols, &plan.ColDef{
			Name: name,
			Typ:  plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
		})
		_, err := BuildIcebergDMLMetadataProjection(context.Background(), tableDef, 2, true)
		if err == nil || !strings.Contains(err.Error(), "internal row-level DML metadata column") {
			t.Fatalf("expected metadata column conflict for %s, got %v", name, err)
		}
	}
}

func icebergDMLProjectionTableDef() *plan.TableDef {
	return &plan.TableDef{
		Name: "gold_orders",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
			{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar), Width: 1024}},
		},
	}
}
