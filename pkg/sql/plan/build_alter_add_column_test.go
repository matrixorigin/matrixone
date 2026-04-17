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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestDropColumnWithIndex(t *testing.T) {
	var def TableDef
	def.Indexes = []*IndexDef{
		{IndexName: "idx",
			IndexAlgo:  "fulltext",
			TableExist: true,
			Unique:     false,
			Parts:      []string{"body", "title"},
		},
	}

	err := handleDropColumnWithIndex(context.TODO(), "body", &def)
	require.Nil(t, err)
	require.Equal(t, 1, len(def.Indexes[0].Parts))
	require.Equal(t, "title", def.Indexes[0].Parts[0])
}

func TestCheckGeometryKeyPartTypes(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_geometry)}

	err := checkPrimaryKeyPartType(context.Background(), typ, "g")
	require.Error(t, err)
	require.Contains(t, err.Error(), "GEOMETRY column 'g' cannot be in primary key")

	err = checkUniqueKeyPartType(context.Background(), typ, "g")
	require.Error(t, err)
	require.Contains(t, err.Error(), "GEOMETRY column 'g' cannot be in unique index")
}

func TestCheckIndexedColumnTypeChangeGeometry(t *testing.T) {
	tableDef := &TableDef{
		Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Indexes: []*plan.IndexDef{
			{IndexName: "u_g", Unique: true, Parts: []string{"g"}},
			{IndexName: "idx_g", Parts: []string{"g"}},
		},
	}

	oldCol := &ColDef{Name: "g", OriginName: "g", Typ: plan.Type{Id: int32(types.T_varchar)}}
	newCol := &ColDef{Name: "g", OriginName: "g", Typ: plan.Type{Id: int32(types.T_geometry)}}

	err := checkIndexedColumnTypeChange(context.Background(), tableDef, oldCol, newCol)
	require.Error(t, err)
	require.Contains(t, err.Error(), "GEOMETRY column 'g' cannot be in unique index")

	pkOldCol := &ColDef{Name: "id", OriginName: "id", Typ: plan.Type{Id: int32(types.T_int64)}}
	pkNewCol := &ColDef{Name: "id", OriginName: "id", Typ: plan.Type{Id: int32(types.T_geometry)}}
	err = checkIndexedColumnTypeChange(context.Background(), tableDef, pkOldCol, pkNewCol)
	require.Error(t, err)
	require.Contains(t, err.Error(), "GEOMETRY column 'id' cannot be in primary key")
}
