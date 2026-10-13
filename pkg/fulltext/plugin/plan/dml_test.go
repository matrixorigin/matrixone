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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestDMLMaintenanceNoOpColumns(t *testing.T) {
	tableDef := &planpb.TableDef{
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "body", Typ: planpb.Type{Id: int32(types.T_text)}},
			{Name: "summary", Typ: planpb.Type{Id: int32(types.T_varchar)}},
			{Name: "metadata", Typ: planpb.Type{Id: int32(types.T_json)}},
			{Name: "resource", Typ: planpb.Type{Id: int32(types.T_datalink)}},
			{Name: "headline", Typ: planpb.Type{Id: int32(types.T_char)}},
			{Name: "unsupported", Typ: planpb.Type{Id: int32(types.T_int64)}},
		},
		Name2ColIndex: map[string]int32{
			"id": 0, "body": 1, "summary": 2, "metadata": 3, "resource": 4, "headline": 5, "unsupported": 6,
		},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
	}

	t.Run("varchar and text stored bytes determine postings", func(t *testing.T) {
		columns, supported, err := Hooks{}.DMLMaintenanceNoOpColumns(tableDef, &planpb.IndexDef{
			Parts: []string{catalog.CreateAlias("body"), "summary", "body"},
		})
		require.NoError(t, err)
		require.True(t, supported)
		require.Equal(t, []string{"id", "body", "summary"}, columns)
	})

	for _, tc := range []struct {
		name     string
		tableDef *planpb.TableDef
		indexDef *planpb.IndexDef
	}{
		{name: "char comparison is not byte identity", tableDef: tableDef, indexDef: &planpb.IndexDef{Parts: []string{"headline"}}},
		{name: "json comparison is not tokenizer identity", tableDef: tableDef, indexDef: &planpb.IndexDef{Parts: []string{"metadata"}}},
		{name: "datalink has external content dependency", tableDef: tableDef, indexDef: &planpb.IndexDef{Parts: []string{"body", "resource"}}},
		{name: "unexpected type is conservative", tableDef: tableDef, indexDef: &planpb.IndexDef{Parts: []string{"unsupported"}}},
		{name: "missing column is conservative", tableDef: tableDef, indexDef: &planpb.IndexDef{Parts: []string{"missing"}}},
		{name: "empty definition is conservative", tableDef: tableDef, indexDef: &planpb.IndexDef{}},
		{name: "nil definition is conservative", tableDef: tableDef},
		{name: "nil table is conservative", indexDef: &planpb.IndexDef{Parts: []string{"body"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			columns, supported, err := Hooks{}.DMLMaintenanceNoOpColumns(tc.tableDef, tc.indexDef)
			require.NoError(t, err)
			require.False(t, supported)
			require.Nil(t, columns)
		})
	}
}
