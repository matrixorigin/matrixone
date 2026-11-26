// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func addEnumSetTableForODKUTest(mock *MockOptimizer) {
	const tableName = "odku_enum_set_t"
	const tableID = 33281
	idType := plan.Type{Id: int32(types.T_int32), NotNullable: true}
	enumType := plan.Type{Id: int32(types.T_enum), Enumvalues: "a,b,c", NotNullable: true}
	setType := plan.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c", NotNullable: true}
	rowIDType := plan.Type{Id: int32(types.T_Rowid), NotNullable: true, Width: 16}
	cols := []*ColDef{
		{ColId: 0, Name: "id", OriginName: "id", Typ: idType, Primary: true, Pkidx: 1, Default: &plan.Default{}},
		{ColId: 1, Name: "e", OriginName: "e", Typ: enumType, Default: &plan.Default{}},
		{ColId: 2, Name: "s", OriginName: "s", Typ: setType, Default: &plan.Default{}},
		{ColId: 3, Name: catalog.Row_ID, OriginName: catalog.Row_ID, Typ: rowIDType, Hidden: true, Default: &plan.Default{}},
	}
	tableDef := &TableDef{
		TableType: catalog.SystemOrdinaryRel,
		TblId:     tableID,
		Name:      tableName,
		Cols:      cols,
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: "id",
			Cols:        []uint64{0},
			Names:       []string{"id"},
			CompPkeyCol: cols[0],
		},
		Defs: []*plan.TableDef_DefType{{
			Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{
				Properties: []*plan.Property{{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemOrdinaryRel}},
			}},
		}},
	}
	mock.ctxt.objects[tableName] = &ObjectRef{SchemaName: "tpch", ObjName: tableName, Obj: tableID}
	mock.ctxt.tables[tableName] = tableDef
	mock.ctxt.id2name[tableID] = tableName
}

// #33281: ON DUPLICATE KEY UPDATE must accept string values for ENUM/SET
// columns, the same as INSERT and UPDATE. It used the generic assignment cast,
// which has no VARCHAR->ENUM/SET overload, and failed with 20203.
func TestOnDuplicateUpdateEnumSetStringAssignment33281(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	addEnumSetTableForODKUTest(mock)
	for _, sql := range []string{
		"insert into odku_enum_set_t(id, e, s) values (1, 'a', 'a') on duplicate key update e = 'b'",
		"insert into odku_enum_set_t(id, e, s) values (1, 'a', 'a') on duplicate key update s = 'b,c'",
		"insert into odku_enum_set_t(id, e, s) values (1, 'a', 'a') on duplicate key update e = concat('', 'c'), s = cast('a' as char)",
		"insert into odku_enum_set_t(id, e, s) values (1, 'a', 'a') on duplicate key update e = 3, s = 5",
		"insert into odku_enum_set_t(id, e, s) values (1, 'a', 'a') on duplicate key update e = values(e), s = values(s)",
		"insert into odku_enum_set_t(id, e, s) values (1, 'a', 'a') on duplicate key update e = e, s = s",
	} {
		_, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, sql)
	}
}
