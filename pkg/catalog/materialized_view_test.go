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

package catalog

import (
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewDefinitionPersistence(t *testing.T) {
	def := MVDefinition{FormatVersion: 1, Query: "select k,sum(v) from db.t group by k", Sources: []MVSource{{AccountID: 7, DatabaseID: 3, TableID: 8, LogicalID: 9, Version: 2, Database: "db", Table: "t"}}, SQLMode: "NO_UNSIGNED_SUBTRACTION", TimeZone: "Asia/Shanghai", OwnerUser: 10, OwnerRole: 11, OwnerName: "owner"}
	data, err := json.Marshal(def)
	require.NoError(t, err)
	for _, props := range []bool{true, false} {
		table := &plan.TableDef{TableType: SystemMaterializedViewRel}
		if props {
			table.Props = []*plan.PropertyDef{{Key: MaterializedViewProperty, Value: string(data)}}
		} else {
			table.Defs = []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: MaterializedViewProperty, Value: string(data)}}}}}}
		}
		decoded, err := MaterializedViewDefinition(table)
		require.NoError(t, err)
		require.Equal(t, def, *decoded)
	}
	for _, raw := range []string{"", "invalid", `{"format_version":2,"query":"select * from t","sources":[{}]}`, `{"format_version":1,"query":"select 1"}`} {
		_, err := MaterializedViewDefinition(&plan.TableDef{TableType: SystemMaterializedViewRel, Props: []*plan.PropertyDef{{Key: MaterializedViewProperty, Value: raw}}})
		require.Error(t, err)
	}
	decoded, err := MaterializedViewDefinition(&plan.TableDef{TableType: SystemOrdinaryRel})
	require.NoError(t, err)
	require.Nil(t, decoded)
}
