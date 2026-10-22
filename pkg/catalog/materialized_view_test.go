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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
		if raw != "invalid" {
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "stored definition failures must use the engine error contract")
		}
	}
	decoded, err := MaterializedViewDefinition(&plan.TableDef{TableType: SystemOrdinaryRel})
	require.NoError(t, err)
	require.Nil(t, decoded)
}

func TestMaterializedViewSemanticContext(t *testing.T) {
	for _, precision := range []int32{0, 4, 10, 30} {
		for _, property := range []bool{false, true} {
			t.Run(fmt.Sprintf("precision%d/property%t", precision, property), func(t *testing.T) {
				def := MVDefinition{FormatVersion: MVDefinitionFormatVersion, Query: "select v/3 from t", Sources: []MVSource{{Table: "t"}}, DivPrecisionIncrement: new(precision), TimeZoneKind: "fixed", TimeZone: "UTC"}
				raw, err := json.Marshal(def)
				require.NoError(t, err)
				table := &plan.TableDef{TableType: SystemMaterializedViewRel}
				props := []*plan.PropertyDef{{Key: MaterializedViewProperty, Value: string(raw)}}
				if property {
					table.Props = props
				} else {
					table.Defs = []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: MaterializedViewProperty, Value: string(raw)}}}}}}
				}
				got, err := MaterializedViewDefinition(table)
				require.NoError(t, err)
				require.Equal(t, def, *got)
				_, err = got.RefreshTimeZone()
				require.NoError(t, err)
			})
		}
	}
	for _, value := range []any{nil, int32(-1), int32(31), "4"} {
		raw, err := json.Marshal(map[string]any{"format_version": 2, "query": "select v/3 from t", "sources": []MVSource{{Table: "t"}}, "time_zone_kind": "fixed", "time_zone": "UTC", "sql_mode": "", "div_precision_increment": value})
		require.NoError(t, err)
		_, err = DecodeMaterializedViewDefinition(string(raw))
		require.Error(t, err)
	}
	for _, suffix := range []string{"", `,"sql_mode":null`} {
		_, err := DecodeMaterializedViewDefinition(`{"format_version":2,"query":"select v/3 from t","sources":[{}],"div_precision_increment":4,"time_zone_kind":"fixed","time_zone":"UTC"` + suffix + `}`)
		require.ErrorContains(t, err, "SQL mode is missing")
	}
	legacy, err := DecodeMaterializedViewDefinition(`{"format_version":1,"query":"select v/3 from t","sources":[{}]}`)
	require.NoError(t, err)
	_, err = legacy.RefreshTimeZone()
	require.ErrorContains(t, err, "recreate")
	named := MVDefinition{FormatVersion: MVDefinitionFormatVersion, TimeZoneKind: "named", TimeZone: "America/New_York", TimeZoneOffset: 1234}
	zone, err := named.RefreshTimeZone()
	require.NoError(t, err)
	for _, tc := range []struct {
		month  time.Month
		offset int
	}{{time.January, -18000}, {time.July, -14400}} {
		_, offset := time.Date(2024, tc.month, 15, 12, 0, 0, 0, time.UTC).In(zone).Zone()
		require.Equal(t, tc.offset, offset, "named rules, not persisted observation offset")
	}
	for _, name := range []string{"Local", "unavailable/MV-zone"} {
		named.TimeZone = name
		_, err = named.RefreshTimeZone()
		require.Error(t, err, "never silently replace rules with a fixed offset")
	}
}
