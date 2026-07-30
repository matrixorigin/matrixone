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

package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestMarkTableDefTemporary(t *testing.T) {
	tests := []struct {
		name     string
		tableDef *plan.TableDef
	}{
		{
			name:     "missing properties",
			tableDef: &plan.TableDef{},
		},
		{
			name: "unrelated empty definition",
			tableDef: &plan.TableDef{Defs: []*plan.TableDef_DefType{
				{},
			}},
		},
		{
			name: "nil properties definition",
			tableDef: &plan.TableDef{Defs: []*plan.TableDef_DefType{
				{Def: &plan.TableDef_DefType_Properties{}},
			}},
		},
		{
			name: "missing relkind",
			tableDef: &plan.TableDef{Defs: []*plan.TableDef_DefType{
				{
					Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{{Key: SystemRelAttr_Comment, Value: "comment"}},
					}},
				},
			}},
		},
		{
			name: "ordinary relkind",
			tableDef: &plan.TableDef{Defs: []*plan.TableDef_DefType{
				{
					Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{{Key: "RELKIND", Value: SystemOrdinaryRel}},
					}},
				},
			}},
		},
		{
			name: "duplicate relkind",
			tableDef: &plan.TableDef{Defs: []*plan.TableDef_DefType{
				{
					Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							nil,
							{Key: SystemRelAttr_Kind, Value: SystemOrdinaryRel},
							{Key: "RELKIND", Value: SystemIndexRel},
						},
					}},
				},
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.tableDef.IsTemporary = true
			MarkTableDefTemporary(test.tableDef)
			MarkTableDefTemporary(test.tableDef)

			require.Equal(t, SystemTemporaryTable, test.tableDef.TableType)
			// This field belongs to session alias resolution and must not be
			// rewritten by the durable catalog marker helper.
			require.True(t, test.tableDef.IsTemporary)

			kindCount := 0
			for _, def := range test.tableDef.Defs {
				properties, ok := def.GetDef().(*plan.TableDef_DefType_Properties)
				if !ok {
					continue
				}
				for _, property := range properties.Properties.Properties {
					if property == nil {
						continue
					}
					if property.Key == SystemRelAttr_Kind {
						kindCount++
						require.Equal(t, SystemTemporaryTable, property.Value)
					}
				}
			}
			require.Equal(t, 1, kindCount)
		})
	}
}

func TestMarkTableDefTemporaryNil(t *testing.T) {
	require.NotPanics(t, func() { MarkTableDefTemporary(nil) })
}

func TestMarkTableDefTemporaryDoesNotCreateSessionState(t *testing.T) {
	tableDef := &plan.TableDef{}
	MarkTableDefTemporary(tableDef)
	require.False(t, tableDef.IsTemporary)
}

func TestNonTemporaryTableSQLPredicate(t *testing.T) {
	tests := []struct {
		name      string
		alias     string
		relKind   string
		relName   string
		createSQL string
		database  string
		extraInfo string
	}{
		{
			name:      "unqualified",
			relKind:   SystemRelAttr_Kind,
			relName:   SystemRelAttr_Name,
			createSQL: SystemRelAttr_CreateSQL,
			database:  SystemRelAttr_DBName,
			extraInfo: SystemRelAttr_ExtraInfo,
		},
		{
			name:      "qualified",
			alias:     "tbl",
			relKind:   "tbl." + SystemRelAttr_Kind,
			relName:   "tbl." + SystemRelAttr_Name,
			createSQL: "tbl." + SystemRelAttr_CreateSQL,
			database:  "tbl." + SystemRelAttr_DBName,
			extraInfo: "tbl." + SystemRelAttr_ExtraInfo,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := NonTemporaryTableSQLPredicate(test.alias)

			require.Contains(t, got, test.relKind+" = '"+SystemTemporaryTable+"'")
			require.Contains(t, got, legacyTemporaryTableFunction+"(coalesce("+test.relKind+", ''), coalesce("+test.relName+", ''), coalesce("+test.database+", ''), coalesce("+test.createSQL+", ''), coalesce("+test.extraInfo+", ''))")
			require.Contains(t, got, "coalesce("+test.relKind+", '') not in (")
			for _, kind := range []string{
				SystemOrdinaryRel,
				SystemViewRel,
				SystemExternalRel,
				SystemMaterializedRel,
				SystemSourceRel,
				SystemClusterRel,
				SystemPartitionRel,
				SystemSequenceRel,
			} {
				require.Contains(t, got, "'"+kind+"'")
			}
			require.Contains(t, got, "regexp_like("+test.relName+", '"+legacyTemporaryTableNameRegexp+"')")
		})
	}
}
