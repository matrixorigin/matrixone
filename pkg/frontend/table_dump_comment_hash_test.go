// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package frontend

import (
	"testing"

	"github.com/gogo/protobuf/proto"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func commentHashTableDef(column, table, index string) *plan.TableDef {
	return &plan.TableDef{
		Name: "source", Cols: []*plan.ColDef{{Name: "a", OriginName: "a", Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}, Comment: column}},
		Defs:    []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: catalog.SystemRelAttr_Comment, Value: table}}}}}},
		Indexes: []*plan.IndexDef{{IndexName: "idx", Parts: []string{"a"}, Comment: index}},
	}
}

// Frozen by executing tableSchemaHash with the production files from main
// 12eeeb0205b5e87faef48660ddbcd8cb5e18baea, before comment escaping changed.
func TestTableSchemaHashStoredCommentCompatibility(t *testing.T) {
	for _, tc := range []struct{ name, column, table, index, want string }{
		{"empty", "", "", "", "843ce3869058255f31bf65b6a95f9e25d0251027e59df6b8c42317d763a27c90"},
		{"plain", "column", "table", "index", "6eb79f80665efe4b76caa3d4625383f58c614225fb87346e2fe7d8c5744dea10"},
		{"quote", "it''s", "table''s", "index's", "9488bb6c25ab91ba0158f5a308c0dabc2e1f87cbfcc54aa36b719f08666b073b"},
		{"column", `C:\dir\new`, "", "", "3f3d3de234f442fccd0fad616632d5d3073bec6e3116b9b39d9812dc4b476421"},
		{"table", "", `x\ty`, "", "c7b1a9ccfd5795d9716b080ff36e2fd7f88e755676fdbe1bdf59ef6a3c2578fd"},
		{"mixed", `C:\dir\new it''s`, `x\ty''z`, `idx\q's`, "4c8bfeecfe92ad19eacdbb3e18f461191c84536dc0cc41cb2a6e27a2b65f7fe4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			def := commentHashTableDef(tc.column, tc.table, tc.index)
			before := proto.Clone(def)
			got, err := tableSchemaHash(def)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.True(t, proto.Equal(before, def), "hashing must not mutate engine metadata")
		})
	}
}

func TestTableSchemaHashStillRejectsCommentAndSchemaDifferences(t *testing.T) {
	source := commentHashTableDef(`C:\dir\new it''s`, `x\ty''z`, `idx\q's`)
	want, err := tableSchemaHash(source)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		change func(*plan.TableDef)
	}{
		{"column comment", func(d *plan.TableDef) { d.Cols[0].Comment = `different\column` }},
		{"table comment", func(d *plan.TableDef) { d.Defs[0].GetProperties().Properties[0].Value = `different\table` }},
		{"index comment", func(d *plan.TableDef) { d.Indexes[0].Comment = `different\index` }},
		{"column type", func(d *plan.TableDef) { d.Cols[0].Typ.Id = int32(types.T_int64) }},
		{"default", func(d *plan.TableDef) { d.Cols[0].Default.OriginString = "1" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := proto.Clone(source).(*plan.TableDef)
			tc.change(target)
			got, err := tableSchemaHash(target)
			require.NoError(t, err)
			require.NotEqual(t, want, got)
		})
	}
}
