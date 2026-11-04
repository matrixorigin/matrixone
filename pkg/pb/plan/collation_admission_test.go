// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/gogo/protobuf/proto"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/stretchr/testify/require"
)

func TestTypedCollationAdmissionMatchesOriginalOwnerBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Plan)
	}{
		{"legacy", func(*Plan) {}},
		{"column future revision", func(p *Plan) { p.GetQuery().Nodes[0].TableDef.Cols[0].Typ.CollationVersion = 1 }},
		{"column unknown identity", func(p *Plan) { p.GetQuery().Nodes[0].TableDef.Cols[0].Typ.Charset = 259 }},
		{"default expression", func(p *Plan) { p.GetQuery().Nodes[0].TableDef.Cols[0].Default.Expr.Typ.Charset = 4 }},
		{"projection", func(p *Plan) { p.GetQuery().Nodes[0].ProjectList[1].Typ.Charset = 4 }},
		{"table key", func(p *Plan) { p.GetQuery().Nodes[0].TableDef.KeyFormat = 1 }},
		{"table default", func(p *Plan) { p.GetQuery().Nodes[0].TableDef.DefaultCharset = 4 }},
		{"index key", func(p *Plan) { p.GetQuery().Nodes[0].TableDef.Indexes[0].KeyFormat = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := collationAdmissionBenchmarkPlan(2)
			tc.mutate(p)
			before, after := legacyReflectCollationAdmission(p), RequireLegacyCollations(p)
			require.Equal(t, before == nil, after == nil)
			if tc.name != "legacy" {
				require.Error(t, after)
			} else {
				require.NoError(t, after)
			}
		})
	}
}

func TestTypedCollationAdmissionCyclesAndSharedExpressions(t *testing.T) {
	expr := &Expr{Typ: Type{Id: 61, Charset: 3}}
	expr.Expr = &Expr_List{List: &ExprList{List: []*Expr{expr, expr}}}
	require.NoError(t, RequireLegacyCollations(expr))
	require.NoError(t, RequireLegacyCollations([]*Expr{nil, expr, expr}))
	expr.Typ.CollationVersion = 1
	require.Error(t, RequireLegacyCollations(expr))
	require.Error(t, RequireLegacyCollations([]*Expr{nil, expr, expr}))
}

func TestTypedCollationAdmissionSchemaOnlyDDL(t *testing.T) {
	table := &TableDef{DefaultCharset: 3, Cols: []*ColDef{{Name: "c", Typ: Type{Id: 61, Charset: 3}}}}
	owner := &Plan{Plan: &Plan_Ddl{Ddl: &DataDefinition{
		Definition: &DataDefinition_CreateTable{CreateTable: &CreateTable{TableDef: table}},
	}}}
	require.NoError(t, RequireLegacyCollations(owner))
	table.KeyFormat = 1
	require.Error(t, RequireLegacyCollations(owner))
	table.KeyFormat = 0
	table.Cols[0].Typ.Charset = 4
	require.Error(t, RequireLegacyCollations(owner))
}

func TestNativeUnicodeUniqueTableAdmission(t *testing.T) {
	native := Type{
		Id:               61,
		Charset:          uint32(collation.UTF8MB4UnicodeCIIdentity),
		CollationVersion: uint32(collation.RevisionV1),
	}
	table := &TableDef{
		DefaultCharset: 3,
		KeyFormat:      collation.KeyFormatV1,
		Cols:           []*ColDef{{Name: "v", Typ: native}},
		Indexes: []*IndexDef{{
			Parts:     []string{"v"},
			Unique:    true,
			KeyFormat: collation.KeyFormatV1,
		}},
	}
	require.NoError(t, RequireLegacyCollations(table))

	hidden := &TableDef{
		Name:      "__mo_index_unique_test",
		KeyFormat: collation.KeyFormatV1,
		Cols:      []*ColDef{{Name: "__mo_index_idx_col", Typ: Type{Id: 65, Charset: 1}}},
		Pkey:      &PrimaryKeyDef{Names: []string{"__mo_index_idx_col"}, PkeyColName: "__mo_index_idx_col"},
		Defs: []*TableDef_DefType{{Def: &TableDef_DefType_Properties{
			Properties: &PropertiesDef{Properties: []*Property{{Key: "relkind", Value: "i"}}},
		}}},
	}
	require.NoError(t, RequireLegacyCollations(hidden))
	// Catalog-resolved physical index relations carry the system index table
	// type, while planner-created shells may leave it empty.  Both forms must
	// pass the physical-index compatibility gate.
	resolvedPhysical := proto.Clone(hidden).(*TableDef)
	resolvedPhysical.TableType = "i"
	require.NoError(t, RequireLegacyCollations(resolvedPhysical))

	withoutUnique := *table
	withoutUnique.Indexes = nil
	require.Error(t, RequireLegacyCollations(&withoutUnique))
	require.Error(t, RequireLegacyCollations(&IndexDef{KeyFormat: collation.KeyFormatV1}))

	mixed := proto.Clone(table).(*TableDef)
	mixed.Indexes = append(mixed.Indexes, &IndexDef{Parts: []string{"v"}, KeyFormat: collation.KeyFormatV1})
	require.Error(t, RequireLegacyCollations(mixed))

	for _, mutate := range []func(*TableDef){
		func(v *TableDef) { v.IsTemporary = true },
		func(v *TableDef) { v.TableType = SystemExternalRel },
		func(v *TableDef) { v.TableType = "cluster" },
		func(v *TableDef) {
			v.Defs = []*TableDef_DefType{{Def: &TableDef_DefType_Properties{
				Properties: &PropertiesDef{Properties: []*Property{{Key: "relkind", Value: "cluster"}}},
			}}}
		},
		func(v *TableDef) { v.Pkey = &PrimaryKeyDef{Names: []string{"v"}, PkeyColName: "v"} },
	} {
		copy := proto.Clone(table).(*TableDef)
		mutate(copy)
		require.Error(t, RequireLegacyCollations(copy))
	}

	spoofedPhysical := proto.Clone(hidden).(*TableDef)
	spoofedPhysical.Cols[0].Typ = native
	require.Error(t, RequireLegacyCollations(spoofedPhysical))
	wrongPhysicalShape := proto.Clone(hidden).(*TableDef)
	wrongPhysicalShape.Pkey.PkeyColName = "not_the_index_key"
	require.Error(t, RequireLegacyCollations(wrongPhysicalShape))
	missingPhysicalKeyColumn := proto.Clone(hidden).(*TableDef)
	missingPhysicalKeyColumn.Cols[0].Name = "not_the_index_key"
	require.Error(t, RequireLegacyCollations(missingPhysicalKeyColumn))
	spoofedPhysicalKind := proto.Clone(hidden).(*TableDef)
	spoofedPhysicalKind.TableType = "cluster"
	require.Error(t, RequireLegacyCollations(spoofedPhysicalKind))
	extraPhysicalIndex := proto.Clone(hidden).(*TableDef)
	extraPhysicalIndex.Indexes = []*IndexDef{{Unique: true, KeyFormat: collation.KeyFormatV1}}
	require.Error(t, RequireLegacyCollations(extraPhysicalIndex))
}

func TestReflectiveTableValueAdmissionVisitsNestedColumns(t *testing.T) {
	type foreignWrapper struct {
		Table TableDef
	}
	owner := foreignWrapper{Table: TableDef{
		DefaultCharset: 3,
		Cols:           []*ColDef{{Typ: Type{Id: 61, Charset: 4}}},
	}}
	require.Error(t, RequireLegacyCollations(owner))
}

func TestCollationAdmissionPointPlanAllocationBudget(t *testing.T) {
	owner := collationAdmissionBenchmarkPlan(8)
	allocs := testing.AllocsPerRun(10, func() {
		if err := RequireLegacyCollations(owner); err != nil {
			panic(err)
		}
	})
	// A normal point plan fits the inline cycle set. No scalar boxing,
	// metadata sidecar, or heap map is needed at either admission boundary.
	require.Zero(t, allocs)
}
