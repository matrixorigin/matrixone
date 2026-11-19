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
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/stretchr/testify/require"
)

func TestFulltextUpdateRewriteDependencies(t *testing.T) {
	hook, ok := any(Hooks{}).(planplugin.UpdateColumnRewriteHook)
	require.True(t, ok, "FULLTEXT must implement the update-rewrite hook")

	ref := func(pos int32, name string) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: pos, Name: name}}}
	}
	table := &planpb.TableDef{
		Cols: []*planpb.ColDef{
			{Name: "id"}, {Name: "tenant"}, {Name: "source"},
			{Name: "route", GeneratedCol: &planpb.GeneratedCol{Expr: ref(2, "source")}},
			{Name: "body"},
		},
		Name2ColIndex: map[string]int32{"id": 0, "tenant": 1, "source": 2, "route": 3, "body": 4},
		Pkey:          &planpb.PrimaryKeyDef{Names: []string{"id", "tenant"}, PkeyColName: catalog.CPrimaryKeyColName},
		Partition: &planpb.Partition{PartitionDefs: []*planpb.PartitionDef{{
			Def: &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Args: []*planpb.Expr{ref(3, "docs.route"), ref(1, "docs.tenant")}}}},
		}}},
	}
	index := &planpb.IndexDef{Parts: []string{"body"}}
	for _, tc := range []struct {
		name    string
		column  string
		rewrite bool
	}{
		{"primary key component", catalog.CreateAlias("id"), true},
		{"partition column", "route", true},
		{"generated partition dependency", "source", true},
		{"unrelated column", "body", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := hook.UpdateColumnRequiresIndexRewrite(table, index, tc.column)
			require.NoError(t, err)
			require.Equal(t, tc.rewrite, got)
		})
	}

	broken := *table
	broken.Partition = &planpb.Partition{PartitionDefs: []*planpb.PartitionDef{{Def: ref(99, "missing")}}}
	_, err := hook.UpdateColumnRequiresIndexRewrite(&broken, index, "body")
	require.Error(t, err, "invalid partition dependencies must not be silently ignored")
}

func TestFulltextPrimaryKeyUpdateCapability(t *testing.T) {
	var hook planplugin.UpdatePrimaryKeyHook = Hooks{}
	require.False(t, hook.SupportsPrimaryKeyUpdate(nil, false))
	require.False(t, hook.SupportsPrimaryKeyUpdate(&planpb.TableDef{}, false))
	require.True(t, hook.SupportsPrimaryKeyUpdate(&planpb.TableDef{FeatureFlag: features.Partitioned}, false))
	require.False(t, hook.SupportsPrimaryKeyUpdate(&planpb.TableDef{FeatureFlag: features.Partitioned}, true))
}

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

	t.Run("partition route is an index-owned no-op dependency", func(t *testing.T) {
		tableDef.Partition = &planpb.Partition{PartitionDefs: []*planpb.PartitionDef{{
			Def: &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{Name: "t.summary", ColPos: 2}}},
		}}}
		t.Cleanup(func() { tableDef.Partition = nil })
		columns, supported, err := Hooks{}.DMLMaintenanceNoOpColumns(tableDef, &planpb.IndexDef{Parts: []string{"body"}})
		require.NoError(t, err)
		require.True(t, supported)
		require.Equal(t, []string{"id", "body", "summary"}, columns)
		tableDef.Partition.PartitionDefs[0].Def.GetCol().ColPos = 99
		tableDef.Partition.PartitionDefs[0].Def.GetCol().Name = "missing"
		columns, supported, err = Hooks{}.DMLMaintenanceNoOpColumns(tableDef, &planpb.IndexDef{Parts: []string{"body"}})
		require.NoError(t, err)
		require.False(t, supported, "unresolvable route metadata cannot prove posting equality")
		require.Nil(t, columns)
	})
}
