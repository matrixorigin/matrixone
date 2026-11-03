// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func rebaseSchemaFixture() [4]*plan.TableDef {
	base := &plan.TableDef{Name: "t", TableType: catalog.SystemOrdinaryRel,
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}, NotNull: true},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_varchar), Width: 20}},
		}}
	var defs [4]*plan.TableDef
	for i := range defs {
		defs[i] = proto.Clone(base).(*plan.TableDef)
		defs[i].TblId = uint64(i + 1)
		defs[i].Cols[0].ColId = uint64(i + 7)
	}
	return defs
}

func TestRebaseSchemaContract(t *testing.T) {
	defs := rebaseSchemaFixture()
	columns, err := validateRebaseTableSchema(t.Context(), defs)
	require.NoError(t, err)
	require.Len(t, columns, 2)
	require.True(t, columns[0].primary)
	require.False(t, columns[1].primary)
	for _, def := range defs {
		def.Pkey = &plan.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, CompPkeyCol: &plan.ColDef{}, Names: []string{"id", "v"}}
	}
	columns, err = validateRebaseTableSchema(t.Context(), defs)
	require.NoError(t, err)
	require.True(t, columns[1].primary)
	for _, def := range defs {
		def.Pkey = &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName}
	}
	columns, err = validateRebaseTableSchema(t.Context(), defs)
	require.NoError(t, err)
	require.False(t, columns[0].primary, "no-PK layout still requires a separate unchanged-bag proof")
}

func TestRebaseSchemaRejectsUnsupportedOrDifferent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*plan.TableDef)
	}{
		{"view", func(d *plan.TableDef) { d.ViewSql = &plan.ViewDef{} }},
		{"external", func(d *plan.TableDef) { d.TableType = catalog.SystemExternalRel }},
		{"temporary", func(d *plan.TableDef) { d.IsTemporary = true }},
		{"partition", func(d *plan.TableDef) { d.Partition = &plan.Partition{} }},
		{"index", func(d *plan.TableDef) { d.Indexes = []*plan.IndexDef{{}} }},
		{"foreign key", func(d *plan.TableDef) { d.Fkeys = []*plan.ForeignKeyDef{{}} }},
		{"referenced parent", func(d *plan.TableDef) { d.RefChildTbls = []uint64{999} }},
		{"check", func(d *plan.TableDef) { d.Checks = []*plan.CheckDef{{}} }},
		{"generated", func(d *plan.TableDef) { d.Cols[1].GeneratedCol = &plan.GeneratedCol{} }},
		{"auto increment key", func(d *plan.TableDef) { d.Cols[0].Typ.AutoIncr = true }},
		{"auto increment value", func(d *plan.TableDef) { d.Cols[1].Typ.AutoIncr = true }},
		{"on update", func(d *plan.TableDef) { d.Cols[1].OnUpdate = &plan.OnUpdate{} }},
		{"width", func(d *plan.TableDef) { d.Cols[1].Typ.Width++ }},
		{"collation", func(d *plan.TableDef) { d.Cols[1].Typ.Charset = 42 }},
		{"nullability", func(d *plan.TableDef) { d.Cols[1].NotNull = true }},
		{"JSON", func(d *plan.TableDef) { d.Cols[1].Typ.Id = int32(types.T_json) }},
		{"missing key", func(d *plan.TableDef) { d.Pkey.PkeyColName = "absent" }},
		{"duplicate column", func(d *plan.TableDef) { d.Cols[1].Name = "ID" }},
		{"nil column", func(d *plan.TableDef) { d.Cols[1] = nil }},
		{"nonliteral default", func(d *plan.TableDef) {
			d.Cols[1].Default = &plan.Default{Expr: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "now"}}}}}
		}},
		{"folded nonliteral default", func(d *plan.TableDef) {
			d.Cols[1].Default = &plan.Default{Expr: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Src: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "now"}}}}}}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defs := rebaseSchemaFixture()
			tc.change(defs[2])
			columns, err := validateRebaseTableSchema(t.Context(), defs)
			require.Error(t, err)
			require.Nil(t, columns)
		})
	}
	defs := rebaseSchemaFixture()
	defs[1] = nil
	_, err := validateRebaseTableSchema(t.Context(), defs)
	require.Error(t, err)
}

func TestRebaseOmittedTableSchema(t *testing.T) {
	fixture := func() [4]*plan.TableDef {
		defs := rebaseSchemaFixture()
		for i, def := range defs {
			def.Indexes = []*plan.IndexDef{{IndexName: "ix_v", IndexTableName: fmt.Sprintf("hidden_%d", i),
				IdxId: fmt.Sprint(i), Parts: []string{"v", "id"}, TableExist: true}}
		}
		return defs
	}
	columns, err := validateRebaseOmittedTableSchema(t.Context(), fixture())
	require.NoError(t, err)
	require.Len(t, columns, 2)
	_, err = validateRebaseTableSchema(t.Context(), fixture())
	require.ErrorContains(t, err, "non-PK indexes", "selected tables retain their original restrictions")
	for _, tc := range []struct {
		name   string
		change func(*plan.TableDef)
	}{
		{"unique", func(d *plan.TableDef) { d.Indexes[0].Unique = true }},
		{"plugin", func(d *plan.TableDef) { d.Indexes[0].IndexAlgo = "fulltext2" }},
		{"index role", func(d *plan.TableDef) { d.Indexes[0].IndexAlgoTableType = "storage" }},
		{"prefix params", func(d *plan.TableDef) { d.Indexes[0].IndexAlgoParams = "{}" }},
		{"included columns", func(d *plan.TableDef) { d.Indexes[0].IncludedColumns = []string{"v"} }},
		{"missing storage", func(d *plan.TableDef) { d.Indexes[0].IndexTableName = "" }},
		{"not materialized", func(d *plan.TableDef) { d.Indexes[0].TableExist = false }},
		{"different parts", func(d *plan.TableDef) { d.Indexes[0].Parts = []string{"id"} }},
		{"different visibility", func(d *plan.TableDef) { d.Indexes[0].Visible = !d.Indexes[0].Visible }},
		{"different count", func(d *plan.TableDef) { d.Indexes = nil }},
		{"different column", func(d *plan.TableDef) { d.Cols[1].Typ.Width++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defs := fixture()
			tc.change(defs[2])
			_, err := validateRebaseOmittedTableSchema(t.Context(), defs)
			require.Error(t, err)
		})
	}
}

func TestRebaseSchemaFloatingPointKeys(t *testing.T) {
	for _, typ := range []types.T{types.T_float32, types.T_float64} {
		t.Run(typ.String(), func(t *testing.T) {
			defs := rebaseSchemaFixture()
			for _, def := range defs {
				def.Cols[0].Typ.Id = int32(typ)
			}
			_, err := validateRebaseTableSchema(t.Context(), defs)
			require.ErrorContains(t, err, "floating-point primary keys")

			for _, def := range defs {
				def.Cols[0].Typ.Id = int32(types.T_int64)
				def.Cols[1].Typ.Id = int32(typ)
			}
			_, err = validateRebaseTableSchema(t.Context(), defs)
			require.NoError(t, err, "floating-point non-key columns remain supported")

			for _, def := range defs {
				def.Pkey = &plan.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, CompPkeyCol: &plan.ColDef{}, Names: []string{"id", "v"}}
			}
			_, err = validateRebaseTableSchema(t.Context(), defs)
			require.ErrorContains(t, err, "floating-point primary keys")
		})
	}
}

func TestRebaseBagMismatchLowering(t *testing.T) {
	columns := []rebaseSQLColumn{{"n", types.T_int32.ToType(), false}, {"c0", types.T_varchar.ToType(), false}}
	query, err := buildRebaseBagMismatchSQL(t.Context(), "t", rebaseSQLRead{"left", 1}, rebaseSQLRead{"right", 2}, columns)
	require.NoError(t, err)
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, query, 1)
	require.NoError(t, err)
	stmt.Free()
	require.Contains(t, query, "`n` as c0, serial(`c0`) as c1, count(*) as n")
	require.Contains(t, query, "l.c0 <=> r.c0 and l.c1 <=> r.c1")
	require.Contains(t, query, "where not (l.n <=> r.n)")
	_, err = buildRebaseBagMismatchSQL(t.Context(), "t", rebaseSQLRead{"left", 0}, rebaseSQLRead{"right", 2}, columns)
	require.Error(t, err)
}
