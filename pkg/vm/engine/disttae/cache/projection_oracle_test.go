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

package cache

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// The expected planner message is specified independently of getTableDef and
// compared as a Go value so absent pointers and empty collections are visible.
func TestPublishedProjectionIndependentOracle(t *testing.T) {
	cb := util.BuildCompositeClusterByColumnName([]string{"a", "b"})
	typ := types.T_decimal64.ToType()
	typ.Width, typ.Scale = 15, 3
	// Field 100 is unknown to Default; unmarshal twice to avoid sharing the oracle.
	makeDefault := func() *plan.Default {
		d := &plan.Default{OriginString: "42"}
		require.NoError(t, d.Unmarshal([]byte{0xa0, 0x06, 0x07}))
		return d
	}
	inputDefault := makeDefault()
	item := &TableItem{Id: 11, DatabaseId: 12, LogicalId: 13, Name: "T", DatabaseName: "DB", Version: 8,
		Kind: "r", Comment: "table comment", ViewDef: "select 1", CreateSql: "create table T",
		ExtraInfo: &api.SchemaExtra{FeatureFlag: 1, AutoIncrOffset: 2, AutoIncrEpoch: 3, AutoIdCache: 4, DefaultCharset: 5, CollationVersion: 6, KeyFormat: 7, Checks: []*plan.CheckDef{{Name: "check", OriginSql: "a > 0"}}},
	}
	constraint := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.PrimaryKeyDef{Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, Names: []string{"a", "b"}}},
		&engine.IndexDef{Indexes: []*plan.IndexDef{{IndexName: "idx", Parts: []string{"a", "b"}, Unique: true}}},
		&engine.ForeignKeyDef{Fkeys: []*plan.ForeignKeyDef{{Name: "fk", Cols: []uint64{21}, ForeignTbl: 31, ForeignCols: []uint64{32}}}},
		&engine.RefChildTableDef{Tables: []uint64{33, 34}},
		&engine.StreamConfigsDef{Configs: []*plan.Property{{Key: "topic", Value: "events"}}},
	}}
	partitionWire, err := (&plan.Partition{PartitionDefs: []*plan.PartitionDef{{Def: &plan.Expr{Typ: plan.Type{Id: 42}}}}}).Marshal()
	require.NoError(t, err)
	item.Partitioned, item.Partition = 1, string(partitionWire)
	item.Constraint, err = constraint.MarshalBinary()
	require.NoError(t, err)
	typeWire, err := typ.Marshal()
	require.NoError(t, err)
	zeroTypeWire, err := (&types.Type{}).Marshal()
	require.NoError(t, err)
	defaultWire, err := inputDefault.Marshal()
	require.NoError(t, err)
	updateWire, err := (&plan.OnUpdate{OriginString: "update"}).Marshal()
	require.NoError(t, err)
	generatedWire, err := (&plan.GeneratedCol{OriginString: "generate", IsStored: true}).Marshal()
	require.NoError(t, err)
	InitTableItemWithColumns(item, Columns{
		{Num: 21, Name: "A", Typ: typeWire, ConstraintType: catalog.SystemColPKConstraint, IsAutoIncrement: 1, Seqnum: 9, IsUnsigned: 3, EnumValues: "one,two", Comment: "column", HasDef: 1, DefaultExpr: defaultWire, HasUpdate: 1, UpdateExpr: updateWire, HasGenerated: 1, GeneratedExpr: generatedWire},
		{Num: 22, Name: catalog.CPrimaryKeyColName, Typ: zeroTypeWire, IsHidden: 1},
		{Num: 23, Name: cb, Typ: zeroTypeWire, IsHidden: 1, IsClusterBy: 1},
	})
	require.Nil(t, item.Defs)
	require.NotNil(t, item.schema)
	source := item.SchemaSource()
	require.True(t, source.Valid())
	pkCol := &plan.ColDef{ColId: 22, Name: catalog.CPrimaryKeyColName, OriginName: catalog.CPrimaryKeyColName, Hidden: true, Typ: plan.Type{Table: "T", NotNullable: true}, Default: &plan.Default{}}
	cbCol := &plan.ColDef{ColId: 23, Name: cb, OriginName: cb, Hidden: true, ClusterBy: true, Typ: plan.Type{Table: "T", NotNullable: true}, Default: &plan.Default{}}
	want := &plan.TableDef{TblId: 11, DbId: 12, LogicalId: 13, Name: "T", DbName: "DB", Version: 8,
		Partition: &plan.Partition{PartitionDefs: []*plan.PartitionDef{{Def: &plan.Expr{Typ: plan.Type{Id: 42}}}}},
		TableType: "r", Createsql: "create table T", ViewSql: &plan.ViewDef{View: "select 1"},
		Cols:          []*plan.ColDef{{ColId: 21, Name: "a", OriginName: "A", Primary: true, Seqnum: 9, Comment: "column", Typ: plan.Type{Id: int32(types.T_decimal64), Width: 15, Scale: 3, AutoIncr: true, Table: "T", NotNullable: true, Enumvalues: "one,two", NumericAttributes: 3}, Default: makeDefault(), OnUpdate: &plan.OnUpdate{OriginString: "update"}, GeneratedCol: &plan.GeneratedCol{OriginString: "generate", IsStored: true}}, pkCol, cbCol},
		Name2ColIndex: map[string]int32{"a": 0, catalog.CPrimaryKeyColName: 1, cb: 2},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, Names: []string{"a", "b"}, CompPkeyCol: pkCol},
		ClusterBy:     &plan.ClusterByDef{Name: cb, CompCbkeyCol: cbCol},
		Indexes:       []*plan.IndexDef{{IndexName: "idx", Parts: []string{"a", "b"}, Unique: true}},
		Fkeys:         []*plan.ForeignKeyDef{{Name: "fk", Cols: []uint64{21}, ForeignTbl: 31, ForeignCols: []uint64{32}}}, RefChildTbls: []uint64{33, 34},
		Checks: []*plan.CheckDef{{Name: "check", OriginSql: "a > 0"}}, FeatureFlag: 1, AutoIncrOffset: 2, AutoIncrEpoch: 3, AutoIdCache: 4, DefaultCharset: 5, CollationVersion: 6, KeyFormat: 7,
		Defs: []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: catalog.SystemRelAttr_Comment, Value: "table comment"}, {Key: "topic", Value: "events"}, {Key: catalog.SystemRelAttr_Kind, Value: "r"}, {Key: catalog.SystemRelAttr_CreateSQL, Value: "create table T"}}}}}},
	}
	first, second := source.PlanTableDef(), item.BuildPlanTableDef()
	require.Equal(t, want, first)
	require.Equal(t, want, second)
	require.NotSame(t, first.Cols[1], first.Pkey.CompPkeyCol)
	require.NotSame(t, first.Cols[2], first.ClusterBy.CompCbkeyCol)
	defs, err := source.EngineDefs(item)
	require.NoError(t, err)
	wantDefs := []engine.TableDef{
		&engine.CommentDef{Comment: "table comment"},
		&engine.ViewDef{View: "select 1"},
		constraint,
		&engine.PartitionDef{Partitioned: 1, Partition: string(partitionWire)},
		&engine.PropertiesDef{Properties: []engine.Property{{Key: catalog.SystemRelAttr_Kind, Value: "r"}, {Key: catalog.PropSchemaExtra, Value: string(api.MustMarshalTblExtra(item.ExtraInfo))}, {Key: catalog.SystemRelAttr_CreateSQL, Value: "create table T"}}},
		&engine.AttributeDef{Attr: engine.Attribute{ID: 21, Name: "A", Alg: compress.Lz4, Type: typ, Default: makeDefault(), OnUpdate: &plan.OnUpdate{OriginString: "update"}, Primary: true, Comment: "column", AutoIncrement: true, Seqnum: 9, EnumVlaues: "one,two", GeneratedCol: &plan.GeneratedCol{OriginString: "generate", IsStored: true}, NumericAttributes: 3}},
		&engine.AttributeDef{Attr: engine.Attribute{ID: 22, Name: catalog.CPrimaryKeyColName, Alg: compress.Lz4, IsHidden: true, Default: &plan.Default{}}},
		&engine.AttributeDef{Attr: engine.Attribute{ID: 23, Name: cb, Alg: compress.Lz4, IsHidden: true, ClusterBy: true, Default: &plan.Default{}}},
	}
	require.Equal(t, wantDefs, defs)
	defs[5].(*engine.AttributeDef).Attr.Default.OriginString = "private"
	require.Equal(t, "42", source.BorrowPlanTableDef().Cols[0].Default.OriginString)
	wire, err := first.Cols[0].Default.Marshal()
	require.NoError(t, err)
	require.True(t, bytes.Contains(wire, []byte{0xa0, 0x06, 0x07}))
	first.Cols[0].Default.XXX_DiscardUnknown()
	first.Cols[0].Default.OriginString = "changed"
	first.Cols[0].OnUpdate.OriginString = "changed"
	first.Cols[0].GeneratedCol.OriginString = "changed"
	first.Pkey.CompPkeyCol.Name = "changed"
	first.ClusterBy.CompCbkeyCol.Name = "changed"
	first.Indexes[0].Parts[0] = "changed"
	first.Fkeys[0].ForeignCols[0] = 0
	first.RefChildTbls[0] = 0
	first.Checks[0].Name = "changed"
	first.Partition.PartitionDefs[0].Def.Typ.Id = 0
	first.Defs[0].GetProperties().Properties[0].Value = "changed"
	first.Name2ColIndex["a"] = 99
	require.Equal(t, want, second)
	require.Equal(t, want, source.PlanTableDef())
	require.Equal(t, want, source.BorrowPlanTableDef())
	require.Equal(t, "42", inputDefault.OriginString)
}

// Empty published schemas retain their explicit empty column/map and checks,
// while absent optional messages and slices stay nil.
func TestPublishedProjectionEmptyOracle(t *testing.T) {
	item := &TableItem{Name: "empty", Defs: []engine.TableDef{}}
	item.schema = newCandidateSchema(item)
	require.NotNil(t, item.schema)
	item.Defs = nil
	want := &plan.TableDef{Name: "empty", Cols: []*plan.ColDef{}, Name2ColIndex: map[string]int32{}, Checks: []*plan.CheckDef{}, Defs: []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: catalog.SystemRelAttr_Kind}}}}}}}
	require.Equal(t, want, item.SchemaSource().PlanTableDef())
}
