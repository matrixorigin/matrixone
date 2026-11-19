// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.

package plan

import (
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestCopyCatalogTableTemplate(t *testing.T) {
	// Keep the field inventory explicit so new reference fields require review.
	typ := reflect.TypeOf(TableDef{})
	references := map[string]bool{"Cols": true, "TblFunc": true, "Pkey": true, "Indexes": true, "Fkeys": true, "RefChildTbls": true, "Checks": true, "ClusterBy": true, "Props": true, "ViewSql": true, "Defs": true, "Name2ColIndex": true, "Partition": true, "XXX_unrecognized": true}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		switch f.Type.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map:
			require.True(t, references[f.Name], "unhandled reference field %s", f.Name)
			delete(references, f.Name)
		}
	}
	require.Empty(t, references)
	src := &TableDef{TblId: 17, TblFunc: &TableFunction{}, Pkey: &PrimaryKeyDef{Names: []string{"x"}, CompPkeyCol: &ColDef{Name: "independent"}}, Indexes: []*IndexDef{{}, nil}, Fkeys: []*ForeignKeyDef{{}}, RefChildTbls: []uint64{42}, Checks: []*CheckDef{{}}, ClusterBy: &ClusterByDef{CompCbkeyCol: &ColDef{Name: "cluster"}}, Props: []*PropertyDef{{}}, ViewSql: &ViewDef{}, Defs: []*TableDef_DefType{{}}, Name2ColIndex: map[string]int32{"x": 1}, Partition: &Partition{}, XXX_unrecognized: []byte{0xa0, 0x06, 0x01}}
	got := CopyCatalogTableTemplate(src)
	require.True(t, proto.Equal(src, got))
	got.RefChildTbls[0] = 7
	got.Name2ColIndex["x"] = 2
	got.XXX_unrecognized[2] = 2
	got.Pkey.Names[0] = "changed"
	got.Pkey.CompPkeyCol.Name = "changed"
	got.ClusterBy.CompCbkeyCol.Name = "changed"
	require.Equal(t, "x", src.Pkey.Names[0])
	require.Equal(t, "independent", src.Pkey.CompPkeyCol.Name)
	require.Equal(t, "cluster", src.ClusterBy.CompCbkeyCol.Name)
	require.Equal(t, uint64(42), src.RefChildTbls[0])
	require.Equal(t, int32(1), src.Name2ColIndex["x"])
	require.Equal(t, byte(1), src.XXX_unrecognized[2])
	for _, f := range []string{"TblFunc", "Pkey", "Indexes", "Fkeys", "Checks", "ClusterBy", "Props", "ViewSql", "Defs", "Partition"} {
		a, b := reflect.ValueOf(src).Elem().FieldByName(f), reflect.ValueOf(got).Elem().FieldByName(f)
		if a.Kind() == reflect.Slice {
			a, b = a.Index(0), b.Index(0)
		}
		require.NotEqual(t, a.Pointer(), b.Pointer(), f)
	}
	empty := CopyCatalogTableTemplate(&TableDef{Indexes: []*IndexDef{}, Name2ColIndex: map[string]int32{}})
	require.NotNil(t, empty.Indexes)
	require.NotNil(t, empty.Name2ColIndex)
}

func TestCopyCatalogTypeUnknown(t *testing.T) {
	typ := reflect.TypeOf(Type{})
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		switch f.Type.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map:
			require.Equal(t, "XXX_unrecognized", f.Name, "unhandled reference field")
		}
	}
	src := Type{Id: 3, XXX_unrecognized: []byte{0xa0, 0x06, 0x01}}
	dst := CopyCatalogType(src)
	require.True(t, proto.Equal(&src, &dst))
	dst.XXX_unrecognized[2] = 2
	require.Equal(t, byte(1), src.XXX_unrecognized[2])
}

func TestCatalogTemplatePropertyGroupsPrivate(t *testing.T) {
	src := &TableDef{Defs: []*TableDef_DefType{
		{Def: &TableDef_DefType_Properties{Properties: &PropertiesDef{Properties: []*Property{{Key: "a", Value: "1", XXX_unrecognized: []byte{8, 1}}, nil, {Key: "a", Value: "2"}}}}},
		nil,
		{Def: &TableDef_DefType_Properties{Properties: &PropertiesDef{Properties: []*Property{}}}},
	}}
	a, b := CopyCatalogTableTemplate(src), CopyCatalogTableTemplate(src)
	ap := a.Defs[0].GetProperties().Properties
	bp := b.Defs[0].GetProperties().Properties
	if len(ap) != 3 || ap[1] != nil || ap[2].Value != "2" || a.Defs[1] != nil || a.Defs[2].GetProperties().Properties == nil {
		t.Fatal("property layout changed")
	}
	ap[0].Value = "changed"
	ap[0].XXX_unrecognized[1] = 2
	if bp[0].Value != "1" || bp[0].XXX_unrecognized[1] != 1 || src.Defs[0].GetProperties().Properties[0].Value != "1" {
		t.Fatal("property storage shared")
	}
}

// The hand-copied messages must be revisited when protobuf adds a reference.
func TestCatalogTypedCopyReferenceInventory(t *testing.T) {
	cases := []struct {
		value any
		refs  []string
	}{
		{PrimaryKeyDef{}, []string{"Cols", "Option", "Names", "CompPkeyCol", "XXX_unrecognized"}},
		{ClusterByDef{}, []string{"CompCbkeyCol", "XXX_unrecognized"}},
		{ViewDef{}, []string{"XXX_unrecognized"}},
		{PropertyDef{}, []string{"XXX_unrecognized"}},
		{Property{}, []string{"XXX_unrecognized"}},
		{PropertiesDef{}, []string{"Properties", "XXX_unrecognized"}},
		{TableDef_DefType{}, []string{"Def", "XXX_unrecognized"}},
	}
	for _, tc := range cases {
		typ := reflect.TypeOf(tc.value)
		expected := map[string]bool{}
		for _, name := range tc.refs {
			expected[name] = true
		}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			switch f.Type.Kind() {
			case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
				require.True(t, expected[f.Name], "%s.%s needs an ownership decision", typ.Name(), f.Name)
				delete(expected, f.Name)
			}
		}
		require.Empty(t, expected, typ.Name())
	}
}

func TestCatalogTemplateComplexPrivate(t *testing.T) {
	unknown := func() []byte { return []byte{0xa0, 0x06, 0x01} }
	src := &TableDef{
		TblFunc:       &TableFunction{Name: "fn", Param: []byte{1, 2}, XXX_unrecognized: unknown()},
		Pkey:          &PrimaryKeyDef{Cols: []uint64{1, 2}, Names: []string{"a", "b"}, Option: &IndexOption{}, CompPkeyCol: &ColDef{Name: "pk"}, XXX_unrecognized: unknown()},
		ClusterBy:     &ClusterByDef{Name: "cluster", CompCbkeyCol: &ColDef{Name: "cb"}, XXX_unrecognized: unknown()},
		ViewSql:       &ViewDef{View: "select 1", XXX_unrecognized: unknown()},
		Indexes:       []*IndexDef{{IndexName: "idx", Parts: []string{"a"}, IncludedColumns: []string{"b"}, XXX_unrecognized: unknown()}},
		Fkeys:         []*ForeignKeyDef{{Name: "fk", Cols: []uint64{1}, ForeignCols: []uint64{2}, XXX_unrecognized: unknown()}},
		Checks:        []*CheckDef{{Name: "check", Check: &Expr{XXX_unrecognized: unknown()}, XXX_unrecognized: unknown()}},
		Props:         []*PropertyDef{{Key: "k", Value: "v", XXX_unrecognized: unknown()}},
		Defs:          []*TableDef_DefType{{Def: &TableDef_DefType_Properties{Properties: &PropertiesDef{Properties: []*Property{{Key: "k", Value: "v", XXX_unrecognized: unknown()}}, XXX_unrecognized: unknown()}}, XXX_unrecognized: unknown()}},
		Name2ColIndex: map[string]int32{"MiXeD": 7}, XXX_unrecognized: unknown(),
	}
	snapshot := proto.Clone(src).(*TableDef)
	a, b := CopyCatalogTableTemplate(src), CopyCatalogTableTemplate(src)
	require.True(t, proto.Equal(src, a))
	require.True(t, proto.Equal(src, b))
	a.TblFunc.Param[0] = 9
	a.Pkey.Cols[0] = 9
	a.Pkey.Names[0] = "changed"
	a.Pkey.CompPkeyCol.Name = "changed"
	a.Pkey.XXX_unrecognized[2] = 9
	a.ClusterBy.CompCbkeyCol.Name = "changed"
	a.ClusterBy.XXX_unrecognized[2] = 9
	a.ViewSql.XXX_unrecognized[2] = 9
	a.Indexes[0].Parts[0] = "changed"
	a.Indexes[0].IncludedColumns[0] = "changed"
	a.Indexes[0].XXX_unrecognized[2] = 9
	a.Fkeys[0].Cols[0] = 9
	a.Fkeys[0].ForeignCols[0] = 9
	a.Fkeys[0].XXX_unrecognized[2] = 9
	a.Checks[0].Check.XXX_unrecognized[2] = 9
	a.Checks[0].XXX_unrecognized[2] = 9
	a.Props[0].XXX_unrecognized[2] = 9
	a.Defs[0].XXX_unrecognized[2] = 9
	a.Defs[0].GetProperties().XXX_unrecognized[2] = 9
	a.Defs[0].GetProperties().Properties[0].XXX_unrecognized[2] = 9
	a.Name2ColIndex["MiXeD"] = 9
	a.XXX_unrecognized[2] = 9
	require.True(t, proto.Equal(snapshot, b), "mutating one projection changed another")
	require.True(t, proto.Equal(snapshot, src), "mutating a projection changed the source")
	require.Equal(t, []byte{1, 2}, src.TblFunc.Param)
	require.Equal(t, []uint64{1, 2}, src.Pkey.Cols)
	require.Equal(t, "v", src.Defs[0].GetProperties().Properties[0].Value)
}
