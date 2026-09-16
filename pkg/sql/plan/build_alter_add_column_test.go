// Copyright 2024 Matrix Origin
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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestDropColumnWithIndex(t *testing.T) {
	var def TableDef
	def.Indexes = []*IndexDef{
		{IndexName: "idx",
			IndexAlgo:  "fulltext",
			TableExist: true,
			Unique:     false,
			Parts:      []string{"body", "title"},
		},
	}

	err := handleDropColumnWithIndex(context.TODO(), "body", &def)
	require.Nil(t, err)
	require.Equal(t, 1, len(def.Indexes[0].Parts))
	require.Equal(t, "title", def.Indexes[0].Parts[0])
}

func TestDropColumnWithNative0900PrimaryKeyPreservesOrRemovesHiddenKey(t *testing.T) {
	native := &ColDef{
		Name: "name",
		Typ:  Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900AI)},
	}
	hidden := MakeHiddenColDefByName(catalog.CPrimaryKeyColName)
	hidden.Primary = true

	def := TableDef{
		Cols:      []*ColDef{native, hidden},
		KeyFormat: uint32(types.PADSpaceKeyV1),
		Pkey: &PrimaryKeyDef{
			Names:       []string{"name"},
			PkeyColName: hidden.Name,
			CompPkeyCol: hidden,
		},
	}
	require.NoError(t, handleDropColumnWithPrimaryKey(context.Background(), "name", &def))
	require.Nil(t, def.Pkey)
	require.Nil(t, FindColumn(def.Cols, hidden.Name))
	require.Zero(t, def.KeyFormat)

	first := &ColDef{Name: "first", Typ: Type{Id: int32(types.T_int64)}}
	remaining := &ColDef{
		Name: "name",
		Typ:  Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900AI)},
	}
	hidden = MakeHiddenColDefByName(catalog.CPrimaryKeyColName)
	hidden.Primary = true
	def = TableDef{
		Cols:      []*ColDef{first, remaining, hidden},
		KeyFormat: uint32(types.PADSpaceKeyV1),
		Pkey: &PrimaryKeyDef{
			Names:       []string{"first", "name"},
			PkeyColName: hidden.Name,
			CompPkeyCol: hidden,
		},
	}
	require.NoError(t, handleDropColumnWithPrimaryKey(context.Background(), "first", &def))
	require.NotNil(t, def.Pkey)
	require.Equal(t, []string{"name"}, def.Pkey.Names)
	require.Equal(t, hidden.Name, def.Pkey.PkeyColName)
	require.Same(t, hidden, def.Pkey.CompPkeyCol)
	require.NotNil(t, FindColumn(def.Cols, hidden.Name))
	require.Equal(t, uint32(types.PADSpaceKeyV1), def.KeyFormat)
}

func TestAddPrimaryKeyWithNative0900StringUsesHiddenPhysicalKey(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.SetContext(context.Background())
	name := &ColDef{
		Name: "name",
		Typ:  Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900AI)},
		Default: &plan.Default{
			NullAbility: true,
		},
	}
	def := &TableDef{Cols: []*ColDef{name}}
	spec := &tree.PrimaryKeyIndex{
		KeyParts: []*tree.KeyPart{{ColName: tree.NewUnresolvedColName("name")}},
	}

	err := AddPrimaryKey(ctx, &plan.AlterTable{CopyTableDef: def}, spec, nil)
	require.NoError(t, err)
	require.NotNil(t, def.Pkey)
	require.Equal(t, []string{"name"}, def.Pkey.Names)
	require.Equal(t, catalog.CPrimaryKeyColName, def.Pkey.PkeyColName)
	require.NotNil(t, def.Pkey.CompPkeyCol)
	require.True(t, def.Pkey.CompPkeyCol.Hidden)
	require.Same(t, def.Pkey.CompPkeyCol, FindColumn(def.Cols, catalog.CPrimaryKeyColName))
	require.True(t, name.NotNull)
	require.False(t, name.Primary)
	require.False(t, name.Default.NullAbility)
	require.Equal(t, uint32(types.PADSpaceKeyV1), def.KeyFormat)
}

func TestAddCompositePrimaryKeyWithNative0900PartUsesVersionedPhysicalKey(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.SetContext(context.Background())
	name := &ColDef{
		Name: "name",
		Typ:  Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900AI)},
		Default: &plan.Default{
			NullAbility: true,
		},
	}
	id := &ColDef{Name: "id", Typ: Type{Id: int32(types.T_int64)}, Default: &plan.Default{NullAbility: true}}
	def := &TableDef{Cols: []*ColDef{id, name}}
	spec := &tree.PrimaryKeyIndex{
		KeyParts: []*tree.KeyPart{
			{ColName: tree.NewUnresolvedColName("id")},
			{ColName: tree.NewUnresolvedColName("name")},
		},
	}

	require.NoError(t, AddPrimaryKey(ctx, &plan.AlterTable{CopyTableDef: def}, spec, nil))
	require.Equal(t, uint32(types.PADSpaceKeyV1), def.KeyFormat)
	require.Equal(t, catalog.CPrimaryKeyColName, def.Pkey.PkeyColName)
}

func TestDropColumnRemovesEveryAdjacentSingleColumnIndex(t *testing.T) {
	def := TableDef{Indexes: []*IndexDef{
		{IndexName: "uk_body", Unique: true, Parts: []string{"body"}},
		{
			IndexName: "idx_body",
			IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
			Parts:     []string{"body", catalog.CreateAlias("id")},
		},
		{
			IndexName: "idx_body_title",
			IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
			Parts:     []string{"body", "title", catalog.CreateAlias("id")},
		},
	}}

	require.NoError(t, handleDropColumnWithIndex(context.Background(), "body", &def))
	require.Len(t, def.Indexes, 1)
	require.Equal(t, "idx_body_title", def.Indexes[0].IndexName)
	require.Equal(t, []string{"title", catalog.CreateAlias("id")}, def.Indexes[0].Parts)
}

func TestCheckGeometryKeyPartTypes(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_geometry)}

	err := checkPrimaryKeyPartType(context.Background(), typ, "g")
	require.Error(t, err)
	require.Contains(t, err.Error(), "GEOMETRY column 'g' cannot be in primary key")

	err = checkUniqueKeyPartType(context.Background(), typ, "g")
	require.Error(t, err)
	require.Contains(t, err.Error(), "GEOMETRY column 'g' cannot be in unique index")
}

func TestCheckEnumPrimaryKeyPartType(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_enum), Enumvalues: "ACW,BT,XS3"}
	require.NoError(t, checkPrimaryKeyPartType(context.Background(), typ, "source"))
}

func TestCheckAddColumnWithUniqueKeyVisibility(t *testing.T) {
	tests := []struct {
		name    string
		option  *tree.IndexOption
		visible bool
	}{
		{
			name:    "default is visible",
			visible: true,
		},
		{
			name: "explicit invisible is preserved",
			option: &tree.IndexOption{
				Visible: tree.VISIBLE_TYPE_INVISIBLE,
			},
			visible: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			indexDef, err := checkAddColumWithUniqueKey(context.Background(), &TableDef{}, &tree.UniqueIndex{
				Name:        "idx_a",
				KeyParts:    []*tree.KeyPart{{ColName: tree.NewUnresolvedColName("a")}},
				IndexOption: tc.option,
			})
			require.NoError(t, err)
			got, isSet := catalog.GetIndexVisibility(indexDef)
			require.True(t, isSet)
			require.Equal(t, tc.visible, got)
			require.Equal(t, tc.visible, indexDef.Visible)
		})
	}
}

// TestCheckVectorPrimaryKeyPartTypes verifies ALTER ... ADD PRIMARY KEY rejects
// every vector element type — including the narrow types (bf16/f16/int8/uint8),
// which previously slipped through admission and hit the txn duplicate checker's
// default panic on insert.
func TestCheckVectorPrimaryKeyPartTypes(t *testing.T) {
	for _, id := range []types.T{
		types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16,
		types.T_array_int8, types.T_array_uint8,
	} {
		require.True(t, id.IsArrayRelate(), "%s should be array-related", id)
		err := checkPrimaryKeyPartType(context.Background(), plan.Type{Id: int32(id)}, "v")
		require.Error(t, err, "type %s must be rejected in primary key", id)
		require.Contains(t, err.Error(), "VECTOR column 'v' cannot be in primary key")
	}
}

func TestCheckIndexedColumnTypeChangeGeometry(t *testing.T) {
	tableDef := &TableDef{
		Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Indexes: []*plan.IndexDef{
			{IndexName: "u_g", Unique: true, Parts: []string{"g"}},
			{IndexName: "idx_g", Parts: []string{"g"}},
		},
	}

	oldCol := &ColDef{Name: "g", OriginName: "g", Typ: plan.Type{Id: int32(types.T_varchar)}}
	newCol := &ColDef{Name: "g", OriginName: "g", Typ: plan.Type{Id: int32(types.T_geometry)}}

	err := checkIndexedColumnTypeChange(context.Background(), tableDef, oldCol, newCol)
	require.Error(t, err)
	require.Contains(t, err.Error(), "GEOMETRY column 'g' cannot be in unique index")

	pkOldCol := &ColDef{Name: "id", OriginName: "id", Typ: plan.Type{Id: int32(types.T_int64)}}
	pkNewCol := &ColDef{Name: "id", OriginName: "id", Typ: plan.Type{Id: int32(types.T_geometry)}}
	err = checkIndexedColumnTypeChange(context.Background(), tableDef, pkOldCol, pkNewCol)
	require.Error(t, err)
	require.Contains(t, err.Error(), "GEOMETRY column 'id' cannot be in primary key")
}
