// Copyright 2023 Matrix Origin
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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func keyPartWithLength(colName string, length int) *tree.KeyPart {
	return &tree.KeyPart{
		ColName: tree.NewUnresolvedColName(colName),
		Length:  length,
	}
}

func TestIndexTableKeyTypeForPrefix(t *testing.T) {
	tests := []struct {
		name     string
		typeId   types.T
		wantOk   bool
		wantId   types.T
		wantWide int32
	}{
		{name: "text", typeId: types.T_text, wantOk: true, wantId: types.T_varchar, wantWide: types.MaxVarcharLen},
		{name: "blob", typeId: types.T_blob, wantOk: true, wantId: types.T_varbinary, wantWide: types.MaxVarBinaryLen},
		{name: "varchar not prefixable", typeId: types.T_varchar, wantOk: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := indexTableKeyTypeForPrefix(Type{Id: int32(tt.typeId)})
			require.Equal(t, tt.wantOk, ok)
			if tt.wantOk {
				require.Equal(t, int32(tt.wantId), got.Id)
				require.Equal(t, tt.wantWide, got.Width)
			}
		})
	}
}

func TestIndexTableKeyTypeForSinglePart(t *testing.T) {
	t.Run("nil column returns empty type", func(t *testing.T) {
		require.Equal(t, Type{}, indexTableKeyTypeForSinglePart(nil, keyPartWithLength("t", 10)))
	})

	t.Run("text prefix maps to varchar", func(t *testing.T) {
		col := &ColDef{Typ: Type{Id: int32(types.T_text), Width: int32(types.MaxVarcharLen)}}
		got := indexTableKeyTypeForSinglePart(col, keyPartWithLength("t", 10))
		require.Equal(t, int32(types.T_varchar), got.Id)
		require.Equal(t, int32(types.MaxVarcharLen), got.Width)
	})

	t.Run("blob prefix maps to varbinary", func(t *testing.T) {
		col := &ColDef{Typ: Type{Id: int32(types.T_blob), Width: int32(types.MaxVarBinaryLen)}}
		got := indexTableKeyTypeForSinglePart(col, keyPartWithLength("b", 10))
		require.Equal(t, int32(types.T_varbinary), got.Id)
		require.Equal(t, int32(types.MaxVarBinaryLen), got.Width)
	})

	t.Run("non prefixable column keeps original type even with length", func(t *testing.T) {
		col := &ColDef{Typ: Type{Id: int32(types.T_varchar), Width: 50, Scale: 0}}
		got := indexTableKeyTypeForSinglePart(col, keyPartWithLength("v", 10))
		require.Equal(t, int32(types.T_varchar), got.Id)
		require.Equal(t, int32(50), got.Width)
	})

	t.Run("no length keeps original type", func(t *testing.T) {
		col := &ColDef{Typ: Type{Id: int32(types.T_text), Width: 100, Scale: 2}}
		got := indexTableKeyTypeForSinglePart(col, keyPartWithLength("t", 0))
		require.Equal(t, int32(types.T_text), got.Id)
		require.Equal(t, int32(100), got.Width)
		require.Equal(t, int32(2), got.Scale)
	})

	t.Run("nil key part keeps original type", func(t *testing.T) {
		col := &ColDef{Typ: Type{Id: int32(types.T_text), Width: 100}}
		got := indexTableKeyTypeForSinglePart(col, nil)
		require.Equal(t, int32(types.T_text), got.Id)
		require.Equal(t, int32(100), got.Width)
	})
}

func TestIndexColumnCheckKind(t *testing.T) {
	tests := []struct {
		indexType tree.IndexType
		want      string
	}{
		{tree.INDEX_TYPE_IVFFLAT, "ivfflat"},
		{tree.INDEX_TYPE_HNSW, "hnsw"},
		{tree.INDEX_TYPE_RTREE, "rtree"},
		{tree.INDEX_TYPE_BTREE, "secondary"},
		{tree.INDEX_TYPE_INVALID, "secondary"},
	}

	for _, tt := range tests {
		require.Equal(t, tt.want, indexColumnCheckKind(tt.indexType))
	}
}

func TestCheckIndexColumnSupportability(t *testing.T) {
	ctx := context.Background()

	colOf := func(typeId types.T, enumValues ...string) *ColDef {
		typ := plan.Type{Id: int32(typeId)}
		if len(enumValues) > 0 {
			typ.Enumvalues = strings.Join(enumValues, ",")
		}
		return &ColDef{Name: "c", Typ: typ}
	}
	keyPart := keyPartWithLength("c", 0)
	prefixKeyPart := keyPartWithLength("c", 10)

	t.Run("nil inputs", func(t *testing.T) {
		require.Error(t, checkIndexColumnSupportability(ctx, nil, keyPart, "secondary"))
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_int64), nil, "secondary"))
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_int64), &tree.KeyPart{}, "secondary"))
	})

	t.Run("blob requires prefix in non-primary index", func(t *testing.T) {
		require.NoError(t, checkIndexColumnSupportability(ctx, colOf(types.T_blob), prefixKeyPart, "secondary"))
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_blob), keyPart, "secondary"))
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_blob), prefixKeyPart, "primary"))
	})

	t.Run("text requires prefix in non-primary index", func(t *testing.T) {
		require.NoError(t, checkIndexColumnSupportability(ctx, colOf(types.T_text), prefixKeyPart, "unique"))
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_text), keyPart, "secondary"))
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_text), prefixKeyPart, "primary"))
	})

	t.Run("datalink and json never allowed", func(t *testing.T) {
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_datalink), keyPart, "secondary"))
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_json), keyPart, "secondary"))
	})

	t.Run("vector only allowed for ivfflat and hnsw", func(t *testing.T) {
		require.NoError(t, checkIndexColumnSupportability(ctx, colOf(types.T_array_float32), keyPart, "ivfflat"))
		require.NoError(t, checkIndexColumnSupportability(ctx, colOf(types.T_array_float64), keyPart, "hnsw"))
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_array_float32), keyPart, "secondary"))
	})

	t.Run("enum rejected only in primary key", func(t *testing.T) {
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_enum, "a", "b"), keyPart, "primary"))
		require.NoError(t, checkIndexColumnSupportability(ctx, colOf(types.T_enum, "a", "b"), keyPart, "secondary"))
		require.NoError(t, checkIndexColumnSupportability(ctx, colOf(types.T_enum, "a", "b"), keyPart, "unique"))
	})

	t.Run("set rejected in primary and unique", func(t *testing.T) {
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_uint64, "a", "b"), keyPart, "primary"))
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_uint64, "a", "b"), keyPart, "unique"))
		require.NoError(t, checkIndexColumnSupportability(ctx, colOf(types.T_uint64, "a", "b"), keyPart, "secondary"))
	})

	t.Run("geometry only allowed for rtree", func(t *testing.T) {
		require.NoError(t, checkIndexColumnSupportability(ctx, colOf(types.T_geometry), keyPart, "rtree"))
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_geometry), keyPart, "primary"))
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_geometry), keyPart, "unique"))
		require.Error(t, checkIndexColumnSupportability(ctx, colOf(types.T_geometry), keyPart, "secondary"))
	})

	t.Run("ordinary column allowed", func(t *testing.T) {
		require.NoError(t, checkIndexColumnSupportability(ctx, colOf(types.T_int64), keyPart, "primary"))
		require.NoError(t, checkIndexColumnSupportability(ctx, colOf(types.T_varchar), keyPart, "secondary"))
	})
}
