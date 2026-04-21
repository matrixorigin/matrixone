// Copyright 2022 Matrix Origin
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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func makeTestColDef(name string, oid types.T) *ColDef {
	return &ColDef{
		Name: name,
		Typ: Type{
			Id: int32(oid),
		},
	}
}

func makeIncludeIndexOption(names ...string) *tree.IndexOption {
	cols := make([]*tree.UnresolvedName, len(names))
	for i, name := range names {
		cols[i] = tree.NewUnresolvedName(tree.NewCStr(name, 1))
	}
	return &tree.IndexOption{
		AlgoParamList:  10,
		IncludeColumns: cols,
	}
}

func makeIvfIndexWithInclude(vecCol string, includeCols ...string) *tree.Index {
	return tree.NewIndex(
		false,
		[]*tree.KeyPart{tree.NewKeyPart(tree.NewUnresolvedName(tree.NewCStr(vecCol, 1)), -1, tree.DefaultDirection, nil)},
		"idx_ivf",
		tree.INDEX_TYPE_IVFFLAT,
		makeIncludeIndexOption(includeCols...),
	)
}

func TestBuildIvfFlatSecondaryIndexDef_StoresIncludeColumnsInAlgoParams(t *testing.T) {
	ctx := NewMockOptimizer(false).CurrentContext()
	indexInfo := makeIvfIndexWithInclude("embedding", "title", "category")
	colMap := map[string]*ColDef{
		"id":        makeTestColDef("id", types.T_int64),
		"embedding": makeTestColDef("embedding", types.T_array_float32),
		"title":     makeTestColDef("title", types.T_varchar),
		"category":  makeTestColDef("category", types.T_varchar),
	}

	indexDefs, _, err := buildIvfFlatSecondaryIndexDef(ctx, indexInfo, colMap, nil, "id")
	require.NoError(t, err)
	require.Len(t, indexDefs, 3)

	for _, indexDef := range indexDefs {
		paramMap, err := catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams)
		require.NoError(t, err)
		require.Equal(t, `["title","category"]`, paramMap[catalog.IndexAlgoParamIncludeColumns])
	}
}

func TestBuildIvfFlatSecondaryIndexDef_ExtendsEntriesTableWithIncludeColumns(t *testing.T) {
	ctx := NewMockOptimizer(false).CurrentContext()
	indexInfo := makeIvfIndexWithInclude("embedding", "title", "category")
	colMap := map[string]*ColDef{
		"id":        makeTestColDef("id", types.T_int64),
		"embedding": makeTestColDef("embedding", types.T_array_float32),
		"title":     makeTestColDef("title", types.T_varchar),
		"category":  makeTestColDef("category", types.T_int32),
	}

	_, tableDefs, err := buildIvfFlatSecondaryIndexDef(ctx, indexInfo, colMap, nil, "id")
	require.NoError(t, err)
	require.Len(t, tableDefs, 3)

	entriesTable := tableDefs[2]
	require.Len(t, entriesTable.Cols, 7)
	require.Equal(t, catalog.SystemSI_IVFFLAT_TblCol_Entries_version, entriesTable.Cols[0].Name)
	require.Equal(t, catalog.SystemSI_IVFFLAT_TblCol_Entries_id, entriesTable.Cols[1].Name)
	require.Equal(t, catalog.SystemSI_IVFFLAT_TblCol_Entries_pk, entriesTable.Cols[2].Name)
	require.Equal(t, catalog.SystemSI_IVFFLAT_TblCol_Entries_entry, entriesTable.Cols[3].Name)
	require.Equal(t, catalog.SystemSI_IVFFLAT_IncludeColPrefix+"title", entriesTable.Cols[4].Name)
	require.Equal(t, catalog.SystemSI_IVFFLAT_IncludeColPrefix+"category", entriesTable.Cols[5].Name)
	require.Equal(t, catalog.CPrimaryKeyColName, entriesTable.Cols[6].Name)
	require.Same(t, entriesTable.Cols[6], entriesTable.Pkey.CompPkeyCol)
}

func TestBuildIvfFlatSecondaryIndexDef_RejectsTooManyIncludeColumns(t *testing.T) {
	ctx := NewMockOptimizer(false).CurrentContext()

	includeCols := make([]string, 11)
	colMap := map[string]*ColDef{
		"id":        makeTestColDef("id", types.T_int64),
		"embedding": makeTestColDef("embedding", types.T_array_float32),
	}
	for i := range includeCols {
		name := string(rune('a' + i))
		includeCols[i] = "c_" + name
		colMap[includeCols[i]] = makeTestColDef(includeCols[i], types.T_varchar)
	}

	_, _, err := buildIvfFlatSecondaryIndexDef(ctx, makeIvfIndexWithInclude("embedding", includeCols...), colMap, nil, "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "supports at most 10 columns")
}
