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

package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestIsIndexAsync(t *testing.T) {
	var (
		json string
		err  error
		ok   bool
	)

	json = ""
	ok, err = IsIndexAsync(json)
	require.Nil(t, err)
	require.Equal(t, ok, false)

	json = "{}"
	ok, err = IsIndexAsync(json)
	require.Nil(t, err)
	require.Equal(t, ok, false)

	json = `{"async": "true"}`
	ok, err = IsIndexAsync(json)
	require.Nil(t, err)
	require.Equal(t, ok, true)

	json = `{"async": 1}`
	_, err = IsIndexAsync(json)
	require.NotNil(t, err)
}

func unresolvedName(name string) *tree.UnresolvedName {
	return tree.NewUnresolvedName(tree.NewCStr(name, 1))
}

func TestIndexParamsToJsonString_IvfFlatIncludeColumns(t *testing.T) {
	idx := tree.NewIndex(
		false,
		[]*tree.KeyPart{tree.NewKeyPart(unresolvedName("embedding"), -1, tree.DefaultDirection, nil)},
		"idx1",
		tree.INDEX_TYPE_IVFFLAT,
		&tree.IndexOption{
			AlgoParamList:         10,
			AlgoParamVectorOpType: "vector_l2_ops",
			IncludeColumns: []*tree.UnresolvedName{
				unresolvedName("title"),
				unresolvedName("category"),
			},
		},
	)

	params, err := IndexParamsToJsonString(idx)
	require.NoError(t, err)

	paramMap, err := IndexParamsStringToMap(params)
	require.NoError(t, err)
	require.Equal(t, "10", paramMap[IndexAlgoParamLists])
	require.Equal(t, "vector_l2_ops", paramMap[IndexAlgoParamOpType])
	require.Equal(t, `["title","category"]`, paramMap[IndexAlgoParamIncludeColumns])
}

func TestIndexParamsToStringList_IvfFlatIncludeColumns(t *testing.T) {
	paramList, err := IndexParamsToStringList(`{"lists":"10","op_type":"vector_l2_ops","include_columns":"[\"title\",\"category\"]"}`)
	require.NoError(t, err)
	require.Contains(t, paramList, "lists = 10")
	require.Contains(t, paramList, "op_type 'vector_l2_ops'")
	require.Contains(t, paramList, "INCLUDE (title, category)")
}

func TestParseIncludeColumnsValue_BackwardCompatible(t *testing.T) {
	cols, err := ParseIncludeColumnsValue(`["title","category"]`)
	require.NoError(t, err)
	require.Equal(t, []string{"title", "category"}, cols)

	cols, err = ParseIncludeColumnsValue("title,category")
	require.NoError(t, err)
	require.Equal(t, []string{"title", "category"}, cols)
}
