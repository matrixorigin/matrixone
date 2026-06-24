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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// TestIndexBuildParamsRoundTrip covers the promoted build params
// (kmeans_train_percent, kmeans_max_iteration, max_index_capacity) as
// first-class flat algo_params keys: SHOW CREATE rendering and the flat-map
// read-back.
func TestIndexBuildParamsRoundTrip(t *testing.T) {
	algoParams := `{"lists":"8","op_type":"vector_l2_ops","kmeans_train_percent":"5","kmeans_max_iteration":"30","max_index_capacity":"2000"}`

	// SHOW CREATE / restore DDL rendering must emit the new keys in a
	// re-parseable form ("<key> = <val>").
	s, err := IndexParamsToStringList(algoParams)
	require.Nil(t, err)
	require.True(t, strings.Contains(s, IndexAlgoParamKmeansTrainPercent+" = 5"), s)
	require.True(t, strings.Contains(s, IndexAlgoParamKmeansMaxIteration+" = 30"), s)
	require.True(t, strings.Contains(s, IndexAlgoParamMaxIndexCapacity+" = 2000"), s)

	// Flat-map read-back (what the build path consumes).
	m, err := IndexParamsStringToMap(algoParams)
	require.Nil(t, err)
	require.Equal(t, "5", m[IndexAlgoParamKmeansTrainPercent])
	require.Equal(t, "30", m[IndexAlgoParamKmeansMaxIteration])
	require.Equal(t, "2000", m[IndexAlgoParamMaxIndexCapacity])

	// Absent keys (legacy index) round-trip cleanly with no rendering.
	s, err = IndexParamsToStringList(`{"lists":"8"}`)
	require.Nil(t, err)
	require.False(t, strings.Contains(s, IndexAlgoParamKmeansTrainPercent), s)
	require.False(t, strings.Contains(s, IndexAlgoParamMaxIndexCapacity), s)
}

func newPrefixKeyPart(colName string, length int) *tree.KeyPart {
	return &tree.KeyPart{
		ColName: tree.NewUnresolvedColName(colName),
		Length:  length,
	}
}

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

func TestIndexPrefixLengthsFromParamsWithError(t *testing.T) {
	tests := []struct {
		name      string
		params    string
		want      map[string]int
		wantError bool
	}{
		{
			name:   "empty params",
			params: "",
		},
		{
			name:   "no prefix lengths",
			params: `{"lists":"1"}`,
		},
		{
			name:   "valid prefix lengths",
			params: `{"prefix_lengths":"b:20,t:10"}`,
			want: map[string]int{
				"b": 20,
				"t": 10,
			},
		},
		{
			name:      "invalid json",
			params:    "{bad json",
			wantError: true,
		},
		{
			name:      "missing separator",
			params:    `{"prefix_lengths":"t"}`,
			wantError: true,
		},
		{
			name:      "non numeric length",
			params:    `{"prefix_lengths":"t:abc"}`,
			wantError: true,
		},
		{
			name:      "non positive length",
			params:    `{"prefix_lengths":"t:0"}`,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := IndexPrefixLengthsFromParamsWithError(tt.params)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIndexPrefixLengthsToString(t *testing.T) {
	tests := []struct {
		name     string
		keyParts []*tree.KeyPart
		want     string
	}{
		{
			name:     "nil key parts",
			keyParts: nil,
			want:     "",
		},
		{
			name: "all without length",
			keyParts: []*tree.KeyPart{
				newPrefixKeyPart("a", 0),
				newPrefixKeyPart("b", 0),
			},
			want: "",
		},
		{
			name: "single prefix",
			keyParts: []*tree.KeyPart{
				newPrefixKeyPart("t", 10),
			},
			want: "t:10",
		},
		{
			name: "sorted by column name",
			keyParts: []*tree.KeyPart{
				newPrefixKeyPart("b", 20),
				newPrefixKeyPart("a", 10),
			},
			want: "a:10,b:20",
		},
		{
			name: "skip nil and zero length entries",
			keyParts: []*tree.KeyPart{
				{ColName: nil, Length: 5},
				newPrefixKeyPart("a", 0),
				newPrefixKeyPart("b", 7),
			},
			want: "b:7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IndexPrefixLengthsToString(tt.keyParts))
		})
	}
}

func TestAddIndexPrefixLengthsToParams(t *testing.T) {
	t.Run("no prefix returns input unchanged", func(t *testing.T) {
		got, err := AddIndexPrefixLengthsToParams(`{"lists":"1"}`, []*tree.KeyPart{
			newPrefixKeyPart("a", 0),
		})
		require.NoError(t, err)
		require.Equal(t, `{"lists":"1"}`, got)
	})

	t.Run("empty params with prefix", func(t *testing.T) {
		got, err := AddIndexPrefixLengthsToParams("", []*tree.KeyPart{
			newPrefixKeyPart("t", 10),
		})
		require.NoError(t, err)

		prefixLengths := IndexPrefixLengthsFromParams(got)
		require.Equal(t, map[string]int{"t": 10}, prefixLengths)
	})

	t.Run("merge into existing params", func(t *testing.T) {
		got, err := AddIndexPrefixLengthsToParams(`{"lists":"1"}`, []*tree.KeyPart{
			newPrefixKeyPart("t", 10),
		})
		require.NoError(t, err)

		params, err := IndexParamsStringToMap(got)
		require.NoError(t, err)
		require.Equal(t, "1", params[IndexAlgoParamLists])
		require.Equal(t, "t:10", params[IndexAlgoParamPrefixLengths])
	})

	t.Run("invalid existing params", func(t *testing.T) {
		_, err := AddIndexPrefixLengthsToParams("{bad json", []*tree.KeyPart{
			newPrefixKeyPart("t", 10),
		})
		require.Error(t, err)
	})

	t.Run("round trip", func(t *testing.T) {
		keyParts := []*tree.KeyPart{
			newPrefixKeyPart("b", 20),
			newPrefixKeyPart("a", 10),
		}
		got, err := AddIndexPrefixLengthsToParams("", keyParts)
		require.NoError(t, err)
		require.Equal(t, map[string]int{"a": 10, "b": 20}, IndexPrefixLengthsFromParams(got))
	})
}

func TestIndexPrefixLengthsFromParams(t *testing.T) {
	require.Equal(t, map[string]int{"t": 10}, IndexPrefixLengthsFromParams(`{"prefix_lengths":"t:10"}`))
	require.Nil(t, IndexPrefixLengthsFromParams(""))
	// invalid params return nil instead of panicking.
	require.Nil(t, IndexPrefixLengthsFromParams("{bad json"))
	require.Nil(t, IndexPrefixLengthsFromParams(`{"prefix_lengths":"t:0"}`))
}

func unresolvedName(name string) *tree.UnresolvedName {
	return tree.NewUnresolvedName(tree.NewCStr(name, 1))
}

func TestIndexParamsToJsonString_RejectsPluginIvfFlatPath(t *testing.T) {
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
	require.Error(t, err)
	require.Empty(t, params)
	require.Contains(t, err.Error(), "invalid index alogorithm type")
}

func TestIndexParamsToStringList_DoesNotRenderIncludeColumns(t *testing.T) {
	paramList, err := IndexParamsToStringList(`{"lists":"10","op_type":"vector_l2_ops","include_columns":"[\"title\",\"category\"]"}`)
	require.NoError(t, err)
	require.Contains(t, paramList, "lists = 10")
	require.Contains(t, paramList, "op_type 'vector_l2_ops'")
	require.NotContains(t, paramList, "INCLUDE")
	require.NotContains(t, paramList, "title")
}

func TestParseIncludeColumnsValue_BackwardCompatible(t *testing.T) {
	cols, err := ParseIncludeColumnsValue(`["title","category"]`)
	require.NoError(t, err)
	require.Equal(t, []string{"title", "category"}, cols)

	cols, err = ParseIncludeColumnsValue("title,category")
	require.NoError(t, err)
	require.Equal(t, []string{"title", "category"}, cols)
}
