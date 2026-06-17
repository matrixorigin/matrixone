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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

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
