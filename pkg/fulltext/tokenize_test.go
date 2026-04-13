// Copyright 2025 Matrix Origin
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

package fulltext

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestTokenizeIndexValuesDefault(t *testing.T) {
	tokens, err := TokenizeIndexValues(
		FullTextParserParam{},
		[]IndexValue{
			{Text: "matrix origin", Type: types.T_text},
			{Text: "vector search", Type: types.T_varchar},
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t,
		[]IndexToken{
			{Word: "matrix", Pos: 0},
			{Word: "origin", Pos: 7},
			{Word: "vector", Pos: 14},
			{Word: "search", Pos: 21},
		},
		tokens,
	)
}

func TestTokenizeIndexValuesJSON(t *testing.T) {
	tokens, err := TokenizeIndexValues(
		FullTextParserParam{Parser: "json"},
		[]IndexValue{{Text: `["matrix origin","fts"]`, Type: types.T_varchar}},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t,
		[]IndexToken{
			{Word: "matrix", Pos: 0},
			{Word: "origin", Pos: 7},
			{Word: "fts", Pos: 13},
		},
		tokens,
	)
}

func TestTokenizeIndexValuesJSONValue(t *testing.T) {
	tokens, err := TokenizeIndexValues(
		FullTextParserParam{Parser: "json_value"},
		[]IndexValue{{Text: `["matrix origin","fts"]`, Type: types.T_varchar}},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t,
		[]IndexToken{
			{Word: "matrix origin", Pos: 0},
			{Word: "fts", Pos: 13},
		},
		tokens,
	)
}
