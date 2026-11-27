// Copyright 2026 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestRenameIndexColumnPreservesPrefixMetadata(t *testing.T) {
	idx := &planpb.IndexDef{
		IndexName:       "idx",
		Parts:           []string{"a", "b", catalog.CreateAlias("a")},
		IndexAlgoParams: `{"prefix_lengths":"a:4,b:2","option":"keep","session_vars":{"cfg":{"value":7}}}`,
	}
	table := &TableDef{Indexes: []*planpb.IndexDef{idx}}
	changed, updated, err := renameIndexColumnParts(table, "a", "x:y")
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, []*planpb.IndexDef{idx}, updated)
	require.Equal(t, []string{"x:y", "b", catalog.CreateAlias("x:y")}, idx.Parts)
	require.Equal(t, map[string]int{"x:y": 4, "b": 2}, catalog.IndexPrefixLengthsFromParams(idx.IndexAlgoParams))
	params, err := catalog.IndexParamsStringToMap(idx.IndexAlgoParams)
	require.NoError(t, err)
	require.Equal(t, "keep", params["option"])
	require.Contains(t, params, catalog.IndexAlgoParamPrefixLengthsV2)
	session, err := catalog.IndexParamsSessionVars(idx.IndexAlgoParams)
	require.NoError(t, err)
	require.JSONEq(t, `{"cfg":{"value":7}}`, string(session))

	// A second rename switches back from delimiter-safe to ordinary encoding.
	changed, updated, err = renameIndexColumnParts(table, "x:y", "x")
	require.NoError(t, err)
	require.True(t, changed)
	require.Len(t, updated, 1)
	require.Equal(t, map[string]int{"x": 4, "b": 2}, catalog.IndexPrefixLengthsFromParams(idx.IndexAlgoParams))
	params, err = catalog.IndexParamsStringToMap(idx.IndexAlgoParams)
	require.NoError(t, err)
	require.Contains(t, params, catalog.IndexAlgoParamPrefixLengths)
	require.NotContains(t, params, catalog.IndexAlgoParamPrefixLengthsV2)

	before := idx.IndexAlgoParams
	changed, updated, err = renameIndexColumnParts(table, "x", "x")
	require.NoError(t, err)
	require.True(t, changed)
	require.Empty(t, updated)
	require.Equal(t, before, idx.IndexAlgoParams)
}

func TestRenameIndexColumnRejectsInvalidPrefixMetadata(t *testing.T) {
	for _, params := range []string{
		`{"prefix_lengths":"a:bad"}`,
		`{"prefix_lengths":"a:4,x:2"}`,
	} {
		t.Run(params, func(t *testing.T) {
			table := &TableDef{Indexes: []*planpb.IndexDef{{Parts: []string{"a"}, IndexAlgoParams: params}}}
			_, _, err := renameIndexColumnParts(table, "a", "x")
			require.Error(t, err)
		})
	}
}
