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

package native

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/stretchr/testify/require"
)

func TestBuilderLookupAndPhrase(t *testing.T) {
	builder := NewBuilder(fulltext.FullTextParserParam{}, nil)
	require.NoError(t, builder.Add(Document{
		Block: 1,
		Row:   10,
		PK:    []byte("pk-1"),
		Values: []fulltext.IndexValue{
			{Text: "matrix origin", Type: types.T_text},
		},
	}))
	require.NoError(t, builder.Add(Document{
		Block: 1,
		Row:   11,
		PK:    []byte("pk-2"),
		Values: []fulltext.IndexValue{
			{Text: "matrix for origin", Type: types.T_text},
		},
	}))

	segment := builder.Build()

	matrix := segment.Lookup("matrix")
	require.Len(t, matrix, 2)
	require.Equal(t, int32(2), matrix[0].DocLen)
	require.Equal(t, []int32{0}, matrix[0].Positions)

	matches := segment.SearchPhrase([]PhraseToken{
		{Word: "matrix", Pos: 0},
		{Word: "origin", Pos: 7},
	})
	require.Len(t, matches, 1)
	require.Equal(t, uint32(10), matches[0].Ref.Row)
	require.Equal(t, []int32{7}, matches[0].Positions["origin"])
}

func TestSegmentMarshalRoundTrip(t *testing.T) {
	builder := NewBuilder(fulltext.FullTextParserParam{Parser: "json_value"}, nil)
	require.NoError(t, builder.Add(Document{
		Block: 2,
		Row:   7,
		PK:    []byte("pk-json"),
		Values: []fulltext.IndexValue{
			{Text: `{"title":"matrix origin","tag":"fts"}`, Type: types.T_varchar},
		},
	}))
	segment := builder.Build()

	data, err := segment.MarshalBinary()
	require.NoError(t, err)

	decoded, err := UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, segment.Lookup("matrix origin"), decoded.Lookup("matrix origin"))
	require.Equal(t, segment.Lookup("fts"), decoded.Lookup("fts"))
}

func TestSidecarPathDeterministic(t *testing.T) {
	path1 := SidecarPath("obj_001", "ft_idx")
	path2 := SidecarPath("obj_001", "ft_idx")
	path3 := SidecarPath("obj_001", "ft_idx_other")

	require.Equal(t, path1, path2)
	require.NotEqual(t, path1, path3)
	require.Contains(t, path1, "obj_001.fts.")
}
