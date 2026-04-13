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
	"bytes"
	"encoding/binary"
	"sort"
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
	require.Equal(t, int64(2), segment.DocCount)
	require.Equal(t, int64(5), segment.TokenSum)

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
	require.Equal(t, segment.DocCount, decoded.DocCount)
	require.Equal(t, segment.TokenSum, decoded.TokenSum)
}

func TestSidecarPathDeterministic(t *testing.T) {
	path1 := SidecarPath("obj_001", "ft_idx")
	path2 := SidecarPath("obj_001", "ft_idx")
	path3 := SidecarPath("obj_001", "ft_idx_other")

	require.Equal(t, path1, path2)
	require.NotEqual(t, path1, path3)
	require.Contains(t, path1, "obj_001.fts.")
}

func TestSegmentUnmarshalLegacyRebuildsStats(t *testing.T) {
	builder := NewBuilder(fulltext.FullTextParserParam{}, nil)
	require.NoError(t, builder.Add(Document{
		Block: 1,
		Row:   1,
		PK:    []byte("pk-1"),
		Values: []fulltext.IndexValue{
			{Text: "matrix origin", Type: types.T_text},
		},
	}))
	require.NoError(t, builder.Add(Document{
		Block: 1,
		Row:   2,
		PK:    []byte("pk-2"),
		Values: []fulltext.IndexValue{
			{Text: "native search", Type: types.T_text},
		},
	}))
	segment := builder.Build()

	data, err := marshalLegacyV1Segment(segment)
	require.NoError(t, err)

	decoded, err := UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, segment.DocCount, decoded.DocCount)
	require.Equal(t, segment.TokenSum, decoded.TokenSum)
	require.Equal(t, segment.Lookup("matrix"), decoded.Lookup("matrix"))
}

func TestSegmentUnmarshalLegacyV2Stats(t *testing.T) {
	builder := NewBuilder(fulltext.FullTextParserParam{}, nil)
	require.NoError(t, builder.Add(Document{
		Block: 2,
		Row:   1,
		PK:    []byte("pk-1"),
		Values: []fulltext.IndexValue{
			{Text: "matrix origin", Type: types.T_text},
		},
	}))
	require.NoError(t, builder.Add(Document{
		Block: 2,
		Row:   2,
		PK:    []byte("pk-2"),
		Values: []fulltext.IndexValue{
			{Text: "native search", Type: types.T_text},
		},
	}))
	segment := builder.Build()

	data, err := marshalLegacyV2Segment(segment)
	require.NoError(t, err)

	decoded, err := UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, segment.DocCount, decoded.DocCount)
	require.Equal(t, segment.TokenSum, decoded.TokenSum)
	require.Equal(t, segment.Lookup("native"), decoded.Lookup("native"))
}

func marshalLegacyV1Segment(s *Segment) ([]byte, error) {
	return marshalLegacySegmentWithMagic(s, segmentMagicV1, false)
}

func marshalLegacyV2Segment(s *Segment) ([]byte, error) {
	return marshalLegacySegmentWithMagic(s, segmentMagicV2, true)
}

func marshalLegacySegmentWithMagic(s *Segment, magic [8]byte, includeStats bool) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := buf.Write(magic[:]); err != nil {
		return nil, err
	}
	if includeStats {
		if err := binary.Write(&buf, binary.LittleEndian, s.DocCount); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, s.TokenSum); err != nil {
			return nil, err
		}
	}
	terms := make([]string, 0, len(s.Terms))
	for term := range s.Terms {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(terms))); err != nil {
		return nil, err
	}
	for _, term := range terms {
		postings := append([]Posting(nil), s.Terms[term]...)
		sortPostings(postings)
		if err := writeBytes(&buf, []byte(term)); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(postings))); err != nil {
			return nil, err
		}
		for _, posting := range postings {
			if err := binary.Write(&buf, binary.LittleEndian, posting.Ref.Block); err != nil {
				return nil, err
			}
			if err := binary.Write(&buf, binary.LittleEndian, posting.Ref.Row); err != nil {
				return nil, err
			}
			if err := writeBytes(&buf, posting.Ref.PK); err != nil {
				return nil, err
			}
			if err := binary.Write(&buf, binary.LittleEndian, posting.DocLen); err != nil {
				return nil, err
			}
			if err := binary.Write(&buf, binary.LittleEndian, uint32(len(posting.Positions))); err != nil {
				return nil, err
			}
			for _, pos := range posting.Positions {
				if err := binary.Write(&buf, binary.LittleEndian, pos); err != nil {
					return nil, err
				}
			}
		}
	}
	return buf.Bytes(), nil
}
