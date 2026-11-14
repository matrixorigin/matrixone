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

package fulltext2

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestMoIndexStatsFulltext2LiveDocumentFrequencies(t *testing.T) {
	for _, loaded := range []bool{false, true} {
		name := "build"
		if loaded {
			name = "loaded"
		}
		t.Run(name, func(t *testing.T) {
			base, err := BuildSegmentFromTokenized("base", int32(types.T_int64), []TokenizedDoc{
				{Pk: int64(1), Terms: []string{"apple", "apple", "banana"}},
				{Pk: int64(2), Terms: []string{"obsolete"}},
				{Pk: int64(3), Terms: []string{"deleted"}},
			})
			require.NoError(t, err)
			tail, err := BuildSegmentFromTokenized("tail", int32(types.T_int64), []TokenizedDoc{
				{Pk: int64(2), Terms: []string{"apple", "pear"}},
				{Pk: int64(4), Terms: []string{"banana"}},
			})
			require.NoError(t, err)
			segments := []*Segment{base, tail}
			if loaded {
				for i, seg := range segments {
					data, err := seg.Serialize()
					require.NoError(t, err)
					segments[i], err = Deserialize(seg.Id, bytes.NewReader(data))
					require.NoError(t, err)
				}
			}
			segments[1].Recency = 1
			idx := NewIndex(segments, map[any]int64{normalizeKey(int64(3)): 1})
			defer idx.Free()
			stats, err := idx.Stats(t.Context())
			require.NoError(t, err)
			require.Equal(t, int64(3), stats.Documents)
			require.Equal(t, int64(3), stats.Terms)
			require.Equal(t, int64(1), stats.MinDF)
			require.Equal(t, int64(2), stats.MaxDF)
			require.InDelta(t, 5.0/3, stats.MeanDF, 1e-12)
			// The scan must not change query liveness or mutate the generation.
			require.Equal(t, []any{int64(2)}, iquery(t, idx, "pear"))
			require.Empty(t, iquery(t, idx, "obsolete"))
			require.Empty(t, iquery(t, idx, "deleted"))
		})
	}
}

func TestMoIndexStatsFulltext2EmptyAndCancellation(t *testing.T) {
	idx := NewIndex(nil, nil)
	stats, err := idx.Stats(t.Context())
	require.NoError(t, err)
	require.Zero(t, stats.Documents)
	require.Zero(t, stats.Terms)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = idx.Stats(ctx)
	require.ErrorIs(t, err, context.Canceled)
}
