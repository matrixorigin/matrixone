// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planreader

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type scriptedSearcher struct {
	chunks []Chunk
	err    error
	nexts  int
	closes int
}

func (s *scriptedSearcher) Next(context.Context) (Chunk, bool, error) {
	if s.err != nil {
		return Chunk{}, false, s.err
	}
	s.nexts++
	chunk := s.chunks[0]
	s.chunks = s.chunks[1:]
	return chunk, len(s.chunks) > 0, nil
}

func (s *scriptedSearcher) Close() error {
	s.closes++
	return nil
}

func newOut(attrs []string, typs []types.Type) *batch.Batch {
	out := batch.NewWithSize(len(attrs))
	out.Attrs = attrs
	for i, typ := range typs {
		out.Vecs[i] = vector.NewVec(typ)
	}
	return out
}

// readAll drains r and returns the key and score columns of every row.
func readAll(t *testing.T, r *Reader, attrs []string, typs []types.Type, mp *mpool.MPool) ([]int64, []float64) {
	t.Helper()
	out := newOut(attrs, typs)
	defer out.Clean(mp)
	var keys []int64
	var scores []float64
	for {
		done, err := r.Read(context.Background(), attrs, nil, mp, out)
		require.NoError(t, err)
		if done {
			return keys, scores
		}
		require.Positive(t, out.RowCount())
		require.LessOrEqual(t, out.RowCount(), maxBatchRows)
		keys = append(keys, vector.MustFixedColNoTypeCheck[int64](out.Vecs[0])...)
		scores = append(scores, vector.MustFixedColNoTypeCheck[float64](out.Vecs[1])...)
	}
}

func TestReaderEmitsChunksInRankOrder(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	big := make([]int64, maxBatchRows+3)
	bigScores := make([]float64, len(big))
	for i := range big {
		big[i] = int64(i + 10)
		bigScores[i] = float64(i)
	}
	s := &scriptedSearcher{chunks: []Chunk{
		{Keys: []int64{1, 2}, Scores: []float64{0.5, 0.75}},
		{},
		{Keys: big, Scores: bigScores},
	}}
	r := New(s)
	attrs := []string{KeyColumn, ScoreColumn}
	typs := []types.Type{types.T_int64.ToType(), types.T_float64.ToType()}
	keys, scores := readAll(t, r, attrs, typs, mp)
	require.Equal(t, append([]int64{1, 2}, big...), keys)
	require.Equal(t, append([]float64{0.5, 0.75}, bigScores...), scores)
	require.Equal(t, 3, s.nexts)
	require.NoError(t, r.Close())
	require.NoError(t, r.Close())
	require.Equal(t, 1, s.closes)
	done, err := r.Read(context.Background(), attrs, nil, mp, newOut(attrs, typs))
	require.NoError(t, err)
	require.True(t, done)
}

func TestReaderEmitsAnyKeysAndIncludedColumns(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	s := &scriptedSearcher{chunks: []Chunk{{
		Keys:         []any{int32(7), int32(8)},
		Scores:       []float64{1, 2},
		Include:      map[string][]any{"tag": {int64(3), int64(0)}},
		IncludeNulls: map[string][]bool{"tag": {false, true}},
	}}}
	r := New(s)
	attrs := []string{ScoreColumn, IncludePrefix + "tag", KeyColumn}
	out := newOut(attrs, []types.Type{types.T_float64.ToType(), types.T_int64.ToType(), types.T_int32.ToType()})
	defer out.Clean(mp)
	done, err := r.Read(context.Background(), attrs, nil, mp, out)
	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, 2, out.RowCount())
	require.Equal(t, []float64{1, 2}, vector.MustFixedColNoTypeCheck[float64](out.Vecs[0]))
	require.Equal(t, int64(3), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[1], 0))
	require.True(t, out.Vecs[1].IsNull(1))
	require.Equal(t, []int32{7, 8}, vector.MustFixedColNoTypeCheck[int32](out.Vecs[2]))
	done, err = r.Read(context.Background(), attrs, nil, mp, out)
	require.NoError(t, err)
	require.True(t, done)
	require.NoError(t, r.Close())
}

func TestReaderRejectsMalformedResults(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	attrs := []string{KeyColumn, ScoreColumn}
	typs := []types.Type{types.T_int64.ToType(), types.T_float64.ToType()}
	read := func(s Searcher, attrs []string, typs []types.Type) error {
		out := newOut(attrs, typs)
		defer out.Clean(mp)
		r := New(s)
		defer r.Close()
		_, err := r.Read(context.Background(), attrs, nil, mp, out)
		return err
	}
	require.ErrorContains(t, read(&scriptedSearcher{chunks: []Chunk{{Keys: []int64{1}}}}, attrs, typs),
		"keys and scores are not aligned")
	require.ErrorContains(t, read(&scriptedSearcher{chunks: []Chunk{{Keys: []int64{1}, Scores: []float64{1}}}},
		[]string{"distance"}, []types.Type{types.T_float64.ToType()}), `unknown index search output "distance"`)
	require.ErrorContains(t, read(&scriptedSearcher{chunks: []Chunk{{Keys: []int64{1}, Scores: []float64{1}}}},
		[]string{IncludePrefix + "tag"}, []types.Type{types.T_int64.ToType()}), `include output "tag" is not aligned`)
	searchErr := errors.New("search failed")
	require.ErrorIs(t, read(&scriptedSearcher{err: searchErr}, attrs, typs), searchErr)
}

func TestReaderStopsOnCancellationAndEmpty(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	attrs := []string{KeyColumn, ScoreColumn}
	typs := []types.Type{types.T_int64.ToType(), types.T_float64.ToType()}
	out := newOut(attrs, typs)
	defer out.Clean(mp)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := &scriptedSearcher{chunks: []Chunk{{Keys: []int64{1}, Scores: []float64{1}}}}
	r := New(s)
	_, err := r.Read(ctx, attrs, nil, mp, out)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, s.nexts)
	require.NoError(t, r.Close())
	require.Equal(t, 1, s.closes)

	empty := Empty()
	done, err := empty.Read(context.Background(), attrs, nil, mp, out)
	require.NoError(t, err)
	require.True(t, done)
	require.NoError(t, empty.Close())
	require.Nil(t, empty.GetOrderBy())
}
