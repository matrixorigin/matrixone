// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package fulltext2

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

type rankedStreamRow struct {
	key  string
	bits uint32
}

func streamRows(o *vectorindex.SearchOutput) []rankedStreamRow {
	rows := make([]rankedStreamRow, o.Keys.N)
	data := o.Keys.Data
	for i := range rows {
		n := int(binary.LittleEndian.Uint32(data))
		data = data[4:]
		rows[i] = rankedStreamRow{string(data[:n]), math.Float32bits(o.Dists[i])}
		data = data[n:]
	}
	return rows
}

func TestConditionalStreamTopKExhaustiveOracle(t *testing.T) {
	segs := make([]*Segment, 0, 2)
	for si, keys := range [][]string{{"z", "aa", "a", "é", "A", "a "}, {"\x00", "a\x00", "中", "b", "aa", "A ", "É", "e"}} {
		b := NewBuilder(fmt.Sprint(si), int32(types.T_varchar))
		for i, key := range keys {
			feed(t, b, key, "alpha", "beta")
			if i%2 == 0 {
				feed(t, b, key, "alpha")
			}
		}
		feed(t, b, fmt.Sprintf("padding%d", si), "noise")
		seg := loadedSeg(t, b)
		t.Cleanup(func() { require.NoError(t, seg.dict.Close()) })
		seg.Recency = int64(si)
		segs = append(segs, seg)
	}
	idx := NewIndex(segs, map[any]int64{"z": 2})
	for _, pattern := range []string{"alpha beta", "alpha", "absent", ""} {
		var oracle []rankedStreamRow
		err := idx.StreamBagOfWords([]byte(pattern), ParserDefault, BM25, nil, false, func(o *vectorindex.SearchOutput) error {
			defer PutColumnBuffer(o.Keys)
			for _, row := range streamRows(o) {
				if math.Float32frombits(row.bits) > 0 {
					oracle = append(oracle, row)
				}
			}
			return nil
		})
		require.NoError(t, err)
		if len(oracle) > 0 {
			byKey := make(map[string]uint32, len(oracle))
			for _, row := range oracle {
				byKey[row.key] = row.bits
			}
			require.Equal(t, byKey["a"], byKey["A"], "mixed-case keys must tie on score")
			require.Equal(t, byKey["a "], byKey["A "], "trailing-space keys must tie on score")
			require.Equal(t, byKey["é"], byKey["e"], "non-ASCII and ASCII keys must tie on score")
		}
		oracle = sqlOrderedStreamRows(t, oracle)
		for _, k := range []int{1, 3, 8, 9, MaxConditionalStreamTopK} {
			var got []rankedStreamRow
			err := idx.streamBagOfWordsTopK(context.Background(), []byte(pattern), ParserDefault, BM25, nil, k, func(o *vectorindex.SearchOutput) error {
				defer PutColumnBuffer(o.Keys)
				got = append(got, streamRows(o)...)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, oracle[:min(k, len(oracle))], got, "pattern=%q k=%d", pattern, k)
		}
	}
}

// sqlOrderedStreamRows uses the ORDER BY comparator used by Top/MergeTop,
// independently of the collector's byte comparator and heap implementation.
func sqlOrderedStreamRows(t *testing.T, rows []rankedStreamRow) []rankedStreamRow {
	t.Helper()
	if len(rows) == 0 {
		return rows
	}
	mp := mpool.MustNewZero()
	pkType := types.New(types.T_varchar, 128, 0)
	require.Equal(t, types.CharsetUTF8, pkType.Charset)
	require.Zero(t, pkType.CollationVersion)
	pkVec := vector.NewVec(pkType)
	defer pkVec.Free(mp)
	scoreType := types.T_float32.ToType()
	scoreVec := vector.NewVec(scoreType)
	defer scoreVec.Free(mp)
	order := make([]int, len(rows))
	for i, row := range rows {
		require.NoError(t, vector.AppendBytes(pkVec, []byte(row.key), false, mp))
		require.NoError(t, vector.AppendFixed(scoreVec, math.Float32frombits(row.bits), false, mp))
		order[i] = i
	}
	scoreCmp := compare.NewOrder(scoreType, true, false)
	pkCmp := compare.NewOrder(pkType, false, false)
	scoreCmp.Set(0, scoreVec)
	scoreCmp.Set(1, scoreVec)
	pkCmp.Set(0, pkVec)
	pkCmp.Set(1, pkVec)
	sort.Slice(order, func(i, j int) bool {
		left, right := int64(order[i]), int64(order[j])
		if c := scoreCmp.Compare(0, 1, left, right); c != 0 {
			return c < 0
		}
		return pkCmp.Compare(0, 1, left, right) < 0
	})
	result := make([]rankedStreamRow, len(rows))
	for i, row := range order {
		result[i] = rows[row]
	}
	return result
}

func TestConditionalStreamTopKHeapScoreDomain(t *testing.T) {
	keys := []any{"z", "a", "aa", "\x00", "é", "x", "y", "nan"}
	seg := &Segment{pks: keys, N: int64(len(keys)), PkType: int32(types.T_varchar)}
	scores := []float32{1, 1, 1, math.Float32frombits(0x3f800001), 2, 0, -1, float32(math.NaN())}
	var want []rankedStreamRow
	for i, s := range scores {
		if s > 0 {
			want = append(want, rankedStreamRow{keys[i].(string), math.Float32bits(s)})
		}
	}
	sort.Slice(want, func(i, j int) bool {
		a, b := math.Float32frombits(want[i].bits), math.Float32frombits(want[j].bits)
		if a != b {
			return a > b
		}
		return want[i].key < want[j].key
	})
	for _, k := range []int{1, 3, 5, 8} {
		h := &streamTopK{k: k}
		for i, s := range scores {
			h.push(streamRef{seg: seg, ord: int64(i), score: s})
			require.LessOrEqual(t, len(h.refs), k)
		}
		sort.Slice(h.refs, func(i, j int) bool { return h.better(h.refs[i], h.refs[j]) })
		var got []rankedStreamRow
		for _, r := range h.refs {
			got = append(got, rankedStreamRow{keys[r.ord].(string), math.Float32bits(r.score)})
		}
		require.NoError(t, h.err)
		require.Equal(t, want[:min(k, len(want))], got)
	}
}

type cancelVisitContext struct {
	context.Context
	checks int
}

func (c *cancelVisitContext) Err() error {
	c.checks++
	if c.checks >= 3 {
		return context.Canceled
	}
	return nil
}

func TestConditionalStreamTopKCancelAndEmitFailure(t *testing.T) {
	b := NewBuilder("cancel", int32(types.T_varchar))
	for i := 0; i < streamBatch+1; i++ {
		feed(t, b, fmt.Sprint(i), "alpha")
	}
	feed(t, b, "padding", "noise")
	seg := loadedSeg(t, b)
	t.Cleanup(func() { require.NoError(t, seg.dict.Close()) })
	idx := NewIndex([]*Segment{seg}, nil)
	ctx := &cancelVisitContext{Context: context.Background()}
	emitted := 0
	err := idx.streamBagOfWordsTopK(ctx, []byte("alpha"), ParserDefault, BM25, &prefilter{scoreRange: &ScoreRange{HasMin: true, Min: math.MaxFloat32}}, 1, func(o *vectorindex.SearchOutput) error { emitted++; PutColumnBuffer(o.Keys); return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, emitted)
	require.Equal(t, 3, ctx.checks, "cancellation must inspect visited documents even when scores are rejected")
	emitErr := errors.New("consumer stopped")
	var owned *vectorindex.ColumnBuffer
	err = idx.streamBagOfWordsTopK(context.Background(), []byte("alpha"), ParserDefault, BM25, nil, 1, func(o *vectorindex.SearchOutput) error { owned = o.Keys; return emitErr })
	require.ErrorIs(t, err, emitErr)
	require.NotNil(t, owned)
}

func TestConditionalStreamSinkOwnsUnflushedErrorBuffer(t *testing.T) {
	b := NewBuilder("error", int32(types.T_varchar))
	feed(t, b, "key", "alpha")
	seg := loadedSeg(t, b)
	t.Cleanup(func() { require.NoError(t, seg.dict.Close()) })
	idx := NewIndex([]*Segment{seg}, nil)
	emitted := false
	sink := newStreamSink(idx, true, nil, func(*vectorindex.SearchOutput) error { emitted = true; return nil })
	// A requested INCLUDE column absent from this segment fails before flush.
	sink.includeTypes = []int32{int32(types.T_int64)}
	sink.pushPk(seg, 0, 1)
	require.Error(t, sink.err)
	require.NotNil(t, sink.keys)
	require.False(t, emitted)
	sink.close()
	require.Nil(t, sink.keys)
	sink.close()
}
