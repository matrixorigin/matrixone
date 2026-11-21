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
	"container/heap"
	"context"
	"errors"
	"iter"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

type statsTerm struct {
	term    string
	df      int64
	segment int
}

type statsTermHeap []statsTerm

func (h statsTermHeap) Len() int           { return len(h) }
func (h statsTermHeap) Less(i, j int) bool { return h[i].term < h[j].term }
func (h statsTermHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *statsTermHeap) Push(x any)        { *h = append(*h, x.(statsTerm)) }
func (h *statsTermHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

var errStatsIterationStopped = moerr.NewInternalErrorNoCtx("fulltext stats iteration stopped")

// diagnosticTerms streams one segment's vocabulary with live DF. The iterator
// keeps no vocabulary-sized slice; stopping it also closes the FST iterator.
func (idx *Index) diagnosticTerms(ctx context.Context, si int) iter.Seq2[statsTerm, error] {
	return func(yield func(statsTerm, error) bool) {
		seg := idx.segments[si]
		visit := func(word string, offset uint64) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var p *termPostings
			var ok bool
			if seg.dict != nil && idx.liveOrd[si] == nil {
				df, found := seg.termDFAt(offset)
				if !found {
					return moerr.NewInternalError(ctx, "fulltext stats: invalid term frequency header")
				}
				if df > 0 && !yield(statsTerm{term: word, df: int64(df), segment: si}, nil) {
					return errStatsIterationStopped
				}
				return nil
			}
			if seg.dict != nil {
				p, ok = seg.decodeTermEntry(int(offset))
			} else {
				p, ok = seg.Lookup(word)
			}
			if !ok {
				return moerr.NewInternalError(ctx, "fulltext stats: missing term posting")
			}
			df := int64(idx.liveTermDF(si, p))
			if df > 0 && !yield(statsTerm{term: word, df: df, segment: si}, nil) {
				return errStatsIterationStopped
			}
			return nil
		}
		var err error
		if seg.dict != nil {
			err = seg.dict.forEachEntry(visit)
		} else {
			for _, word := range seg.sortedTerms {
				if err = visit(word, 0); err != nil {
					break
				}
			}
		}
		if err != nil && !errors.Is(err, errStatsIterationStopped) {
			yield(statsTerm{}, err)
		}
	}
}

// Stats summarizes the live vocabulary, merging sorted segment dictionaries in
// O(segment count) extra memory. Superseded and deleted document copies do not
// contribute. No model or segment is changed and no compaction is performed.
func (idx *Index) Stats(ctx context.Context) (indexstats.FullTextSummary, error) {
	result := indexstats.FullTextSummary{Documents: idx.globalN}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if result.Documents == 0 {
		return result, nil
	}
	next := make([]func() (statsTerm, error, bool), len(idx.segments))
	heads := make(statsTermHeap, 0, len(idx.segments))
	for si := range idx.segments {
		pull, stop := iter.Pull2(idx.diagnosticTerms(ctx, si))
		defer stop()
		next[si] = pull
		item, err, ok := pull()
		if err != nil {
			return result, err
		}
		if ok {
			heap.Push(&heads, item)
		}
	}
	for heads.Len() > 0 {
		term := heads[0].term
		var df int64
		for heads.Len() > 0 && heads[0].term == term {
			item := heap.Pop(&heads).(statsTerm)
			if math.MaxInt64-df < item.df {
				return result, moerr.NewInvalidInput(ctx, "fulltext stats: document frequency overflow")
			}
			df += item.df
			item, err, ok := next[item.segment]()
			if err != nil {
				return result, err
			}
			if ok {
				heap.Push(&heads, item)
			}
		}
		if df > result.Documents {
			return result, moerr.NewInvalidInput(ctx, "fulltext stats: document frequency exceeds live documents")
		}
		if result.Terms == 0 || df < result.MinDF {
			result.MinDF = df
		}
		result.MaxDF = max(result.MaxDF, df)
		result.Terms++
		result.MeanDF += (float64(df) - result.MeanDF) / float64(result.Terms)
	}
	return result, nil
}

// Stats reads an already-loaded immutable generation. Its owner must protect
// the generation from eviction for the duration of this call.
func (s *Fulltext2Search) Stats(ctx context.Context) (indexstats.FullTextSummary, error) {
	if s.idx == nil {
		return indexstats.FullTextSummary{}, moerr.NewInternalError(ctx, "fulltext stats: index is not loaded")
	}
	return s.idx.Stats(ctx)
}

// ReadStats shares a matching warm generation, or loads an exact snapshot into
// the normal cache. Both paths use its read lock, admission and byte governor.
func ReadStats(ctx context.Context, sqlproc *sqlexec.SqlProcess, cfg TableConfig) (result indexstats.FullTextSummary, err error) {
	ts, tail, err := LoadGeneration(sqlproc, cfg)
	if err != nil {
		return result, err
	}
	veccache.Cache.Once()
	matched := false
	_, err = veccache.Cache.ReadIfLoaded(cfg.IndexTable, func(algo veccache.VectorIndexSearchIf) error {
		reader, ok := algo.(*Fulltext2Search)
		if !ok {
			return moerr.NewInternalError(ctx, "fulltext stats: unexpected cached index type")
		}
		if !reader.genValid || reader.loadedTs != ts || reader.loadedTail != tail {
			return nil
		}
		matched = true
		result, err = reader.Stats(ctx)
		return err
	})
	if err != nil || matched {
		return result, err
	}
	if sqlproc.BuildSnapshotTS() == 0 {
		return result, moerr.NewInternalError(ctx, "fulltext stats: read snapshot is unavailable")
	}
	key := veccache.GenerationKey(cfg.IndexTable, ts, tail)
	err = veccache.Cache.Read(sqlproc, key, NewFulltext2Search(cfg), func(algo veccache.VectorIndexSearchIf) error {
		reader, ok := algo.(*Fulltext2Search)
		if !ok {
			return moerr.NewInternalError(ctx, "fulltext stats: unexpected cached index type")
		}
		if !reader.genValid || reader.loadedTs != ts || reader.loadedTail != tail {
			return moerr.NewInternalError(ctx, "fulltext stats: loaded generation does not match the read snapshot")
		}
		result, err = reader.Stats(ctx)
		return err
	})
	return result, err
}
