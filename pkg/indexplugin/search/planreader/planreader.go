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

// Package planreader is the engine.Reader of an IndexSearchScan whose index
// algorithm produces its ranked results through a Searcher. It emits the
// IndexSearchScan output schema: "pkid", "score" and IncludePrefix + column.
package planreader

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	// KeyColumn is the output column of the source table's primary key.
	KeyColumn = "pkid"
	// ScoreColumn is the output column of the distance or relevance score.
	ScoreColumn = "score"
	// IncludePrefix prefixes the output column of an included source column.
	IncludePrefix = catalog.SystemSI_IVFFLAT_IncludeColPrefix
	// maxBatchRows is the row count of one emitted batch.
	maxBatchRows = 8192
)

// Chunk is one part of a search result, in rank order. Keys is []int64 or
// []any; Include and IncludeNulls hold one slice per included column, aligned
// with Keys.
type Chunk struct {
	Keys         any
	Scores       []float64
	Include      map[string][]any
	IncludeNulls map[string][]bool
}

func (c *Chunk) len() int {
	switch keys := c.Keys.(type) {
	case []int64:
		return len(keys)
	case []any:
		return len(keys)
	}
	return 0
}

// Searcher runs one index search. Next returns the next chunk of the result;
// more is false when no chunk follows it. Close releases the search and is
// called once, after the last Next or on an error or cancellation.
type Searcher interface {
	Next(ctx context.Context) (chunk Chunk, more bool, err error)
	Close() error
}

// Reader emits the results of a Searcher as IndexSearchScan batches.
type Reader struct {
	searcher Searcher
	chunk    Chunk
	offset   int
	done     bool
	closed   bool
}

var _ engine.Reader = (*Reader)(nil)

// New returns the reader of searcher; the reader owns searcher.
func New(searcher Searcher) *Reader {
	return &Reader{searcher: searcher}
}

// Empty returns a reader with no rows.
func Empty() *Reader {
	return &Reader{done: true}
}

func (r *Reader) Read(ctx context.Context, attrs []string, _ *plan.Expr, mp *mpool.MPool, out *batch.Batch) (bool, error) {
	if r.closed {
		return true, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	for r.offset >= r.chunk.len() {
		if r.done || r.searcher == nil {
			return true, nil
		}
		chunk, more, err := r.searcher.Next(ctx)
		if err != nil {
			return false, err
		}
		if len(chunk.Scores) != chunk.len() {
			return false, moerr.NewInternalErrorNoCtx("index search chunk keys and scores are not aligned")
		}
		r.chunk, r.offset, r.done = chunk, 0, !more
	}

	out.CleanOnlyData()
	end := min(r.chunk.len(), r.offset+maxBatchRows)
	for col, attr := range attrs {
		if err := r.appendColumn(out.Vecs[col], attr, end, mp); err != nil {
			return false, err
		}
	}
	out.SetRowCount(end - r.offset)
	r.offset = end
	return false, nil
}

func (r *Reader) appendColumn(vec *vector.Vector, attr string, end int, mp *mpool.MPool) error {
	switch {
	case attr == KeyColumn:
		switch keys := r.chunk.Keys.(type) {
		case []int64:
			return vector.AppendFixedList(vec, keys[r.offset:end], nil, mp)
		case []any:
			for _, key := range keys[r.offset:end] {
				if err := vector.AppendAny(vec, key, false, mp); err != nil {
					return err
				}
			}
		}
		return nil
	case attr == ScoreColumn:
		return vector.AppendFixedList(vec, r.chunk.Scores[r.offset:end], nil, mp)
	case strings.HasPrefix(attr, IncludePrefix):
		name := strings.TrimPrefix(attr, IncludePrefix)
		values, ok := r.chunk.Include[name]
		if !ok || len(values) < end {
			return moerr.NewInternalErrorNoCtxf("index search include output %q is not aligned", name)
		}
		nulls := r.chunk.IncludeNulls[name]
		for row := r.offset; row < end; row++ {
			if err := vector.AppendAny(vec, values[row], row < len(nulls) && nulls[row], mp); err != nil {
				return err
			}
		}
		return nil
	}
	return moerr.NewInternalErrorNoCtxf("unknown index search output %q", attr)
}

// Close releases the searcher. It is idempotent.
func (r *Reader) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	r.chunk = Chunk{}
	if r.searcher == nil {
		return nil
	}
	searcher := r.searcher
	r.searcher = nil
	return searcher.Close()
}

func (*Reader) SetOrderBy([]*plan.OrderBySpec)       {}
func (*Reader) GetOrderBy() []*plan.OrderBySpec      { return nil }
func (*Reader) SetIndexParam(*plan.IndexReaderParam) {}
func (*Reader) SetFilterZM(objectio.ZoneMap)         {}
