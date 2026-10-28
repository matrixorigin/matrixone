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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// MaxConditionalStreamTopK bounds query-local references and literal pagination.
const MaxConditionalStreamTopK = 8192

type streamRef struct {
	seg   *Segment
	ord   int64
	score float32
}

// streamTopK keeps the worst retained reference at the root. Keys remain borrowed
// from the locked index generation; losers never materialize a key.
type streamTopK struct {
	refs []streamRef
	k    int
	err  error
}

func (h *streamTopK) key(r streamRef) []byte {
	if r.seg.pks != nil {
		switch pk := r.seg.pks[r.ord].(type) {
		case string:
			// Compare only: the immutable build-side key stays owned by the segment.
			return util.UnsafeStringToBytes(pk)
		case []byte:
			return pk
		default:
			h.err = moerr.NewInternalErrorNoCtx("fulltext2 conditional top-k requires VARCHAR keys")
			return nil
		}
	}
	b, err := r.seg.pkContent(r.ord)
	if err != nil {
		h.err = err
	}
	return b
}

func (h *streamTopK) better(a, b streamRef) bool {
	if a.score != b.score {
		return a.score > b.score
	}
	return bytes.Compare(h.key(a), h.key(b)) < 0
}

func (h *streamTopK) push(r streamRef) {
	// SQL's retained score > 0 predicate also excludes NaNs.
	if !(r.score > 0) || h.err != nil {
		return
	}
	if len(h.refs) < h.k {
		h.refs = append(h.refs, r)
		for i := len(h.refs) - 1; i > 0; {
			p := (i - 1) / 2
			if !h.better(h.refs[p], h.refs[i]) {
				break
			}
			h.refs[p], h.refs[i] = h.refs[i], h.refs[p]
			i = p
		}
		return
	}
	if !h.better(r, h.refs[0]) {
		return
	}
	h.refs[0] = r
	for i := 0; ; {
		child := i*2 + 1
		if child >= len(h.refs) {
			break
		}
		if child+1 < len(h.refs) && h.better(h.refs[child], h.refs[child+1]) {
			child++
		}
		if !h.better(h.refs[i], h.refs[child]) {
			break
		}
		h.refs[i], h.refs[child] = h.refs[child], h.refs[i]
		i = child
	}
}

// streamBagOfWordsTopK uses exactly the unlimited walk and accumulation order.
// Only after complete collection are winner keys copied into output batches.
func (idx *Index) streamBagOfWordsTopK(ctx context.Context, pattern []byte, parser string, algo ScoreAlgo, filter *prefilter, k int, emit func(*vectorindex.SearchOutput) error, dfCutoff float64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if idx.globalN == 0 {
		return nil
	}
	if k < 1 || k > MaxConditionalStreamTopK || idx.pkType() != int32(types.T_varchar) {
		return moerr.NewInternalErrorNoCtx("invalid conditional fulltext2 top-k")
	}
	q, err := buildBagOfWordsQuery(string(pattern), normalizeParser(parser), idx.tokenFilters...)
	if err != nil {
		return err
	}
	terms, ok := disjunctiveTerms(q)
	if !ok {
		return ctx.Err()
	}
	sink := newStreamSink(idx, false, filter, emit)
	defer sink.close()
	sink.ctx = ctx
	sink.topK = &streamTopK{k: k, refs: make([]streamRef, 0, k)}
	if err := idx.streamDisjunction(terms, algo, filter, sink, dfCutoff); err != nil {
		return err
	}
	h := sink.topK
	sort.Slice(h.refs, func(i, j int) bool { return h.better(h.refs[i], h.refs[j]) })
	if h.err != nil {
		return h.err
	}
	if !sink.checkContext() {
		return sink.err
	}
	sink.topK = nil
	for _, r := range h.refs {
		sink.pushPk(r.seg, r.ord, r.score)
	}
	if !sink.checkContext() {
		return sink.err
	}
	sink.flush()
	return sink.err
}
