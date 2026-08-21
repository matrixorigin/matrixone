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

package table_function

import (
	"context"
	"fmt"

	"encoding/json"
	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// fulltext2SearchState answers a MATCH over a fulltext2 index: it loads the
// index's segments (base + CDC tail) once via the shared VectorIndexCache and reuses
// them across queries (evicted on CDC append / compaction / rebuild), runs the WAND
// positional query, and emits (doc_id, score) rows; the top-k is bounded by the
// pushed LIMIT, and a pushed-down WHERE prefilter is applied inside the walk.
type fulltext2SearchState struct {
	inited      bool
	tblcfg      fulltext2.TableConfig
	limit       uint64
	offset      int
	filterBytes []byte // serialized docfilter membership (WHERE-clause prefilter), if any
	batch       *batch.Batch

	// LIMIT (non-streaming) path: SearchInto fills this caller-owned, box-free result — pk
	// (out.Keys), scores (out.Dists), covered INCLUDE cols (out.Include). Held on the state
	// and Reset per query by SearchInto, so a warm query allocates nothing for its results
	// (the alloc-free twin of the old keys []any / distances []float64). call() pages it via
	// u.offset with AppendColumnBufferRange.
	out *vectorindex.SearchOutput

	// Streaming no-LIMIT path (u.limit == 0): rather than materialize every matching
	// doc, a producer goroutine runs the search with an Emit callback that hands bounded
	// batches (box-free *vectorindex.SearchOutput, the SAME shape SearchInto fills) to
	// streamCh; call() drains one batch per invocation and the upstream ORDER BY score
	// node ranks them. cancel stops the producer (and releases the cache read lock it
	// holds) if the consumer aborts early. Mirrors bm25_search.
	streaming bool
	streamCh  chan *vectorindex.SearchOutput
	errCh     chan error
	cancel    context.CancelFunc
	done      bool

	// Covered fast path (Phase 6): the TVF outputs pk/score/INCLUDE columns straight from
	// the index (no base-table JOIN). Column pruning can drop ANY unreferenced output
	// column — doc_id when the pk isn't projected, score when the ORDER BY is on another
	// column, an unprojected include col — and COMPACT the batch, so the runtime must NOT
	// assume fixed positions. It maps each SURVIVING result vector by its coldef NAME
	// (u.batch.Attrs): "doc_id" <- pk, "score" <- score, everything else <- the include
	// column of that name. pkVecIdx/scoreVecIdx are the batch vector indices for doc_id /
	// score (-1 when pruned); includeOut lists the surviving include output vectors.
	// includeNames (== includeOut names) drives rt.RequestedIncludeColumns. The non-streaming
	// (LIMIT) include values arrive box-free in u.out.Include (one ColumnBuffer per FULL index
	// include column, segment order, indexed by includeOut.segPos) — the streaming path uses
	// ft2StreamBatch.includes; both share one consumer.
	pkVecIdx     int
	scoreVecIdx  int
	includeOut   []ft2IncludeOut
	includeNames []string
}

// ft2IncludeOut maps one surviving INCLUDE output vector to its source: vecIdx is the
// position in u.batch.Vecs; name is the column name (non-streaming IncludeResult lookup);
// segPos is the column's position in the FULL index include list (= segment / decodeInclude
// order), used to pull the value from the streaming path's segment-ordered include slice.
type ft2IncludeOut struct {
	vecIdx int
	segPos int
	name   string
}

func (u *fulltext2SearchState) end(tf *TableFunction, proc *process.Process) error { return nil }

func (u *fulltext2SearchState) reset(tf *TableFunction, proc *process.Process) {
	u.stopStream()
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
	u.offset = 0
	u.filterBytes = nil
	u.streaming = false
	u.errCh = nil
	u.done = false
	// u.out is kept and REUSED across queries (SearchInto Resets its buffers per query) so
	// the LIMIT path is alloc-free after warmup; includeColumns is stable (cfg-derived).
}

// stopStream cancels the producer goroutine (if streaming) and drains streamCh until
// the producer closes it, so no goroutine — nor the cache read-lock it holds — leaks
// past this query. Idempotent; a no-op when not streaming.
func (u *fulltext2SearchState) stopStream() {
	if u.cancel == nil {
		return
	}
	u.cancel()
	if u.streamCh != nil {
		for b := range u.streamCh { // drain to the producer's close()
			fulltext2.PutColumnBuffer(b.Keys) // recycle drained (unconsumed) batches
		}
	}
	u.cancel = nil
	u.streamCh = nil
}

func (u *fulltext2SearchState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	u.stopStream()
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

func (u *fulltext2SearchState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	u.batch.CleanOnlyData()

	if u.streaming {
		if u.done {
			return vm.CancelResult, nil
		}
		select {
		case b, ok := <-u.streamCh:
			if !ok {
				// producer finished; surface any search error (sent before close).
				u.done = true
				u.cancel = nil
				if e := <-u.errCh; e != nil {
					return vm.CancelResult, e
				}
				return vm.CancelResult, nil
			}
			// b ownership (its pooled Keys buffer) was received from the producer; recycle it
			// on EVERY exit from here (incl. the append error paths in appendOutputRange), else
			// a mid-stream mpool failure leaks the pooled buffer. Safe to defer: the append
			// copies into u.batch, so u.batch never aliases b.Keys.
			defer fulltext2.PutColumnBuffer(b.Keys)
			// Write the whole emitted batch (rows [0, N)) — pk / score / each include col,
			// name-driven — via the SAME consumer the LIMIT path uses.
			if err := u.appendOutputRange(b, 0, b.Keys.N, proc); err != nil {
				return vm.CancelResult, err
			}
			u.batch.SetRowCount(b.Keys.N)
			return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
		case <-proc.Ctx.Done():
			return vm.CancelResult, proc.Ctx.Err()
		}
	}

	// start() bailed before running SearchInto (e.g. a NULL/empty pattern) — u.out is nil, or
	// (on a reused operator) was emptied in start()'s reset. Either way no results this row →
	// signal end of stream. (Before the SearchOutput migration this was a nil u.keys slice.)
	if u.out == nil || u.out.Keys == nil {
		return vm.CancelResult, nil
	}

	// LIMIT (non-streaming) path: page the box-free SearchInto result (u.out) into this
	// batch. pk / score / each include col are bulk-appended via AppendColumnBufferRange over
	// the [start, start+n) rows — no per-row boxing, no reflection append; identical shape to
	// the streaming consumer above.
	nkeys := u.out.Keys.N
	start := u.offset
	n := nkeys - start
	if n > 8192 {
		n = 8192
	}
	if n < 0 {
		n = 0
	}
	// Page rows [start, start+n) of the box-free SearchInto result (u.out) into this batch
	// via the SAME consumer the streaming path uses.
	if err := u.appendOutputRange(u.out, start, n, proc); err != nil {
		return vm.CancelResult, err
	}
	u.offset += n
	u.batch.SetRowCount(n)
	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
}

// appendOutputRange writes rows [start, start+n) of a box-free SearchOutput (pk / score /
// covered INCLUDE cols) into u.batch, name-driven so column pruning can't misalign the
// columns. It is the ONE consumer shared by both result paths: the streaming path passes the
// per-batch emitted output with start=0, n=Keys.N; the LIMIT path pages the whole-result output
// by u.offset. pk and each include col bulk-append via AppendColumnBufferRange (no per-row
// boxing); score narrows float32 per row. Any mpool failure surfaces as the error so the caller
// never SetRowCounts a batch whose vectors disagree in length. A segPos out of range (should not
// happen on the covered path) fills SQL NULLs to keep the batch column-aligned.
func (u *fulltext2SearchState) appendOutputRange(out *vectorindex.SearchOutput, start, n int, proc *process.Process) error {
	mp := proc.Mp()
	if u.pkVecIdx >= 0 {
		if err := vectorindex.AppendColumnBufferRange(out.Keys, u.batch.Vecs[u.pkVecIdx], start, n, mp); err != nil {
			return err
		}
	}
	if u.scoreVecIdx >= 0 {
		vec := u.batch.Vecs[u.scoreVecIdx]
		for i := start; i < start+n; i++ {
			if err := vector.AppendFixed[float32](vec, out.Dists[i], false, mp); err != nil {
				return err
			}
		}
	}
	for _, ic := range u.includeOut {
		vec := u.batch.Vecs[ic.vecIdx]
		if ic.segPos >= 0 && ic.segPos < len(out.Include) {
			if err := vectorindex.AppendColumnBufferRange(out.Include[ic.segPos], vec, start, n, mp); err != nil {
				return err
			}
		} else {
			for j := 0; j < n; j++ {
				if err := vector.AppendAny(vec, nil, true, mp); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func fulltext2SearchPrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &fulltext2SearchState{}
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	if err != nil {
		return nil, err
	}
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))
	// Resolve the pushed-down LIMIT. Prepare runs at EXECUTE time (after '?' parameters are
	// bound), so evalLimitExpression evaluates a literal OR a prepared `LIMIT ?` parameter —
	// unlike the old literal-only path, which left st.limit==0 for a parameter and silently
	// fell back to the unbounded stream (losing the pushed top-k bound). Mirrors classic
	// fulltext (fulltextIndexScanPrepare); default 0 = no pushed limit.
	if st.limit, err = evalLimitExpression(proc, arg.Limit, 0); err != nil {
		return nil, err
	}
	return st, nil
}

// start runs one query. argVecs: [0]=cfg(json const), [1]=pattern(varchar).
func (u *fulltext2SearchState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {
	if !u.inited {
		cfgVec := tf.ctr.argVecs[0]
		if cfgVec.GetType().Oid != types.T_varchar || !cfgVec.IsConst() {
			return moerr.NewInvalidInput(proc.Ctx, "fulltext2_search: first argument (config) must be a string constant")
		}
		cfgstr := cfgVec.UnsafeGetStringAt(0)
		if len(cfgstr) == 0 {
			return moerr.NewInternalError(proc.Ctx, "fulltext2_search: config is empty")
		}
		if err = sonic.Unmarshal([]byte(cfgstr), &u.tblcfg); err != nil {
			return err
		}
		patVec := tf.ctr.argVecs[1]
		if patVec.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "fulltext2_search: second argument (pattern) must be a string")
		}
		u.batch = tf.createResultBatch()
		// Name-driven output map. createResultBatch built one vector per SURVIVING plan
		// coldef (column pruning may have dropped/compacted doc_id, score, and/or any
		// unprojected INCLUDE col), so classify each result vector by its coldef NAME
		// rather than by a fixed position: the RESERVED __mo_ft_doc_id -> pk, __mo_ft_score
		// -> score, everything else -> the INCLUDE column of that name (mapped to its
		// position in the full index include list = segment / decodeInclude order). The pk/
		// score outputs use reserved names (catalog.FullText2Search_OutCol_*) precisely so an
		// INCLUDE column named "doc_id"/"score" can't collide with this classification.
		u.pkVecIdx, u.scoreVecIdx = -1, -1
		u.includeOut = nil
		u.includeNames = nil
		for vi, name := range u.batch.Attrs {
			switch {
			case strings.EqualFold(name, catalog.FullText2Search_OutCol_DocId):
				u.pkVecIdx = vi
			case strings.EqualFold(name, catalog.FullText2Search_OutCol_Score):
				u.scoreVecIdx = vi
			default:
				segPos := -1
				for p, full := range u.tblcfg.IncludeColumns {
					if strings.EqualFold(full, name) {
						segPos = p
						break
					}
				}
				u.includeOut = append(u.includeOut, ft2IncludeOut{vecIdx: vi, segPos: segPos, name: name})
				u.includeNames = append(u.includeNames, name)
			}
		}
		u.inited = true
	}

	u.stopStream()
	u.offset = 0
	u.streaming = false
	u.done = false
	// u.out is kept and REUSED across queries (SearchInto Resets it per query), but EMPTY it
	// here too: a subsequent early-return below (NULL/empty pattern) skips SearchInto, so
	// without this a reused operator would page the PREVIOUS query's results as this row's
	// output. Emptying leaves call() with 0 rows on the bailed path. (Pre-SearchOutput this
	// was a nil u.keys slice.)
	if u.out != nil {
		if u.out.Keys != nil {
			u.out.Keys.Reset()
		}
		u.out.Dists = u.out.Dists[:0]
		u.out.Include = u.out.Include[:0]
	}
	u.batch.CleanOnlyData()

	patVec := tf.ctr.argVecs[1]
	if patVec.IsNull(uint64(nthRow)) {
		return nil
	}
	pattern := patVec.GetStringAt(nthRow)

	// Prefilter pushdown: when the WHERE clause is pushed down as a unique-join-keys
	// runtime filter, wait for it and build the docfilter membership bytes — the same
	// mechanism bm25_search / fulltext_index_scan use. Applied INSIDE the WAND walk so
	// the returned top-K is already filtered (no over-fetch).
	if u.filterBytes == nil && len(tf.RuntimeFilterSpecs) > 0 {
		res, ferr := waitFulltextMembershipFilter(proc, tf.RuntimeFilterSpecs)
		if ferr != nil {
			return ferr
		}
		if res != nil {
			u.filterBytes = res.membershipFilterBytes
		}
	}

	// Run the query through the shared VectorIndexCache: the index (base + CDC tail)
	// is loaded ONCE and reused across queries, evicted on CDC append / compaction /
	// rebuild (fulltext2_{create,compact} + the CDC consumer call Cache.Remove).
	// Before caching, every MATCH reloaded the whole index (~1s at 50K); now warm
	// queries are ~ms, matching bm25. Build+query tokenize identically — both use the
	// index's parser (carried in tblcfg).
	sp := sqlexec.NewSqlProcess(proc)
	veccache.Cache.Once()

	// mode (argVecs[2], a query const): boolean → operator query, else NL phrase.
	var mode int64
	if mv := tf.ctr.argVecs[2]; mv != nil && mv.Length() > 0 {
		mode = vector.GetFixedAtNoTypeCheck[int64](mv, 0)
	}
	// Optional INCLUDE/pk prefilter predicate JSON (argVecs[3], a query const): the planner
	// peels a WHERE predicate on INCLUDE columns / the pk into this ivfpq-aligned JSON, which
	// the engine evaluates against the stored per-doc values inside the WAND walk. Absent on a
	// direct fulltext2_search(...) call (3 args) or when nothing was peeled.
	var includePreds []byte
	if len(tf.ctr.argVecs) > 3 {
		if pv := tf.ctr.argVecs[3]; pv != nil && pv.Length() > 0 && !pv.IsNull(0) {
			if s := pv.GetStringAt(0); len(s) > 0 {
				includePreds = []byte(s)
			}
		}
	}

	// Optional pushed score range (argVecs[4], a query const): the planner turns an
	// AND-reachable `MATCH(...) <op> const` into a relevance interval the engine applies to
	// each scored doc, so out-of-range rows never cross into the join above. Absent on a
	// direct fulltext2_search(...) call or when no score predicate was pushed.
	var scoreRange *fulltext2.ScoreRange
	if len(tf.ctr.argVecs) > 4 {
		if rv := tf.ctr.argVecs[4]; rv != nil && rv.Length() > 0 && !rv.IsNull(0) {
			if s := rv.GetStringAt(0); len(s) > 0 {
				var r fulltext2.ScoreRange
				if err := json.Unmarshal([]byte(s), &r); err != nil {
					return moerr.NewInternalErrorf(proc.Ctx,
						"fulltext2_search: invalid score range %q: %v", s, err)
				}
				scoreRange = &r
			}
		}
	}

	newsearch := fulltext2.NewFulltext2Search(u.tblcfg)
	q := fulltext2.Fulltext2Query{
		ScoreRange:       scoreRange,
		Pattern:          []byte(pattern),
		Boolean:          mode == int64(tree.FULLTEXT_BOOLEAN),
		BagOfWords:       mode == int64(tree.FULLTEXT_BM25),
		Algo:             fulltext2ScoreAlgo(proc),
		FilterBytes:      u.filterBytes,
		IncludePredsJSON: includePreds,
	}

	if u.limit == 0 {
		// No pushed LIMIT: STREAM every matching doc in bounded batches (no top-K heap,
		// no materialization of the whole result set). A producer goroutine runs the
		// search with an Emit callback that hands batches to streamCh; call() drains one
		// per invocation and the upstream ORDER BY score node ranks. cancel/ctx let
		// reset()/free() stop the producer and release the cache read-lock it holds.
		u.streaming = true
		u.streamCh = make(chan *vectorindex.SearchOutput, 4)
		u.errCh = make(chan error, 1)
		ctx, cancel := context.WithCancel(proc.Ctx)
		u.cancel = cancel
		rt := vectorindex.RuntimeConfig{Emit: func(out *vectorindex.SearchOutput) error {
			select {
			case u.streamCh <- out:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
		// Covered fast path: signal the stream to carry each doc's INCLUDE values. The gate in
		// Fulltext2Search.Search is len(RequestedIncludeColumns) > 0; the streaming path carries
		// the includes through Emit's 3rd arg, not through a result buffer.
		if len(u.includeNames) > 0 {
			rt.RequestedIncludeColumns = u.includeNames
		}
		go func() {
			_, _, serr := veccache.Cache.Search(sp, u.tblcfg.IndexTable, newsearch, q, rt)
			u.errCh <- serr // buffered(1): send before close so call() reads it after drain
			close(u.streamCh)
		}()
		return nil
	}

	// With a pushed LIMIT: WAND top-K, filled box-free into the caller-owned u.out via
	// SearchInto (pk/scores/includes as reusable ColumnBuffers) — no []any keys. u.out is
	// pooled on the state and Reset per query by SearchInto, so a warm query allocates nothing
	// for its results; call() pages u.out via u.offset.
	rt := vectorindex.RuntimeConfig{Limit: uint(u.limit)}
	if len(u.includeNames) > 0 {
		// Request the covered INCLUDE columns; SearchInto fills u.out.Include (box-free,
		// column-major, whole result set), which call() pages by segPos.
		rt.RequestedIncludeColumns = u.includeNames
	}
	if u.out == nil {
		u.out = &vectorindex.SearchOutput{}
	}
	return veccache.Cache.SearchInto(sp, u.tblcfg.IndexTable, newsearch, q, rt, u.out)
}

// fulltext2ScoreAlgo resolves the relevance formula from fulltext2's OWN session
// variable ft2_relevancy_algorithm, which defaults to BM25 (distinct from classic
// fulltext's ft_relevancy_algorithm, default TF-IDF). Only an explicit
// SET ft2_relevancy_algorithm='TF-IDF' drops to TF-IDF; on any resolve error the
// BM25 default stands.
func fulltext2ScoreAlgo(proc *process.Process) fulltext2.ScoreAlgo {
	algo := fulltext2.BM25
	val, err := proc.GetResolveVariableFunc()(fulltext2.Fulltext2RelevancyAlgo, true, false)
	if err == nil && val != nil {
		if fmt.Sprintf("%v", val) == fulltext2.Fulltext2RelevancyAlgo_tfidf {
			algo = fulltext2.TfIdf
		}
	}
	return algo
}
