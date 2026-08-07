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

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
	keys        []any
	distances   []float64
	filterBytes []byte // serialized docfilter membership (WHERE-clause prefilter), if any
	batch       *batch.Batch

	// Streaming no-LIMIT path (u.limit == 0): rather than materialize every matching
	// doc, a producer goroutine runs the search with an Emit callback that hands bounded
	// batches to streamCh; call() drains one batch per invocation and the upstream ORDER
	// BY score node ranks them. cancel stops the producer (and releases the cache read
	// lock it holds) if the consumer aborts early. Mirrors bm25_search.
	streaming bool
	streamCh  chan ft2StreamBatch
	errCh     chan error
	cancel    context.CancelFunc
	done      bool

	// Covered fast path (Phase 6): when the result batch has >2 vectors (the plan built
	// include coldefs at positions 2+), the TVF also OUTPUTS each requested INCLUDE
	// column, served straight from the index's per-doc values — no base-table JOIN.
	// includeColumns are the OUTPUT INCLUDE column NAMES, read from the ACTUAL result
	// coldefs (u.batch.Attrs[2:]) — NOT the full cfg list: column pruning may drop an
	// unprojected include col from the TVF output, so the runtime must follow the pruned
	// coldefs. includeSegPos[j] is includeColumns[j]'s position in the FULL index include
	// list (= segment / decodeInclude order = tblcfg.IncludeColumns), used to pull the
	// right value out of the streaming path's segment-ordered include slice.
	// includeResult receives the non-streaming (LIMIT) path's per-doc include values
	// (keyed by output name; fillIncludeResult maps name -> full-list position internally).
	includeColumns []string
	includeSegPos  []int
	includeResult  *vectorindex.IvfIncludeResult
}

// ft2StreamBatch is one emitted batch (<= streamBatch rows); the producer hands
// ownership to the consumer, so the buffers are not reused. keys is a TYPED, box-free
// ColumnBuffer (fixed-width or varlena) — no per-pk interface allocation. includes, when
// the covered fast path is on, is one per-doc INCLUDE slice per row (segment order).
type ft2StreamBatch struct {
	keys      *vectorindex.ColumnBuffer
	distances []float64
	includes  [][]any
}

func (u *fulltext2SearchState) end(tf *TableFunction, proc *process.Process) error { return nil }

func (u *fulltext2SearchState) reset(tf *TableFunction, proc *process.Process) {
	u.stopStream()
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
	u.offset = 0
	u.keys = nil
	u.distances = nil
	u.filterBytes = nil
	u.streaming = false
	u.errCh = nil
	u.done = false
	u.includeResult = nil // per-query; includeColumns is stable (cfg-derived, set at init)
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
			fulltext2.PutColumnBuffer(b.keys) // recycle drained (unconsumed) batches
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
	withScore := u.batch.VectorCount() > 1
	// Covered fast path: the plan built one extra output vector per requested INCLUDE
	// column (Vecs[2..]); serve them straight from the index's per-doc values.
	withInclude := u.batch.VectorCount() > 2

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
			// b.keys ownership was received from the producer; recycle it on EVERY exit
			// from here (incl. the append error paths below), else a mid-stream mpool
			// failure leaks the pooled buffer. Safe to defer: AppendColumnBuffer copies
			// into u.batch, so u.batch never aliases b.keys.
			defer fulltext2.PutColumnBuffer(b.keys)
			// Typed bulk decode of the pk column — no per-pk boxing (the mirror of the
			// producer's box-free encode).
			if err := fulltext2.AppendColumnBuffer(b.keys, u.batch.Vecs[0], proc.Mp()); err != nil {
				return vm.CancelResult, err
			}
			if withScore {
				for i := 0; i < b.keys.N; i++ {
					// score column is T_float32 (matches ftIndexColdefs / classic fulltext);
					// the engine computes float64 relevance, narrow it on append. Propagate
					// the append error — otherwise SetRowCount below would publish a batch
					// with fewer scores than keys (a malformed, mismatched-length batch).
					if err := vector.AppendFixed[float32](u.batch.Vecs[1], float32(b.distances[i]), false, proc.Mp()); err != nil {
						return vm.CancelResult, err
					}
				}
			}
			if withInclude {
				// Covered fast path: b.includes[i] is the doc's include slice in segment
				// order; the plan builds output col j (Vecs[2+j]) in the SAME order, so
				// Vecs[2+j] <- b.includes[i][j]. A nil value becomes a SQL NULL. Propagate
				// the append error (same reason as the score loop).
				for i := 0; i < b.keys.N; i++ {
					for j := 0; j < len(u.includeColumns); j++ {
						// b.includes[i] is the doc's FULL include slice in segment order;
						// includeSegPos[j] maps output col j to its segment position
						// (handles column pruning / reorder). Out-of-range ⇒ SQL NULL.
						var val any
						if p := u.includeSegPos[j]; p >= 0 && i < len(b.includes) && p < len(b.includes[i]) {
							val = b.includes[i][p]
						}
						if err := vector.AppendAny(u.batch.Vecs[2+j], val, val == nil, proc.Mp()); err != nil {
							return vm.CancelResult, err
						}
					}
				}
			}
			u.batch.SetRowCount(b.keys.N)
			return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
		case <-proc.Ctx.Done():
			return vm.CancelResult, proc.Ctx.Err()
		}
	}

	nkeys := len(u.keys)
	n := 0
	for i := u.offset; i < nkeys && n < 8192; i++ {
		// Check each append before incrementing n / publishing the batch — an mpool
		// allocation failure must surface as the error, not a batch that claims more
		// rows than its vectors hold (the streaming sibling propagates these too).
		if err := vector.AppendAny(u.batch.Vecs[0], u.keys[i], false, proc.Mp()); err != nil {
			return vm.CancelResult, err
		}
		if withScore {
			// score column is T_float32 (matches ftIndexColdefs / classic fulltext);
			// the engine computes float64 relevance, narrow it on append.
			if err := vector.AppendFixed[float32](u.batch.Vecs[1], float32(u.distances[i]), false, proc.Mp()); err != nil {
				return vm.CancelResult, err
			}
		}
		if withInclude {
			// Covered fast path: index IncludeResult by the ABSOLUTE result position i
			// (this loop pages via u.offset), NOT the page-relative n. Output col j
			// (Vecs[2+j]) matches includeColumns[j] order (= coldefs order).
			for j := 0; j < len(u.includeColumns); j++ {
				name := u.includeColumns[j]
				var val any
				isNull := true
				if data := u.includeResult.Data[name]; i < len(data) {
					nulls := u.includeResult.Nulls[name]
					if i < len(nulls) && nulls[i] {
						isNull = true
					} else {
						val = data[i]
						isNull = val == nil
					}
				}
				if err := vector.AppendAny(u.batch.Vecs[2+j], val, isNull, proc.Mp()); err != nil {
					return vm.CancelResult, err
				}
			}
		}
		n++
	}
	u.offset += n
	u.batch.SetRowCount(n)
	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
}

func fulltext2SearchPrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &fulltext2SearchState{}
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))
	if arg.Limit != nil {
		if cExpr, ok := arg.Limit.Expr.(*plan.Expr_Lit); ok {
			switch v := cExpr.Lit.Value.(type) {
			case *plan.Literal_U64Val:
				st.limit = v.U64Val
			case *plan.Literal_I64Val:
				if v.I64Val > 0 {
					st.limit = uint64(v.I64Val)
				}
			}
		}
	}
	return st, err
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
		// Covered fast path: createResultBatch built one vector per plan coldef. When the
		// plan added INCLUDE coldefs (Vecs[2..]), the batch has >2 vectors — take the
		// requested INCLUDE column NAMES from cfg IN OUTPUT ORDER (the plan builds coldefs
		// in the SAME order as cfg include_columns). Only then does the TVF emit include
		// columns; a non-covered query leaves this nil and outputs just (doc_id, score).
		if u.batch.VectorCount() > 2 {
			// Output include cols = the ACTUAL coldefs after plan column-pruning
			// (u.batch.Attrs[2:]), which may be a subset of the full index include list.
			// Map each to its position in the full list (segment / decodeInclude order)
			// so the streaming path pulls the right value; the non-streaming path maps by
			// name via fillIncludeResult.
			u.includeColumns = append([]string(nil), u.batch.Attrs[2:]...)
			u.includeSegPos = make([]int, len(u.includeColumns))
			for j, name := range u.includeColumns {
				u.includeSegPos[j] = -1
				for p, full := range u.tblcfg.IncludeColumns {
					if strings.EqualFold(full, name) {
						u.includeSegPos[j] = p
						break
					}
				}
			}
		}
		u.inited = true
	}

	u.stopStream()
	u.offset = 0
	u.keys = nil
	u.distances = nil
	u.streaming = false
	u.done = false
	u.includeResult = nil
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

	newsearch := fulltext2.NewFulltext2Search(u.tblcfg)
	q := fulltext2.Fulltext2Query{
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
		u.streamCh = make(chan ft2StreamBatch, 4)
		u.errCh = make(chan error, 1)
		ctx, cancel := context.WithCancel(proc.Ctx)
		u.cancel = cancel
		rt := vectorindex.RuntimeConfig{Emit: func(keys *vectorindex.ColumnBuffer, dists []float64, includes [][]any) error {
			select {
			case u.streamCh <- ft2StreamBatch{keys: keys, distances: dists, includes: includes}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
		// Covered fast path: signal the stream to carry each doc's INCLUDE values. The gate
		// in Fulltext2Search.Search is (IncludeResult != nil && RequestedIncludeColumns > 0),
		// so set both — the streaming path uses the Emit includes, not IncludeResult.
		if len(u.includeColumns) > 0 {
			rt.RequestedIncludeColumns = u.includeColumns
			rt.IncludeResult = &vectorindex.IvfIncludeResult{}
		}
		go func() {
			_, _, serr := veccache.Cache.Search(sp, u.tblcfg.IndexTable, newsearch, q, rt)
			u.errCh <- serr // buffered(1): send before close so call() reads it after drain
			close(u.streamCh)
		}()
		return nil
	}

	// With a pushed LIMIT: WAND top-K, returned all at once (bounded by the LIMIT).
	rt := vectorindex.RuntimeConfig{Limit: uint(u.limit)}
	// Covered fast path: request the INCLUDE column values; Search fills IncludeResult
	// (indexed by absolute result position), which call() reads on the paged output.
	if len(u.includeColumns) > 0 {
		u.includeResult = &vectorindex.IvfIncludeResult{}
		rt.RequestedIncludeColumns = u.includeColumns
		rt.IncludeResult = u.includeResult
	}
	keys, dists, serr := veccache.Cache.Search(sp, u.tblcfg.IndexTable, newsearch, q, rt)
	if serr != nil {
		return serr
	}
	ks, ok := keys.([]any)
	if !ok {
		return moerr.NewInternalError(proc.Ctx, "fulltext2_search: unexpected result key type")
	}
	u.keys = ks
	u.distances = dists
	return nil
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
