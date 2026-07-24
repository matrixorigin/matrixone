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
}

// ft2StreamBatch is one emitted batch (<= streamBatch rows); the producer hands
// ownership to the consumer, so the slices are not reused.
type ft2StreamBatch struct {
	keys      []any
	distances []float64
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
		for range u.streamCh { // drain to the producer's close()
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
			for i := range b.keys {
				vector.AppendAny(u.batch.Vecs[0], b.keys[i], false, proc.Mp())
				if withScore {
					// score column is T_float32 (matches ftIndexColdefs / classic fulltext);
					// the engine computes float64 relevance, narrow it on append.
					vector.AppendFixed[float32](u.batch.Vecs[1], float32(b.distances[i]), false, proc.Mp())
				}
			}
			u.batch.SetRowCount(len(b.keys))
			return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
		case <-proc.Ctx.Done():
			return vm.CancelResult, proc.Ctx.Err()
		}
	}

	nkeys := len(u.keys)
	n := 0
	for i := u.offset; i < nkeys && n < 8192; i++ {
		vector.AppendAny(u.batch.Vecs[0], u.keys[i], false, proc.Mp())
		if withScore {
			// score column is T_float32 (matches ftIndexColdefs / classic fulltext);
			// the engine computes float64 relevance, narrow it on append.
			vector.AppendFixed[float32](u.batch.Vecs[1], float32(u.distances[i]), false, proc.Mp())
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
		u.inited = true
	}

	u.stopStream()
	u.offset = 0
	u.keys = nil
	u.distances = nil
	u.streaming = false
	u.done = false
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
	newsearch := fulltext2.NewFulltext2Search(u.tblcfg)
	q := fulltext2.Fulltext2Query{
		Pattern:     []byte(pattern),
		Boolean:     mode == int64(tree.FULLTEXT_BOOLEAN),
		BagOfWords:  mode == int64(tree.FULLTEXT_BM25),
		Algo:        fulltext2ScoreAlgo(proc),
		FilterBytes: u.filterBytes,
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
		rt := vectorindex.RuntimeConfig{Emit: func(keys []any, dists []float64) error {
			select {
			case u.streamCh <- ft2StreamBatch{keys: keys, distances: dists}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
		go func() {
			_, _, serr := veccache.Cache.Search(sp, u.tblcfg.IndexTable, newsearch, q, rt)
			u.errCh <- serr // buffered(1): send before close so call() reads it after drain
			close(u.streamCh)
		}()
		return nil
	}

	// With a pushed LIMIT: WAND top-K, returned all at once (bounded by the LIMIT).
	rt := vectorindex.RuntimeConfig{Limit: uint(u.limit)}
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
