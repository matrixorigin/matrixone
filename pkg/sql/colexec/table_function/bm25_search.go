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

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/bm25/wand"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// bm25SearchState answers a retrieval-mode MATCH by running WAND top-K
// over the in-memory index (loaded + cached via VectorIndexCache), emitting
// (doc_id, score) rows. No SQL ORDER BY/LIMIT sort: the top-K is produced by
// the WAND walk itself. Mirrors hnsw_search.
type bm25SearchState struct {
	inited      bool
	tblcfg      wand.TableConfig
	limit       uint64
	offset      int
	keys        []any // doc_id values of the source pk type
	distances   []float64
	filterBytes []byte // serialized docfilter membership (WHERE-clause prefilter), if any
	batch       *batch.Batch

	// Streaming no-LIMIT path (u.limit == 0): rather than materialize every matching
	// doc, a producer goroutine runs the WAND search with an Emit callback that hands
	// bounded batches to streamCh; call() drains one batch per invocation and the
	// upstream ORDER BY score node ranks them. cancel stops the producer (and releases
	// the cache read-lock it holds) if the consumer aborts early.
	streaming bool
	streamCh  chan wandStreamBatch
	errCh     chan error
	cancel    context.CancelFunc
	done      bool
}

// wandStreamBatch is one emitted batch (<= streamBatch rows); the producer hands
// ownership to the consumer, so the slices are not reused.
type wandStreamBatch struct {
	keys      []any
	distances []float64
}

func (u *bm25SearchState) end(tf *TableFunction, proc *process.Process) error { return nil }

func (u *bm25SearchState) reset(tf *TableFunction, proc *process.Process) {
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

// stopStream cancels the producer goroutine (if streaming) and drains streamCh
// until the producer closes it, so no goroutine — nor the cache read-lock it holds
// — leaks past this query. Idempotent; a no-op when not streaming.
func (u *bm25SearchState) stopStream() {
	if u.cancel == nil {
		return
	}
	u.cancel() // unblocks the producer's Emit (its select sees ctx.Done())
	if u.streamCh != nil {
		for range u.streamCh { // drain to the producer's close()
		}
	}
	u.cancel = nil
	u.streamCh = nil
}

func (u *bm25SearchState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	u.batch.CleanOnlyData()
	// The projection may request only doc_id (1 column, e.g. COUNT(*) or a bare
	// WHERE match) or doc_id+score (2 columns) — mirror fulltext_index_scan and
	// only emit score when the batch has it.
	withScore := u.batch.VectorCount() > 1

	if u.streaming {
		if u.done {
			return vm.CancelResult, nil
		}
		select {
		case b, ok := <-u.streamCh:
			if !ok {
				// producer finished; surface any search error. errCh is sent before
				// the channel is closed, so it is ready here.
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
					vector.AppendFixed[float64](u.batch.Vecs[1], b.distances[i], false, proc.Mp())
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
			vector.AppendFixed[float64](u.batch.Vecs[1], u.distances[i], false, proc.Mp())
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

func (u *bm25SearchState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	u.stopStream()
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

func bm25SearchPrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &bm25SearchState{}
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))

	// Top-K limit, pushed down onto the node by the planner (apply_indices).
	// When absent (e.g. the LIMIT lives on a SORT above the join), leave it 0 —
	// the search then returns all matches and the SORT bounds them, matching
	// fulltext_index_scan. Do NOT default to 1.
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
func (u *bm25SearchState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {
	if !u.inited {
		cfgVec := tf.ctr.argVecs[0]
		if cfgVec.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "bm25_search: first argument (config) must be a string")
		}
		if !cfgVec.IsConst() {
			return moerr.NewInternalError(proc.Ctx, "bm25_search: config must be a string constant")
		}
		cfgstr := cfgVec.UnsafeGetStringAt(0)
		if len(cfgstr) == 0 {
			return moerr.NewInternalError(proc.Ctx, "bm25_search: config is empty")
		}
		if err = sonic.Unmarshal([]byte(cfgstr), &u.tblcfg); err != nil {
			return err
		}
		patVec := tf.ctr.argVecs[1]
		if patVec.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "bm25_search: second argument (pattern) must be a string")
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

	// Tokenize the query exactly as the index was built: jieba (HMM=false),
	// the same tokenizer bm25_create uses to build the index.
	jtok, err := tokenizer.SharedJiebaTokenizer(false)
	if err != nil {
		return err
	}
	terms := make([]string, 0, 8)
	for t, terr := range jtok.Tokenize([]byte(pattern)) {
		if terr != nil {
			return terr
		}
		slen := t.TokenBytes[0]
		if w := string(t.TokenBytes[1 : slen+1]); w != "" {
			terms = append(terms, w)
		}
	}
	if len(terms) == 0 {
		return nil // empty query → no hits
	}

	// Prefilter pushdown: when the WHERE clause is pushed down as a unique-join-
	// keys runtime filter, wait for it and build the docfilter membership bytes —
	// the same mechanism fulltext_index_scan uses. Applied inside the WAND walk
	// so the returned top-K is already filtered (no over-fetch).
	if u.filterBytes == nil && len(tf.RuntimeFilterSpecs) > 0 {
		res, ferr := waitFulltextMembershipFilter(proc, tf.RuntimeFilterSpecs)
		if ferr != nil {
			return ferr
		}
		if res != nil {
			u.filterBytes = res.membershipFilterBytes
		}
	}

	veccache.Cache.Once()

	algo := wand.NewWandSearch(u.tblcfg)
	q := wand.WandQuery{Terms: terms, FilterBytes: u.filterBytes}

	if u.limit == 0 {
		// No pushed LIMIT: STREAM every matching doc in bounded batches (no top-K
		// heap, no materialization of the whole result set). A producer goroutine runs
		// the search with an Emit callback that hands batches to streamCh; call() drains
		// one per invocation and the upstream ORDER BY score node ranks. cancel/ctx let
		// reset()/free() stop the producer and release the cache read-lock it holds.
		u.streaming = true
		u.streamCh = make(chan wandStreamBatch, 4)
		u.errCh = make(chan error, 1)
		ctx, cancel := context.WithCancel(proc.Ctx)
		u.cancel = cancel
		rt := vectorindex.RuntimeConfig{Emit: func(keys []any, dists []float64) error {
			select {
			case u.streamCh <- wandStreamBatch{keys: keys, distances: dists}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
		sp := sqlexec.NewSqlProcess(proc)
		go func() {
			_, _, serr := veccache.Cache.Search(sp, u.tblcfg.IndexTable, algo, q, rt)
			u.errCh <- serr // buffered(1): send before close so call() reads it after drain
			close(u.streamCh)
		}()
		return nil
	}

	// With a pushed LIMIT: WAND top-K, returned all at once (bounded by the LIMIT).
	rt := vectorindex.RuntimeConfig{Limit: uint(u.limit)}
	keys, dists, err := veccache.Cache.Search(sqlexec.NewSqlProcess(proc), u.tblcfg.IndexTable, algo, q, rt)
	if err != nil {
		return err
	}
	ks, ok := keys.([]any)
	if !ok {
		return moerr.NewInternalError(proc.Ctx, "wand search: keys is not []any")
	}
	u.keys = ks
	u.distances = dists
	return nil
}
