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
	"math"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/fulltext/wand"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// fulltextWandSearchState answers a retrieval-mode MATCH by running WAND top-K
// over the in-memory index (loaded + cached via VectorIndexCache), emitting
// (doc_id, score) rows. No SQL ORDER BY/LIMIT sort: the top-K is produced by
// the WAND walk itself. Mirrors hnsw_search.
type fulltextWandSearchState struct {
	inited      bool
	tblcfg      wand.TableConfig
	limit       uint64
	offset      int
	keys        []any // doc_id values of the source pk type
	distances   []float64
	filterBytes []byte // serialized docfilter membership (WHERE-clause prefilter), if any
	batch       *batch.Batch
}

func (u *fulltextWandSearchState) end(tf *TableFunction, proc *process.Process) error { return nil }

func (u *fulltextWandSearchState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *fulltextWandSearchState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	u.batch.CleanOnlyData()

	nkeys := len(u.keys)
	n := 0
	for i := u.offset; i < nkeys && n < 8192; i++ {
		vector.AppendAny(u.batch.Vecs[0], u.keys[i], false, proc.Mp())
		vector.AppendFixed[float64](u.batch.Vecs[1], u.distances[i], false, proc.Mp())
		n++
	}
	u.offset += n
	u.batch.SetRowCount(n)

	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
}

func (u *fulltextWandSearchState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

func fulltextWandSearchPrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &fulltextWandSearchState{}
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
func (u *fulltextWandSearchState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {
	if !u.inited {
		cfgVec := tf.ctr.argVecs[0]
		if cfgVec.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "fulltext_wand_search: first argument (config) must be a string")
		}
		if !cfgVec.IsConst() {
			return moerr.NewInternalError(proc.Ctx, "fulltext_wand_search: config must be a string constant")
		}
		cfgstr := cfgVec.UnsafeGetStringAt(0)
		if len(cfgstr) == 0 {
			return moerr.NewInternalError(proc.Ctx, "fulltext_wand_search: config is empty")
		}
		if err = sonic.Unmarshal([]byte(cfgstr), &u.tblcfg); err != nil {
			return err
		}
		patVec := tf.ctr.argVecs[1]
		if patVec.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "fulltext_wand_search: second argument (pattern) must be a string")
		}
		u.batch = tf.createResultBatch()
		u.inited = true
	}

	u.offset = 0
	u.keys = nil
	u.distances = nil
	u.batch.CleanOnlyData()

	patVec := tf.ctr.argVecs[1]
	if patVec.IsNull(uint64(nthRow)) {
		return nil
	}
	pattern := patVec.GetStringAt(nthRow)

	// Tokenize the query exactly as the index was built: jieba (HMM=false),
	// the same path fulltext_index_tokenize uses for the retrieval parser.
	patterns, err := fulltext.ParsePatternInNLMode(pattern, "retrieval")
	if err != nil {
		return err
	}
	terms := make([]string, 0, len(patterns))
	for _, p := range patterns {
		if p.Text != "" {
			terms = append(terms, p.Text)
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

	// limit 0 ("no pushed limit") means return all matches; a SORT above the
	// join bounds the final top-K. Use a large K so the WAND walk emits every
	// matching doc.
	limit := u.limit
	if limit == 0 {
		limit = math.MaxInt32
	}

	algo := wand.NewWandSearch(u.tblcfg)
	rt := vectorindex.RuntimeConfig{Limit: uint(limit)}
	q := wand.WandQuery{Terms: terms, FilterBytes: u.filterBytes}
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
