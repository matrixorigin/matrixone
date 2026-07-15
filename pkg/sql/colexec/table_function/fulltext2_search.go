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
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// fulltext2SearchState answers a MATCH over a fulltext2 index: it loads the
// index's segments (base + CDC tail), runs the WAND positional query, and emits
// (doc_id, score) rows. Step-4 first cut: load-and-materialize each query (no
// VectorIndexCache yet); the top-k is bounded by the pushed LIMIT.
type fulltext2SearchState struct {
	inited    bool
	tblcfg    fulltext2.TableConfig
	limit     uint64
	offset    int
	keys      []any
	distances []float64
	batch     *batch.Batch
}

func (u *fulltext2SearchState) end(tf *TableFunction, proc *process.Process) error { return nil }

func (u *fulltext2SearchState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
	u.offset = 0
	u.keys = nil
	u.distances = nil
}

func (u *fulltext2SearchState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

func (u *fulltext2SearchState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	u.batch.CleanOnlyData()
	withScore := u.batch.VectorCount() > 1

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

	u.offset = 0
	u.keys = nil
	u.distances = nil
	u.batch.CleanOnlyData()

	patVec := tf.ctr.argVecs[1]
	if patVec.IsNull(uint64(nthRow)) {
		return nil
	}
	pattern := patVec.GetStringAt(nthRow)

	// Load the index (base + CDC tail) and run the query. Build+query must
	// tokenize identically — both use the index's parser (step-4: SimpleTokenizer).
	sp := sqlexec.NewSqlProcess(proc)
	bases, lerr := fulltext2.LoadAllBases(sp, u.tblcfg)
	if lerr != nil {
		return lerr
	}
	tails, _, terr := fulltext2.LoadTailSegments(sp, u.tblcfg)
	if terr != nil {
		return terr
	}
	segs := append(bases, tails...)
	if len(segs) == 0 {
		return nil // empty index → no hits
	}
	idx := fulltext2.NewIndex(segs, nil)

	k := int(u.limit)
	if k <= 0 {
		k = int(idx.NumDocs())
	}
	// mode (argVecs[2], a query const): boolean → operator query, else NL phrase.
	var mode int64
	if mv := tf.ctr.argVecs[2]; mv != nil && mv.Length() > 0 {
		mode = vector.GetFixedAtNoTypeCheck[int64](mv, 0)
	}
	algo := fulltext2ScoreAlgo(proc)
	results, err := idx.SearchQuery([]byte(pattern), mode == int64(tree.FULLTEXT_BOOLEAN), u.tblcfg.Parser, algo, k)
	if err != nil {
		return err
	}
	u.keys = make([]any, len(results))
	u.distances = make([]float64, len(results))
	for i, r := range results {
		u.keys[i] = r.Pk
		u.distances[i] = r.Score
	}
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
