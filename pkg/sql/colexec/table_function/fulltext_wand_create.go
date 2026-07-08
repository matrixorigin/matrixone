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
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/fulltext/wand"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var wand_runSql = sqlexec.RunSql

// fulltextWandCreateState builds a WAND retrieval index from a postings stream
// fed in (word, doc_id, tf) order — the engine-sorted/grouped query
// `SELECT word, doc_id, <cappedTf> FROM <postings> GROUP BY word, doc_id
// ORDER BY word, doc_id`. It accumulates into a wand.Builder and, at end(),
// serializes + persists the index (metadata + chunk rows) via SQL. Its own
// output is a single discarded status row (mirrors hnsw_create).
type fulltextWandCreateState struct {
	inited  bool
	tblcfg  wand.TableConfig
	builder *wand.Builder
	batch   *batch.Batch
}

func (u *fulltextWandCreateState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *fulltextWandCreateState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	u.batch.CleanOnlyData()
	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
}

func (u *fulltextWandCreateState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

// end finalizes the build and persists the index (idempotent: existing chunks
// for this id are deleted first).
func (u *fulltextWandCreateState) end(tf *TableFunction, proc *process.Process) error {
	if !u.inited || u.builder == nil {
		return nil
	}
	sqlproc := sqlexec.NewSqlProcess(proc)

	// Capacity is carried in the cfg by the compile layer (resolved from the index's immutable
	// max_index_capacity flat param), so the base splits at the same value every compaction
	// later reads. Fall back to the resolver only for an index built before the flat param
	// existed (cfg.Capacity == 0). Unresolved / 0 => a single unbounded base.
	capacity := u.tblcfg.Capacity
	if capacity == 0 {
		if rf := sqlproc.GetResolveVariableFunc(); rf != nil {
			if v, verr := rf("fulltext_max_index_capacity", true, false); verr == nil {
				if c, ok := v.(int64); ok {
					capacity = c
				}
			}
		}
	}

	// Split the compacted base into capacity-bounded sub-indexes (a single model when
	// the corpus fits within capacity), each stored under its own index_id so a large
	// corpus builds several tag=0 bases instead of one monolith.
	models := u.builder.FinishSegments(capacity)

	// Drop empty sub-models (a segment whose rows carried no searchable tokens). If the
	// whole corpus is empty, persist nothing — matching the single-index build.
	nonEmpty := models[:0]
	for _, m := range models {
		if m.NumTerms() == 0 {
			m.Free()
			continue
		}
		nonEmpty = append(nonEmpty, m)
	}
	if len(nonEmpty) == 0 {
		return nil
	}
	// Clear any previous tag=0 bases so the build is idempotent (the tag=1 CdcTail is
	// untouched — CREATE has no tail yet anyway).
	for _, s := range wand.DeleteAllBasesSqls(u.tblcfg) {
		res, err := wand_runSql(sqlproc, s)
		if err != nil {
			return err
		}
		res.Close()
	}

	// Synchronous CREATE build → the compacted main index (tag=0). Each sub-model is
	// spilled to a temp file and read via load_file, so keep the temps until the INSERTs
	// have run. A per-build-unique id prefix (index table + build ts) keeps concurrent /
	// repeated builds from writing colliding sub-index ids (mirrors HNSW's uid:n).
	ts := time.Now().UnixMicro()
	uid := fmt.Sprintf("%s:%d", u.tblcfg.IndexTable, ts)
	var cleanups []func()
	defer func() {
		for _, c := range cleanups {
			c()
		}
	}()
	for i, m := range nonEmpty {
		m.Id = wand.SubIndexId(uid, i)
		sqls, cleanup, err := m.ToInsertSqls(u.tblcfg, ts, 0)
		if err != nil {
			return err
		}
		cleanups = append(cleanups, cleanup)
		for _, s := range sqls {
			res, err := wand_runSql(sqlproc, s)
			if err != nil {
				return err
			}
			res.Close()
		}
	}
	// A fresh tag=0 was written (CREATE build) — evict any cached search index so the
	// next query reloads the new base(s) instead of the stale one held until the TTL.
	veccache.Cache.Remove(u.tblcfg.IndexTable)
	return nil
}

func fulltextWandCreatePrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &fulltextWandCreateState{}
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))
	return st, err
}

// start processes one postings row. argVecs: [0]=cfg(json const), [1]=word,
// [2]=doc_id(int64). One row == one token occurrence; the builder sums tf per
// (word, doc_id). The __DocLen BM25 length sentinel is skipped.
func (u *fulltextWandCreateState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {
	if !u.inited {
		cfgVec := tf.ctr.argVecs[0]
		if cfgVec.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "fulltext_wand_create: first argument (config) must be a string")
		}
		if !cfgVec.IsConst() {
			return moerr.NewInternalError(proc.Ctx, "fulltext_wand_create: config must be a string constant")
		}
		cfgstr := cfgVec.UnsafeGetStringAt(0)
		if len(cfgstr) == 0 {
			return moerr.NewInternalError(proc.Ctx, "fulltext_wand_create: config is empty")
		}
		if err = sonic.Unmarshal([]byte(cfgstr), &u.tblcfg); err != nil {
			return err
		}

		wordVec := tf.ctr.argVecs[1]
		if wordVec.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "fulltext_wand_create: second argument (word) must be a string")
		}
		// doc_id may be any source-pk type; the builder maps it to a dense ord
		// and the pk type is persisted for output decode + membership.
		docVec := tf.ctr.argVecs[2]
		u.builder = wand.NewBuilder(u.tblcfg.IndexTable, int32(docVec.GetType().Oid))
		u.batch = tf.createResultBatch()
		u.inited = true
	}

	u.batch.CleanOnlyData()

	wordVec := tf.ctr.argVecs[1]
	docVec := tf.ctr.argVecs[2]
	if wordVec.IsNull(uint64(nthRow)) || docVec.IsNull(uint64(nthRow)) {
		return nil
	}
	word := wordVec.GetStringAt(nthRow)
	if word == fulltext.DOC_LEN_WORD {
		return nil // BM25 doc-length sentinel, not a real term
	}
	pk := vector.GetAny(docVec, nthRow, false)
	return u.builder.Add(word, pk)
}
