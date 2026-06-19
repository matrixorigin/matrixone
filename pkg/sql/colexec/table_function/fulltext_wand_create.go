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
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/fulltext/wand"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
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
	model := u.builder.Finish()
	if model.NumTerms() == 0 {
		return nil // empty corpus — nothing to persist
	}

	sqlproc := sqlexec.NewSqlProcess(proc)
	for _, s := range wand.DeleteSqls(u.tblcfg, model.Id) {
		res, err := wand_runSql(sqlproc, s)
		if err != nil {
			return err
		}
		res.Close()
	}
	sqls, err := model.ToInsertSqls(u.tblcfg, time.Now().UnixMicro())
	if err != nil {
		return err
	}
	for _, s := range sqls {
		res, err := wand_runSql(sqlproc, s)
		if err != nil {
			return err
		}
		res.Close()
	}
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
