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
	"bytes"
	"fmt"
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/datalink"
	"github.com/matrixorigin/matrixone/pkg/bm25/wand"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var wand_runSql = sqlexec.RunSql

// bm25DocLenSentinel is the reserved word the classic postings pipeline uses to
// carry per-doc length; the bm25 builder tracks doc length itself, so an Add of
// this word is skipped. Kept identical to the classic fulltext sentinel so a
// postings-fed build (if ever used) stays compatible.
const bm25DocLenSentinel = "__DocLen"

// bm25CreateState builds a WAND retrieval index from a postings stream
// fed in (word, doc_id, tf) order — the engine-sorted/grouped query
// `SELECT word, doc_id, <cappedTf> FROM <postings> GROUP BY word, doc_id
// ORDER BY word, doc_id`. It accumulates into a wand.Builder and, at end(),
// serializes + persists the index (metadata + chunk rows) via SQL. Its own
// output is a single discarded status row (mirrors hnsw_create).
type bm25CreateState struct {
	inited  bool
	tblcfg  wand.TableConfig
	builder *wand.Builder
	batch   *batch.Batch
}

func (u *bm25CreateState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *bm25CreateState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	u.batch.CleanOnlyData()
	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
}

func (u *bm25CreateState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

// end finalizes the build and persists the index (idempotent: existing chunks
// for this id are deleted first).
func (u *bm25CreateState) end(tf *TableFunction, proc *process.Process) error {
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
	cleanups := make([]func(), 0, len(nonEmpty))
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

func bm25CreatePrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &bm25CreateState{}
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))
	return st, err
}

// start feeds one row into the builder. Two input shapes, selected by cfg.FromSource:
//   - postings mode (default): argVecs [0]=cfg, [1]=word, [2]=doc_id — one row is one
//     token occurrence; the builder sums tf per (word, doc_id), skipping __DocLen.
//   - source mode: argVecs [0]=cfg, [1]=pk, [2..]=text cols — the row is tokenized in-Go
//     (jieba, HMM=false) and every token is Add'd, so no separate postings table or
//     tokenize pass is needed. Builder.Add caps tf and tracks doc length internally, so
//     there is no __DocLen sentinel here.
func (u *bm25CreateState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {
	if !u.inited {
		cfgVec := tf.ctr.argVecs[0]
		if cfgVec.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "bm25_create: first argument (config) must be a string")
		}
		if !cfgVec.IsConst() {
			return moerr.NewInternalError(proc.Ctx, "bm25_create: config must be a string constant")
		}
		cfgstr := cfgVec.UnsafeGetStringAt(0)
		if len(cfgstr) == 0 {
			return moerr.NewInternalError(proc.Ctx, "bm25_create: config is empty")
		}
		if err = sonic.Unmarshal([]byte(cfgstr), &u.tblcfg); err != nil {
			return err
		}

		// The builder's pk type comes from the doc_id column (postings mode) or the pk
		// column (source mode); either is the source pk type.
		pkVec := tf.ctr.argVecs[2]
		if u.tblcfg.FromSource {
			pkVec = tf.ctr.argVecs[1]
		} else if tf.ctr.argVecs[1].GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "bm25_create: second argument (word) must be a string")
		}
		u.builder = wand.NewBuilder(u.tblcfg.IndexTable, int32(pkVec.GetType().Oid))
		u.batch = tf.createResultBatch()
		u.inited = true
	}

	u.batch.CleanOnlyData()

	if u.tblcfg.FromSource {
		return u.addSourceRow(tf, proc, nthRow)
	}

	wordVec := tf.ctr.argVecs[1]
	docVec := tf.ctr.argVecs[2]
	if wordVec.IsNull(uint64(nthRow)) || docVec.IsNull(uint64(nthRow)) {
		return nil
	}
	word := wordVec.GetStringAt(nthRow)
	if word == bm25DocLenSentinel {
		return nil // BM25 doc-length sentinel, not a real term
	}
	pk := vector.GetAny(docVec, nthRow, false)
	return u.builder.Add(word, pk)
}

// addSourceRow tokenizes one source row (argVecs [1]=pk, [2..]=text cols) with the
// retrieval jieba tokenizer and Add's every token to the builder. It mirrors
// fulltext_index_tokenize's retrieval branch (concat columns with '\n', datalink →
// plain text, HMM=false), minus the position/__DocLen bookkeeping the WAND builder
// does not need.
func (u *bm25CreateState) addSourceRow(tf *TableFunction, proc *process.Process, nthRow int) error {
	argVecs := tf.ctr.argVecs
	pkVec := argVecs[1]
	if pkVec.IsNull(uint64(nthRow)) {
		return nil
	}
	// Match fulltext_index_tokenize: if any text column is NULL the doc yields no tokens.
	for i := 2; i < len(argVecs); i++ {
		if argVecs[i].IsNull(uint64(nthRow)) {
			return nil
		}
	}
	var content bytes.Buffer
	for i := 2; i < len(argVecs); i++ {
		if i > 2 {
			content.WriteByte('\n')
		}
		data := argVecs[i].GetStringAt(nthRow)
		if types.T(tf.Args[i].Typ.Id) == types.T_datalink {
			dl, err := datalink.NewDatalink(data, proc)
			if err != nil {
				return err
			}
			b, err := dl.GetPlainText(proc)
			if err != nil {
				return err
			}
			content.Write(b)
		} else {
			content.WriteString(data)
		}
	}
	if content.Len() == 0 {
		return nil
	}
	jtok, err := tokenizer.SharedJiebaTokenizer(false)
	if err != nil {
		return err
	}
	pk := vector.GetAny(pkVec, nthRow, false)
	for t, terr := range jtok.Tokenize(content.Bytes()) {
		if terr != nil {
			return terr
		}
		slen := t.TokenBytes[0]
		if aerr := u.builder.Add(string(t.TokenBytes[1:slen+1]), pk); aerr != nil {
			return aerr
		}
	}
	return nil
}
