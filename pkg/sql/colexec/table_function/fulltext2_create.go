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

// fulltext2_create is the execution-side build TVF for the fulltext2 index (the
// positional analogue of bm25_create). CROSS APPLY'd over the source table by the
// compile layer, it tokenizes each row in execution — resolving datalink columns
// (fetch + PDF/DOCX plain text) and applying the ngram/gojieba/json parser — Add's
// the ordered terms to a fulltext2.Builder, and at end() splits the corpus into
// capacity-bounded tag=0 base segments and persists their chunk rows. Its own
// output is a single discarded status row (mirrors bm25_create / hnsw_create).
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
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var fulltext2_runSql = sqlexec.RunSql

// fulltext2CreateState builds a positional fulltext2 index from the source rows.
// argVecs: [0]=cfg(JSON const), [1]=pk, [2..]=indexed text columns.
type fulltext2CreateState struct {
	inited  bool
	tblcfg  fulltext2.TableConfig
	builder *fulltext2.Builder
	batch   *batch.Batch
}

func (u *fulltext2CreateState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *fulltext2CreateState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	u.batch.CleanOnlyData()
	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
}

func (u *fulltext2CreateState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

func fulltext2CreatePrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &fulltext2CreateState{}
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))
	return st, err
}

// start tokenizes source row nthRow and Add's its ordered terms to the builder.
func (u *fulltext2CreateState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {
	if !u.inited {
		cfgVec := tf.ctr.argVecs[0]
		if cfgVec.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "fulltext2_create: first argument (config) must be a string")
		}
		if !cfgVec.IsConst() {
			return moerr.NewInternalError(proc.Ctx, "fulltext2_create: config must be a string constant")
		}
		cfgstr := cfgVec.UnsafeGetStringAt(0)
		if len(cfgstr) == 0 {
			return moerr.NewInternalError(proc.Ctx, "fulltext2_create: config is empty")
		}
		if err = sonic.Unmarshal([]byte(cfgstr), &u.tblcfg); err != nil {
			return err
		}
		pkVec := tf.ctr.argVecs[1]
		u.builder = fulltext2.NewBuilder(u.tblcfg.IndexTable, int32(pkVec.GetType().Oid))
		u.batch = tf.createResultBatch()
		u.inited = true
	}

	u.batch.CleanOnlyData()

	pkVec := tf.ctr.argVecs[1]
	if pkVec.IsNull(uint64(nthRow)) {
		return nil
	}
	terms, err := u.rowTerms(tf, proc, nthRow)
	if err != nil {
		return err
	}
	if len(terms) == 0 {
		return nil
	}
	// Feed this doc's tokens contiguously and in order, each with its BYTE position
	// (positional phrase queries match by byte offset), mirroring the base build.
	pk := vector.GetAny(pkVec, nthRow, false)
	for _, w := range terms {
		if aerr := u.builder.Add(w.Word, w.Pos, pk); aerr != nil {
			return aerr
		}
	}
	return nil
}

// rowTerms tokenizes source row nthRow (columns argVecs[2..]) into ordered terms,
// applying the index's parser. datalink columns are resolved to plain text and
// json columns to their flattened values; a NULL column yields no tokens (matches
// the classic tokenizer's per-row NULL handling).
func (u *fulltext2CreateState) rowTerms(tf *TableFunction, proc *process.Process, nthRow int) ([]fulltext2.WordPos, error) {
	argVecs := tf.ctr.argVecs
	for i := 2; i < len(argVecs); i++ {
		if argVecs[i].IsNull(uint64(nthRow)) {
			return nil, nil
		}
	}

	jsonValue := fulltext2.IsJSONValueParser(u.tblcfg.Parser)
	var content bytes.Buffer
	if fulltext2.IsJSONParser(u.tblcfg.Parser) {
		for i := 2; i < len(argVecs); i++ {
			binary := argVecs[i].GetType().Oid == types.T_json
			var raw []byte
			if binary {
				raw = argVecs[i].GetRawBytesAt(nthRow)
			} else {
				raw = []byte(argVecs[i].GetStringAt(nthRow))
			}
			// json: ngram over space-joined values; json_value: each value is one whole
			// atomic token, so keep them '\n'-separated for jsonValueTokenize below.
			var ft []byte
			var err error
			if jsonValue {
				ft, err = fulltext2.FlattenJSONLines(raw, binary)
			} else {
				ft, err = fulltext2.FlattenJSON(raw, binary)
			}
			if err != nil {
				return nil, err
			}
			if content.Len() > 0 {
				content.WriteByte('\n')
			}
			content.Write(ft)
		}
	} else {
		for i := 2; i < len(argVecs); i++ {
			if content.Len() > 0 {
				content.WriteByte('\n')
			}
			data := argVecs[i].GetStringAt(nthRow)
			if types.T(tf.Args[i].Typ.Id) == types.T_datalink {
				dl, err := datalink.NewDatalink(data, proc)
				if err != nil {
					return nil, err
				}
				b, err := dl.GetPlainText(proc)
				if err != nil {
					return nil, err
				}
				content.Write(b)
			} else {
				content.WriteString(data)
			}
		}
	}
	if content.Len() == 0 {
		return nil, nil
	}

	// json_value indexes each whole '\n'-separated value as one atomic token (no ngram).
	if jsonValue {
		return fulltext2.JSONValueTokenize(content.Bytes()), nil
	}

	// json is indexed as ngram over its flattened values (SimpleTokenizer).
	var tok tokenizer.Tokenizer
	if fulltext2.IsJSONParser(u.tblcfg.Parser) {
		tok = tokenizer.NewSimpleTokenizer()
	} else {
		t, err := fulltext2.DocTokenizer(u.tblcfg.Parser)
		if err != nil {
			return nil, err
		}
		tok = t
	}

	var terms []fulltext2.WordPos
	for t, terr := range tok.Tokenize(content.Bytes()) {
		if terr != nil {
			return nil, terr
		}
		slen := t.TokenBytes[0]
		terms = append(terms, fulltext2.WordPos{Word: string(t.TokenBytes[1 : slen+1]), Pos: t.BytePos})
	}
	return terms, nil
}

// end splits the accumulated corpus into capacity-bounded tag=0 base segments and
// persists them (idempotent: existing tag=0 bases are cleared first).
func (u *fulltext2CreateState) end(tf *TableFunction, proc *process.Process) error {
	if !u.inited || u.builder == nil || u.builder.NumDocs() == 0 {
		return nil
	}
	sqlproc := sqlexec.NewSqlProcess(proc)

	// A per-build-unique id prefix (index table + build ts) keeps concurrent /
	// repeated builds from colliding on sub-index ids (mirrors bm25 / HNSW).
	segs, err := u.builder.FinishSegments(u.tblcfg.Capacity)
	if err != nil {
		return err
	}
	if len(segs) == 0 {
		return nil
	}

	// Clear any previous tag=0 bases so the build is idempotent (the tag=1 tail is
	// untouched — CREATE has no tail yet).
	for _, s := range fulltext2.DeleteAllBasesSqls(u.tblcfg) {
		res, err := fulltext2_runSql(sqlproc, s)
		if err != nil {
			return err
		}
		res.Close()
	}

	// A per-build-unique id prefix (index table + build ts) keeps concurrent /
	// repeated builds from colliding on sub-index ids (mirrors bm25_create / HNSW).
	ts := time.Now().UnixMicro()
	uid := fmt.Sprintf("%s:%d", u.tblcfg.IndexTable, ts)
	cleanups := make([]func(), 0, len(segs))
	defer func() {
		for _, c := range cleanups {
			c()
		}
	}()
	for i, seg := range segs {
		seg.Id = fulltext2.SubIndexId(uid, i)
		sqls, cleanup, err := seg.ToInsertSqls(u.tblcfg, ts, 0 /* tag=0 base */)
		if err != nil {
			return err
		}
		cleanups = append(cleanups, cleanup)
		for _, s := range sqls {
			res, err := fulltext2_runSql(sqlproc, s)
			if err != nil {
				return err
			}
			res.Close()
		}
	}
	// A fresh tag=0 was written (CREATE build, or a REBUILD reusing this TVF) — evict
	// any cached search index so the next query reloads the new base(s) instead of the
	// stale one held until the TTL. Local to this CN's cache.
	veccache.Cache.Remove(u.tblcfg.IndexTable)
	return nil
}
