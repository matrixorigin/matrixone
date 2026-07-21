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
	inited bool
	tblcfg fulltext2.TableConfig
	// Streaming base build: only ONE open segment is held in memory. It is sealed +
	// persisted (and its postings freed) the moment it reaches `capacity` docs, so
	// peak build memory is one capacity-bounded segment rather than the whole corpus.
	cur          *fulltext2.Builder // current open segment (nil after a seal, recreated on the next row)
	capacity     int64              // docs per sealed segment (floored to DefaultBuildCapacity)
	postingCap   int64              // postings per sealed segment (floored to DefaultPostingCapacity)
	pkType       int32
	bopts        []fulltext2.BuildOpt
	uid          string // per-build-unique sub-index id prefix (IndexTable:ts)
	ts           int64
	segIdx       int  // next sub-index id == count of segments sealed so far
	basesCleared bool // DeleteAllBases run once, before the first sealed segment
	batch        *batch.Batch
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
		u.pkType = int32(pkVec.GetType().Oid)
		if u.tblcfg.PositionFree {
			u.bopts = append(u.bopts, fulltext2.WithPositionFree())
		}
		// Floor both caps so a segment is sealed+spilled every ~capacity docs OR
		// ~postingCap postings even when the WITH options are unset, keeping build
		// memory bounded regardless of doc size (a long-doc corpus seals on postings).
		u.capacity = u.tblcfg.Capacity
		if u.capacity <= 0 {
			u.capacity = fulltext2.DefaultBuildCapacity
		}
		u.postingCap = u.tblcfg.PostingCapacity
		if u.postingCap <= 0 {
			u.postingCap = fulltext2.DefaultPostingCapacity
		}
		// A per-build-unique id prefix (index table + build ts) keeps concurrent /
		// repeated builds from colliding on sub-index ids (mirrors bm25 / HNSW).
		u.ts = time.Now().UnixMicro()
		u.uid = fmt.Sprintf("%s:%d", u.tblcfg.IndexTable, u.ts)
		u.cur = fulltext2.NewBuilder(u.uid, u.pkType, u.bopts...)
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
		if aerr := u.cur.Add(w.Word, w.Pos, pk); aerr != nil {
			return aerr
		}
	}
	// A document's tokens are fed contiguously, so once the open segment reaches
	// capacity DISTINCT docs (or its posting cap) the current doc is complete: seal +
	// persist it as a tag=0 base and start a fresh segment, freeing the sealed postings.
	if fulltext2.ReachedSegmentCap(u.cur, u.capacity, u.postingCap) {
		if err := u.sealSegment(proc); err != nil {
			return err
		}
		u.cur = fulltext2.NewBuilder(u.uid, u.pkType, u.bopts...)
	}
	return nil
}

// sealSegment finalizes the open builder into one tag=0 base sub-index and persists
// it, then drops it so its postings are freed. Prior bases are cleared once, before
// the first sealed segment, so a REBUILD reusing this TVF stays idempotent (the tag=1
// tail is untouched). A no-op when the open segment holds no docs.
func (u *fulltext2CreateState) sealSegment(proc *process.Process) (err error) {
	if u.cur == nil || u.cur.NumDocs() == 0 {
		u.cur = nil
		return nil
	}
	seg, err := u.cur.Finish()
	u.cur = nil
	if err != nil {
		return err
	}
	sqlproc := sqlexec.NewSqlProcess(proc)
	if !u.basesCleared {
		for _, s := range fulltext2.DeleteAllBasesSqls(u.tblcfg) {
			res, e := fulltext2_runSql(sqlproc, s)
			if e != nil {
				return e
			}
			res.Close()
		}
		u.basesCleared = true
	}
	seg.Id = fulltext2.SubIndexId(u.uid, u.segIdx)
	u.segIdx++
	sqls, cleanup, err := seg.ToInsertSqls(u.tblcfg, u.ts, 0 /* tag=0 base */)
	if err != nil {
		return err
	}
	defer cleanup() // remove the spill file once its INSERTs have run — bounds temp disk too
	for _, s := range sqls {
		res, e := fulltext2_runSql(sqlproc, s)
		if e != nil {
			return e
		}
		res.Close()
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

	// token count is unknown up front; estimate from input length to avoid regrowth.
	terms := make([]fulltext2.WordPos, 0, content.Len()/4)
	for t, terr := range tok.Tokenize(content.Bytes()) {
		if terr != nil {
			return nil, terr
		}
		slen := t.TokenBytes[0]
		terms = append(terms, fulltext2.WordPos{Word: string(t.TokenBytes[1 : slen+1]), Pos: t.BytePos})
	}
	return terms, nil
}

// end seals the final open segment (the trailing docs below capacity). Base
// clearing and per-segment persistence already happened incrementally as segments
// filled during the feed, so peak memory never exceeded one open segment.
func (u *fulltext2CreateState) end(tf *TableFunction, proc *process.Process) error {
	if !u.inited {
		return nil
	}
	if err := u.sealSegment(proc); err != nil {
		return err
	}
	// Nothing was persisted (empty corpus): leave existing tag=0 bases untouched,
	// mirroring the prior zero-doc no-op.
	if u.segIdx == 0 {
		return nil
	}
	// A fresh tag=0 was written (CREATE build, or a REBUILD reusing this TVF) — evict
	// any cached search index so the next query reloads the new base(s) instead of the
	// stale one held until the TTL. Local to this CN's cache.
	veccache.Cache.Remove(u.tblcfg.IndexTable)
	return nil
}
