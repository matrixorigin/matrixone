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
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/datalink"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/fulltext2/tokenfilter"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
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
	filters      []tokenfilter.TokenFilter // token_filter chain (stop), applied at tokenize
	bopts        []fulltext2.BuildOpt
	uid          string // per-build-unique sub-index id prefix (IndexTable:ts)
	ts           int64
	segIdx       int  // next sub-index id == count of segments sealed so far
	basesCleared bool // DeleteAllBases run once, before the first sealed segment
	batch        *batch.Batch
	// threads is the resolved fulltext2_threads_build; above 1 the rows go through pipe,
	// which tokenizes in parallel and finishes sealed segments in the background.
	threads int
	pipe    *fulltext2.BuildPipeline
}

func (u *fulltext2CreateState) reset(tf *TableFunction, proc *process.Process) {
	u.abortPipeline()
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *fulltext2CreateState) abortPipeline() {
	if u.pipe != nil {
		u.pipe.Abort()
		u.pipe = nil
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
	u.abortPipeline()
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
		if u.filters, err = tokenfilter.Build(u.tblcfg.TokenFilters); err != nil {
			return err
		}
		pkVec := tf.ctr.argVecs[1]
		u.pkType = int32(pkVec.GetType().Oid)
		if u.tblcfg.PositionFree {
			u.bopts = append(u.bopts, fulltext2.WithPositionFree())
		}
		if len(u.tblcfg.IncludeTypes) > 0 {
			u.bopts = append(u.bopts, fulltext2.WithIncludeTypes(u.tblcfg.IncludeTypes))
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
		indexIdentity := u.tblcfg.IndexTable
		// Session generations can make a hidden relation name much longer than
		// a user identifier. Segment IDs are opaque varchar(128) keys; bound
		// their prefix while retaining the complete physical identity. Reserve
		// room for the timestamp and segment ordinal (up to 20 digits each).
		if len(indexIdentity) > 64 {
			indexIdentity = fmt.Sprintf("%x", sha256.Sum256([]byte(indexIdentity)))
		}
		u.uid = fmt.Sprintf("%s:%d", indexIdentity, u.ts)
		u.cur = fulltext2.NewBuilder(u.uid, u.pkType, u.bopts...)
		u.batch = tf.createResultBatch()
		u.threads = int(vectorindex.GetConcurrencyForBuild(u.tblcfg.ThreadsBuild))
		u.inited = true
	}

	u.batch.CleanOnlyData()

	pkVec := tf.ctr.argVecs[1]
	if pkVec.IsNull(uint64(nthRow)) {
		return nil
	}
	if u.threads > 1 {
		return u.addToPipeline(tf, proc, nthRow)
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
	// Attach this row's INCLUDE column values (actual values → segment docmap for
	// prefilter + coverage). Only for docs the Add loop created, i.e. rows with
	// searchable content (a no-searchable-content row is skipped above), consistent
	// with the CDC/build empty-doc skip.
	if len(u.tblcfg.IncludeTypes) > 0 {
		u.cur.SetInclude(pk, u.rowInclude(tf, nthRow))
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
	if err = u.clearBasesOnce(sqlproc); err != nil {
		return err
	}
	seg.Id = fulltext2.SubIndexId(u.uid, u.segIdx)
	u.segIdx++
	return seg.Persist(sqlproc, u.tblcfg, u.ts, 0 /* tag=0 base */, sqlproc.BuildSnapshotTS())
}

// clearBasesOnce deletes the prior tag=0 bases before the first segment is written.
func (u *fulltext2CreateState) clearBasesOnce(sqlproc *sqlexec.SqlProcess) error {
	if u.basesCleared {
		return nil
	}
	for _, s := range fulltext2.DeleteAllBasesSqls(u.tblcfg) {
		res, e := fulltext2_runSql(sqlproc, s)
		if e != nil {
			return e
		}
		res.Close()
	}
	u.basesCleared = true
	return nil
}

// fulltext2BuildMaxFinishing bounds the segments finished in the background at once.
func fulltext2BuildMaxFinishing(threads int) int {
	if threads >= 4 {
		return 2
	}
	return 1
}

func (u *fulltext2CreateState) newBuildPipeline(proc *process.Process) *fulltext2.BuildPipeline {
	// Segment ids continue from the segments already written by this state, like the serial path.
	base := u.segIdx
	return fulltext2.NewBuildPipeline(fulltext2.BuildPipelineConfig{
		Threads:      u.threads,
		MaxFinishing: fulltext2BuildMaxFinishing(u.threads),
		Capacity:     u.capacity,
		PostingCap:   u.postingCap,
		NewBuilder: func() *fulltext2.Builder {
			return fulltext2.NewBuilder(u.uid, u.pkType, u.bopts...)
		},
		SegmentID: func(i int) string { return fulltext2.SubIndexId(u.uid, base+i) },
		Tokenize:  u.tokenizeContent,
		SpillDir:  fulltext2.LocalSpillDir(proc.Ctx, proc.Base.FileService),
		Write: func(spilled *fulltext2.SpilledSegment) error {
			sqlproc := sqlexec.NewSqlProcess(proc)
			if err := u.clearBasesOnce(sqlproc); err != nil {
				return err
			}
			if err := spilled.Write(sqlproc, u.tblcfg, u.ts, 0 /* tag=0 base */, sqlproc.BuildSnapshotTS()); err != nil {
				return err
			}
			u.segIdx++
			return nil
		},
	})
}

// addToPipeline queues row nthRow with owned copies of its text, pk and INCLUDE values.
func (u *fulltext2CreateState) addToPipeline(tf *TableFunction, proc *process.Process, nthRow int) error {
	if u.pipe == nil {
		u.cur = nil
		u.pipe = u.newBuildPipeline(proc)
	}
	content, terms, direct, err := u.rowContent(tf, proc, nthRow)
	if err != nil {
		return err
	}
	if (direct && len(terms) == 0) || (!direct && len(content) == 0) {
		return nil
	}
	pk := vector.GetAny(tf.ctr.argVecs[1], nthRow, false)
	if b, ok := pk.([]byte); ok {
		pk = append([]byte(nil), b...)
	}
	doc := fulltext2.BuildDoc{PK: pk, Content: content, Terms: terms}
	if len(u.tblcfg.IncludeTypes) > 0 {
		doc.Include = u.rowInclude(tf, nthRow)
		doc.HasInclude = true
	}
	return u.pipe.Add(doc)
}

// rowInclude reads row nthRow's INCLUDE column values (the trailing argVecs after the text
// columns) in column order. A NULL value stays nil. A varchar value is a view into the
// batch vector, which is recycled across batches while the builder accumulates — so copy
// []byte out; scalar types are returned by value and need no copy.
func (u *fulltext2CreateState) rowInclude(tf *TableFunction, nthRow int) []any {
	n := len(u.tblcfg.IncludeTypes)
	if n == 0 {
		return nil
	}
	argVecs := tf.ctr.argVecs
	start := len(argVecs) - n
	out := make([]any, n)
	for j := 0; j < n; j++ {
		v := argVecs[start+j]
		if v.IsNull(uint64(nthRow)) {
			continue // NULL
		}
		val := vector.GetAny(v, nthRow, false)
		if b, ok := val.([]byte); ok {
			val = append([]byte(nil), b...) // copy: the batch vector is recycled across batches
		}
		out[j] = val
	}
	return out
}

// fulltext2SourceColumnBytes keeps source admission before every parser, and
// shares the resolved Datalink between classification and actual acquisition.
func fulltext2SourceColumnBytes(vec *vector.Vector, proc *process.Process, row int) ([]byte, bool, error) {
	if vec.GetType().Oid == types.T_datalink {
		dl, err := datalink.NewAsyncDatalink(vec.GetStringAt(row), proc)
		if err != nil {
			return nil, false, err
		}
		text, err := dl.GetPlainText(proc)
		return text, false, err
	}
	if vec.GetType().Oid == types.T_json {
		return vec.GetRawBytesAt(row), true, nil
	}
	return []byte(vec.GetStringAt(row)), false, nil
}

// rowTerms tokenizes source row nthRow (columns argVecs[2..]) into ordered terms,
// applying the index's parser. datalink columns are resolved to plain text and
// json columns to their flattened values; a SQL NULL column yields no tokens
// while other columns in the row continue to contribute terms.
func (u *fulltext2CreateState) rowTerms(tf *TableFunction, proc *process.Process, nthRow int) ([]fulltext2.WordPos, error) {
	content, terms, direct, err := u.rowContent(tf, proc, nthRow)
	if err != nil || direct {
		return terms, err
	}
	if len(content) == 0 {
		return nil, nil
	}
	return u.tokenizeContent(content)
}

// rowContent returns source row nthRow's text to tokenize, or with direct set the terms the
// parser produces without a tokenizer (json tuple terms, json_value values). It reads the
// argument vectors, so it runs on the operator goroutine; the returned slices are owned.
func (u *fulltext2CreateState) rowContent(tf *TableFunction, proc *process.Process, nthRow int) (content []byte, terms []fulltext2.WordPos, direct bool, err error) {
	argVecs := tf.ctr.argVecs
	// Text columns are argVecs[2 : textEnd); the trailing len(IncludeTypes) args are INCLUDE
	// columns (their values are stored verbatim, NOT tokenized). A SQL NULL text column
	// contributes no terms; the other indexed columns in the same row remain searchable.
	textEnd := len(argVecs) - len(u.tblcfg.IncludeTypes)

	// The tuple json breaker must run BEFORE any flatten: flattening joins the
	// leaf values and throws the keys away, and the keys are the whole point.
	// This is the create-side half of the CREATE/ISCP pair — the ISCP writer
	// calls the same per-column encoder (see Fulltext2SqlWriter.rowText).
	if u.tblcfg.UsesJSONTupleTerms() {
		opt := u.tblcfg.JSONTermOptions()
		for i := 2; i < textEnd; i++ {
			if argVecs[i].IsNull(uint64(nthRow)) {
				continue
			}

			raw, binary, err := fulltext2SourceColumnBytes(argVecs[i], proc, nthRow)
			if err != nil {
				return nil, nil, false, err
			}
			ts, err := fulltext2.JSONTupleColumnTerms(raw, binary, opt)
			if err != nil {
				return nil, nil, false, err
			}
			for _, t := range ts {
				// ordinal position: a tuple term has no meaningful byte offset,
				// and ISCP numbers them the same way so the two agree.
				terms = append(terms, fulltext2.WordPos{Word: t, Pos: int32(len(terms))})
			}
		}
		return nil, terms, true, nil
	}

	jsonValue := fulltext2.IsJSONValueParser(u.tblcfg.Parser)
	var buf bytes.Buffer
	if fulltext2.IsJSONParser(u.tblcfg.Parser) {
		for i := 2; i < textEnd; i++ {
			if argVecs[i].IsNull(uint64(nthRow)) {
				continue
			}

			raw, binary, err := fulltext2SourceColumnBytes(argVecs[i], proc, nthRow)
			if err != nil {
				return nil, nil, false, err
			}
			// json: ngram over space-joined values; json_value: each value is one whole
			// atomic token, so keep them '\n'-separated for jsonValueTokenize below.
			var ft []byte
			if jsonValue {
				ft, err = fulltext2.FlattenJSONLines(raw, binary)
			} else {
				ft, err = fulltext2.FlattenJSON(raw, binary)
			}
			if err != nil {
				return nil, nil, false, err
			}
			if buf.Len() > 0 {
				buf.WriteByte('\n')
			}
			buf.Write(ft)
		}
	} else {
		for i := 2; i < textEnd; i++ {
			if argVecs[i].IsNull(uint64(nthRow)) {
				continue
			}
			if buf.Len() > 0 {
				buf.WriteByte('\n')
			}
			data := argVecs[i].GetStringAt(nthRow)
			if types.T(tf.Args[i].Typ.Id) == types.T_datalink {
				b, _, err := fulltext2SourceColumnBytes(argVecs[i], proc, nthRow)
				if err != nil {
					return nil, nil, false, err
				}
				buf.Write(b)
			} else {
				buf.WriteString(data)
			}
		}
	}
	if buf.Len() == 0 {
		return nil, nil, false, nil
	}

	// json_value indexes each whole '\n'-separated value as one atomic token (no ngram).
	if jsonValue {
		return nil, fulltext2.JSONValueTokenize(buf.Bytes()), true, nil
	}
	return buf.Bytes(), nil, false, nil
}

// tokenizeContent tokenizes a row's text with the index parser and token_filter chain. It
// reads only state fixed at init, so the pipelined build calls it from worker goroutines.
func (u *fulltext2CreateState) tokenizeContent(content []byte) ([]fulltext2.WordPos, error) {
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
	// Apply the token_filter chain (stop) so build drops the same tokens the query
	// drops. Empty filters ⇒ tok unchanged.
	tok = tokenfilter.Apply(tok, u.filters)

	// token count is unknown up front; estimate from input length to avoid regrowth.
	terms := make([]fulltext2.WordPos, 0, len(content)/4)
	for t, terr := range tok.Tokenize(content) {
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
	if u.pipe != nil {
		if _, err := u.pipe.Close(); err != nil {
			u.abortPipeline()
			return err
		}
		u.pipe = nil
	} else if err := u.sealSegment(proc); err != nil {
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
