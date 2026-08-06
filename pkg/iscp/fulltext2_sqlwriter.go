// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iscp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const defaultFulltext2Capacity int64 = 1000000
const defaultFulltext2PostingCapacity int64 = 8_000_000

// Fulltext2SqlWriter is the ISCP sink adapter for the fulltext2 positional index.
// Like WandSqlWriter it is model-building: it accumulates one flush's CDC rows
// into a binary fulltext2.Cdc blob (ToSql), which RunFulltext2 decodes, tokenizes
// (per the index parser), and turns into tag=1 CdcTail frames. Binary (typed pk)
// because a fulltext2 pk is `any` (int64 OR varchar).
type Fulltext2SqlWriter struct {
	cfg       fulltext2.TableConfig // DbName + ftv2_index (storage) + ftv2_meta (metadata) + parser
	pkType    int32                 // types.T of the source primary key
	pkPos     int32                 // pk column index in the extracted row
	textPos   []int32               // indexed text columns (all idxdef.Parts) — multi-column joins with '\n'
	textTypes []int32               // types.T of each textPos column (to detect T_datalink), textPos-aligned
	// INCLUDE columns: their positions in the extracted row + types.T (column order), so a
	// CDC insert/upsert carries their actual values into the tail segment's docmap.
	includePos   []int32
	includeTypes []int32
	capacity     int64 // max docs per delta segment (max_index_capacity)
	postingCap   int64 // max postings per delta segment (max_postings_capacity)

	// cnEngine/cnTxnClient/cnUUID are the CDC consumer's handles (set by NewIndexConsumer),
	// used to open a short txn with a FULL sqlproc so a DATALINK column can be resolved to
	// its file CONTENT during CDC — catalog-backed stage:// resolution + PDF/DOCX text
	// extraction, exactly as the sync build does (fulltext2_create GetPlainText). A minimal
	// FileService-only proc is NOT enough: stage:// needs catalog/txn. nil (e.g. unit tests)
	// ⇒ datalink columns index the URL string.
	cnEngine    engine.Engine
	cnTxnClient client.TxnClient
	cnUUID      string
	datalinkPos bool // any textPos column is a datalink
	// srcAccountID is the SOURCE tenant that owns the indexed table (r.GetAccountID()),
	// set per-iteration by IndexConsumer.Consume. A datalink stage:// is a per-account
	// catalog object, so its resolution MUST run under this tenant — NOT the consumer
	// ctx's tenant, which ISCP forces to System_Account (iteration.go), which would look
	// the stage up in the wrong tenant's mo_stages.
	srcAccountID uint32

	cdc   *fulltext2.Cdc
	ndata int
	last  string
}

// datalinkText resolves a datalink column to its file's plain text (PDF/DOCX extracted),
// exactly as the sync build does in fulltext2_create.rowTerms, so a datalink column is
// indexed by CONTENT consistently whether a row arrives at CREATE or via CDC. It opens a
// short txn to get a FULL sqlproc, then resolves via SELECT load_text(cast(url as datalink))
// run by the internal SQL executor (which has a real proc: FileService + catalog for
// stage://). The bare background sqlproc has Proc==nil, so calling datalink.GetPlainText
// directly panics — hence the SQL round-trip. load_text (not load_file) returns the
// EXTRACTED plain text (PDF/DOCX parsed), the same text the synchronous build indexes, so
// CDC and build tokenize identically.
//
// On any resolution error the error is PROPAGATED (not swallowed) — parity with the sync
// build (fulltext2_create returns the error on a bad datalink). The ISCP consumer then
// retries the iteration without advancing the watermark, so a transient failure self-heals
// and a permanently-bad datalink surfaces as a stalled-job error rather than silently
// indexing the URL string (which would bake wrong terms into the index forever).
func (w *Fulltext2SqlWriter) datalinkText(ctx context.Context, rawurl string) (out []byte, err error) {
	if w.cnEngine == nil {
		// no resolver context attached (unit tests) — index the URL string.
		return []byte(rawurl), nil
	}
	// A panic in the resolution path must not crash the CDC consumer goroutine (that would
	// stall ALL maintenance for the index); convert it to an error so the iteration retries.
	defer func() {
		if r := recover(); r != nil {
			out = nil
			err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("fulltext2 datalink resolve panic for %q: %v", rawurl, r))
		}
	}()
	// Resolve under the SOURCE tenant (srcAccountID), NOT GetAccountId(ctx): ISCP forces the
	// consumer ctx tenant to System_Account (iteration.go), so a tenant-owned stage://name/...
	// would otherwise be looked up in the system tenant's mo_stages and fail (or resolve an
	// unrelated same-named system stage). runTxnWithSqlContext sets the txn tenant from the
	// account we pass, overriding the ctx tenant.
	err = runTxnWithSqlContext(ctx, w.cnEngine, w.cnTxnClient, w.cnUUID, w.srcAccountID, time.Minute, nil, nil,
		func(sqlproc *sqlexec.SqlProcess, _ any) error {
			sql := fmt.Sprintf("SELECT load_text(cast(%s as datalink))", sqlquote.String(rawurl))
			res, e := sqlexec.RunSql(sqlproc, sql)
			if e != nil {
				return e
			}
			defer res.Close()
			for _, b := range res.Batches {
				if b == nil || len(b.Vecs) == 0 || b.Vecs[0] == nil || b.Vecs[0].Length() == 0 {
					continue
				}
				if b.Vecs[0].IsNull(0) {
					continue // empty content → no tokens (matches the sync build's empty-doc skip)
				}
				content := b.Vecs[0].GetBytesAt(0)
				out = append([]byte(nil), content...) // copy out before the txn (and its mpool) unwinds
				return nil
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return out, nil
}

var _ IndexSqlWriter = (*Fulltext2SqlWriter)(nil)

// NewFulltext2SqlWriter resolves the pk / text columns, the storage+metadata
// tables, the parser, and the capacity from the fulltext2 index def.
func NewFulltext2SqlWriter(algo string, jobID JobID, info *ConsumerInfo, tabledef *plan.TableDef, indexdef []*plan.IndexDef) (IndexSqlWriter, error) {
	idxdef := indexdef[0]

	var storage, meta string
	for _, idx := range indexdef {
		switch idx.IndexAlgoTableType {
		case catalog.FullText2Index_TblType_Storage:
			storage = idx.IndexTableName
		case catalog.FullText2Index_TblType_Metadata:
			meta = idx.IndexTableName
		}
	}
	if storage == "" || meta == "" {
		return nil, moerr.NewInternalErrorNoCtx("fulltext2 sink: storage/metadata hidden tables not found")
	}
	if len(idxdef.Parts) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("fulltext2 sink: index has no source column")
	}

	pkPos := tabledef.Name2ColIndex[tabledef.Pkey.PkeyColName]
	pkTyp := tabledef.Cols[pkPos].Typ
	textPos := make([]int32, 0, len(idxdef.Parts))
	textTypes := make([]int32, 0, len(idxdef.Parts))
	datalinkPos := false
	for _, p := range idxdef.Parts {
		ci := tabledef.Name2ColIndex[p]
		textPos = append(textPos, ci)
		ty := tabledef.Cols[ci].Typ.Id
		textTypes = append(textTypes, ty)
		if types.T(ty) == types.T_datalink {
			datalinkPos = true
		}
	}

	// parser + capacity from algo_params (the sinker runs in an internal ISCP proc
	// with no live resolver — resolve captured session_vars directly, like bm25).
	flat := map[string]string{}
	if m, e := catalog.IndexParamsStringToMap(idxdef.IndexAlgoParams); e == nil {
		flat = m
	}
	var resolve indexplugin.ResolveVarFunc
	if sv, e := catalog.IndexParamsSessionVars(idxdef.IndexAlgoParams); e == nil && len(sv) > 0 {
		if md, e2 := sqlexec.NewMetadataFromJson(string(sv)); e2 == nil && md != nil {
			// Soft resolver: AlgoParamInt treats absent as "use default", so a var
			// the blob doesn't hold must return (nil, nil), not a logged error.
			resolve = md.ResolveVariableSoft
		}
	}
	capacity, err := indexplugin.AlgoParamInt(flat[catalog.IndexAlgoParamMaxIndexCapacity], resolve, "fulltext_max_index_capacity", defaultFulltext2Capacity)
	if err != nil {
		return nil, err
	}
	postingCap, err := indexplugin.AlgoParamInt(flat[catalog.IndexAlgoParamMaxPostingsCapacity], resolve, "fulltext_max_postings_capacity", defaultFulltext2PostingCapacity)
	if err != nil {
		return nil, err
	}

	// INCLUDE columns (reload-safe from algo_params): resolve each to its row position +
	// type so CDC carries its value into the tail docmap. cdc.IncludeTypes travels in the
	// blob so RunFulltext2's TailBuilder decodes it without an external schema.
	var includePos, includeTypes []int32
	if raw := flat[catalog.IncludedColumns]; raw != "" {
		names, perr := catalog.ParseIncludeColumnsValue(raw)
		if perr != nil {
			return nil, perr
		}
		includePos = make([]int32, len(names))
		includeTypes = make([]int32, len(names))
		for i, name := range names {
			ci, ok := tabledef.Name2ColIndex[name]
			if !ok {
				return nil, moerr.NewInternalErrorNoCtx("fulltext2 sink: INCLUDE column " + name + " not found")
			}
			includePos[i] = ci
			includeTypes[i] = tabledef.Cols[ci].Typ.Id
		}
	}

	cdc := fulltext2.NewCdc(int32(pkTyp.Id))
	cdc.IncludeTypes = includeTypes
	return &Fulltext2SqlWriter{
		cfg:          fulltext2.TableConfig{DbName: info.DBName, IndexTable: storage, MetadataTable: meta, Parser: flat["parser"], PositionFree: flat[catalog.IndexAlgoParamPositionFree] == "true"},
		pkType:       int32(pkTyp.Id),
		pkPos:        pkPos,
		textPos:      textPos,
		textTypes:    textTypes,
		includePos:   includePos,
		includeTypes: includeTypes,
		datalinkPos:  datalinkPos,
		capacity:     capacity,
		postingCap:   postingCap,
		cdc:          cdc,
	}, nil
}

// rowInclude reads this row's INCLUDE column values (column order) out of the extracted
// row, copying them out before the txn's mpool unwinds. nil when the index has no INCLUDE
// columns; a NULL value stays nil.
func (w *Fulltext2SqlWriter) rowInclude(row []any) []any {
	if len(w.includePos) == 0 {
		return nil
	}
	out := make([]any, len(w.includePos))
	for i, pos := range w.includePos {
		out[i] = ftCopyPk(row[pos])
	}
	return out
}

func (w *Fulltext2SqlWriter) CheckLastOp(op string) bool { return len(w.last) == 0 || w.last == op }
func (w *Fulltext2SqlWriter) Empty() bool                { return w.cdc.Len() == 0 }
func (w *Fulltext2SqlWriter) Full() bool                 { return w.ndata >= MAX_CDC_DATA_SIZE }
func (w *Fulltext2SqlWriter) ToSql() ([]byte, error)     { return w.cdc.Encode() }

func (w *Fulltext2SqlWriter) Reset() {
	w.cdc = fulltext2.NewCdc(w.pkType)
	w.cdc.IncludeTypes = w.includeTypes
	w.ndata = 0
	w.last = ""
}

func (w *Fulltext2SqlWriter) Insert(ctx context.Context, row []any) error {
	w.last = vectorindex.CDC_INSERT
	text, err := w.rowText(ctx, row)
	if err != nil {
		return err
	}
	w.cdc.Insert(ftCopyPk(row[w.pkPos]), text, w.rowInclude(row))
	w.ndata += len(text) + 16
	return nil
}

func (w *Fulltext2SqlWriter) Upsert(ctx context.Context, row []any) error {
	w.last = vectorindex.CDC_UPSERT
	text, err := w.rowText(ctx, row)
	if err != nil {
		return err
	}
	w.cdc.Upsert(ftCopyPk(row[w.pkPos]), text, w.rowInclude(row))
	w.ndata += len(text) + 16
	return nil
}

func (w *Fulltext2SqlWriter) Delete(ctx context.Context, row []any) error {
	// a delete row carries only the pk in position 0 (mirrors HnswSqlWriter).
	w.last = vectorindex.CDC_DELETE
	w.cdc.Delete(ftCopyPk(row[0]))
	w.ndata += 16
	return nil
}

// rowText joins the indexed columns with '\n' (matching fulltext2_create's build
// tokenization). If ANY indexed column is NULL the doc yields no tokens — the
// whole-doc-skip the create-TVF's rowTerms does — so CREATE and CDC tokenize a
// row identically (a doc's searchability must not depend on which path indexed
// it). For a json parser each column is flattened to its leaf values PER COLUMN
// (FlattenJSONColumn), exactly as rowTerms does, so CdcTokenizer then just ngrams the
// flattened text (it no longer re-flattens). A datalink column is resolved to its file
// CONTENT via load_text (datalinkText) — parity with the sync build's GetPlainText, so
// identical values index identically whether seen at CREATE or via CDC; a resolution error
// is propagated (the CDC iteration retries), matching the sync build.
func (w *Fulltext2SqlWriter) rowText(ctx context.Context, row []any) (string, error) {
	for _, pos := range w.textPos {
		if row[pos] == nil {
			return "", nil
		}
	}
	isJSON := fulltext2.IsJSONParser(w.cfg.Parser)
	jsonValue := fulltext2.IsJSONValueParser(w.cfg.Parser)
	var b strings.Builder
	for i, pos := range w.textPos {
		if i > 0 {
			b.WriteByte('\n')
		}
		if isJSON {
			// json_value keeps each value as a separable '\n'-joined whole token (matched
			// exactly); json space-joins for ngram. CdcTokenizer then tokenizes to match.
			var ft []byte
			var err error
			if jsonValue {
				ft, err = fulltext2.FlattenJSONValueColumn(row[pos])
			} else {
				ft, err = fulltext2.FlattenJSONColumn(row[pos])
			}
			if err != nil {
				return "", err
			}
			b.Write(ft)
		} else if types.T(w.textTypes[i]) == types.T_datalink {
			// Resolve the datalink to its file CONTENT (parity with the sync build). Any
			// resolution error is propagated so the CDC iteration retries (see datalinkText);
			// with no resolver attached (unit tests) it indexes the URL string.
			dt, err := w.datalinkText(ctx, ftRowText(row[pos]))
			if err != nil {
				return "", err
			}
			b.Write(dt)
		} else {
			b.WriteString(ftRowText(row[pos]))
		}
	}
	return b.String(), nil
}

func ftRowText(v any) string {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case string:
		return t
	default:
		return ""
	}
}

func ftCopyPk(v any) any {
	if b, ok := v.([]byte); ok {
		return append([]byte(nil), b...)
	}
	return v
}
