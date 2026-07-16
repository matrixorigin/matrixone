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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

const defaultFulltext2Capacity int64 = 1000000

// Fulltext2SqlWriter is the ISCP sink adapter for the fulltext2 positional index.
// Like WandSqlWriter it is model-building: it accumulates one flush's CDC rows
// into a binary fulltext2.Cdc blob (ToSql), which RunFulltext2 decodes, tokenizes
// (per the index parser), and turns into tag=1 CdcTail frames. Binary (typed pk)
// because a fulltext2 pk is `any` (int64 OR varchar).
type Fulltext2SqlWriter struct {
	cfg      fulltext2.TableConfig // DbName + ftv2_index (storage) + ftv2_meta (metadata) + parser
	pkType   int32                 // types.T of the source primary key
	pkPos    int32                 // pk column index in the extracted row
	textPos  []int32               // indexed text columns (all idxdef.Parts) — multi-column joins with '\n'
	capacity int64                 // max docs per delta segment (max_index_capacity)

	cdc   *fulltext2.Cdc
	ndata int
	last  string
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
	for _, p := range idxdef.Parts {
		textPos = append(textPos, tabledef.Name2ColIndex[p])
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
			resolve = md.ResolveVariableFunc
		}
	}
	capacity, err := indexplugin.AlgoParamInt(flat[catalog.IndexAlgoParamMaxIndexCapacity], resolve, "fulltext_max_index_capacity", defaultFulltext2Capacity)
	if err != nil {
		return nil, err
	}

	return &Fulltext2SqlWriter{
		cfg:      fulltext2.TableConfig{DbName: info.DBName, IndexTable: storage, MetadataTable: meta, Parser: flat["parser"]},
		pkType:   int32(pkTyp.Id),
		pkPos:    pkPos,
		textPos:  textPos,
		capacity: capacity,
		cdc:      fulltext2.NewCdc(int32(pkTyp.Id)),
	}, nil
}

func (w *Fulltext2SqlWriter) CheckLastOp(op string) bool { return len(w.last) == 0 || w.last == op }
func (w *Fulltext2SqlWriter) Empty() bool                { return w.cdc.Len() == 0 }
func (w *Fulltext2SqlWriter) Full() bool                 { return w.ndata >= MAX_CDC_DATA_SIZE }
func (w *Fulltext2SqlWriter) ToSql() ([]byte, error)     { return w.cdc.Encode() }

func (w *Fulltext2SqlWriter) Reset() {
	w.cdc = fulltext2.NewCdc(w.pkType)
	w.ndata = 0
	w.last = ""
}

func (w *Fulltext2SqlWriter) Insert(ctx context.Context, row []any) error {
	w.last = vectorindex.CDC_INSERT
	text, err := w.rowText(row)
	if err != nil {
		return err
	}
	w.cdc.Insert(ftCopyPk(row[w.pkPos]), text)
	w.ndata += len(text) + 16
	return nil
}

func (w *Fulltext2SqlWriter) Upsert(ctx context.Context, row []any) error {
	w.last = vectorindex.CDC_UPSERT
	text, err := w.rowText(row)
	if err != nil {
		return err
	}
	w.cdc.Upsert(ftCopyPk(row[w.pkPos]), text)
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
// flattened text (it no longer re-flattens). datalink columns are NOT resolved here —
// CDC of a datalink column indexes the URL string; file-content datalink is build-only.
func (w *Fulltext2SqlWriter) rowText(row []any) (string, error) {
	for _, pos := range w.textPos {
		if row[pos] == nil {
			return "", nil
		}
	}
	isJSON := fulltext2.IsJSONParser(w.cfg.Parser)
	var b strings.Builder
	for i, pos := range w.textPos {
		if i > 0 {
			b.WriteByte('\n')
		}
		if isJSON {
			ft, err := fulltext2.FlattenJSONColumn(row[pos])
			if err != nil {
				return "", err
			}
			b.Write(ft)
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
