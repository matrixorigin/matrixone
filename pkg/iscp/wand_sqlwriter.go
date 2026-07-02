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
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fulltext/wand"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// defaultWandCapacity caps docs-per-segment for a CDC delta flush. It must keep
// a serialized segment within vectorindex.MaxChunkSize (one frame = one chunk
// row; the load path does not reassemble a frame across rows). Overridable via
// the max_index_capacity algo-param.
const defaultWandCapacity int64 = 10000

// WandSqlWriter is the ISCP sink adapter for the WAND "retrieval" fulltext index.
// Unlike the postings FulltextSqlWriter (which emits SQL), it is model-building
// like HnswSqlWriter: it accumulates the CDC rows of one flush into a binary
// WandCdc blob (ToSql), which RunWand decodes, tokenizes, and turns into tag=1
// CdcTail frames. The blob is binary (typed pk) because a retrieval pk is `any`
// (int64 OR varchar) — a JSON blob would corrupt a non-integer pk.
type WandSqlWriter struct {
	cfg      wand.TableConfig // DbName + ft_index (storage) + ft_meta (metadata)
	pkType   int32            // types.T of the source primary key
	pkPos    int32            // pk column index in the extracted row
	textPos  int32            // indexed text column index (idxdef.Parts[0])
	capacity int64            // max docs per delta segment (max_index_capacity)

	cdc    *wand.WandCdc // accumulated events for the current flush
	ndata  int           // approx bytes buffered, for Full()
	lastOp string        // last CDC op, for CheckLastOp batching
}

var _ IndexSqlWriter = (*WandSqlWriter)(nil)

// NewWandSqlWriter resolves the pk / text columns and the ft_index/ft_meta
// storage tables (reused with a tag column: tag=0 base, tag=1 CdcTail) from the
// retrieval index def.
func NewWandSqlWriter(algo string, jobID JobID, info *ConsumerInfo, tabledef *plan.TableDef, indexdef []*plan.IndexDef) (IndexSqlWriter, error) {
	idxdef := indexdef[0]

	var storage, meta string
	for _, idx := range indexdef {
		switch idx.IndexAlgoTableType {
		case catalog.FullTextIndex_TblType_Storage:
			storage = idx.IndexTableName
		case catalog.FullTextIndex_TblType_Metadata:
			meta = idx.IndexTableName
		}
	}
	if len(storage) == 0 || len(meta) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("wand sink: ft_index/ft_meta hidden tables not found on retrieval index")
	}
	if len(idxdef.Parts) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("wand sink: retrieval index has no source column")
	}

	pkPos := tabledef.Name2ColIndex[tabledef.Pkey.PkeyColName]
	pkTyp := tabledef.Cols[pkPos].Typ
	textPos := tabledef.Name2ColIndex[idxdef.Parts[0]]

	capacity := defaultWandCapacity
	if len(idxdef.IndexAlgoParams) > 0 {
		var m map[string]any
		if json.Unmarshal([]byte(idxdef.IndexAlgoParams), &m) == nil {
			if v, ok := m["max_index_capacity"].(float64); ok && v > 0 {
				capacity = int64(v)
			}
		}
	}

	return &WandSqlWriter{
		cfg:      wand.TableConfig{DbName: info.DBName, IndexTable: storage, MetadataTable: meta},
		pkType:   int32(pkTyp.Id),
		pkPos:    pkPos,
		textPos:  textPos,
		capacity: capacity,
		cdc:      wand.NewWandCdc(int32(pkTyp.Id)),
	}, nil
}

func (w *WandSqlWriter) CheckLastOp(op string) bool { return len(w.lastOp) == 0 || w.lastOp == op }
func (w *WandSqlWriter) Empty() bool                 { return w.cdc.Len() == 0 }
func (w *WandSqlWriter) Full() bool                  { return w.ndata >= MAX_CDC_DATA_SIZE }
func (w *WandSqlWriter) ToSql() ([]byte, error)      { return w.cdc.Encode() }

func (w *WandSqlWriter) Reset() {
	w.cdc = wand.NewWandCdc(w.pkType)
	w.ndata = 0
	w.lastOp = ""
}

func (w *WandSqlWriter) Insert(ctx context.Context, row []any) error {
	w.lastOp = vectorindex.CDC_INSERT
	text := wandRowText(row[w.textPos])
	w.cdc.Insert(wandCopyPk(row[w.pkPos]), text)
	w.ndata += len(text) + 16
	return nil
}

func (w *WandSqlWriter) Upsert(ctx context.Context, row []any) error {
	w.lastOp = vectorindex.CDC_UPSERT
	text := wandRowText(row[w.textPos])
	w.cdc.Upsert(wandCopyPk(row[w.pkPos]), text)
	w.ndata += len(text) + 16
	return nil
}

func (w *WandSqlWriter) Delete(ctx context.Context, row []any) error {
	// a delete row carries only the pk in position 0 (mirrors HnswSqlWriter).
	w.lastOp = vectorindex.CDC_DELETE
	w.cdc.Delete(wandCopyPk(row[0]))
	w.ndata += 16
	return nil
}

// wandRowText reads the source text column as a string (varchar → []byte/string;
// a NULL text yields ""; such a doc simply contributes no terms).
func wandRowText(v any) string {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case string:
		return t
	default:
		return ""
	}
}

// wandCopyPk defensively copies a byte-slice pk out of the reused row buffer;
// value pks (int64, etc.) are copied by assignment.
func wandCopyPk(v any) any {
	if b, ok := v.([]byte); ok {
		return append([]byte(nil), b...)
	}
	return v
}
