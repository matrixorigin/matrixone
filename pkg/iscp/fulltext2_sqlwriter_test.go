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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

func newFT2Writer(parser string) *Fulltext2SqlWriter {
	return &Fulltext2SqlWriter{
		cfg:       fulltext2.TableConfig{DbName: "db", IndexTable: "__store", MetadataTable: "__meta", Parser: parser},
		pkType:    int32(types.T_int64),
		pkPos:     0,
		textPos:   []int32{1},
		textTypes: []int32{int32(types.T_varchar)},
		cdc:       fulltext2.NewCdc(int32(types.T_int64)),
	}
}

func TestFtRowTextAndCopyPk(t *testing.T) {
	require.Equal(t, "hello", ftRowText([]byte("hello")))
	require.Equal(t, "world", ftRowText("world"))
	require.Equal(t, "", ftRowText(int64(5))) // non-text → empty

	// ftCopyPk clones a []byte pk (so the CDC blob never aliases the source row buffer).
	src := []byte("pk")
	cp := ftCopyPk(src).([]byte)
	require.Equal(t, src, cp)
	src[0] = 'X'
	require.Equal(t, "pk", string(cp)) // clone unaffected by the mutation
	// a value pk passes through unchanged.
	require.Equal(t, int64(7), ftCopyPk(int64(7)))
}

func TestFulltext2WriterRowText(t *testing.T) {
	w := newFT2Writer(fulltext2.ParserNgram)
	w.textPos = []int32{1, 2}
	w.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}

	// two text columns joined with '\n'.
	txt, err := w.rowText(context.Background(), []any{int64(1), []byte("hello"), "world"})
	require.NoError(t, err)
	require.Equal(t, "hello\nworld", txt)

	// any NULL indexed column → whole doc yields no tokens.
	txt, err = w.rowText(context.Background(), []any{int64(1), nil, "world"})
	require.NoError(t, err)
	require.Equal(t, "", txt)

	// json parser flattens each column's values.
	wj := newFT2Writer(fulltext2.ParserJSON)
	txt, err = wj.rowText(context.Background(), []any{int64(1), []byte(`{"a":"matrix"}`)})
	require.NoError(t, err)
	require.Contains(t, txt, "matrix")

	// json_value parser flattens to whole atomic values.
	wjv := newFT2Writer(fulltext2.ParserJSONValue)
	txt, err = wjv.rowText(context.Background(), []any{int64(1), []byte(`{"a":"origin"}`)})
	require.NoError(t, err)
	require.Contains(t, txt, "origin")
}

// TestFulltext2WriterDatalinkFallback: with no resolver context (cnEngine==nil, e.g. unit tests) a datalink column falls back to indexing the URL
// string rather than panicking or erroring.
func TestFulltext2WriterDatalinkFallback(t *testing.T) {
	w := newFT2Writer(fulltext2.ParserNgram)
	w.textTypes = []int32{int32(types.T_datalink)} // the single indexed column is a datalink

	txt, err := w.rowText(context.Background(), []any{int64(1), "file:///docs/a.txt"})
	require.NoError(t, err)
	require.Equal(t, "file:///docs/a.txt", txt) // URL fallback, no resolution
}

// TestNewFulltext2SqlWriterDatalinkDetected: a datalink indexed column is flagged
// (datalinkPos) and its type recorded, so rowText knows to resolve it to file content.
func TestNewFulltext2SqlWriterDatalinkDetected(t *testing.T) {
	tabledef := &plan.TableDef{
		Name2ColIndex: map[string]int32{"id": 0, "doc": 1},
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "doc", Typ: plan.Type{Id: int32(types.T_datalink)}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
	}
	indexdef := []*plan.IndexDef{
		{IndexName: "ft2", IndexAlgoTableType: catalog.FullText2Index_TblType_Storage, IndexTableName: "__store", Parts: []string{"doc"}, IndexAlgoParams: `{"parser":"ngram"}`},
		{IndexName: "ft2", IndexAlgoTableType: catalog.FullText2Index_TblType_Metadata, IndexTableName: "__meta", Parts: []string{"doc"}},
	}
	wr, err := NewFulltext2SqlWriter("fulltext2", JobID{}, &ConsumerInfo{DBName: "db"}, tabledef, indexdef)
	require.NoError(t, err)
	w := wr.(*Fulltext2SqlWriter)
	require.True(t, w.datalinkPos)
	require.Equal(t, []int32{int32(types.T_datalink)}, w.textTypes)
}

func TestFulltext2WriterOps(t *testing.T) {
	ctx := context.Background()
	w := newFT2Writer(fulltext2.ParserNgram)

	require.True(t, w.Empty())
	require.True(t, w.CheckLastOp(vectorindex.CDC_INSERT)) // empty last matches anything
	require.False(t, w.Full())

	require.NoError(t, w.Insert(ctx, []any{int64(1), []byte("hello world")}))
	require.False(t, w.Empty())
	require.Equal(t, 1, w.cdc.Len())
	require.True(t, w.CheckLastOp(vectorindex.CDC_INSERT))
	require.False(t, w.CheckLastOp(vectorindex.CDC_DELETE))

	require.NoError(t, w.Upsert(ctx, []any{int64(2), []byte("brown fox")}))
	require.Equal(t, 2, w.cdc.Len())

	// a delete row carries only the pk in position 0.
	require.NoError(t, w.Delete(ctx, []any{int64(1)}))
	require.Equal(t, 3, w.cdc.Len())

	// ToSql encodes the accumulated blob.
	blob, err := w.ToSql()
	require.NoError(t, err)
	require.NotEmpty(t, blob)

	// Full trips once ndata crosses the flush threshold.
	w.ndata = MAX_CDC_DATA_SIZE
	require.True(t, w.Full())

	// Reset clears everything.
	w.Reset()
	require.True(t, w.Empty())
	require.Equal(t, 0, w.ndata)
	require.True(t, w.CheckLastOp(vectorindex.CDC_DELETE))
}

func TestNewFulltext2SqlWriter(t *testing.T) {
	tabledef := &plan.TableDef{
		Name2ColIndex: map[string]int32{"id": 0, "body": 1},
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "body", Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
	}
	indexdef := []*plan.IndexDef{
		{IndexName: "ft2", IndexAlgoTableType: catalog.FullText2Index_TblType_Storage, IndexTableName: "__store", Parts: []string{"body"}, IndexAlgoParams: `{"parser":"ngram"}`},
		{IndexName: "ft2", IndexAlgoTableType: catalog.FullText2Index_TblType_Metadata, IndexTableName: "__meta", Parts: []string{"body"}},
	}
	info := &ConsumerInfo{DBName: "db"}

	wr, err := NewFulltext2SqlWriter("fulltext2", JobID{}, info, tabledef, indexdef)
	require.NoError(t, err)
	w := wr.(*Fulltext2SqlWriter)
	require.Equal(t, "__store", w.cfg.IndexTable)
	require.Equal(t, "__meta", w.cfg.MetadataTable)
	require.Equal(t, "ngram", w.cfg.Parser)
	require.Equal(t, int32(0), w.pkPos)
	require.Equal(t, []int32{1}, w.textPos)
	require.Equal(t, defaultFulltext2Capacity, w.capacity)

	// missing metadata sibling → error.
	_, err = NewFulltext2SqlWriter("fulltext2", JobID{}, info, tabledef, indexdef[:1])
	require.ErrorContains(t, err, "not found")

	// no source column (empty Parts) → error.
	bad := []*plan.IndexDef{
		{IndexName: "ft2", IndexAlgoTableType: catalog.FullText2Index_TblType_Storage, IndexTableName: "__store"},
		{IndexName: "ft2", IndexAlgoTableType: catalog.FullText2Index_TblType_Metadata, IndexTableName: "__meta"},
	}
	_, err = NewFulltext2SqlWriter("fulltext2", JobID{}, info, tabledef, bad)
	require.ErrorContains(t, err, "no source column")
}
