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
	"os"
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

	// A SQL NULL indexed column contributes no text; the other column remains searchable.
	txt, err = w.rowText(context.Background(), []any{int64(1), nil, "world"})
	require.NoError(t, err)
	require.Equal(t, "world", txt)

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

	wd := newFT2Writer(fulltext2.ParserDefault)
	wd.textPos = []int32{1, 2}
	wd.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}
	txt, err = wd.rowText(context.Background(), []any{int64(1), nil, "default-sibling"})
	require.NoError(t, err)
	require.Equal(t, "default-sibling", txt)
}

func TestFulltext2WriterNullColumnMatrix(t *testing.T) {
	w := newFT2Writer(fulltext2.ParserNgram)
	w.textPos = []int32{1, 2, 3}
	w.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar), int32(types.T_varchar)}

	checks := []struct {
		row  []any
		want string
	}{
		{[]any{int64(1), nil, "right", nil}, "right"},
		{[]any{int64(1), "left", nil, nil}, "left"},
		{[]any{int64(1), "left", nil, "right"}, "left\nright"},
		{[]any{int64(1), nil, nil, nil}, ""},
		{[]any{int64(1), "", "right", nil}, "right"},
		{[]any{int64(1), "left", "", "right"}, "left\n\nright"},
	}
	for _, tc := range checks {
		got, err := w.rowText(context.Background(), tc.row)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}

	// The type lookup remains aligned with the original textPos ordinal when an earlier
	// content column is NULL, so a later datalink still takes the resolver path.
	w.textPos = []int32{1, 2}
	w.textTypes = []int32{int32(types.T_datalink), int32(types.T_varchar)}
	got, err := w.rowText(context.Background(), []any{int64(1), nil, "right"})
	require.NoError(t, err)
	require.Equal(t, "right", got)
}

func TestFulltext2WriterJSONValueNullLiteralAndMalformedSibling(t *testing.T) {
	w := newFT2Writer(fulltext2.ParserJSONValue)
	w.textPos = []int32{1, 2}
	w.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}

	got, err := w.rowText(context.Background(), []any{int64(1), nil, []byte(`{"k":"right"}`)})
	require.NoError(t, err)
	require.Equal(t, "right", got)
	got, err = w.rowText(context.Background(), []any{int64(1), []byte(`{"k":"left"}`), nil})
	require.NoError(t, err)
	require.Equal(t, "left", got)
	got, err = w.rowText(context.Background(), []any{int64(1), []byte(`{"k":"left"}`), []byte(`{"k":"right"}`)})
	require.NoError(t, err)
	require.Equal(t, "left\nright", got)
	tokenize, err := fulltext2.CdcTokenizer(fulltext2.ParserJSONValue)
	require.NoError(t, err)
	require.Equal(t, []fulltext2.WordPos{{Word: "left", Pos: 0}, {Word: "right", Pos: 5}}, tokenize(got))

	// JSON literal null is an empty document, while a JSON string "null" is one value.
	got, err = w.rowText(context.Background(), []any{int64(1), nil, []byte("null")})
	require.NoError(t, err)
	require.Empty(t, got)
	got, err = w.rowText(context.Background(), []any{int64(1), []byte(`"null"`), nil})
	require.NoError(t, err)
	require.Equal(t, "null", got)
	wj := newFT2Writer(fulltext2.ParserJSON)
	wj.textPos = []int32{1, 2}
	wj.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}
	_, err = wj.rowText(context.Background(), []any{int64(1), "", []byte(`{"k":"right"}`)})
	require.Error(t, err)

	for _, row := range [][]any{
		{int64(1), nil, []byte("{bad")},
		{int64(1), []byte("{bad"), nil},
	} {
		_, err = w.rowText(context.Background(), row)
		require.Error(t, err)
	}
	// A malformed non-NULL sibling fails before Insert publishes a CDC event.
	w.Reset()
	err = w.Insert(context.Background(), []any{int64(1), nil, []byte("{bad")})
	require.Error(t, err)
	require.True(t, w.Empty())
}

func TestFulltext2WriterJSONTupleNullSibling(t *testing.T) {
	w := newFT2Writer(fulltext2.ParserJSON)
	w.textPos = []int32{1, 2}
	w.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}

	got, err := w.rowText(context.Background(), []any{int64(1), nil, []byte(`{"b":"right"}`)})
	require.NoError(t, err)
	terms := fulltext2.DecodeJSONTermCarrier(got)
	require.Len(t, terms, 1)
	want, err := fulltext2.JSONTupleColumn([]byte(`{"b":"right"}`), fulltext2.DefaultJSONTermOptions())
	require.NoError(t, err)
	require.Equal(t, want[0], terms[0].Word)
	require.Equal(t, int32(0), terms[0].Pos)

	got, err = w.rowText(context.Background(), []any{int64(1), nil, nil})
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestFulltext2WriterNullSiblingCarrierAndInclude(t *testing.T) {
	w := newFT2Writer(fulltext2.ParserNgram)
	w.textPos = []int32{1, 2}
	w.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}
	w.includePos = []int32{3}
	w.includeTypes = []int32{int32(types.T_varchar)}
	w.cdc.IncludeTypes = w.includeTypes

	require.NoError(t, w.Insert(context.Background(), []any{int64(1), nil, "right", "cover"}))
	blob, err := w.ToSql()
	require.NoError(t, err)
	cdc, err := fulltext2.DecodeCdc(blob)
	require.NoError(t, err)
	require.Len(t, cdc.Events, 1)
	require.Equal(t, "right", cdc.Events[0].Text)
	require.Equal(t, []any{[]byte("cover")}, cdc.Events[0].Include)

	w.Reset()
	require.NoError(t, w.Upsert(context.Background(), []any{int64(1), nil, nil, nil}))
	blob, err = w.ToSql()
	require.NoError(t, err)
	cdc, err = fulltext2.DecodeCdc(blob)
	require.NoError(t, err)
	require.Len(t, cdc.Events, 1)
	require.Empty(t, cdc.Events[0].Text)
	require.Equal(t, []any{nil}, cdc.Events[0].Include)
}

func TestFulltext2WriterRealCarrierLWW(t *testing.T) {
	// Exercise the production writer -> Cdc codec -> TailBuilder -> query path. The query
	// oracle is independent of rowText: it checks that a later all-NULL upsert replaces an
	// earlier sibling term and that a later value/delete/reinsert sequence obeys LWW.
	writer := func() *Fulltext2SqlWriter {
		w := newFT2Writer(fulltext2.ParserNgram)
		w.textPos = []int32{1, 2}
		w.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}
		return w
	}
	makeBlob := func(t *testing.T, w *Fulltext2SqlWriter, op func() error) []byte {
		t.Helper()
		require.NoError(t, op())
		blob, err := w.ToSql()
		require.NoError(t, err)
		return blob
	}

	w := writer()
	blobs := [][]byte{
		makeBlob(t, w, func() error { return w.Insert(context.Background(), []any{int64(1), nil, "old"}) }),
	}
	w.Reset()
	blobs = append(blobs, makeBlob(t, w, func() error { return w.Upsert(context.Background(), []any{int64(1), nil, nil}) }))
	w.Reset()
	blobs = append(blobs, makeBlob(t, w, func() error { return w.Upsert(context.Background(), []any{int64(1), "new", nil}) }))
	w.Reset()
	blobs = append(blobs, makeBlob(t, w, func() error { return w.Delete(context.Background(), []any{int64(1)}) }))
	w.Reset()
	blobs = append(blobs, makeBlob(t, w, func() error { return w.Upsert(context.Background(), []any{int64(1), nil, "reborn"}) }))

	tokenize, err := fulltext2.CdcTokenizer(fulltext2.ParserNgram)
	require.NoError(t, err)
	tb, err := fulltext2.NewTailBuilder(int32(types.T_int64), 1000, 1000, "", tokenize)
	require.NoError(t, err)
	defer tb.Cleanup()
	for _, blob := range blobs {
		cdc, err := fulltext2.DecodeCdc(blob)
		require.NoError(t, err)
		require.NoError(t, tb.AddBatch(cdc))
	}
	frames, err := tb.Finish()
	require.NoError(t, err)
	var segments []*fulltext2.Segment
	deletes := make(map[any]int64)
	for i, frame := range frames {
		data, err := os.ReadFile(frame.Path)
		require.NoError(t, err)
		end := frame.Offset + int64(frame.FrameLen)
		require.LessOrEqual(t, end, int64(len(data)))
		seg, dels, err := fulltext2.UnframeTail("writer-lww", data[frame.Offset:end])
		require.NoError(t, err)
		recency := int64(100 + i)
		if seg != nil {
			seg.Recency = recency
			segments = append(segments, seg)
		}
		for _, d := range dels {
			deletes[d.Pk] = recency
		}
	}
	idx := fulltext2.NewIndex(segments, deletes)
	query := func(word string) []fulltext2.Result {
		res, err := idx.SearchQuery([]byte(word), true, fulltext2.ParserNgram, fulltext2.BM25, 100, nil)
		require.NoError(t, err)
		return res
	}
	require.Empty(t, query("old"), "all-NULL upsert must shadow the earlier sibling term")
	require.Empty(t, query("new"), "delete must shadow the prior value")
	require.Len(t, query("reborn"), 1, "reinsert after delete must be live")
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
