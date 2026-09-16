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
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
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

	// A SQL NULL indexed column contributes no text; the other column remains.
	txt, err = w.rowText(context.Background(), []any{int64(1), nil, "world"})
	require.NoError(t, err)
	require.Equal(t, "world", txt)

	// NULL columns do not create separators. Use a three-column writer for this
	// matrix so textPos/textTypes remain aligned with the row shape.
	w3 := newFT2Writer(fulltext2.ParserNgram)
	w3.textPos = []int32{1, 2, 3}
	w3.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar), int32(types.T_varchar)}
	txt, err = w3.rowText(context.Background(), []any{int64(1), "alpha", nil, "beta"})
	require.NoError(t, err)
	require.Equal(t, "alpha\nbeta", txt)
	txt, err = w3.rowText(context.Background(), []any{int64(1), "alpha", "", "beta"})
	require.NoError(t, err)
	require.Equal(t, "alpha\n\nbeta", txt)
	tok, err := fulltext2.CdcTokenizer(fulltext2.ParserNgram)
	require.NoError(t, err)
	require.Equal(t, []fulltext2.WordPos{{Word: "alpha", Pos: 0}, {Word: "beta", Pos: 7}}, tok(txt))
	txt, err = w.rowText(context.Background(), []any{int64(1), nil, "alpha"})
	require.NoError(t, err)
	require.Equal(t, "alpha", txt)

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

// writerTailSegments materializes the exact writer output through the production
// CDC codec and TailBuilder. This keeps the integration assertion independent
// from hand-built Cdc events used by the lower-level LWW tests.
func writerTailSegments(t *testing.T, parser string, cdcs ...*fulltext2.Cdc) []*fulltext2.Segment {
	t.Helper()
	tokenize, err := fulltext2.CdcTokenizerWithJSONOptions(parser, fulltext2.JSONTermOptions{})
	require.NoError(t, err)
	tb, err := fulltext2.NewTailBuilder(int32(types.T_int64), 1, 0, "", tokenize)
	require.NoError(t, err)
	defer tb.Cleanup()
	for _, cdc := range cdcs {
		require.NoError(t, tb.AddBatch(cdc))
	}
	frames, err := tb.Finish()
	require.NoError(t, err)
	segments := make([]*fulltext2.Segment, 0, len(frames))
	for i, frame := range frames {
		data, err := os.ReadFile(frame.Path)
		require.NoError(t, err)
		start := frame.Offset
		end := start + int64(frame.FrameLen)
		require.GreaterOrEqual(t, start, int64(0))
		require.LessOrEqual(t, end, int64(len(data)))
		seg, deletes, err := fulltext2.UnframeTail("writer-tail", data[start:end])
		require.NoError(t, err)
		require.Empty(t, deletes)
		if seg != nil {
			seg.Recency = int64(i + 1)
			segments = append(segments, seg)
		}
	}
	return segments
}

func writerCdc(t *testing.T, w *Fulltext2SqlWriter) *fulltext2.Cdc {
	t.Helper()
	blob, err := w.ToSql()
	require.NoError(t, err)
	cdc, err := fulltext2.DecodeCdc(blob)
	require.NoError(t, err)
	return cdc
}

func TestFulltext2WriterOutputReachesTailBuilder(t *testing.T) {
	ctx := context.Background()
	w := newFT2Writer(fulltext2.ParserNgram)
	w.textPos = []int32{1, 2}
	w.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}

	// Build a base version, then drive each replacement from the real writer.
	baseBuilder := fulltext2.NewBuilder("writer-base", int32(types.T_int64))
	require.NoError(t, baseBuilder.Add("right", 0, int64(1)))
	base, err := baseBuilder.Finish()
	require.NoError(t, err)
	base.Recency = 0

	require.NoError(t, w.Upsert(ctx, []any{int64(1), nil, "right"}))
	partial := writerCdc(t, w)
	require.Len(t, partial.Events, 1)
	require.Equal(t, "right", partial.Events[0].Text)

	w.Reset()
	require.NoError(t, w.Upsert(ctx, []any{int64(1), nil, nil}))
	empty := writerCdc(t, w)
	require.Len(t, empty.Events, 1)
	require.Empty(t, empty.Events[0].Text)
	// Stop before recovery so the zero-word writer event is the newest copy and
	// must shadow the base term on its own.
	tails := writerTailSegments(t, fulltext2.ParserNgram, partial, empty)
	idx := fulltext2.NewIndex(append([]*fulltext2.Segment{base}, tails...), nil)
	got, err := idx.SearchQuery([]byte("right"), false, fulltext2.ParserNgram, fulltext2.BM25, 100, nil)
	require.NoError(t, err)
	require.Empty(t, got)

	w.Reset()
	require.NoError(t, w.Upsert(ctx, []any{int64(1), nil, "revived"}))
	revived := writerCdc(t, w)
	tails = writerTailSegments(t, fulltext2.ParserNgram, partial, empty, revived)
	// Include the base copy so the zero-word writer event must actually shadow it.
	idx = fulltext2.NewIndex(append([]*fulltext2.Segment{base}, tails...), nil)
	got, err = idx.SearchQuery([]byte("revived"), false, fulltext2.ParserNgram, fulltext2.BM25, 100, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, int64(1), got[0].Pk)
}

func TestFulltext2WriterOutputCarriesIncludeThroughTail(t *testing.T) {
	w := newFT2Writer(fulltext2.ParserNgram)
	w.textPos = []int32{1, 2}
	w.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}
	w.includePos = []int32{3}
	w.includeTypes = []int32{int32(types.T_varchar)}
	w.cdc.IncludeTypes = w.includeTypes

	require.NoError(t, w.Upsert(context.Background(), []any{int64(1), nil, "right", "active"}))
	partial := writerCdc(t, w)
	require.Equal(t, []int32{int32(types.T_varchar)}, partial.IncludeTypes)
	// Varlena INCLUDE values decode through the shared pk codec as []byte.
	require.Equal(t, []any{[]byte("active")}, partial.Events[0].Include)
	partialTail := writerTailSegments(t, fulltext2.ParserNgram, partial)
	idx := fulltext2.NewIndex(partialTail, nil)
	got, err := idx.SearchQuery([]byte("right"), false, fulltext2.ParserNgram, fulltext2.BM25, 100, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, []any{[]byte("active")}, got[0].Include)

	// A later all-NULL content upsert still carries its NULL INCLUDE value and
	// shadows the prior searchable version.
	w.Reset()
	require.NoError(t, w.Upsert(context.Background(), []any{int64(1), nil, nil, nil}))
	empty := writerCdc(t, w)
	require.Equal(t, []any{nil}, empty.Events[0].Include)
	tails := writerTailSegments(t, fulltext2.ParserNgram, partial, empty)
	idx = fulltext2.NewIndex(tails, nil)
	got, err = idx.SearchQuery([]byte("right"), false, fulltext2.ParserNgram, fulltext2.BM25, 100, nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestFulltext2WriterJSONValueSkipsNullColumn(t *testing.T) {
	w := newFT2Writer(fulltext2.ParserJSONValue)
	w.textPos = []int32{1, 2}
	w.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}

	txt, err := w.rowText(context.Background(), []any{int64(1), nil, []byte(`{"k":"right"}`)})
	require.NoError(t, err)
	require.Equal(t, "right", txt)

	txt, err = w.rowText(context.Background(), []any{int64(1), []byte(`{"k":"left"}`), nil})
	require.NoError(t, err)
	require.Equal(t, "left", txt)

	txt, err = w.rowText(context.Background(), []any{int64(1), []byte(`{"k":"left"}`), []byte(`{"k":"right"}`)})
	require.NoError(t, err)
	require.Equal(t, "left\nright", txt)
	tok, err := fulltext2.CdcTokenizer(fulltext2.ParserJSONValue)
	require.NoError(t, err)
	require.Equal(t, []fulltext2.WordPos{{Word: "left", Pos: 0}, {Word: "right", Pos: 5}}, tok(txt))

	// JSON literal null has no value term, while the JSON string "null" is a
	// searchable whole value. A SQL NULL sibling must not change either result.
	txt, err = w.rowText(context.Background(), []any{int64(1), nil, []byte("null")})
	require.NoError(t, err)
	require.Empty(t, txt)
	txt, err = w.rowText(context.Background(), []any{int64(1), []byte(`"null"`), nil})
	require.NoError(t, err)
	require.Equal(t, "null", txt)
}

func TestFulltext2WriterJSONTupleSkipsNullColumn(t *testing.T) {
	w := newFT2Writer(fulltext2.ParserJSON)
	w.textPos = []int32{1, 2}
	w.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}

	txt, err := w.rowText(context.Background(), []any{int64(1), nil, []byte(`{"b":"right"}`)})
	require.NoError(t, err)
	got := fulltext2.DecodeJSONTermCarrier(txt)
	require.Len(t, got, 1)
	require.Equal(t, fulltext2.JSONStringTerm("b", "right"), got[0].Word)
	require.Equal(t, int32(0), got[0].Pos)

	// A native T_json value arrives at the writer as ByteJson. Keep the sibling
	// SQL NULL in the same row to exercise the representation boundary directly.
	bj, err := bytejson.ParseFromString(`{"b":"binary"}`)
	require.NoError(t, err)
	txt, err = w.rowText(context.Background(), []any{int64(1), nil, bj})
	require.NoError(t, err)
	got = fulltext2.DecodeJSONTermCarrier(txt)
	require.Equal(t, []fulltext2.WordPos{{Word: fulltext2.JSONStringTerm("b", "binary"), Pos: 0}}, got)
}

func TestFulltext2WriterJSONFlatSkipsNullColumn(t *testing.T) {
	w := newFT2Writer(fulltext2.ParserJSON)
	w.cfg.JSONNoKeys = true
	w.textPos = []int32{1, 2}
	w.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}
	txt, err := w.rowText(context.Background(), []any{int64(1), nil, []byte(`{"k":"right"}`)})
	require.NoError(t, err)
	require.Equal(t, "right", txt)
}

func TestFulltext2WriterPartialNullRoundTrip(t *testing.T) {
	w := newFT2Writer(fulltext2.ParserNgram)
	w.textPos = []int32{1, 2}
	w.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}

	require.NoError(t, w.Insert(context.Background(), []any{int64(1), nil, "right"}))
	blob, err := w.ToSql()
	require.NoError(t, err)
	cdc, err := fulltext2.DecodeCdc(blob)
	require.NoError(t, err)
	require.Len(t, cdc.Events, 1)
	require.Equal(t, "right", cdc.Events[0].Text)
	words, err := fulltext2.CdcTokenizer(fulltext2.ParserNgram)
	require.NoError(t, err)
	require.Equal(t, []fulltext2.WordPos{{Word: "right", Pos: 0}}, words(cdc.Events[0].Text))

	w.Reset()
	require.NoError(t, w.Upsert(context.Background(), []any{int64(1), nil, nil}))
	blob, err = w.ToSql()
	require.NoError(t, err)
	cdc, err = fulltext2.DecodeCdc(blob)
	require.NoError(t, err)
	require.Len(t, cdc.Events, 1)
	require.Empty(t, cdc.Events[0].Text)
}

func TestFulltext2WriterMalformedJSONWithNullSibling(t *testing.T) {
	for _, row := range [][]any{
		{int64(1), nil, []byte("{bad")},
		{int64(1), []byte("{bad"), nil},
	} {
		w := newFT2Writer(fulltext2.ParserJSONValue)
		w.textPos = []int32{1, 2}
		w.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}
		_, err := w.rowText(context.Background(), row)
		require.Error(t, err)
	}
}

func TestFulltext2WriterNullDatalinkSiblingIsSkipped(t *testing.T) {
	w := newFT2Writer(fulltext2.ParserNgram)
	w.textPos = []int32{1, 2}
	w.textTypes = []int32{int32(types.T_datalink), int32(types.T_varchar)}
	txt, err := w.rowText(context.Background(), []any{int64(1), nil, "right"})
	require.NoError(t, err)
	require.Equal(t, "right", txt)
}

func TestFulltext2WriterNullSiblingAcrossTextParsers(t *testing.T) {
	for _, parser := range []string{fulltext2.ParserDefault, fulltext2.ParserNgram, fulltext2.ParserGojieba} {
		t.Run(parser, func(t *testing.T) {
			w := newFT2Writer(parser)
			w.textPos = []int32{1, 2}
			w.textTypes = []int32{int32(types.T_varchar), int32(types.T_varchar)}
			got, err := w.rowText(context.Background(), []any{int64(1), nil, "sibling"})
			require.NoError(t, err)
			require.Equal(t, "sibling", got)
		})
	}
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
