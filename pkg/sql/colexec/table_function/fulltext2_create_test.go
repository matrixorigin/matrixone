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
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// ft2CreateCfg marshals a minimal TableConfig JSON constant (the shape the compile
// layer passes as argVecs[0]).
func ft2CreateCfg(t *testing.T, parser string, positionFree bool) string {
	t.Helper()
	b, err := sonic.Marshal(fulltext2.TableConfig{
		ThreadsBuild: 1,
		DbName:       "db",
		IndexTable:   "__idx",
		Parser:       parser,
		PositionFree: positionFree,
	})
	require.NoError(t, err)
	return string(b)
}

// ft2CreateArgVecs builds [cfg(const), pk(int64), text(varchar)...] input vectors and a
// matching tf.Args (rowTerms reads tf.Args[i].Typ.Id for the datalink check).
func ft2CreateArgVecs(t *testing.T, mp *mpool.MPool, cfg string, pk int64, texts ...string) (*TableFunction, []*vector.Vector) {
	t.Helper()
	pkVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](pkVec, pk, false, mp))
	vecs := make([]*vector.Vector, 0, 2+len(texts))
	vecs = append(vecs, ft2ConstStr(t, mp, cfg), pkVec)
	args := make([]*plan.Expr, 0, 2+len(texts))
	args = append(args, makeStrConstExpr(cfg), makeStrConstExpr("pk"))
	for _, s := range texts {
		tv := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(tv, []byte(s), false, mp))
		vecs = append(vecs, tv)
		args = append(args, makeStrConstExpr("col"))
	}
	tf := newFT2TF([]string{"status"}, ft2StatusRets())
	tf.Args = args
	tf.ctr.argVecs = vecs
	return tf, vecs
}

// ft2CreateNullableArgVecs builds the same TVF inputs as ft2CreateArgVecs while
// allowing a nil value to represent a SQL NULL content vector. Keeping the null
// bitmap in the vector is important: rowTerms must skip only the current NULL
// column and continue processing the other indexed columns.
func ft2CreateNullableArgVecs(t *testing.T, mp *mpool.MPool, cfg string, pk int64, values ...any) (*TableFunction, []*vector.Vector) {
	t.Helper()
	pkVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](pkVec, pk, false, mp))
	vecs := make([]*vector.Vector, 0, 2+len(values))
	vecs = append(vecs, ft2ConstStr(t, mp, cfg), pkVec)
	args := make([]*plan.Expr, 0, 2+len(values))
	args = append(args, makeStrConstExpr(cfg), makeStrConstExpr("pk"))
	for _, value := range values {
		tv := vector.NewVec(types.T_varchar.ToType())
		s, ok := value.(string)
		if value == nil {
			require.NoError(t, vector.AppendBytes(tv, nil, true, mp))
		} else {
			require.True(t, ok, "test content must be string or nil")
			require.NoError(t, vector.AppendBytes(tv, []byte(s), false, mp))
		}
		vecs = append(vecs, tv)
		args = append(args, makeStrConstExpr("col"))
	}
	tf := newFT2TF([]string{"status"}, ft2StatusRets())
	tf.Args = args
	tf.ctr.argVecs = vecs
	return tf, vecs
}

func TestFulltext2CreatePrepare(t *testing.T) {
	proc := testutil.NewProc(t)
	arg := &TableFunction{
		Args:     []*plan.Expr{makeStrConstExpr("{}"), makeStrConstExpr("pk"), makeStrConstExpr("col")},
		FuncName: "fulltext2_create",
	}
	st, err := fulltext2CreatePrepare(proc, arg)
	require.NoError(t, err)
	require.NotNil(t, st)
	require.Len(t, arg.ctr.argVecs, 3)
}

func TestFulltext2CreateStartValidation(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)

	// non-varchar config.
	{
		st := &fulltext2CreateState{}
		tf := newFT2TF([]string{"status"}, ft2StatusRets())
		nonStr, err := vector.NewConstFixed(types.T_int64.ToType(), int64(1), 1, mp)
		require.NoError(t, err)
		pkVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed[int64](pkVec, 1, false, mp))
		tf.ctr.argVecs = []*vector.Vector{nonStr, pkVec}
		require.ErrorContains(t, st.start(tf, proc, 0, nil), "must be a string")
	}
	// non-const config.
	{
		st := &fulltext2CreateState{}
		tf := newFT2TF([]string{"status"}, ft2StatusRets())
		pkVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed[int64](pkVec, 1, false, mp))
		tf.ctr.argVecs = []*vector.Vector{ft2NonConstStr(t, mp, "{}"), pkVec}
		require.ErrorContains(t, st.start(tf, proc, 0, nil), "must be a string constant")
	}
	// empty config.
	{
		st := &fulltext2CreateState{}
		tf := newFT2TF([]string{"status"}, ft2StatusRets())
		pkVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed[int64](pkVec, 1, false, mp))
		tf.ctr.argVecs = []*vector.Vector{ft2ConstStr(t, mp, ""), pkVec}
		require.ErrorContains(t, st.start(tf, proc, 0, nil), "config is empty")
	}
	// invalid json config.
	{
		st := &fulltext2CreateState{}
		tf := newFT2TF([]string{"status"}, ft2StatusRets())
		pkVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed[int64](pkVec, 1, false, mp))
		tf.ctr.argVecs = []*vector.Vector{ft2ConstStr(t, mp, "{not json"), pkVec}
		require.Error(t, st.start(tf, proc, 0, nil))
	}
}

// TestFulltext2CreateStartFeedsBuilder feeds two ordinary text rows and checks the
// builder accumulates docs (below capacity ⇒ no seal, so no DB access).
func TestFulltext2CreateStartFeedsBuilder(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)

	st := &fulltext2CreateState{}
	tf, _ := ft2CreateArgVecs(t, mp, ft2CreateCfg(t, fulltext2.ParserNgram, false), 1, "hello world")
	require.NoError(t, st.start(tf, proc, 0, nil))
	require.True(t, st.inited)
	require.Equal(t, fulltext2.DefaultBuildCapacity, st.capacity)
	require.Equal(t, fulltext2.DefaultPostingCapacity, st.postingCap)
	require.NotNil(t, st.cur)
	require.Equal(t, 1, st.cur.NumDocs())

	// a NULL pk row is skipped (no doc added).
	pkNull := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](pkNull, 0, true, mp))
	tf.ctr.argVecs[1] = pkNull
	require.NoError(t, st.start(tf, proc, 0, nil))
	require.Equal(t, 1, st.cur.NumDocs())

	// call() yields the empty status batch; reset/free are safe.
	res, err := st.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, vm.CancelResult, res)
	st.reset(tf, proc)
	st.free(tf, proc, false, nil)
}

func TestFulltext2CreateLongPhysicalIdentity(t *testing.T) {
	var ids []string
	for _, suffix := range []string{"first", "second"} {
		t.Run(suffix, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
			cfg, err := sonic.Marshal(fulltext2.TableConfig{DbName: "db", IndexTable: strings.Repeat("long_physical_generation_", 8) + suffix, Parser: fulltext2.ParserNgram})
			require.NoError(t, err)
			tf, vecs := ft2CreateArgVecs(t, mp, string(cfg), 1, "apple")
			defer func() {
				for _, vec := range vecs {
					vec.Free(mp)
				}
			}()
			st := &fulltext2CreateState{}
			defer st.free(tf, proc, false, nil)
			require.NoError(t, st.start(tf, proc, 0, nil))
			require.LessOrEqual(t, len(fulltext2.SubIndexId(st.uid, int(^uint(0)>>1))), 128)
			ids = append(ids, strings.Split(st.uid, ":")[0])
		})
	}
	require.NotEqual(t, ids[0], ids[1], "do not truncate distinct physical identities to a shared prefix")
}

func TestFulltext2CreatePartialNullKeepsInclude(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	cfg, err := sonic.Marshal(fulltext2.TableConfig{
		ThreadsBuild: 1,
		DbName:       "db", IndexTable: "__idx", Parser: fulltext2.ParserNgram,
		IncludeTypes: []int32{int32(types.T_varchar)},
	})
	require.NoError(t, err)

	pk := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](pk, 1, false, mp))
	left := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(left, nil, true, mp))
	right := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(right, []byte("right"), false, mp))
	status := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(status, []byte("active"), false, mp))

	st := &fulltext2CreateState{}
	tf := newFT2TF([]string{"status"}, ft2StatusRets())
	tf.Args = []*plan.Expr{
		makeStrConstExpr(string(cfg)), makeStrConstExpr("pk"),
		makeStrConstExpr("left"), makeStrConstExpr("right"), makeStrConstExpr("status"),
	}
	tf.ctr.argVecs = []*vector.Vector{ft2ConstStr(t, mp, string(cfg)), pk, left, right, status}
	defer st.free(tf, proc, false, nil)
	require.NoError(t, st.start(tf, proc, 0, nil))
	require.Equal(t, 1, st.cur.NumDocs())

	seg, err := st.cur.Finish()
	require.NoError(t, err)
	idx := fulltext2.NewIndex([]*fulltext2.Segment{seg}, nil)
	got, err := idx.SearchQuery([]byte("right"), false, fulltext2.ParserNgram, fulltext2.BM25, 100, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, int64(1), got[0].Pk)
	require.Equal(t, []any{[]byte("active")}, got[0].Include)
}

// TestFulltext2CreateRowTerms exercises the tokenization paths of rowTerms across the
// ngram / json / json_value parsers and NULL/empty inputs, without touching the DB.
func TestFulltext2CreateRowTerms(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)

	run := func(parser string, texts ...string) ([]fulltext2.WordPos, error) {
		st := &fulltext2CreateState{tblcfg: fulltext2.TableConfig{Parser: parser}}
		tf, _ := ft2CreateArgVecs(t, mp, "{}", 1, texts...)
		return st.rowTerms(tf, proc, 0)
	}

	// ngram over plain text → non-empty ordered terms.
	terms, err := run(fulltext2.ParserNgram, "hello world")
	require.NoError(t, err)
	require.NotEmpty(t, terms)

	// two text columns are joined and both contribute.
	terms, err = run(fulltext2.ParserNgram, "alpha", "beta")
	require.NoError(t, err)
	require.NotEmpty(t, terms)

	// json parser: flattened values indexed as ngram.
	terms, err = run(fulltext2.ParserJSON, `{"a":"matrix","b":"origin"}`)
	require.NoError(t, err)
	require.NotEmpty(t, terms)

	// json_value parser: each whole value is one atomic token.
	terms, err = run(fulltext2.ParserJSONValue, `{"a":"matrix","b":"origin"}`)
	require.NoError(t, err)
	require.NotEmpty(t, terms)

	// empty text → no tokens.
	terms, err = run(fulltext2.ParserNgram, "")
	require.NoError(t, err)
	require.Empty(t, terms)

	// A SQL NULL column contributes no terms, while another indexed column still
	// contributes its terms.
	{
		st := &fulltext2CreateState{tblcfg: fulltext2.TableConfig{Parser: fulltext2.ParserNgram}}
		tf, _ := ft2CreateNullableArgVecs(t, mp, "{}", 1, nil, "world")
		terms, err = st.rowTerms(tf, proc, 0)
		require.NoError(t, err)
		require.Equal(t, []fulltext2.WordPos{{Word: "world", Pos: 0}}, terms)
	}

	// invalid json → error surfaced.
	_, err = run(fulltext2.ParserJSON, "{not json")
	require.Error(t, err)
}

func TestFulltext2CreateRowTermsNullColumnMatrix(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)

	tests := []struct {
		name       string
		parser     string
		jsonNoKeys bool
		values     []any
		want       []fulltext2.WordPos
	}{
		{
			name:   "plain left null",
			parser: fulltext2.ParserNgram,
			values: []any{nil, "beta"},
			want:   []fulltext2.WordPos{{Word: "beta", Pos: 0}},
		},
		{
			name:   "plain right null",
			parser: fulltext2.ParserNgram,
			values: []any{"alpha", nil},
			want:   []fulltext2.WordPos{{Word: "alpha", Pos: 0}},
		},
		{
			name:   "plain middle null",
			parser: fulltext2.ParserNgram,
			values: []any{"alpha", nil, "beta"},
			want:   []fulltext2.WordPos{{Word: "alpha", Pos: 0}, {Word: "beta", Pos: 6}},
		},
		{
			name:   "plain all null",
			parser: fulltext2.ParserNgram,
			values: []any{nil, nil},
			want:   nil,
		},
		{
			name:   "json value left null",
			parser: fulltext2.ParserJSONValue,
			values: []any{nil, `{"k":"right"}`},
			want:   []fulltext2.WordPos{{Word: "right", Pos: 0}},
		},
		{
			name:   "json value right null",
			parser: fulltext2.ParserJSONValue,
			values: []any{`{"k":"left"}`, nil},
			want:   []fulltext2.WordPos{{Word: "left", Pos: 0}},
		},
		{
			name:   "json value both",
			parser: fulltext2.ParserJSONValue,
			values: []any{`{"k":"left"}`, `{"k":"right"}`},
			want:   []fulltext2.WordPos{{Word: "left", Pos: 0}, {Word: "right", Pos: 5}},
		},
		{
			name:       "json flat left null",
			parser:     fulltext2.ParserJSON,
			jsonNoKeys: true,
			values:     []any{nil, `{"k":"right"}`},
			want:       []fulltext2.WordPos{{Word: "right", Pos: 0}},
		},
		{
			name:       "json flat right null",
			parser:     fulltext2.ParserJSON,
			jsonNoKeys: true,
			values:     []any{`{"k":"left"}`, nil},
			want:       []fulltext2.WordPos{{Word: "left", Pos: 0}},
		},
		{
			name:       "json flat middle null",
			parser:     fulltext2.ParserJSON,
			jsonNoKeys: true,
			values:     []any{`{"k":"left"}`, nil, `{"k":"right"}`},
			want:       []fulltext2.WordPos{{Word: "left", Pos: 0}, {Word: "right", Pos: 5}},
		},
		{
			name:       "json flat all null",
			parser:     fulltext2.ParserJSON,
			jsonNoKeys: true,
			values:     []any{nil, nil, nil},
			want:       nil,
		},
		{
			name:       "json flat literal null",
			parser:     fulltext2.ParserJSON,
			jsonNoKeys: true,
			values:     []any{"null", nil},
			want:       nil,
		},
		{
			name:       "json flat string null",
			parser:     fulltext2.ParserJSON,
			jsonNoKeys: true,
			values:     []any{`"null"`, nil},
			want:       []fulltext2.WordPos{{Word: "null", Pos: 0}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &fulltext2CreateState{tblcfg: fulltext2.TableConfig{Parser: tt.parser, JSONNoKeys: tt.jsonNoKeys}}
			tf, _ := ft2CreateNullableArgVecs(t, mp, "{}", 1, tt.values...)
			got, err := st.rowTerms(tf, proc, 0)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestFulltext2CreateRowTermsJSONTupleSkipsNull(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	st := &fulltext2CreateState{tblcfg: fulltext2.TableConfig{Parser: fulltext2.ParserJSON}}
	tf, _ := ft2CreateNullableArgVecs(t, mp, "{}", 1, `{"a":"left"}`, nil, `{"b":"right"}`)

	got, err := st.rowTerms(tf, proc, 0)
	require.NoError(t, err)
	require.Len(t, got, 2)
	// Keep the expected carrier terms independent of the adapter under test.
	require.Equal(t, []string{
		fulltext2.JSONStringTerm("a", "left"),
		fulltext2.JSONStringTerm("b", "right"),
	}, []string{got[0].Word, got[1].Word})
	require.Equal(t, []int32{0, 1}, []int32{got[0].Pos, got[1].Pos})
}

func TestFulltext2CreateRowTermsBinaryJSONNullSibling(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	st := &fulltext2CreateState{tblcfg: fulltext2.TableConfig{Parser: fulltext2.ParserJSON}}

	pk := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](pk, 1, false, mp))
	left := vector.NewVec(types.T_json.ToType())
	bj, err := bytejson.ParseFromString(`{"k":"binary"}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(left, bj, false, mp))
	right := vector.NewVec(types.T_json.ToType())
	require.NoError(t, vector.AppendByteJson(right, bytejson.ByteJson{}, true, mp))

	tf := newFT2TF([]string{"status"}, ft2StatusRets())
	tf.Args = []*plan.Expr{makeStrConstExpr("{}"), makeStrConstExpr("pk"), makeStrConstExpr("left"), makeStrConstExpr("right")}
	tf.ctr.argVecs = []*vector.Vector{ft2ConstStr(t, mp, "{}"), pk, left, right}
	got, err := st.rowTerms(tf, proc, 0)
	require.NoError(t, err)
	require.Equal(t, []fulltext2.WordPos{{Word: fulltext2.JSONStringTerm("k", "binary"), Pos: 0}}, got)
}

func TestFulltext2CreateRowTermsNullBitmapAndConstNull(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	st := &fulltext2CreateState{tblcfg: fulltext2.TableConfig{Parser: fulltext2.ParserNgram}}

	// Two rows carry opposite NULL patterns. Each row must use its own bitmap;
	// a NULL in row 0 must not suppress row 1's sibling (or vice versa).
	cfg := ft2ConstStr(t, mp, "{}")
	pk := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](pk, 1, false, mp))
	require.NoError(t, vector.AppendFixed[int64](pk, 2, false, mp))
	left := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(left, []byte("alpha"), false, mp))
	require.NoError(t, vector.AppendBytes(left, nil, true, mp))
	right := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(right, nil, true, mp))
	require.NoError(t, vector.AppendBytes(right, []byte("beta"), false, mp))
	tf := newFT2TF([]string{"status"}, ft2StatusRets())
	tf.Args = []*plan.Expr{makeStrConstExpr("{}"), makeStrConstExpr("pk"), makeStrConstExpr("left"), makeStrConstExpr("right")}
	tf.ctr.argVecs = []*vector.Vector{cfg, pk, left, right}
	got, err := st.rowTerms(tf, proc, 0)
	require.NoError(t, err)
	require.Equal(t, []fulltext2.WordPos{{Word: "alpha", Pos: 0}}, got)
	got, err = st.rowTerms(tf, proc, 1)
	require.NoError(t, err)
	require.Equal(t, []fulltext2.WordPos{{Word: "beta", Pos: 0}}, got)

	// A constant NULL vector represents a NULL expression for every row and
	// must be skipped without affecting the other vector's row values.
	constNull := vector.NewConstNull(types.T_varchar.ToType(), 2, mp)
	other := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(other, []byte("one"), false, mp))
	require.NoError(t, vector.AppendBytes(other, []byte("two"), false, mp))
	tf.Args = []*plan.Expr{makeStrConstExpr("{}"), makeStrConstExpr("pk"), makeStrConstExpr("null"), makeStrConstExpr("other")}
	tf.ctr.argVecs = []*vector.Vector{cfg, pk, constNull, other}
	got, err = st.rowTerms(tf, proc, 0)
	require.NoError(t, err)
	require.Equal(t, []fulltext2.WordPos{{Word: "one", Pos: 0}}, got)
	got, err = st.rowTerms(tf, proc, 1)
	require.NoError(t, err)
	require.Equal(t, []fulltext2.WordPos{{Word: "two", Pos: 0}}, got)
}

func TestFulltext2CreateRowTermsMalformedJSONWithNullSibling(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	st := &fulltext2CreateState{tblcfg: fulltext2.TableConfig{Parser: fulltext2.ParserJSONValue}}
	for _, values := range [][]any{
		{nil, "{bad"},
		{"{bad", nil},
	} {
		tf, _ := ft2CreateNullableArgVecs(t, mp, "{}", 1, values...)
		got, err := st.rowTerms(tf, proc, 0)
		require.Error(t, err)
		require.Nil(t, got)
	}
	st = &fulltext2CreateState{tblcfg: fulltext2.TableConfig{Parser: fulltext2.ParserJSON, JSONNoKeys: true}}
	for _, values := range [][]any{
		{nil, "{bad"},
		{"{bad", nil},
	} {
		tf, _ := ft2CreateNullableArgVecs(t, mp, "{}", 1, values...)
		got, err := st.rowTerms(tf, proc, 0)
		require.Error(t, err)
		require.Nil(t, got)
	}
	st = &fulltext2CreateState{tblcfg: fulltext2.TableConfig{Parser: fulltext2.ParserJSON}}
	for _, values := range [][]any{
		{nil, "{bad"},
		{"{bad", nil},
	} {
		tf, _ := ft2CreateNullableArgVecs(t, mp, "{}", 1, values...)
		got, err := st.rowTerms(tf, proc, 0)
		require.Error(t, err)
		require.Nil(t, got)
	}
}

func TestFulltext2CreateEndNotInited(t *testing.T) {
	proc := testutil.NewProc(t)
	st := &fulltext2CreateState{}
	// end() before start() must be a no-op (no seal / DB access).
	require.NoError(t, st.end(&TableFunction{}, proc))
}

// TestFulltext2CreateSealAndEnd forces a seal (capacity=1) so the persistence path runs:
// DeleteAllBasesSqls + Persist (spilling to an os temp file, since no LOCAL
// fileservice) are captured at the borrowed executor boundary, then end() seals the final
// (empty) segment and evicts the cache.
func TestFulltext2CreateSealAndEnd(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"success", nil},
		{"storage error", errors.New("storage write failed")},
		{"cancelled storage", context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
			proc.Base.SessionInfo.Account = "tenant"
			proc.Base.SessionInfo.Role = "accountadmin"
			prev := fulltext2_runSql
			t.Cleanup(func() { fulltext2_runSql = prev })
			var ran int
			dir := t.TempDir()
			t.Setenv("TMPDIR", dir)
			proc.Base.FileService = nil
			fulltext2_runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				ran++
				account, role := sqlproc.StatementOption().SessionIdentity()
				require.Equal(t, "tenant", account)
				require.Equal(t, "accountadmin", role)
				return executor.Result{}, nil
			}
			mockIndexTxn(t, proc, func(sql string, opts executor.StatementOption) (executor.Result, error) {
				ran++
				account, role := opts.SessionIdentity()
				require.Equal(t, "tenant", account)
				require.Equal(t, "accountadmin", role)
				require.NotContains(t, sql, "load_file")
				files, err := filepath.Glob(filepath.Join(dir, "ft2build*"))
				require.NoError(t, err)
				require.Empty(t, files, "the spill is unlinked while its handle serves the insert")
				if opts.HasParams() {
					vec, err := opts.Params(mp)
					require.NoError(t, err)
					require.NotEmpty(t, vec.GetRawBytesAt(1))
					vec.Free(mp)
					return executor.Result{}, tc.err
				}
				return executor.Result{}, nil
			})

			// capacity=1 seals one real spill without additional data or services.
			cfg, err := sonic.Marshal(fulltext2.TableConfig{DbName: "db", IndexTable: "__idx", MetadataTable: "__meta", Parser: fulltext2.ParserNgram, Capacity: 1, ThreadsBuild: 1})
			require.NoError(t, err)
			st := &fulltext2CreateState{}
			tf, vecs := ft2CreateArgVecs(t, mp, string(cfg), 1, "hello world")
			t.Cleanup(func() {
				st.free(tf, proc, false, nil)
				for _, v := range vecs {
					v.Free(mp)
				}
			})
			err = st.start(tf, proc, 0, nil)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, st.cur)
				require.Zero(t, st.cur.NumDocs())
				// The final empty segment adds no storage statements.
				before := ran
				require.NoError(t, st.end(tf, proc))
				require.Equal(t, before, ran)
			}
			require.Equal(t, int64(1), st.capacity)
			require.Equal(t, 1, st.segIdx)
			require.True(t, st.basesCleared)
			files, err := filepath.Glob(filepath.Join(dir, "ft2build*"))
			require.NoError(t, err)
			require.Empty(t, files, "success/error/cancellation removes the owned spill")
			require.Equal(t, "tenant", proc.GetSessionInfo().Account)
			require.Equal(t, "accountadmin", proc.GetSessionInfo().Role)
		})
	}
}

// ft2PipelineHarness drives the create TVF with threads_build through the pipelined build
// and records the metadata inserts the build writes.
type ft2PipelineHarness struct {
	t        *testing.T
	mp       *mpool.MPool
	proc     *process.Process
	dir      string
	metaSQLs []string
	writeErr error
}

func newFT2PipelineHarness(t *testing.T, writeErr error) *ft2PipelineHarness {
	h := &ft2PipelineHarness{t: t, mp: mpool.MustNewZero(), dir: t.TempDir(), writeErr: writeErr}
	h.proc = testutil.NewProcessWithOwnedMPool(t, "", h.mp)
	h.proc.Base.FileService = nil
	t.Setenv("TMPDIR", h.dir)
	prev := fulltext2_runSql
	t.Cleanup(func() { fulltext2_runSql = prev })
	fulltext2_runSql = func(*sqlexec.SqlProcess, string) (executor.Result, error) { return executor.Result{}, nil }
	mockIndexTxn(t, h.proc, func(sql string, opts executor.StatementOption) (executor.Result, error) {
		if opts.HasParams() {
			return executor.Result{}, h.writeErr
		}
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "INSERT ") {
			return executor.Result{}, nil
		}
		h.metaSQLs = append(h.metaSQLs, sql)
		return executor.Result{}, nil
	})
	return h
}

func (h *ft2PipelineHarness) feed(st *fulltext2CreateState, threads int64, rows int) error {
	cfg, err := sonic.Marshal(fulltext2.TableConfig{DbName: "db", IndexTable: "__idx", MetadataTable: "__meta",
		Parser: fulltext2.ParserNgram, Capacity: 2, ThreadsBuild: threads})
	require.NoError(h.t, err)
	for i := 0; i < rows; i++ {
		tf, vecs := ft2CreateArgVecs(h.t, h.mp, string(cfg), int64(i+1), "hello world row")
		err := st.start(tf, h.proc, 0, nil)
		for _, v := range vecs {
			v.Free(h.mp)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *ft2PipelineHarness) spillFiles() []string {
	files, err := filepath.Glob(filepath.Join(h.dir, "ft2build*"))
	require.NoError(h.t, err)
	return files
}

func TestFulltext2CreatePipelinedBuild(t *testing.T) {
	h := newFT2PipelineHarness(t, nil)
	st := &fulltext2CreateState{}
	require.NoError(t, h.feed(st, 4, 10))
	require.Equal(t, 4, st.threads)
	require.NotNil(t, st.pipe)
	require.Nil(t, st.cur)
	tf, vecs := ft2CreateArgVecs(t, h.mp, ft2CreateCfg(t, fulltext2.ParserNgram, false), 1, "x")
	t.Cleanup(func() {
		for _, v := range vecs {
			v.Free(h.mp)
		}
	})
	require.NoError(t, st.end(tf, h.proc))
	require.Nil(t, st.pipe)
	require.Equal(t, 5, st.segIdx)
	require.True(t, st.basesCleared)
	require.Len(t, h.metaSQLs, 5)
	for i, sql := range h.metaSQLs {
		require.Contains(t, sql, fulltext2.SubIndexId(st.uid, i))
	}
	require.Empty(t, h.spillFiles())
	st.reset(tf, h.proc)
	st.free(tf, h.proc, false, nil)
}

func TestFulltext2CreatePipelinedBuildWriteError(t *testing.T) {
	boom := errors.New("chunk write failed")
	h := newFT2PipelineHarness(t, boom)
	st := &fulltext2CreateState{}
	err := h.feed(st, 4, 10)
	tf, vecs := ft2CreateArgVecs(t, h.mp, ft2CreateCfg(t, fulltext2.ParserNgram, false), 1, "x")
	t.Cleanup(func() {
		for _, v := range vecs {
			v.Free(h.mp)
		}
	})
	if err == nil {
		err = st.end(tf, h.proc)
	}
	require.ErrorIs(t, err, boom)
	st.free(tf, h.proc, true, err)
	require.Nil(t, st.pipe)
	require.Empty(t, h.spillFiles())
}

func TestFulltext2CreatePipelinedBuildAbortBeforeEnd(t *testing.T) {
	h := newFT2PipelineHarness(t, nil)
	st := &fulltext2CreateState{}
	require.NoError(t, h.feed(st, 4, 9))
	require.NotNil(t, st.pipe)
	tf, vecs := ft2CreateArgVecs(t, h.mp, ft2CreateCfg(t, fulltext2.ParserNgram, false), 1, "x")
	t.Cleanup(func() {
		for _, v := range vecs {
			v.Free(h.mp)
		}
	})
	st.reset(tf, h.proc)
	require.Nil(t, st.pipe)
	st.free(tf, h.proc, true, nil)
	require.Empty(t, h.spillFiles())
}

func TestFulltext2CreateThreadsBuildResolution(t *testing.T) {
	h := newFT2PipelineHarness(t, nil)
	serial := &fulltext2CreateState{}
	require.NoError(t, h.feed(serial, 1, 1))
	require.Equal(t, 1, serial.threads)
	require.Nil(t, serial.pipe)
	require.NotNil(t, serial.cur)

	auto := &fulltext2CreateState{}
	require.NoError(t, h.feed(auto, 0, 1))
	require.Equal(t, int(vectorindex.GetConcurrencyForBuild(0)), auto.threads)
	tf, vecs := ft2CreateArgVecs(t, h.mp, ft2CreateCfg(t, fulltext2.ParserNgram, false), 1, "x")
	t.Cleanup(func() {
		for _, v := range vecs {
			v.Free(h.mp)
		}
	})
	serial.free(tf, h.proc, false, nil)
	auto.free(tf, h.proc, false, nil)

	require.Equal(t, 1, fulltext2BuildMaxFinishing(1))
	require.Equal(t, 1, fulltext2BuildMaxFinishing(3))
	require.Equal(t, 2, fulltext2BuildMaxFinishing(8))
}

func TestFulltext2CreatePipelinedRerunContinuesSegmentIds(t *testing.T) {
	h := newFT2PipelineHarness(t, nil)
	st := &fulltext2CreateState{}
	tf, vecs := ft2CreateArgVecs(t, h.mp, ft2CreateCfg(t, fulltext2.ParserNgram, false), 1, "x")
	t.Cleanup(func() {
		for _, v := range vecs {
			v.Free(h.mp)
		}
	})
	require.NoError(t, h.feed(st, 4, 6))
	require.NoError(t, st.end(tf, h.proc))
	require.Equal(t, 3, st.segIdx)
	st.reset(tf, h.proc)

	require.NoError(t, h.feed(st, 4, 6))
	require.NoError(t, st.end(tf, h.proc))
	require.Equal(t, 6, st.segIdx)
	require.Len(t, h.metaSQLs, 6)
	for i, sql := range h.metaSQLs {
		require.Contains(t, sql, "'"+fulltext2.SubIndexId(st.uid, i)+"'")
	}
	st.free(tf, h.proc, false, nil)
	require.Empty(t, h.spillFiles())
}
