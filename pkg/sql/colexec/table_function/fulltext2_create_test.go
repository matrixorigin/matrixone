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
	"testing"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// ft2CreateCfg marshals a minimal TableConfig JSON constant (the shape the compile
// layer passes as argVecs[0]).
func ft2CreateCfg(t *testing.T, parser string, positionFree bool) string {
	t.Helper()
	b, err := sonic.Marshal(fulltext2.TableConfig{
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
	vecs := []*vector.Vector{ft2ConstStr(t, mp, cfg), pkVec}
	args := []*plan.Expr{makeStrConstExpr(cfg), makeStrConstExpr("pk")}
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
	proc := testutil.NewProcessWithMPool(t, "", mp)

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
	proc := testutil.NewProcessWithMPool(t, "", mp)

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

// TestFulltext2CreateRowTerms exercises the tokenization paths of rowTerms across the
// ngram / json / json_value parsers and NULL/empty inputs, without touching the DB.
func TestFulltext2CreateRowTerms(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

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

	// a NULL column short-circuits to no tokens.
	{
		st := &fulltext2CreateState{tblcfg: fulltext2.TableConfig{Parser: fulltext2.ParserNgram}}
		tf, vecs := ft2CreateArgVecs(t, mp, "{}", 1, "ignored")
		nullCol := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(nullCol, nil, true, mp))
		vecs[2] = nullCol
		tf.ctr.argVecs = vecs
		terms, err = st.rowTerms(tf, proc, 0)
		require.NoError(t, err)
		require.Empty(t, terms)
	}

	// invalid json → error surfaced.
	_, err = run(fulltext2.ParserJSON, "{not json")
	require.Error(t, err)
}

func TestFulltext2CreateEndNotInited(t *testing.T) {
	proc := testutil.NewProc(t)
	st := &fulltext2CreateState{}
	// end() before start() must be a no-op (no seal / DB access).
	require.NoError(t, st.end(&TableFunction{}, proc))
}

// TestFulltext2CreateSealAndEnd forces a seal (capacity=1) so the persistence path runs:
// DeleteAllBasesSqls + ToInsertSqls (spilling to an os temp file, since no LOCAL
// fileservice) are captured by a stubbed fulltext2_runSql, then end() seals the final
// (empty) segment and evicts the cache.
func TestFulltext2CreateSealAndEnd(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	prev := fulltext2_runSql
	defer func() { fulltext2_runSql = prev }()
	var ran int
	fulltext2_runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		ran++
		return executor.Result{}, nil
	}

	// capacity=1 ⇒ each doc seals its own segment.
	cfg, err := sonic.Marshal(fulltext2.TableConfig{DbName: "db", IndexTable: "__idx", Parser: fulltext2.ParserNgram, Capacity: 1})
	require.NoError(t, err)

	st := &fulltext2CreateState{}
	tf, _ := ft2CreateArgVecs(t, mp, string(cfg), 1, "hello world")
	require.NoError(t, st.start(tf, proc, 0, nil))
	require.Equal(t, int64(1), st.capacity)
	// the doc reached capacity ⇒ its segment was sealed and persisted.
	require.Equal(t, 1, st.segIdx)
	require.True(t, st.basesCleared)
	require.NotNil(t, st.cur) // a fresh empty builder replaces the sealed one
	require.Equal(t, 0, st.cur.NumDocs())
	require.Positive(t, ran)

	// end() seals the (empty) trailing segment and evicts the cache; segIdx stays 1.
	require.NoError(t, st.end(tf, proc))
	require.Equal(t, 1, st.segIdx)

	st.free(tf, proc, false, nil)
}
