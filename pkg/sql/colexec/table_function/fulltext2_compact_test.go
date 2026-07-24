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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// ft2ConstStr builds a single-row constant varchar vector (the shape every TVF config
// arg takes: a JSON/string constant produced by the compile layer).
func ft2ConstStr(t *testing.T, mp *mpool.MPool, s string) *vector.Vector {
	t.Helper()
	v, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(s), 1, mp)
	require.NoError(t, err)
	return v
}

// ft2NonConstStr builds a NON-const varchar vector (to exercise the "must be a string
// constant" guard).
func ft2NonConstStr(t *testing.T, mp *mpool.MPool, s string) *vector.Vector {
	t.Helper()
	v := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(v, []byte(s), false, mp))
	return v
}

// newFT2TF assembles a minimal TableFunction with a populated retSchema so
// createResultBatch works, without going through the plan/registry.
func newFT2TF(attrs []string, rets []*plan.ColDef) *TableFunction {
	tf := &TableFunction{Attrs: attrs, Rets: rets}
	schema := make([]types.Type, len(rets))
	for i := range rets {
		schema[i] = types.New(types.T(rets[i].Typ.Id), rets[i].Typ.Width, rets[i].Typ.Scale)
	}
	tf.ctr.retSchema = schema
	return tf
}

func ft2StatusRets() []*plan.ColDef {
	return []*plan.ColDef{{Name: "status", Typ: plan.Type{Id: int32(types.T_int64)}}}
}

func makeStrConstExpr(s string) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar), Width: 256},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: s}}},
	}
}

func TestFulltext2CompactPrepareArgCount(t *testing.T) {
	proc := testutil.NewProc(t)

	// too few / too many args are rejected by prepare.
	for _, n := range []int{0, 1, 3, 7} {
		args := make([]*plan.Expr, n)
		for i := range args {
			args[i] = makeStrConstExpr("x")
		}
		arg := &TableFunction{Args: args, FuncName: "fulltext2_compact"}
		_, err := fulltext2CompactPrepare(proc, arg)
		require.Error(t, err, "arg count %d must be rejected", n)
	}

	// 4, 5 and 6 args are all valid.
	for _, n := range []int{4, 5, 6} {
		args := make([]*plan.Expr, n)
		for i := range args {
			args[i] = makeStrConstExpr("x")
		}
		arg := &TableFunction{Args: args, FuncName: "fulltext2_compact"}
		st, err := fulltext2CompactPrepare(proc, arg)
		require.NoError(t, err, "arg count %d must be accepted", n)
		require.NotNil(t, st)
		require.Len(t, arg.ctr.argVecs, n)
	}
}

func TestFulltext2CompactStartValidation(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	tf := newFT2TF([]string{"status"}, ft2StatusRets())

	good := func() []*vector.Vector {
		return []*vector.Vector{
			ft2ConstStr(t, mp, "db"),
			ft2ConstStr(t, mp, "store"),
			ft2ConstStr(t, mp, "meta"),
			ft2ConstStr(t, mp, "2"),
		}
	}

	// non-varchar arg → invalid input.
	{
		st := &fulltext2CompactState{}
		vecs := good()
		nonStr, nerr := vector.NewConstFixed(types.T_int64.ToType(), int64(2), 1, mp)
		require.NoError(t, nerr)
		vecs[3] = nonStr
		tf.ctr.argVecs = vecs
		require.ErrorContains(t, st.start(tf, proc, 0, nil), "must be strings")
	}
	// non-const arg → internal error.
	{
		st := &fulltext2CompactState{}
		vecs := good()
		vecs[0] = ft2NonConstStr(t, mp, "db")
		tf.ctr.argVecs = vecs
		require.ErrorContains(t, st.start(tf, proc, 0, nil), "must be string constants")
	}
	// empty db/store/meta → internal error.
	{
		st := &fulltext2CompactState{}
		vecs := good()
		vecs[1] = ft2ConstStr(t, mp, "")
		tf.ctr.argVecs = vecs
		require.ErrorContains(t, st.start(tf, proc, 0, nil), "must be non-empty")
	}
	// non-integer capacity → invalid input.
	{
		st := &fulltext2CompactState{}
		vecs := good()
		vecs[3] = ft2ConstStr(t, mp, "notanint")
		tf.ctr.argVecs = vecs
		require.ErrorContains(t, st.start(tf, proc, 0, nil), "capacity must be an integer")
	}
}

func TestFulltext2CompactStartOptionalArgs(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	tf := newFT2TF([]string{"status"}, ft2StatusRets())

	// 6 args: capacity + position_free=true + posting_capacity=4096 all parsed.
	st := &fulltext2CompactState{}
	tf.ctr.argVecs = []*vector.Vector{
		ft2ConstStr(t, mp, "db"),
		ft2ConstStr(t, mp, "store"),
		ft2ConstStr(t, mp, "meta"),
		ft2ConstStr(t, mp, "7"),
		ft2ConstStr(t, mp, "true"),
		ft2ConstStr(t, mp, "4096"),
	}
	require.NoError(t, st.start(tf, proc, 0, nil))
	require.True(t, st.inited)
	require.Equal(t, int64(7), st.capacity)
	require.True(t, st.tblcfg.PositionFree)
	require.Equal(t, int64(4096), st.postingCap)
	require.Equal(t, "db", st.tblcfg.DbName)
	require.Equal(t, "store", st.tblcfg.IndexTable)
	require.Equal(t, "meta", st.tblcfg.MetadataTable)

	// second start() is a no-op once inited.
	require.NoError(t, st.start(tf, proc, 0, nil))

	// lifecycle: call returns cancel, reset/free are safe.
	res, err := st.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, vm.CancelResult, res)
	st.reset(tf, proc)
	st.free(tf, proc, false, nil)
}

func TestFulltext2CompactEndNotInited(t *testing.T) {
	proc := testutil.NewProc(t)
	st := &fulltext2CompactState{}
	// end() before start() must be a no-op (no CompactSegments / DB access).
	require.NoError(t, st.end(&TableFunction{}, proc))
}
