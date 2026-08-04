// Copyright 2022 Matrix Origin
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

package table_function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type fulltextTokenizeTestCase struct {
	arg  *TableFunction
	proc *process.Process
}

var (
	fttdefaultAttrs = []string{"DOC_ID", "POS", "WORD"}

	fftdefaultColdefs = []*plan.ColDef{
		// row_id type should be same as index type
		{
			Name: "DOC_ID",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
			},
		},
		{
			Name: "POS",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
			},
		},
		{
			Name: "WORD",
			Typ: plan.Type{
				Id:          int32(types.T_varchar),
				NotNullable: false,
				Width:       128,
			},
		},
	}
)

func newFTTTestCase(t *testing.T, m *mpool.MPool, attrs []string, param string) fulltextTokenizeTestCase {
	proc := testutil.NewProcessWithMPool(t, "", m)
	colDefs := make([]*plan.ColDef, len(attrs))
	for i := range attrs {
		for j := range fftdefaultColdefs {
			if attrs[i] == fftdefaultColdefs[j].Name {
				colDefs[i] = fftdefaultColdefs[j]
				break
			}
		}
	}

	ret := fulltextTokenizeTestCase{
		proc: proc,
		arg: &TableFunction{
			Attrs:    attrs,
			Rets:     colDefs,
			FuncName: "fulltext_index_tokenize",
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			Params: []byte(param),
		},
	}
	return ret
}

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextTokenizeCall(t *testing.T) {

	ut := newFTTTestCase(t, mpool.MustNewZero(), fttdefaultAttrs, "")

	inbat := makeBatchFTT(ut.proc)

	ut.arg.Args = makeConstInputExprsFTT()
	//fmt.Printf("%v\n", ut.arg.Args)

	// Prepare
	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// start
	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	// first call receive data
	result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)

	require.Equal(t, result.Status, vm.ExecNext)

	require.Equal(t, 5, result.Batch.RowCount())

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// TestFullTextTokenizeCallGojieba verifies that the tokenize TVF, when
// given parser="gojieba", segments Chinese input via jieba and emits one
// row per word plus the trailing __DocLen marker — matching what the index
// rows actually contain, so query-time phrase JOINs can land on real rows.
func TestFullTextTokenizeCallGojieba(t *testing.T) {

	ut := newFTTTestCase(t, mpool.MustNewZero(), fttdefaultAttrs, `{"parser":"gojieba"}`)

	inbat := makeBatchGojiebaFTT(ut.proc)

	ut.arg.Args = makeConstInputGojiebaExprsFTT()

	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecNext)

	// Input "我来到北京" → jieba HMM=false yields 3 tokens (我, 来到, 北京);
	// fulltext_index_tokenize appends one __DocLen marker → 4 rows total.
	require.Equal(t, 4, result.Batch.RowCount())

	wordVec := result.Batch.Vecs[2]
	got := make([]string, result.Batch.RowCount())
	for i := 0; i < result.Batch.RowCount(); i++ {
		got[i] = wordVec.GetStringAt(i)
	}
	require.Equal(t, []string{"我", "来到", "北京", "__DocLen"}, got)

	ut.arg.ctr.state.reset(ut.arg, ut.proc)
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

func makeConstInputGojiebaExprsFTT() []*plan.Expr {
	return []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_int32)},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 1}},
			},
		},
		{
			Typ: plan.Type{Id: int32(types.T_varchar), Width: 128},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "我来到北京"}},
			},
		},
	}
}

func makeBatchGojiebaFTT(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int32, 4, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_varchar, 128, 0))

	vector.AppendFixed[int32](bat.Vecs[0], int32(1), false, proc.Mp())
	vector.AppendBytes(bat.Vecs[1], []byte("我来到北京"), false, proc.Mp())

	bat.SetRowCount(1)
	return bat
}

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextTokenizeCallJSON(t *testing.T) {

	ut := newFTTTestCase(t, mpool.MustNewZero(), fttdefaultAttrs, "{\"parser\":\"json\"}")

	inbat := makeBatchJSONFTT(ut.proc)

	ut.arg.Args = makeConstInputJSONExprsFTT()
	//fmt.Printf("%v\n", ut.arg.Args)

	// Prepare
	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// start
	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	// first call receive data
	result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)

	require.Equal(t, result.Status, vm.ExecNext)

	require.Equal(t, 2, result.Batch.RowCount())

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextTokenizeCallJSONValue(t *testing.T) {

	ut := newFTTTestCase(t, mpool.MustNewZero(), fttdefaultAttrs, "{\"parser\":\"json_value\"}")

	inbat := makeBatchJSONFTT(ut.proc)

	ut.arg.Args = makeConstInputJSONExprsFTT()
	//fmt.Printf("%v\n", ut.arg.Args)

	// Prepare
	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// start
	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	// first call receive data
	result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)

	require.Equal(t, result.Status, vm.ExecNext)

	require.Equal(t, 2, result.Batch.RowCount())

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

func TestFullTextTokenizeSkipsNullContentColumns(t *testing.T) {
	testCases := []struct {
		name  string
		param string
		left  string
		right string
		words []string
	}{
		{name: "ordinary", left: "lefttoken", right: "righttoken", words: []string{"lefttoken", "righttoken"}},
		{name: "default", param: `{"parser":"default"}`, left: "lefttoken", right: "righttoken", words: []string{"lefttoken", "righttoken"}},
		{name: "ngram", param: `{"parser":"ngram"}`, left: "lefttoken", right: "righttoken", words: []string{"lefttoken", "righttoken"}},
		{name: "gojieba", param: `{"parser":"gojieba"}`, left: "北京", right: "清华大学", words: []string{"北京", "清华大学"}},
		{name: "json", param: `{"parser":"json"}`, left: `{"k":"lefttoken"}`, right: `{"k":"righttoken"}`, words: []string{"lefttoken", "righttoken"}},
		{name: "json_value", param: `{"parser":"json_value"}`, left: `{"k":"lefttoken"}`, right: `{"k":"righttoken"}`, words: []string{"lefttoken", "righttoken"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ut := newFTTTestCase(t, mpool.MustNewZero(), fttdefaultAttrs, testCase.param)
			ut.arg.Args = fullTextTokenizeThreeArgExprs()
			inbat := makeNullableFullTextBatch(t, ut.proc, testCase.left, testCase.right)
			defer inbat.Clean(ut.proc.Mp())

			state := &tokenizeState{}
			ut.arg.ctr.argVecs = inbat.Vecs
			defer state.free(ut.arg, ut.proc, false, nil)

			for row := 0; row < inbat.RowCount(); row++ {
				require.NoError(t, state.start(ut.arg, ut.proc, row, nil))

				if row == 2 {
					require.Empty(t, state.doc.Words)
					result, err := state.call(ut.arg, ut.proc)
					require.NoError(t, err)
					require.Equal(t, vm.CancelResult.Status, result.Status)
					continue
				}

				expectedWords := testCase.words
				if row < 2 {
					expectedWords = []string{testCase.words[1-row]}
				}
				require.Len(t, state.doc.Words, len(expectedWords)+1)
				for i, expectedWord := range expectedWords {
					require.Equal(t, expectedWord, state.doc.Words[i].Word)
				}
				require.Equal(t, int32(0), state.doc.Words[0].Pos)
				require.Equal(t, "__DocLen", state.doc.Words[len(expectedWords)].Word)
				require.Equal(t, int32(len(expectedWords)), state.doc.Words[len(expectedWords)].Pos)
			}
		})
	}
}

func TestFullTextTokenizeSkipsConstNullContentColumn(t *testing.T) {
	ut := newFTTTestCase(t, mpool.MustNewZero(), fttdefaultAttrs, "")
	ut.arg.Args = fullTextTokenizeThreeArgExprs()

	idVec, err := vector.NewConstFixed(types.T_int32.ToType(), int32(1), 1, ut.proc.Mp())
	require.NoError(t, err)
	leftVec := vector.NewConstNull(types.T_varchar.ToType(), 1, ut.proc.Mp())
	rightVec, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("righttoken"), 1, ut.proc.Mp())
	require.NoError(t, err)
	for _, vec := range []*vector.Vector{idVec, leftVec, rightVec} {
		defer vec.Free(ut.proc.Mp())
	}
	ut.arg.ctr.argVecs = []*vector.Vector{idVec, leftVec, rightVec}

	state := &tokenizeState{}
	defer state.free(ut.arg, ut.proc, false, nil)
	require.NoError(t, state.start(ut.arg, ut.proc, 0, nil))
	require.Len(t, state.doc.Words, 2)
	require.Equal(t, "righttoken", state.doc.Words[0].Word)
	require.Equal(t, int32(0), state.doc.Words[0].Pos)
	require.Equal(t, "__DocLen", state.doc.Words[1].Word)
}

func fullTextTokenizeThreeArgExprs() []*plan.Expr {
	return []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_int32)}},
		{Typ: plan.Type{Id: int32(types.T_varchar), Width: 128}},
		{Typ: plan.Type{Id: int32(types.T_varchar), Width: 128}},
	}
}

func makeNullableFullTextBatch(t *testing.T, proc *process.Process, left, right string) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())

	rows := []struct {
		leftNull  bool
		rightNull bool
	}{
		{leftNull: true},
		{rightNull: true},
		{leftNull: true, rightNull: true},
		{},
	}
	for i, row := range rows {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(i+1), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(left), row.leftNull, proc.Mp()))
		require.NoError(t, vector.AppendBytes(bat.Vecs[2], []byte(right), row.rightNull, proc.Mp()))
	}
	bat.SetRowCount(len(rows))
	return bat
}

// create const input exprs
func makeConstInputExprsFTT() []*plan.Expr {

	ret := []*plan.Expr{
		{
			Typ: plan.Type{
				Id: int32(types.T_int32),
			},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_I32Val{
						I32Val: 1,
					},
				},
			},
		},

		{
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: 128,
			},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_Sval{
						Sval: "this is a text",
					},
				},
			},
		}}

	return ret
}

// create input vector for arg (id, text)
func makeBatchFTT(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int32, 4, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_varchar, 128, 0))

	vector.AppendFixed[int32](bat.Vecs[0], int32(1), false, proc.Mp())
	vector.AppendBytes(bat.Vecs[1], []byte("this is a text"), false, proc.Mp())

	bat.SetRowCount(1)
	return bat
}

// JSON
// create const input exprs
func makeConstInputJSONExprsFTT() []*plan.Expr {

	ret := []*plan.Expr{
		{
			Typ: plan.Type{
				Id: int32(types.T_int32),
			},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_I32Val{
						I32Val: 1,
					},
				},
			},
		},

		{
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: 128,
			},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_Sval{
						Sval: "{\"a\":\"abcdedfghijklmnopqrstuvwxyz\"}",
					},
				},
			},
		}}

	return ret
}

// create input vector for arg (id, text)
func makeBatchJSONFTT(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int32, 4, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_varchar, 128, 0))

	vector.AppendFixed[int32](bat.Vecs[0], int32(1), false, proc.Mp())
	vector.AppendBytes(bat.Vecs[1], []byte("{\"a\":\"abcdedfghijklmnopqrstuvwxyz\"}"), false, proc.Mp())

	bat.SetRowCount(1)
	return bat
}
