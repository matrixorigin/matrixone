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

func newFTTTestCase(m *mpool.MPool, attrs []string, param string) fulltextTokenizeTestCase {
	proc := testutil.NewProcessWithMPool("", m)
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

	ut := newFTTTestCase(mpool.MustNewZero(), fttdefaultAttrs, "")

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

	require.Equal(t, 4, result.Batch.RowCount())

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextTokenizeCallJSON(t *testing.T) {

	ut := newFTTTestCase(mpool.MustNewZero(), fttdefaultAttrs, "{\"parser\":\"json\"}")

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

	require.Equal(t, 1, result.Batch.RowCount())

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextTokenizeCallJSONValue(t *testing.T) {

	ut := newFTTTestCase(mpool.MustNewZero(), fttdefaultAttrs, "{\"parser\":\"json_value\"}")

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

	require.Equal(t, 1, result.Batch.RowCount())

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
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
	vector.AppendBytes(bat.Vecs[0], []byte("{\"a\":\"abcdedfghijklmnopqrstuvwxyz\"}"), false, proc.Mp())

	bat.SetRowCount(1)
	return bat
}
