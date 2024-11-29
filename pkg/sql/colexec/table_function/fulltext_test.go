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
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type fulltextTestCase struct {
	arg  *TableFunction
	proc *process.Process
}

var (
	ftdefaultAttrs = []string{"DOC_ID", "SCORE"}

	ftdefaultColdefs = []*plan.ColDef{
		// row_id type should be same as index type
		{
			Name: "DOC_ID",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
			},
		},
		{
			Name: "SCORE",
			Typ: plan.Type{
				Id:          int32(types.T_float32),
				NotNullable: false,
				Width:       4,
			},
		},
	}
)

func newFTTestCase(m *mpool.MPool, attrs []string) fulltextTestCase {
	proc := testutil.NewProcessWithMPool("", m)
	colDefs := make([]*plan.ColDef, len(attrs))
	for i := range attrs {
		for j := range ftdefaultColdefs {
			if attrs[i] == ftdefaultColdefs[j].Name {
				colDefs[i] = ftdefaultColdefs[j]
				break
			}
		}
	}

	ret := fulltextTestCase{
		proc: proc,
		arg: &TableFunction{
			Attrs:    attrs,
			Rets:     colDefs,
			FuncName: "fulltext_index_scan",
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			Limit: &plan.Expr{
				Typ: plan.Type{
					Id: int32(types.T_uint64),
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_U64Val{
							U64Val: 0,
						},
					},
				},
			},
		},
	}
	return ret
}

func fake_runSql(proc *process.Process, sql string) (executor.Result, error) {

	// give count
	return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeCountBatchFT(proc)}}, nil
}

func fake_runSql_streaming(proc *process.Process, sql string, ch chan executor.Result) (executor.Result, error) {

	defer close(ch)
	res := executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeTextBatchFT(proc)}}
	ch <- res
	return executor.Result{}, nil
}

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextCall(t *testing.T) {

	ut := newFTTestCase(mpool.MustNewZero(), ftdefaultAttrs)

	inbat := makeBatchFT(ut.proc)

	ut.arg.Args = makeConstInputExprsFT()
	//fmt.Printf("%v\n", ut.arg.Args)

	// Prepare
	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// stub runSql function
	ft_runSql = fake_runSql
	ft_runSql_streaming = fake_runSql_streaming

	// start
	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	var result vm.CallResult

	// first call receive data
	for i := 0; i < 3; i++ {
		result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
		require.Nil(t, err)
		require.Equal(t, result.Status, vm.ExecNext)
		require.Equal(t, result.Batch.RowCount(), 8192)
	}

	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecNext)
	require.Equal(t, result.Batch.RowCount(), 1)
	//fmt.Printf("ROW COUNT = %d  BATCH = %v\n", result.Batch.RowCount(), result.Batch)

	// second call receive channel close
	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecStop)

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextCallOneAttr(t *testing.T) {

	ut := newFTTestCase(mpool.MustNewZero(), ftdefaultAttrs[0:1])

	inbat := makeBatchFT(ut.proc)

	ut.arg.Args = makeConstInputExprsFT()
	//fmt.Printf("%v\n", ut.arg.Args)

	// Prepare
	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// stub runSql function
	ft_runSql = fake_runSql
	ft_runSql_streaming = fake_runSql_streaming

	// start
	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	var result vm.CallResult

	// first call receive data
	for i := 0; i < 3; i++ {
		result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
		require.Nil(t, err)
		require.Equal(t, result.Status, vm.ExecNext)
		require.Equal(t, result.Batch.RowCount(), 8192)
	}

	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecNext)
	require.Equal(t, result.Batch.RowCount(), 1)
	//fmt.Printf("ROW COUNT = %d  BATCH = %v\n", result.Batch.RowCount(), result.Batch)

	// second call receive channel close
	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecStop)

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextEarlyFree(t *testing.T) {

	ut := newFTTestCase(mpool.MustNewZero(), ftdefaultAttrs[0:1])

	inbat := makeBatchFT(ut.proc)

	ut.arg.Args = makeConstInputExprsFT()
	//fmt.Printf("%v\n", ut.arg.Args)

	// Prepare
	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// stub runSql function
	ft_runSql = fake_runSql
	ft_runSql_streaming = fake_runSql_streaming

	// start
	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	/*
		var result vm.CallResult
		// first call receive data
		for i := 0; i < 2; i++ {
			result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
			require.Nil(t, err)
			require.Equal(t, result.Status, vm.ExecNext)
			require.Equal(t, result.Batch.RowCount(), 8192)
		}
	*/

	// early free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// create const input exprs
func makeConstInputExprsFT() []*plan.Expr {

	//ret := make([]*plan.Expr, 4)
	ret := []*plan.Expr{{
		Typ: plan.Type{
			Id:    int32(types.T_varchar),
			Width: 256,
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Sval{
					Sval: "src_table",
				},
			},
		},
	}, {
		Typ: plan.Type{
			Id:    int32(types.T_varchar),
			Width: 256,
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Sval{
					Sval: "index_table",
				},
			},
		},
	}, {
		Typ: plan.Type{
			Id:    int32(types.T_varchar),
			Width: 256,
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Sval{
					Sval: "pattern",
				},
			},
		},
	}, {
		Typ: plan.Type{
			Id: int32(types.T_int64),
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_I64Val{
					I64Val: 0,
				},
			},
		},
	}}

	return ret
}

// create input vector for arg (src_table, index_table, pattern, mode)
func makeBatchFT(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(4)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 256, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_varchar, 256, 0))
	bat.Vecs[2] = vector.NewVec(types.New(types.T_varchar, 256, 0))
	bat.Vecs[3] = vector.NewVec(types.New(types.T_int32, 4, 0))

	vector.AppendBytes(bat.Vecs[0], []byte("src_table"), false, proc.Mp())
	vector.AppendBytes(bat.Vecs[1], []byte("idx_table"), false, proc.Mp())
	vector.AppendBytes(bat.Vecs[2], []byte("pattern"), false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[3], int64(0), false, proc.Mp())

	bat.SetRowCount(1)
	return bat
}

// create count (int64)
func makeCountBatchFT(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))

	vector.AppendFixed[int64](bat.Vecs[0], int64(100), false, proc.Mp())

	bat.SetRowCount(1)
	return bat
}

// create (doc_id, pos, text)
func makeTextBatchFT(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int32, 4, 0))     // doc_id
	bat.Vecs[1] = vector.NewVec(types.New(types.T_int32, 4, 0))     // pos
	bat.Vecs[2] = vector.NewVec(types.New(types.T_varchar, 256, 0)) // text

	nitem := 8192*3 + 1
	for i := 0; i < nitem; i++ {
		// doc_id
		vector.AppendFixed[int32](bat.Vecs[0], int32(i), false, proc.Mp())

		// pos
		vector.AppendFixed[int32](bat.Vecs[1], int32(i+1), false, proc.Mp())

		// word
		vector.AppendBytes(bat.Vecs[2], []byte("pattern"), false, proc.Mp())
	}

	bat.SetRowCount(nitem)
	return bat
}
