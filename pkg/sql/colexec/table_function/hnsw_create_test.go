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
	"fmt"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type hnswCreateTestCase struct {
	arg  *TableFunction
	proc *process.Process
}

var (
	hnswcreatedefaultAttrs = []string{"status"}

	hnswcreatedefaultColdefs = []*plan.ColDef{
		// row_id type should be same as index type
		{
			Name: "status",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
			},
		},
	}
)

func newHnswCreateTestCase(m *mpool.MPool, attrs []string, param string) hnswCreateTestCase {
	proc := testutil.NewProcessWithMPool("", m)
	colDefs := make([]*plan.ColDef, len(attrs))
	for i := range attrs {
		for j := range hnswcreatedefaultColdefs {
			if attrs[i] == hnswcreatedefaultColdefs[j].Name {
				colDefs[i] = hnswcreatedefaultColdefs[j]
				break
			}
		}
	}

	ret := hnswCreateTestCase{
		proc: proc,
		arg: &TableFunction{
			Attrs:    attrs,
			Rets:     colDefs,
			FuncName: "hnsw_create",
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

func mock_hnsw_runSql(proc *process.Process, sql string) (executor.Result, error) {

	return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{}}, nil
}

func TestHnswCreate(t *testing.T) {

	hnsw_runSql = mock_hnsw_runSql

	param := "{\"op_type\": \"vector_l2_ops\"}"
	ut := newHnswCreateTestCase(mpool.MustNewZero(), hnswcreatedefaultAttrs, param)

	inbat := makeBatchHnswCreate(ut.proc)

	ut.arg.Args = makeConstInputExprsHnswCreate()

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

	require.Equal(t, result.Status, vm.ExecStop)

	err = ut.arg.ctr.state.end(ut.arg, ut.proc)
	require.Nil(t, err)

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

var failedparam []string = []string{"{",
	"{\"op_type\": \"vector_cos_ops\"}",
	"{\"op_type\": \"vector_l2_ops\", \"quantization\":\"invalid\"}",
	"{\"op_type\": \"vector_l2_ops\", \"m\":\"notnumber\"}",
	"{\"op_type\": \"vector_l2_ops\", \"ef_construction\":\"notnumber\"}",
	"{\"op_type\": \"vector_l2_ops\"}, \"ef_search\":\"notnumber\"",
}

func TestHnswCreateParamFail(t *testing.T) {

	hnsw_runSql = mock_hnsw_runSql

	for _, param := range failedparam {
		ut := newHnswCreateTestCase(mpool.MustNewZero(), hnswcreatedefaultAttrs, param)

		inbat := makeBatchHnswCreate(ut.proc)

		ut.arg.Args = makeConstInputExprsHnswCreate()

		// Prepare
		err := ut.arg.Prepare(ut.proc)
		require.Nil(t, err)

		for i := range ut.arg.ctr.executorsForArgs {
			ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
			require.Nil(t, err)
		}

		// start
		err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
		require.NotNil(t, err)
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
	}

	/*
		// first call receive data
		result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
		require.Nil(t, err)

		require.Equal(t, result.Status, vm.ExecStop)

		err = ut.arg.ctr.state.end(ut.arg, ut.proc)
		require.Nil(t, err)

		// reset
		ut.arg.ctr.state.reset(ut.arg, ut.proc)

		// free
		ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
	*/
}

func TestHnswCreateIndexTableConfigFail(t *testing.T) {

	hnsw_runSql = mock_hnsw_runSql
	param := "{\"op_type\": \"vector_l2_ops\"}"

	ut := newHnswCreateTestCase(mpool.MustNewZero(), hnswcreatedefaultAttrs, param)
	failbatches := makeBatchHnswCreateFail(ut.proc)
	for _, b := range failbatches {

		inbat := b.bat
		ut.arg.Args = b.args

		// Prepare
		err := ut.arg.Prepare(ut.proc)
		require.Nil(t, err)

		for i := range ut.arg.ctr.executorsForArgs {
			ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
			require.Nil(t, err)
		}

		// start
		err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
		require.NotNil(t, err)
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
	}

	/*
	   // first call receive data
	   result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	   require.Nil(t, err)

	   require.Equal(t, result.Status, vm.ExecStop)

	   err = ut.arg.ctr.state.end(ut.arg, ut.proc)
	   require.Nil(t, err)

	   // reset
	   ut.arg.ctr.state.reset(ut.arg, ut.proc)

	   // free
	   ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
	*/
}

func makeConstInputExprsHnswCreate() []*plan.Expr {

	tblcfg := `{"db":"db", "src":"src", "metadata":"__metadata", "index":"__index", "index_capacity": 10000}`
	ret := []*plan.Expr{
		{
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: 512,
			},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_Sval{
						Sval: tblcfg,
					},
				},
			},
		},
		{

			Typ: plan.Type{
				Id: int32(types.T_int64),
			},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_I64Val{
						I64Val: 1,
					},
				},
			},
		},

		plan2.MakePlan2Vecf32ConstExprWithType("[0,1,2]", 3),
	}

	return ret
}
func makeBatchHnswCreate(proc *process.Process) *batch.Batch {

	bat := batch.NewWithSize(3)

	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))     // index table config
	bat.Vecs[1] = vector.NewVec(types.New(types.T_int64, 8, 0))         // pkid int64
	bat.Vecs[2] = vector.NewVec(types.New(types.T_array_float32, 3, 0)) // float32 array [3]float32

	tblcfg := `{"db":"db", "src":"src", "metadata":"__metadata", "index":"__index", "index_capacity": 10000}`
	vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[1], int64(1), false, proc.Mp())

	v := []float32{0, 1, 2}
	vector.AppendArray[float32](bat.Vecs[2], v, false, proc.Mp())

	bat.SetRowCount(1)
	return bat

}

type failBatch struct {
	args []*plan.Expr
	bat  *batch.Batch
}

func makeBatchHnswCreateFail(proc *process.Process) []failBatch {

	failBatches := make([]failBatch, 0, 3)

	//tblcfg := `{"db":"db", "src":"src", "metadata":"__metadata", "index":"__index", "index_capacity": 10000}`
	{
		tblcfg := ``
		ret := []*plan.Expr{
			{
				Typ: plan.Type{
					Id:    int32(types.T_varchar),
					Width: 512,
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_Sval{
							Sval: tblcfg,
						},
					},
				},
			},
			{

				Typ: plan.Type{
					Id: int32(types.T_int64),
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_I64Val{
							I64Val: 1,
						},
					},
				},
			},

			plan2.MakePlan2Vecf32ConstExprWithType("[0,1,2]", 3),
		}

		bat := batch.NewWithSize(3)

		bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))     // index table config
		bat.Vecs[1] = vector.NewVec(types.New(types.T_int64, 8, 0))         // pkid int64
		bat.Vecs[2] = vector.NewVec(types.New(types.T_array_float32, 3, 0)) // float32 array [3]float32

		vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, proc.Mp())
		vector.AppendFixed[int64](bat.Vecs[1], int64(1), false, proc.Mp())

		v := []float32{0, 1, 2}
		vector.AppendArray[float32](bat.Vecs[2], v, false, proc.Mp())

		bat.SetRowCount(1)

		failBatches = append(failBatches, failBatch{ret, bat})

	}
	{
		tblcfg := `{"db":"db", "src":"src", "metadata":"__metadata", "index":"__index", "index_capacity": 10000}`
		ret := []*plan.Expr{
			{
				Typ: plan.Type{
					Id:    int32(types.T_varchar),
					Width: 512,
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_Sval{
							Sval: tblcfg,
						},
					},
				},
			},
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

			plan2.MakePlan2Vecf32ConstExprWithType("[0,1,2]", 3),
		}

		bat := batch.NewWithSize(3)

		bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))     // index table config
		bat.Vecs[1] = vector.NewVec(types.New(types.T_int32, 8, 0))         // pkid int64
		bat.Vecs[2] = vector.NewVec(types.New(types.T_array_float32, 3, 0)) // float32 array [3]float32

		vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, proc.Mp())
		vector.AppendFixed[int32](bat.Vecs[1], int32(1), false, proc.Mp())

		v := []float32{0, 1, 2}
		vector.AppendArray[float32](bat.Vecs[2], v, false, proc.Mp())

		bat.SetRowCount(1)

		failBatches = append(failBatches, failBatch{ret, bat})

	}
	{
		//tblcfg := `{"db":"db", "src":"src", "metadata":"__metadata", "index":"__index", "index_capacity": 10000}`
		ret := []*plan.Expr{
			{

				Typ: plan.Type{
					Id: int32(types.T_int64),
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_I64Val{
							I64Val: 1,
						},
					},
				},
			},
			{

				Typ: plan.Type{
					Id: int32(types.T_int64),
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_I64Val{
							I64Val: 1,
						},
					},
				},
			},

			plan2.MakePlan2Vecf32ConstExprWithType("[0,1,2]", 3),
		}

		bat := batch.NewWithSize(3)

		bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))         // index table config
		bat.Vecs[1] = vector.NewVec(types.New(types.T_int64, 8, 0))         // pkid int64
		bat.Vecs[2] = vector.NewVec(types.New(types.T_array_float32, 3, 0)) // float32 array [3]float32

		vector.AppendFixed[int64](bat.Vecs[0], int64(1), false, proc.Mp())
		vector.AppendFixed[int64](bat.Vecs[1], int64(1), false, proc.Mp())

		v := []float32{0, 1, 2}
		vector.AppendArray[float32](bat.Vecs[2], v, false, proc.Mp())

		bat.SetRowCount(1)

		failBatches = append(failBatches, failBatch{ret, bat})

	}
	{
		tblcfg := `{"db":"db", "src":"src", "metadata":"__metadata", "index":"__index", "index_capacity": 10000}`
		ret := []*plan.Expr{
			{
				Typ: plan.Type{
					Id:    int32(types.T_varchar),
					Width: 512,
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_Sval{
							Sval: tblcfg,
						},
					},
				},
			},
			{

				Typ: plan.Type{
					Id: int32(types.T_int64),
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_I64Val{
							I64Val: 1,
						},
					},
				},
			},

			{

				Typ: plan.Type{
					Id: int32(types.T_int64),
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_I64Val{
							I64Val: 1,
						},
					},
				},
			},
		}

		bat := batch.NewWithSize(3)

		bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0)) // index table config
		bat.Vecs[1] = vector.NewVec(types.New(types.T_int64, 8, 0))     // pkid int64
		bat.Vecs[2] = vector.NewVec(types.New(types.T_int64, 8, 0))     // pkid int64

		vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, proc.Mp())
		vector.AppendFixed[int64](bat.Vecs[1], int64(1), false, proc.Mp())
		vector.AppendFixed[int64](bat.Vecs[2], int64(1), false, proc.Mp())

		bat.SetRowCount(1)

		failBatches = append(failBatches, failBatch{ret, bat})

	}
	return failBatches
}
