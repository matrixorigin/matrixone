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
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type ivfSearchTestCase struct {
	arg  *TableFunction
	proc *process.Process
}

var (
	ivfsearchdefaultAttrs = []string{"pkid", "score"}

	ivfsearchdefaultColdefs = []*plan.ColDef{
		// row_id type should be same as index type
		{
			Name: "pkid",
			Typ: plan.Type{
				Id:          int32(types.T_int64),
				NotNullable: false,
			},
		},
		{
			Name: "score",
			Typ: plan.Type{
				Id:          int32(types.T_float64),
				NotNullable: false,
				Width:       8,
			},
		},
	}
)

func newIvfSearchTestCase(m *mpool.MPool, attrs []string, param string) ivfSearchTestCase {
	proc := testutil.NewProcessWithMPool("", m)
	colDefs := make([]*plan.ColDef, len(attrs))
	for i := range attrs {
		for j := range ivfsearchdefaultColdefs {
			if attrs[i] == ivfsearchdefaultColdefs[j].Name {
				colDefs[i] = ivfsearchdefaultColdefs[j]
				break
			}
		}
	}

	ret := ivfSearchTestCase{
		proc: proc,
		arg: &TableFunction{
			Attrs:    attrs,
			Rets:     colDefs,
			FuncName: "ivf_search",
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

func mock_ivf_runSql(proc *process.Process, sql string) (executor.Result, error) {

	return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{}}, nil
}

func mockVersion(proc *process.Process, tblcfg vectorindex.IndexTableConfig) (int64, error) {
	return 0, nil
}

type MockIvfSearch[T types.RealNumbers] struct {
	Idxcfg vectorindex.IndexConfig
	Tblcfg vectorindex.IndexTableConfig
}

func (m *MockIvfSearch[T]) Search(proc *process.Process, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	//time.Sleep(2 * time.Millisecond)
	return []any{int64(1)}, []float64{2.0}, nil
}

func (m *MockIvfSearch[T]) Destroy() {
}

func (m *MockIvfSearch[T]) Load(*process.Process) error {
	//time.Sleep(6 * time.Second)
	return nil
}

func (m *MockIvfSearch[T]) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {
	return nil
}

func newMockIvfAlgoFn(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (veccache.VectorIndexSearchIf, error) {
	return &MockIvfSearch[float32]{Idxcfg: idxcfg, Tblcfg: tblcfg}, nil
}

func TestIvfSearch(t *testing.T) {

	newIvfAlgo = newMockIvfAlgoFn
	getVersion = mockVersion

	param := "{\"op_type\": \"vector_l2_ops\", \"lists\": \"3\"}"
	ut := newIvfSearchTestCase(mpool.MustNewZero(), ivfsearchdefaultAttrs, param)

	inbat := makeBatchIvfSearch(ut.proc)

	ut.arg.Args = makeConstInputExprsIvfSearch()

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

	err = ut.arg.ctr.state.end(ut.arg, ut.proc)
	require.Nil(t, err)

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

var failedivfsearchparam []string = []string{"{",
	"{\"op_type\": \"vector_xxx_ops\"}",
	"{\"op_type\": \"vector_l2_ops\", \"lists\":\"\"}",
	"{\"op_type\": \"vector_l2_ops\", \"lists\":\"notnumber\"}",
}

func TestIvfSearchParamFail(t *testing.T) {

	newIvfAlgo = newMockIvfAlgoFn
	getVersion = mockVersion

	for _, param := range failedivfsearchparam {
		ut := newIvfSearchTestCase(mpool.MustNewZero(), ivfsearchdefaultAttrs, param)

		inbat := makeBatchIvfSearch(ut.proc)

		ut.arg.Args = makeConstInputExprsIvfSearch()

		// Prepare
		err := ut.arg.Prepare(ut.proc)
		require.Nil(t, err)

		for i := range ut.arg.ctr.executorsForArgs {
			ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
			require.Nil(t, err)
		}

		// start
		fmt.Println(param)
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

func TestIvfSearchIndexTableConfigFail(t *testing.T) {

	ivf_runSql = mock_ivf_runSql
	getVersion = mockVersion
	param := "{\"op_type\": \"vector_l2_ops\", \"lists\": \"3\"}"

	ut := newIvfSearchTestCase(mpool.MustNewZero(), ivfsearchdefaultAttrs, param)
	failbatches := makeBatchIvfSearchFail(ut.proc)
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

func makeConstInputExprsIvfSearch() []*plan.Expr {

	tblcfg := fmt.Sprintf(`{"db":"db", "src":"src", "metadata":"__metadata", "index":"__index", "entries":"__entries", "parttype": %d}`, int32(types.T_array_float32))
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

		plan2.MakePlan2Vecf32ConstExprWithType("[0,1,2]", 3),
	}

	return ret
}
func makeBatchIvfSearch(proc *process.Process) *batch.Batch {

	bat := batch.NewWithSize(2)

	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))     // index table config
	bat.Vecs[1] = vector.NewVec(types.New(types.T_array_float32, 3, 0)) // float32 array [3]float32

	tblcfg := fmt.Sprintf(`{"db":"db", "src":"src", "metadata":"__metadata", "index":"__index", "entries":"__entries", "parttype": %d}`, int32(types.T_array_float32))
	vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, proc.Mp())

	v := []float32{0, 1, 2}
	vector.AppendArray[float32](bat.Vecs[1], v, false, proc.Mp())

	bat.SetRowCount(1)
	return bat

}

func makeBatchIvfSearchFail(proc *process.Process) []failBatch {

	failBatches := make([]failBatch, 0, 3)

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

			plan2.MakePlan2Vecf32ConstExprWithType("[0,1,2]", 3),
		}

		bat := batch.NewWithSize(2)

		bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))     // index table config
		bat.Vecs[1] = vector.NewVec(types.New(types.T_array_float32, 3, 0)) // float32 array [3]float32

		vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, proc.Mp())

		v := []float32{0, 1, 2}
		vector.AppendArray[float32](bat.Vecs[1], v, false, proc.Mp())

		bat.SetRowCount(1)

		failBatches = append(failBatches, failBatch{ret, bat})

	}
	{
		//tblcfg := `{"db":"db", "src":"src", "metadata":"__metadata", "index":"__index"}`
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

			plan2.MakePlan2Vecf32ConstExprWithType("[0,1,2]", 3),
		}

		bat := batch.NewWithSize(2)

		bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))         // index table config
		bat.Vecs[1] = vector.NewVec(types.New(types.T_array_float32, 3, 0)) // float32 array [3]float32

		vector.AppendFixed[int64](bat.Vecs[0], int64(1), false, proc.Mp())

		v := []float32{0, 1, 2}
		vector.AppendArray[float32](bat.Vecs[1], v, false, proc.Mp())

		bat.SetRowCount(1)

		failBatches = append(failBatches, failBatch{ret, bat})

	}
	{
		tblcfg := `{"db":"db", "src":"src", "metadata":"__metadata", "index":"__index"}`
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
		}

		bat := batch.NewWithSize(2)

		bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0)) // index table config
		bat.Vecs[1] = vector.NewVec(types.New(types.T_int64, 8, 0))     // pkid int64

		vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, proc.Mp())
		vector.AppendFixed[int64](bat.Vecs[1], int64(1), false, proc.Mp())

		bat.SetRowCount(1)

		failBatches = append(failBatches, failBatch{ret, bat})

	}
	return failBatches
}
