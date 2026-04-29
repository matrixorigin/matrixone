//go:build gpu

// Copyright 2022 Matrix Origin
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
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

var (
	ivfpqSearchDefaultAttrs = []string{"pkid", "score"}

	ivfpqSearchDefaultColdefs = []*plan.ColDef{
		{
			Name: "pkid",
			Typ:  plan.Type{Id: int32(types.T_int64), NotNullable: false, Width: 8},
		},
		{
			Name: "score",
			Typ:  plan.Type{Id: int32(types.T_float64), NotNullable: false, Width: 8},
		},
	}
)

type ivfpqSearchTestCase struct {
	arg  *TableFunction
	proc *process.Process
}

func newIvfpqSearchTestCase(t *testing.T, m *mpool.MPool, attrs []string, param string) ivfpqSearchTestCase {
	proc := testutil.NewProcessWithMPool(t, "", m)
	colDefs := make([]*plan.ColDef, len(attrs))
	for i := range attrs {
		for j := range ivfpqSearchDefaultColdefs {
			if attrs[i] == ivfpqSearchDefaultColdefs[j].Name {
				colDefs[i] = ivfpqSearchDefaultColdefs[j]
				break
			}
		}
	}
	return ivfpqSearchTestCase{
		proc: proc,
		arg: &TableFunction{
			Attrs:    attrs,
			Rets:     colDefs,
			FuncName: "ivfpq_search",
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
}

func newIvfpqMockAlgoFn(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) veccache.VectorIndexSearchIf {
	return &MockSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
}

// makeBatchIvfpqSearch builds the 2-column input batch: (tblcfg varchar, vec float32[4]).
func makeBatchIvfpqSearch(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))     // IndexTableConfig JSON
	bat.Vecs[1] = vector.NewVec(types.New(types.T_array_float32, 4, 0)) // float32 array [4]float32

	tblcfg := `{"db":"db","src":"src","metadata":"__meta","index":"__index"}`
	vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, proc.Mp())
	vector.AppendArray(bat.Vecs[1], []float32{1, 2, 3, 4}, false, proc.Mp())
	bat.SetRowCount(1)
	return bat
}

func makeConstInputExprsIvfpqSearch() []*plan.Expr {
	tblcfg := `{"db":"db","src":"src","metadata":"__meta","index":"__index"}`
	return []*plan.Expr{
		{
			Typ:  plan.Type{Id: int32(types.T_varchar), Width: 512},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: tblcfg}}},
		},
		plan2.MakePlan2Vecf32ConstExprWithType("[1,2,3,4]", 4),
	}
}

// TestIvfpqSearch verifies the happy-path: prepare → start → call → end → reset → free.
func TestIvfpqSearch(t *testing.T) {
	newIvfpqAlgo = newIvfpqMockAlgoFn

	param := `{"op_type":"vector_l2_ops","lists":"4","m":"2","bits_per_code":"8"}`
	ut := newIvfpqSearchTestCase(t, mpool.MustNewZero(), ivfpqSearchDefaultAttrs, param)

	inbat := makeBatchIvfpqSearch(ut.proc)
	ut.arg.Args = makeConstInputExprsIvfpqSearch()

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
	require.Equal(t, vm.ExecNext, result.Status)

	err = ut.arg.ctr.state.end(ut.arg, ut.proc)
	require.Nil(t, err)

	ut.arg.ctr.state.reset(ut.arg, ut.proc)
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// TestIvfpqSearchParamFail verifies that invalid params cause start() to fail.
func TestIvfpqSearchParamFail(t *testing.T) {
	newIvfpqAlgo = newIvfpqMockAlgoFn

	failedParams := []string{
		`{`,                                // invalid JSON
		`{"op_type":"vector_cos_ops"}`,     // unsupported op_type
		`{"op_type":"vector_l2_ops","lists":"notnumber"}`,
		`{"op_type":"vector_l2_ops","m":"notnumber"}`,
		`{"op_type":"vector_l2_ops","bits_per_code":"x"}`,
	}

	for _, param := range failedParams {
		ut := newIvfpqSearchTestCase(t, mpool.MustNewZero(), ivfpqSearchDefaultAttrs, param)
		inbat := makeBatchIvfpqSearch(ut.proc)
		ut.arg.Args = makeConstInputExprsIvfpqSearch()

		err := ut.arg.Prepare(ut.proc)
		require.Nil(t, err)

		for i := range ut.arg.ctr.executorsForArgs {
			ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
			require.Nil(t, err)
		}

		err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
		require.NotNil(t, err)
		os.Stderr.WriteString(fmt.Sprintf("expected error: %v\n", err))
	}
}

// TestIvfpqSearchIndexTableConfigFail verifies that bad IndexTableConfig fails.
func TestIvfpqSearchIndexTableConfigFail(t *testing.T) {
	newIvfpqAlgo = newIvfpqMockAlgoFn

	param := `{"op_type":"vector_l2_ops","lists":"4","m":"2","bits_per_code":"8"}`

	type failCase struct {
		args []*plan.Expr
		bat  *batch.Batch
		desc string
	}

	cases := []failCase{
		{desc: "empty tblcfg"},
		{desc: "non-varchar first arg"},
		{desc: "non-array-float32 second arg"},
	}

	// case 0: empty tblcfg string
	{
		args := []*plan.Expr{
			{
				Typ:  plan.Type{Id: int32(types.T_varchar), Width: 512},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: ""}}},
			},
			plan2.MakePlan2Vecf32ConstExprWithType("[1,2,3,4]", 4),
		}
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))
		bat.Vecs[1] = vector.NewVec(types.New(types.T_array_float32, 4, 0))
		vector.AppendBytes(bat.Vecs[0], []byte(""), false, mpool.MustNewZero())
		vector.AppendArray(bat.Vecs[1], []float32{1, 2, 3, 4}, false, mpool.MustNewZero())
		bat.SetRowCount(1)
		cases[0].args = args
		cases[0].bat = bat
	}

	// case 1: first arg is int64 (not varchar)
	{
		args := []*plan.Expr{
			{
				Typ:  plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
			},
			plan2.MakePlan2Vecf32ConstExprWithType("[1,2,3,4]", 4),
		}
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))
		bat.Vecs[1] = vector.NewVec(types.New(types.T_array_float32, 4, 0))
		vector.AppendFixed(bat.Vecs[0], int64(1), false, mpool.MustNewZero())
		vector.AppendArray(bat.Vecs[1], []float32{1, 2, 3, 4}, false, mpool.MustNewZero())
		bat.SetRowCount(1)
		cases[1].args = args
		cases[1].bat = bat
	}

	// case 2: second arg is int64 (not float32 array)
	{
		tblcfg := `{"db":"db","src":"src","metadata":"__meta","index":"__index"}`
		args := []*plan.Expr{
			{
				Typ:  plan.Type{Id: int32(types.T_varchar), Width: 512},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: tblcfg}}},
			},
			{
				Typ:  plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
			},
		}
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))
		bat.Vecs[1] = vector.NewVec(types.New(types.T_int64, 8, 0))
		vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, mpool.MustNewZero())
		vector.AppendFixed(bat.Vecs[1], int64(1), false, mpool.MustNewZero())
		bat.SetRowCount(1)
		cases[2].args = args
		cases[2].bat = bat
	}

	for _, c := range cases {
		ut := newIvfpqSearchTestCase(t, mpool.MustNewZero(), ivfpqSearchDefaultAttrs, param)
		ut.arg.Args = c.args

		err := ut.arg.Prepare(ut.proc)
		require.Nil(t, err)

		for i := range ut.arg.ctr.executorsForArgs {
			ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{c.bat}, nil)
			require.Nil(t, err)
		}

		err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
		require.NotNil(t, err, "expected error for: %s", c.desc)
		os.Stderr.WriteString(fmt.Sprintf("[%s] expected error: %v\n", c.desc, err))
	}
}

// TestNewIvfpqAlgoFn verifies that newIvfpqAlgoFn returns a non-nil algo for each quantization type.
func TestNewIvfpqAlgoFn(t *testing.T) {
	var idxcfg vectorindex.IndexConfig
	var tblcfg vectorindex.IndexTableConfig

	// F32 (default)
	idxcfg.CuvsIvfpq.Quantization = 0
	algo := newIvfpqAlgoFn(idxcfg, tblcfg)
	require.NotNil(t, algo)
	algo.Destroy()

	// F16
	idxcfg.CuvsIvfpq.Quantization = 1 // metric.Quantization_F16
	algo = newIvfpqAlgoFn(idxcfg, tblcfg)
	require.NotNil(t, algo)
	algo.Destroy()

	// INT8
	idxcfg.CuvsIvfpq.Quantization = 2 // metric.Quantization_INT8
	algo = newIvfpqAlgoFn(idxcfg, tblcfg)
	require.NotNil(t, algo)
	algo.Destroy()

	// UINT8
	idxcfg.CuvsIvfpq.Quantization = 3 // metric.Quantization_UINT8
	algo = newIvfpqAlgoFn(idxcfg, tblcfg)
	require.NotNil(t, algo)
	algo.Destroy()
}
