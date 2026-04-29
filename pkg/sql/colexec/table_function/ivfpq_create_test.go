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
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

var (
	ivfpqCreateDefaultAttrs = []string{"status"}

	ivfpqCreateDefaultColdefs = []*plan.ColDef{
		{
			Name: "status",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
			},
		},
	}
)

type ivfpqCreateTestCase struct {
	arg  *TableFunction
	proc *process.Process
}

func newIvfpqCreateTestCase(t *testing.T, m *mpool.MPool, attrs []string, param string) ivfpqCreateTestCase {
	proc := testutil.NewProcessWithMPool(t, "", m)
	colDefs := make([]*plan.ColDef, len(attrs))
	for i := range attrs {
		for j := range ivfpqCreateDefaultColdefs {
			if attrs[i] == ivfpqCreateDefaultColdefs[j].Name {
				colDefs[i] = ivfpqCreateDefaultColdefs[j]
				break
			}
		}
	}
	return ivfpqCreateTestCase{
		proc: proc,
		arg: &TableFunction{
			Attrs:    attrs,
			Rets:     colDefs,
			FuncName: "ivfpq_create",
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

func mock_ivfpq_runSql(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
	proc := sqlproc.Proc
	return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{}}, nil
}

// makeBatchIvfpqCreate builds the 3-column input batch: (tblcfg varchar, pkid int64, vec float32[4]).
func makeBatchIvfpqCreate(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))     // IndexTableConfig JSON
	bat.Vecs[1] = vector.NewVec(types.New(types.T_int64, 8, 0))         // pkid int64
	bat.Vecs[2] = vector.NewVec(types.New(types.T_array_float32, 4, 0)) // float32 array [4]float32

	tblcfg := `{"db":"db","src":"src","metadata":"__meta","index":"__index","index_capacity":100}`
	vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, proc.Mp())
	vector.AppendFixed(bat.Vecs[1], int64(1), false, proc.Mp())
	vector.AppendArray(bat.Vecs[2], []float32{1, 2, 3, 4}, false, proc.Mp())
	bat.SetRowCount(1)
	return bat
}

// makeConstInputExprsIvfpqCreate builds the 3 plan.Expr inputs.
func makeConstInputExprsIvfpqCreate() []*plan.Expr {
	tblcfg := `{"db":"db","src":"src","metadata":"__meta","index":"__index","index_capacity":100}`
	return []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_varchar), Width: 512},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: tblcfg}}},
		},
		{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
		},
		plan2.MakePlan2Vecf32ConstExprWithType("[1,2,3,4]", 4),
	}
}

// TestIvfpqCreate verifies the happy-path: prepare → start → call → end → reset → free.
func TestIvfpqCreate(t *testing.T) {
	ivfpq_runSql = mock_ivfpq_runSql

	param := `{"op_type":"vector_l2_ops","lists":"1","m":"2","bits_per_code":"8"}`
	ut := newIvfpqCreateTestCase(t, mpool.MustNewZero(), ivfpqCreateDefaultAttrs, param)

	inbat := makeBatchIvfpqCreate(ut.proc)
	ut.arg.Args = makeConstInputExprsIvfpqCreate()

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
	require.Equal(t, vm.ExecStop, result.Status)

	err = ut.arg.ctr.state.end(ut.arg, ut.proc)
	require.Nil(t, err)

	ut.arg.ctr.state.reset(ut.arg, ut.proc)
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// TestIvfpqCreateMultiRow verifies that multiple rows can be fed row-by-row.
func TestIvfpqCreateMultiRow(t *testing.T) {
	ivfpq_runSql = mock_ivfpq_runSql

	param := `{"op_type":"vector_l2_ops","lists":"4","m":"2","bits_per_code":"8"}`
	ut := newIvfpqCreateTestCase(t, mpool.MustNewZero(), ivfpqCreateDefaultAttrs, param)

	tblcfg := `{"db":"db","src":"src","metadata":"__meta","index":"__index","index_capacity":100}`
	inbat := batch.NewWithSize(3)
	inbat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))
	inbat.Vecs[1] = vector.NewVec(types.New(types.T_int64, 8, 0))
	inbat.Vecs[2] = vector.NewVec(types.New(types.T_array_float32, 4, 0))
	for i := 0; i < 10; i++ {
		vector.AppendBytes(inbat.Vecs[0], []byte(tblcfg), false, ut.proc.Mp())
		vector.AppendFixed(inbat.Vecs[1], int64(i+1), false, ut.proc.Mp())
		vector.AppendArray(inbat.Vecs[2], []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)}, false, ut.proc.Mp())
	}
	inbat.SetRowCount(10)

	ut.arg.Args = makeConstInputExprsIvfpqCreate()

	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// Feed 10 rows.
	for row := 0; row < 10; row++ {
		err = ut.arg.ctr.state.start(ut.arg, ut.proc, row, nil)
		require.Nil(t, err)
	}

	err = ut.arg.ctr.state.end(ut.arg, ut.proc)
	require.Nil(t, err)

	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// TestIvfpqCreateParamFail verifies that malformed or invalid param JSON causes start() to fail.
func TestIvfpqCreateParamFail(t *testing.T) {
	ivfpq_runSql = mock_ivfpq_runSql

	failedParams := []string{
		`{`,                                                // invalid JSON
		`{"op_type":"vector_cos_ops"}`,                    // unsupported op_type for IVF-PQ
		`{"op_type":"vector_l2_ops","lists":"notnumber"}`, // non-numeric lists
		`{"op_type":"vector_l2_ops","m":"notnumber"}`,     // non-numeric m
		`{"op_type":"vector_l2_ops","bits_per_code":"x"}`, // non-numeric bits_per_code
	}

	for _, param := range failedParams {
		ut := newIvfpqCreateTestCase(t, mpool.MustNewZero(), ivfpqCreateDefaultAttrs, param)
		inbat := makeBatchIvfpqCreate(ut.proc)
		ut.arg.Args = makeConstInputExprsIvfpqCreate()

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

// TestIvfpqCreateIndexTableConfigFail verifies that bad IndexTableConfig or wrong arg types fail.
func TestIvfpqCreateIndexTableConfigFail(t *testing.T) {
	ivfpq_runSql = mock_ivfpq_runSql

	param := `{"op_type":"vector_l2_ops","lists":"4","m":"2","bits_per_code":"8"}`

	type failCase struct {
		args   []*plan.Expr
		bat    *batch.Batch
		desc   string
	}

	makeArgs := func(tblcfg string, idTyp types.T, vecTyp types.T, vecDim int32) ([]*plan.Expr, *batch.Batch) {
		args := []*plan.Expr{
			{
				Typ:  plan.Type{Id: int32(types.T_varchar), Width: 512},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: tblcfg}}},
			},
			{
				Typ:  plan.Type{Id: int32(idTyp)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
			},
			plan2.MakePlan2Vecf32ConstExprWithType("[1,2,3,4]", vecDim),
		}
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))
		bat.Vecs[1] = vector.NewVec(types.New(idTyp, 8, 0))
		bat.Vecs[2] = vector.NewVec(types.New(vecTyp, vecDim, 0))
		vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, mpool.MustNewZero())
		if idTyp == types.T_int64 {
			vector.AppendFixed(bat.Vecs[1], int64(1), false, mpool.MustNewZero())
		} else {
			vector.AppendFixed(bat.Vecs[1], int32(1), false, mpool.MustNewZero())
		}
		vector.AppendArray(bat.Vecs[2], []float32{1, 2, 3, 4}, false, mpool.MustNewZero())
		bat.SetRowCount(1)
		return args, bat
	}

	goodTblcfg := `{"db":"db","src":"src","metadata":"__meta","index":"__index","index_capacity":100}`
	zeroCapTblcfg := `{"db":"db","src":"src","metadata":"__meta","index":"__index","index_capacity":0}`

	cases := []failCase{
		{desc: "empty tblcfg"},
		{desc: "zero capacity"},
		{desc: "wrong id type (int32 instead of int64)"},
		{desc: "wrong vec type (int64 instead of float32 array)"},
	}

	// case 0: empty tblcfg
	{
		args, bat := makeArgs("", types.T_int64, types.T_array_float32, 4)
		cases[0].args = args
		cases[0].bat = bat
	}
	// case 1: zero capacity
	{
		args, bat := makeArgs(zeroCapTblcfg, types.T_int64, types.T_array_float32, 4)
		cases[1].args = args
		cases[1].bat = bat
	}
	// case 2: wrong id type (int32)
	{
		args, bat := makeArgs(goodTblcfg, types.T_int32, types.T_array_float32, 4)
		cases[2].args = args
		cases[2].bat = bat
	}
	// case 3: wrong vec type (T_int64 instead of array)
	{
		tblcfg := goodTblcfg
		args := []*plan.Expr{
			{
				Typ:  plan.Type{Id: int32(types.T_varchar), Width: 512},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: tblcfg}}},
			},
			{
				Typ:  plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
			},
			{
				Typ:  plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
			},
		}
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))
		bat.Vecs[1] = vector.NewVec(types.New(types.T_int64, 8, 0))
		bat.Vecs[2] = vector.NewVec(types.New(types.T_int64, 8, 0))
		vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, mpool.MustNewZero())
		vector.AppendFixed(bat.Vecs[1], int64(1), false, mpool.MustNewZero())
		vector.AppendFixed(bat.Vecs[2], int64(1), false, mpool.MustNewZero())
		bat.SetRowCount(1)
		cases[3].args = args
		cases[3].bat = bat
	}

	for _, c := range cases {
		ut := newIvfpqCreateTestCase(t, mpool.MustNewZero(), ivfpqCreateDefaultAttrs, param)
		ut.arg.Args = c.args

		err := ut.arg.Prepare(ut.proc)
		require.Nil(t, err)

		for i := range ut.arg.ctr.executorsForArgs {
			ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{c.bat}, nil)
			require.Nil(t, err)
		}

		err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
		require.NotNil(t, err, "expected error for case: %s", c.desc)
		os.Stderr.WriteString(fmt.Sprintf("[%s] expected error: %v\n", c.desc, err))
	}
}
