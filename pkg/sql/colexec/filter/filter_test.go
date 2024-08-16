// Copyright 2024 Matrix Origin
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

package filter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type filterTestCase struct {
	arg         *Filter
	proc        *process.Process
	getRowCount int
}

var (
	tcs []filterTestCase
)

func init() {
	boolType := types.T_bool.ToType()
	int32Type := types.T_int32.ToType()

	fr0, _ := function.GetFunctionByName(context.TODO(), "and", []types.Type{boolType, boolType})
	fid0 := fr0.GetEncodedOverloadID()

	fr1, _ := function.GetFunctionByName(context.TODO(), ">", []types.Type{int32Type, int32Type})
	fid1 := fr1.GetEncodedOverloadID()

	fr2, _ := function.GetFunctionByName(context.TODO(), "<", []types.Type{int32Type, int32Type})
	fid2 := fr2.GetEncodedOverloadID()

	tcs = []filterTestCase{
		// case1: Contains one conditional expression
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			arg: &Filter{
				E: &plan.Expr{
					Typ: plan2.MakePlan2Type(&boolType),
					Expr: &plan.Expr_F{
						F: &plan.Function{
							Func: &plan.ObjectRef{
								ObjName: ">",
								Obj:     fid1,
							},

							Args: []*plan.Expr{
								{
									Typ: plan2.MakePlan2Type(&int32Type),
									Expr: &plan.Expr_Col{
										Col: &plan.ColRef{
											RelPos: 0,
											ColPos: 0,
											Name:   "a",
										},
									},
								},
								makePlan2Int32ConstExprWithType(10),
							},
						},
					},
				},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
			getRowCount: 20,
		},
		// case2: Contains two conditional expressions
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			arg: &Filter{
				E: &plan.Expr{
					Typ: plan2.MakePlan2Type(&boolType),
					Expr: &plan.Expr_F{
						F: &plan.Function{
							Func: &plan.ObjectRef{
								ObjName: "and",
								Obj:     fid0,
							},
							Args: []*plan.Expr{
								{
									Typ: plan2.MakePlan2Type(&boolType),
									Expr: &plan.Expr_F{
										F: &plan.Function{
											Func: &plan.ObjectRef{
												ObjName: ">",
												Obj:     fid1,
											},

											Args: []*plan.Expr{
												{
													Typ: plan2.MakePlan2Type(&int32Type),
													Expr: &plan.Expr_Col{
														Col: &plan.ColRef{
															RelPos: 0,
															ColPos: 0,
															Name:   "a",
														},
													},
												},
												makePlan2Int32ConstExprWithType(10),
											},
										},
									},
								},
								{
									Typ: plan2.MakePlan2Type(&boolType),
									Expr: &plan.Expr_F{
										F: &plan.Function{
											Func: &plan.ObjectRef{
												ObjName: "<",
												Obj:     fid2,
											},

											Args: []*plan.Expr{
												{
													Typ: plan2.MakePlan2Type(&int32Type),
													Expr: &plan.Expr_Col{
														Col: &plan.ColRef{
															RelPos: 0,
															ColPos: 1,
															Name:   "b",
														},
													},
												},
												makePlan2Int32ConstExprWithType(40),
											},
										},
									},
								},
							},
						},
					},
				},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
			getRowCount: 10,
		},
	}
}

func TestFilter(t *testing.T) {
	for _, tc := range tcs {
		resetChildren(tc.arg)
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		// 1. First call
		res, _ := tc.arg.Call(tc.proc)
		if tc.getRowCount > 0 {
			require.Equal(t, tc.getRowCount, res.Batch.RowCount())
		} else {
			require.Equal(t, res.Batch == nil, true)
		}

		// 2. Second call
		for {
			res, _ = tc.arg.Call(tc.proc)
			if res.Batch == nil {
				break
			}
			if tc.getRowCount > 0 {
				require.Equal(t, tc.getRowCount, res.Batch.RowCount())
			} else {
				require.Equal(t, res.Batch == nil, true)
			}
		}
		tc.arg.Reset(tc.proc, false, nil)

		//--------------------------------------------------------
		// Re enable the operator after reset
		resetChildren(tc.arg)
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		res, _ = tc.arg.Call(tc.proc)
		if tc.getRowCount > 0 {
			require.Equal(t, res.Batch.RowCount(), tc.getRowCount)
		} else {
			require.Equal(t, res.Batch == nil, true)
		}
		tc.arg.Reset(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)

		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func resetChildren(arg *Filter) {
	bat0 := MakeFilterMockBatchs()
	bat1 := MakeFilterMockBatchs()
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat0, bat1})
	arg.Children = nil
	arg.AppendChild(op)
}

func makePlan2Int32ConstExprWithType(v int32) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Int32ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_int32),
			NotNullable: true,
		},
	}
}

func makePlan2Int32ConstExpr(v int32) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_I32Val{
			I32Val: v,
		},
	}}
}

// new batchs with schema : (a int, b uuid, c varchar, d json, e datetime)
func MakeFilterMockBatchs() *batch.Batch {
	bat := batch.New(true, []string{"a", "b", "c"})
	vecs := make([]*vector.Vector, 3)
	vecs[0] = testutil.MakeInt32Vector([]int32{
		1,
		2,
		3,
		4,
		5,
		6,
		7,
		8,
		9,
		10,
		11,
		12,
		13,
		14,
		15,
		16,
		17,
		18,
		19,
		20,
		21,
		22,
		23,
		24,
		25,
		26,
		27,
		28,
		29,
		30}, nil)
	vecs[1] = testutil.MakeInt32Vector([]int32{
		20,
		21,
		22,
		23,
		24,
		25,
		26,
		27,
		28,
		29,
		30,
		31,
		32,
		33,
		34,
		35,
		36,
		37,
		38,
		39,
		40,
		41,
		42,
		43,
		44,
		45,
		46,
		47,
		48,
		49,
	}, nil)

	vecs[2] = testutil.MakeVarcharVector([]string{
		"xfgj",
		"xasj",
		"xasj",
		"xrtx",
		"xrtx",
		"xghx",
		"xghx",
		"cwhx",
		"cwmn",
		"cwhx",
		"cwmn",
		"cwmn",
		"xgmn",
		"pkhx",
		"prtx",
		"prtx",
		"prtx",
		"xrtx",
		"xrtx",
		"xghx",
		"xghx",
		"xgmn",
		"pkmn",
		"okmn",
		"okmn",
		"pkhx",
		"prtx",
		"prtx",
		"prtx",
		"xrtx",
	},
		nil)
	bat.Vecs = vecs
	bat.SetRowCount(vecs[0].Length())
	return bat
}
