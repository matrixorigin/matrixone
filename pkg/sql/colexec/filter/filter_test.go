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

var tcs []filterTestCase

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
		resetChildren(tc.arg, tc.proc)
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
		resetChildren(tc.arg, tc.proc)
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		res, _ = tc.arg.Call(tc.proc)
		if tc.getRowCount > 0 {
			require.Equal(t, res.Batch.RowCount(), tc.getRowCount)
		} else {
			require.Equal(t, res.Batch == nil, true)
		}
		for _, child := range tc.arg.Children {
			child.Reset(tc.proc, false, nil)
			child.Free(tc.proc, false, nil)
		}
		tc.arg.Reset(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)

		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func resetChildren(arg *Filter, proc *process.Process) {
	for _, child := range arg.Children {
		child.Reset(proc, false, nil)
		child.Free(proc, false, nil)
	}
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
		30,
	}, nil)
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

func BenchmarkPlanConstandFold1(b *testing.B) {
	mp, _ := mpool.NewMPool("", 0, 0)
	proc := testutil.NewProcessWithMPool("", mp)
	expr := generateFoldCase1()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filterExpr, err := plan2.ConstantFold(batch.EmptyForConstFoldBatch, plan2.DeepCopyExpr(expr), proc, true, true)
		require.NoError(b, err)
		executor, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, colexec.SplitAndExprs([]*plan.Expr{filterExpr}))
		require.NoError(b, err)
		_, err = executor[0].Eval(proc, newBatchForFoldCase(proc, 1), nil)
		require.NoError(b, err)
	}
}

func BenchmarkExecutorConstandFold1(b *testing.B) {
	mp, _ := mpool.NewMPool("", 0, 0)
	proc := testutil.NewProcessWithMPool("", mp)
	expr := generateFoldCase1()
	proc.SetBaseProcessRunningStatus(true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, colexec.SplitAndExprs([]*plan.Expr{expr}))
		require.NoError(b, err)
		_, err = executor[0].Eval(proc, newBatchForFoldCase(proc, 1), nil)
		require.NoError(b, err)
	}
}

func Test2(t *testing.T) {
	mp, _ := mpool.NewMPool("", 0, 0)
	proc := testutil.NewProcessWithMPool("", mp)
	newParamForFoldCase2(proc)
	expr := generateFoldCase2()
	filterExpr, err := plan2.ConstantFold(batch.EmptyForConstFoldBatch, plan2.DeepCopyExpr(expr), proc, true, true)
	require.NoError(t, err)
	executor, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, colexec.SplitAndExprs([]*plan.Expr{filterExpr}))
	require.NoError(t, err)
	_, err = executor[0].Eval(proc, newBatchForFoldCase(proc, 1), nil)
	require.NoError(t, err)
}

func BenchmarkPlanConstandFold2(b *testing.B) {
	mp, _ := mpool.NewMPool("", 0, 0)
	proc := testutil.NewProcessWithMPool("", mp)
	newParamForFoldCase2(proc)
	expr := generateFoldCase2()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filterExpr, err := plan2.ConstantFold(batch.EmptyForConstFoldBatch, plan2.DeepCopyExpr(expr), proc, true, true)
		require.NoError(b, err)
		executor, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, colexec.SplitAndExprs([]*plan.Expr{filterExpr}))
		require.NoError(b, err)
		_, err = executor[0].Eval(proc, newBatchForFoldCase(proc, 1), nil)
		require.NoError(b, err)
	}
}

func BenchmarkExecutorConstandFold2_NoFree(b *testing.B) {
	mp, _ := mpool.NewMPool("", 0, 0)
	proc := testutil.NewProcessWithMPool("", mp)
	newParamForFoldCase2(proc)
	expr := generateFoldCase2()
	proc.SetBaseProcessRunningStatus(true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, colexec.SplitAndExprs([]*plan.Expr{expr}))
		require.NoError(b, err)
		_, err = executor[0].Eval(proc, newBatchForFoldCase(proc, 1), nil)
		require.NoError(b, err)

		executor, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, colexec.SplitAndExprs([]*plan.Expr{expr}))
		require.NoError(b, err)
		_, err = executor[0].Eval(proc, newBatchForFoldCase(proc, 1), nil)
		require.NoError(b, err)

		executor, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, colexec.SplitAndExprs([]*plan.Expr{expr}))
		require.NoError(b, err)
		_, err = executor[0].Eval(proc, newBatchForFoldCase(proc, 1), nil)
		require.NoError(b, err)
	}
}

func BenchmarkExecutorConstandFold2_Free(b *testing.B) {
	mp, _ := mpool.NewMPool("", 0, 0)
	proc := testutil.NewProcessWithMPool("", mp)
	newParamForFoldCase2(proc)
	expr := generateFoldCase2()
	proc.SetBaseProcessRunningStatus(true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, colexec.SplitAndExprs([]*plan.Expr{expr}))
		require.NoError(b, err)
		_, err = executor[0].Eval(proc, newBatchForFoldCase(proc, 1), nil)
		require.NoError(b, err)
		executor[0].ResetForNextQuery()

		// _, err = executor[0].Eval(proc, newBatchForFoldCase(proc, 1), nil)
		// require.NoError(b, err)
		// executor[0].ResetForNextQuery()

		_, err = executor[0].Eval(proc, newBatchForFoldCase(proc, 1), nil)
		require.NoError(b, err)
		executor[0].Free()
	}
}

func newBatchForFoldCase(proc *process.Process, rows int64) []*batch.Batch {
	ts := []types.Type{types.New(types.T_varchar, 65535, 0), types.New(types.T_varchar, 65535, 0)}
	bat := testutil.NewBatch(ts, false, int(rows), proc.Mp())
	pkAttr := make([]string, 2)
	pkAttr[0] = "compound_key_col"
	pkAttr[1] = "val"
	bat.SetAttributes(pkAttr)
	return []*batch.Batch{bat, nil}
}

func newParamForFoldCase2(proc *process.Process) {
	values := []string{"3", "3"}
	rowCount := len(values)
	prepareParams := testutil.NewVector(rowCount, types.New(types.T_text, types.MaxVarcharLen, 0), proc.GetMPool(), false, values)
	proc.SetPrepareParams(prepareParams)
	return
}

// util function to generate expr to test constand fold performance
func generateFoldCase1() *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id: int32(types.T_bool),
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "=",
					Obj:     function.EQUAL,
				},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 65535,
						},
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: 0,
								ColPos: 1,
								Name:   "compound_key_col",
							},
						},
					},
					{
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 65535,
						},
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{
									ObjName: "serial",
									Obj:     function.SerialFunctionEncodeID,
								},
								Args: []*plan.Expr{
									{
										Typ: plan.Type{
											Id: int32(types.T_int64),
										},
										Expr: &plan.Expr_Lit{
											Lit: &plan.Literal{
												Isnull: false,
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
												Isnull: false,
												Value: &plan.Literal_I64Val{
													I64Val: 1,
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// Expr_F(
//
//	Func["="](nargs=2)
//	Expr_Col(bmsql_district.__mo_cpkey_col)	Expr_Selectivity(0)
//	Expr_F(
//		Func["serial"](nargs=2)
//		Expr_F(
//			Func["cast"](nargs=2)
//			Expr_P(1)			Expr_Selectivity(0)
//			Expr_T()			Expr_Selectivity(0)
//		)		Expr_Selectivity(0)
//		Expr_F(
//			Func["cast"](nargs=2)
//			Expr_P(2)			Expr_Selectivity(0)
//			Expr_T()			Expr_Selectivity(0)
//		)		Expr_Selectivity(0)
//	)	Expr_Selectivity(0)
//
// )Expr_Selectivity(0.01)
func generateFoldCase2() *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id: int32(types.T_bool),
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "=",
				},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 65535,
						},
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: 0,
								ColPos: 1,
								Name:   "compound_key_col",
							},
						},
					},
					{
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 65535,
						},
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{
									ObjName: "serial",
									Obj:     function.SerialFunctionEncodeID,
								},
								Args: []*plan.Expr{
									{
										Typ: plan.Type{
											Id:    int32(types.T_int32),
											Width: 32,
											Scale: -1,
										},
										Expr: &plan.Expr_F{
											F: &plan.Function{
												Func: &plan.ObjectRef{
													ObjName: "cast",
													Obj:     function.CastFunctionEncodeID,
												},
												Args: []*plan.Expr{
													{
														Typ: plan.Type{
															Id: int32(types.T_text),
														},
														Expr: &plan.Expr_P{
															P: &plan.ParamRef{
																Pos: 0,
															},
														},
													},
													{
														Typ: plan.Type{
															Id:    int32(types.T_int32),
															Width: 32,
															Scale: -1,
														},
														Expr: &plan.Expr_T{},
													},
												},
											},
										},
									},
									{
										Typ: plan.Type{
											Id:    int32(types.T_int32),
											Width: 32,
											Scale: -1,
										},
										Expr: &plan.Expr_F{
											F: &plan.Function{
												Func: &plan.ObjectRef{
													ObjName: "cast",
													Obj:     function.CastFunctionEncodeID,
												},
												Args: []*plan.Expr{
													{
														Typ: plan.Type{
															Id: int32(types.T_text),
														},
														Expr: &plan.Expr_P{
															P: &plan.ParamRef{
																Pos: 1,
															},
														},
													},
													{
														Typ: plan.Type{
															Id:    int32(types.T_int32),
															Width: 32,
															Scale: -1,
														},
														Expr: &plan.Expr_T{},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
