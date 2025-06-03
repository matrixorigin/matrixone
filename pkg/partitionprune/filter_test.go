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

package partitionprune

import (
	"context"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	proc := process.NewTopProcess(
		context.Background(),
		mp,
		nil, // no txn client
		nil, // no txn operator
		nil, // no file service
		nil, // no lock service
		nil, // no query client
		nil, // no hakeeper
		nil, // no udf service
		nil, // no auto increase
	)
	defer proc.Free()

	tests := []struct {
		name     string
		filters  []*plan.Expr
		metadata partition.PartitionMetadata
		want     []int
		wantErr  bool
	}{
		{
			name:    "empty filters",
			filters: []*plan.Expr{},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Range,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestRangeExpr("a", 0)},
					{Position: 1, Expr: newTestRangeExpr("a", 1)},
					{Position: 2, Expr: newTestRangeExpr("a", 2)},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			// a = 1
			// a < 1
			// 1 <= a < 2
			// 2 <= a < 3
			name: "range filter - equal condition",
			filters: []*plan.Expr{
				makeEqualExpr(0, 1),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Range,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestRangeExpr("a", 0)},
					{Position: 1, Expr: newTestRangeExpr("a", 1)},
					{Position: 2, Expr: newTestRangeExpr("a", 2)},
				},
			},
			want:    []int{1},
			wantErr: false,
		},
		{
			// a = 5
			// a % 3
			name: "hash filter - equal condition",
			filters: []*plan.Expr{
				makeEqualExpr(0, 5),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Hash,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestHashExpr("a", 0)},
					{Position: 1, Expr: newTestHashExpr("a", 1)},
					{Position: 2, Expr: newTestHashExpr("a", 2)},
				},
			},
			want:    []int{2},
			wantErr: false,
		},
		{
			// a = 1, 2, 3 -> int32
			// a in (1, 2)
			name: "list filter - in condition",
			filters: []*plan.Expr{
				makeInExpr(0, []int32{1, 2, 3}),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_List,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestValuesInExpr("a")},
					{Position: 1, Expr: newTestValuesInExpr("a")},
					{Position: 2, Expr: newTestValuesInExpr("a")},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			// a = 1 or a = 2
			// a in (1, 2)
			name: "list filter - or condition",
			filters: []*plan.Expr{
				makeOrExpr(
					makeEqualExprInt32(0, 1),
					makeEqualExprInt32(0, 2),
				),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_List,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestValuesInExpr("a")},
					{Position: 1, Expr: newTestValuesInExpr("a")},
					{Position: 2, Expr: newTestValuesInExpr("a")},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			// a = 1 and a = 2
			// a in (1, 2)
			name: "list filter - and condition",
			filters: []*plan.Expr{
				makeAndExpr(
					makeEqualExprInt32(0, 1),
					makeEqualExprInt32(0, 2),
				),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_List,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestValuesInExpr("a")},
					{Position: 1, Expr: newTestValuesInExpr("a")},
					{Position: 2, Expr: newTestValuesInExpr("a")},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			name: "range filter - and condition",
			filters: []*plan.Expr{
				makeAndExpr(
					makeGreaterEqualExpr(0, 15),
					makeLessEqualExpr(0, 25),
				),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Range,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestRangeExpr("a", 0)},
					{Position: 1, Expr: newTestRangeExpr("a", 1)},
					{Position: 2, Expr: newTestRangeExpr("a", 2)},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			// a = 5 or a = 8
			// a % 3
			name: "hash filter - or condition",
			filters: []*plan.Expr{
				makeOrExpr(
					makeEqualExpr(0, 5),
					makeEqualExpr(0, 8),
				),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Hash,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestHashExpr("a", 0)},
					{Position: 1, Expr: newTestHashExpr("a", 1)},
					{Position: 2, Expr: newTestHashExpr("a", 2)},
				},
			},
			want:    []int{2},
			wantErr: false,
		},
		{
			// a = 5 and a = 8
			// a % 3
			name: "hash filter - and condition",
			filters: []*plan.Expr{
				makeAndExpr(
					makeEqualExpr(0, 5),
					makeEqualExpr(0, 8),
				),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Hash,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestHashExpr("a", 0)},
					{Position: 1, Expr: newTestHashExpr("a", 1)},
					{Position: 2, Expr: newTestHashExpr("a", 2)},
				},
			},
			want:    []int{2},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Filter(proc, tt.filters, tt.metadata)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.ElementsMatch(t, tt.want, got)
		})
	}
}

// Helper functions to create test expressions
func makeEqualExpr(colPos int32, value int64) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "=",
				},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: int32(types.T_int64)},
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								ColPos: colPos,
							},
						},
					},
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Isnull: false,
								Value: &plan.Literal_I64Val{
									I64Val: value,
								},
							},
						},
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
					},
				},
			},
		},
	}
}

func makeEqualExprInt32(colPos int32, value int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "=",
				},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: int32(types.T_int32)},
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								ColPos: colPos,
							},
						},
					},
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Isnull: false,
								Value: &plan.Literal_I32Val{
									I32Val: value,
								},
							},
						},
						Typ: plan.Type{
							Id: int32(types.T_int32),
						},
					},
				},
			},
		},
	}
}

func makeInExpr(colPos int32, values []int32) *plan.Expr {
	list := make([]*plan.Expr, len(values))
	for i, v := range values {
		list[i] = &plan.Expr{
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Isnull: false,
					Value: &plan.Literal_I32Val{
						I32Val: v,
					},
				},
			},
			Typ: plan.Type{
				Id: int32(types.T_int32),
			},
		}
	}
	return &plan.Expr{
		Typ: plan.Type{Id: 10},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "in",
				},
				Args: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								ColPos: colPos,
							},
						},
					},
					{
						Expr: &plan.Expr_List{
							List: &plan.ExprList{
								List: list,
							},
						},
					},
				},
			},
		},
	}
}

func newTestHashExpr(col string, id uint64) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: 10},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     0,
					ObjName: "=",
				},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: 28},
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{
									Obj:     64424509440,
									ObjName: "%",
								},
								Args: []*plan.Expr{
									{
										Typ: plan.Type{Id: 28},
										Expr: &plan.Expr_F{
											F: &plan.Function{
												Func: &plan.ObjectRef{
													Obj:     90194313216,
													ObjName: "cast",
												},
												Args: []*plan.Expr{
													{
														Typ: plan.Type{Id: 22},
														Expr: &plan.Expr_Col{
															Col: &plan.ColRef{
																RelPos: 1,
																ColPos: 0,
																Name:   col,
															},
														},
													},
													{
														Typ:  plan.Type{Id: 28},
														Expr: &plan.Expr_T{T: &plan.TargetType{}},
													},
												},
											},
										},
									},
									{
										Typ: plan.Type{Id: 28},
										Expr: &plan.Expr_Lit{
											Lit: &plan.Literal{
												Value: &plan.Literal_U64Val{U64Val: 3},
											},
										},
									},
								},
							},
						},
					},
					{
						Typ: plan.Type{Id: 28},
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Value: &plan.Literal_U64Val{U64Val: id},
							},
						},
					},
				},
			},
		},
	}
}

func newTestRangeExpr(col string, partitionNum int64) *plan.Expr {
	if partitionNum == 0 {
		// a < 1
		return &plan.Expr{
			Typ: plan.Type{Id: 10},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{
						ObjName: "<",
						Obj:     17179869184,
					},
					Args: []*plan.Expr{
						{
							Typ: plan.Type{Id: 22},
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{RelPos: 1, ColPos: 0, Name: col},
							},
						},
						{
							Typ: plan.Type{Id: 22},
							Expr: &plan.Expr_F{
								F: &plan.Function{
									Func: &plan.ObjectRef{
										Obj:     90194313216,
										ObjName: "cast",
									},
									Args: []*plan.Expr{
										{
											Typ: plan.Type{Id: 23},
											Expr: &plan.Expr_Lit{
												Lit: &plan.Literal{
													Value: &plan.Literal_I64Val{I64Val: 1},
												},
											},
										},
										{
											Typ:  plan.Type{Id: 22},
											Expr: &plan.Expr_T{T: &plan.TargetType{}},
										},
									},
								},
							},
						},
					},
				},
			},
		}
	} else {
		// partitionNum <= a < partitionNum + 1
		return &plan.Expr{
			Typ: plan.Type{Id: 10},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{
						ObjName: "and",
						Obj:     73014444032,
					},
					Args: []*plan.Expr{
						// partitionNum <= a
						{
							Typ: plan.Type{Id: 10},
							Expr: &plan.Expr_F{
								F: &plan.Function{
									Func: &plan.ObjectRef{
										ObjName: ">=",
										Obj:     12884901888,
									},
									Args: []*plan.Expr{
										{
											Typ: plan.Type{Id: 22},
											Expr: &plan.Expr_Col{
												Col: &plan.ColRef{RelPos: 1, ColPos: 0, Name: col},
											},
										},
										{
											Typ: plan.Type{Id: 22},
											Expr: &plan.Expr_F{
												F: &plan.Function{
													Func: &plan.ObjectRef{
														Obj:     90194313216,
														ObjName: "cast",
													},
													Args: []*plan.Expr{
														{
															Typ: plan.Type{Id: 23},
															Expr: &plan.Expr_Lit{
																Lit: &plan.Literal{
																	Value: &plan.Literal_I64Val{I64Val: partitionNum},
																},
															},
														},
														{
															Typ:  plan.Type{Id: 22},
															Expr: &plan.Expr_T{T: &plan.TargetType{}},
														},
													},
												},
											},
										},
									},
								},
							},
						},
						// a < partitionNum + 1
						{
							Typ: plan.Type{Id: 10},
							Expr: &plan.Expr_F{
								F: &plan.Function{
									Func: &plan.ObjectRef{
										ObjName: "<",
										Obj:     17179869184,
									},
									Args: []*plan.Expr{
										{
											Typ: plan.Type{Id: 22},
											Expr: &plan.Expr_Col{
												Col: &plan.ColRef{RelPos: 1, ColPos: 0, Name: col},
											},
										},
										{
											Typ: plan.Type{Id: 22},
											Expr: &plan.Expr_F{
												F: &plan.Function{
													Func: &plan.ObjectRef{
														Obj:     90194313216,
														ObjName: "cast",
													},
													Args: []*plan.Expr{
														{
															Typ: plan.Type{Id: 23},
															Expr: &plan.Expr_Lit{
																Lit: &plan.Literal{
																	Value: &plan.Literal_I64Val{I64Val: partitionNum + 1},
																},
															},
														},
														{
															Typ:  plan.Type{Id: 22},
															Expr: &plan.Expr_T{T: &plan.TargetType{}},
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
}

func newTestValuesInExpr(col string) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: 10},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "in",
					Obj:     506806140934,
				},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: 22},
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{RelPos: 1, ColPos: 0, Name: col},
						},
					},
					{
						Typ: plan.Type{Id: 202},
						Expr: &plan.Expr_List{
							List: &plan.ExprList{
								List: []*plan.Expr{
									{
										Typ: plan.Type{Id: 22},
										Expr: &plan.Expr_F{
											F: &plan.Function{
												Func: &plan.ObjectRef{
													Obj:     90194313216,
													ObjName: "cast",
												},
												Args: []*plan.Expr{
													{
														Typ: plan.Type{Id: 23},
														Expr: &plan.Expr_Lit{
															Lit: &plan.Literal{
																Value: &plan.Literal_I64Val{I64Val: 1},
															},
														},
													},
													{
														Typ:  plan.Type{Id: 22},
														Expr: &plan.Expr_T{T: &plan.TargetType{}},
													},
												},
											},
										},
									},
									{
										Typ: plan.Type{Id: 22},
										Expr: &plan.Expr_F{
											F: &plan.Function{
												Func: &plan.ObjectRef{ObjName: "cast", Obj: 90194313216},
												Args: []*plan.Expr{
													{
														Typ: plan.Type{Id: 23},
														Expr: &plan.Expr_Lit{
															Lit: &plan.Literal{
																Value: &plan.Literal_I64Val{I64Val: 2},
															},
														},
													},
													{
														Typ:  plan.Type{Id: 22},
														Expr: &plan.Expr_T{T: &plan.TargetType{}},
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

func makeOrExpr(left, right *plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "or",
				},
				Args: []*plan.Expr{left, right},
			},
		},
	}
}

func makeAndExpr(left, right *plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "and",
				},
				Args: []*plan.Expr{left, right},
			},
		},
	}
}

func makeGreaterEqualExpr(colPos int32, value int64) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: ">=",
				},
				Args: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								ColPos: colPos,
							},
						},
					},
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Isnull: false,
								Value: &plan.Literal_I64Val{
									I64Val: value,
								},
							},
						},
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
					},
				},
			},
		},
	}
}

func makeLessEqualExpr(colPos int32, value int64) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "<=",
				},
				Args: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								ColPos: colPos,
							},
						},
					},
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Isnull: false,
								Value: &plan.Literal_I64Val{
									I64Val: value,
								},
							},
						},
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
					},
				},
			},
		},
	}
}

func TestConvertFoldExprToNormal(t *testing.T) {
	tests := []struct {
		name    string
		expr    *plan.Expr
		want    *plan.Expr
		wantErr bool
	}{
		{
			name: "constant fold expression - int64",
			expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Fold{
					Fold: &plan.FoldVal{
						IsConst: true,
						Data:    types.EncodeInt64(&[]int64{42}[0]),
					},
				},
			},
			want: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_I64Val{I64Val: 42},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "constant fold expression - int32",
			expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int32)},
				Expr: &plan.Expr_Fold{
					Fold: &plan.FoldVal{
						IsConst: true,
						Data:    types.EncodeInt32(&[]int32{42}[0]),
					},
				},
			},
			want: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int32)},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_I32Val{I32Val: 42},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "constant fold expression - float64",
			expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_float64)},
				Expr: &plan.Expr_Fold{
					Fold: &plan.FoldVal{
						IsConst: true,
						Data:    types.EncodeFloat64(&[]float64{42.5}[0]),
					},
				},
			},
			want: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_float64)},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_Dval{Dval: 42.5},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "constant fold expression - bool",
			expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_bool)},
				Expr: &plan.Expr_Fold{
					Fold: &plan.FoldVal{
						IsConst: true,
						Data:    types.EncodeBool(&[]bool{true}[0]),
					},
				},
			},
			want: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_bool)},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_Bval{Bval: true},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "constant fold expression - varchar",
			expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_varchar)},
				Expr: &plan.Expr_Fold{
					Fold: &plan.FoldVal{
						IsConst: true,
						Data:    []byte("test"),
					},
				},
			},
			want: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_varchar)},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_Sval{Sval: "test"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "function expression",
			expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_bool)},
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "="},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{ColPos: 0},
								},
							},
							{
								Expr: &plan.Expr_Lit{
									Lit: &plan.Literal{
										Value: &plan.Literal_I64Val{I64Val: 42},
									},
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "vector fold expression",
			expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Fold{
					Fold: &plan.FoldVal{
						IsConst: false,
						Data: func() []byte {
							mp := mpool.MustNewZeroNoFixed()
							vec := vector.NewVec(types.T_int64.ToType())
							_ = vector.AppendFixed[int64](vec, int64(1), false, mp)
							data, _ := vec.MarshalBinary()
							return data
						}(),
					},
				},
			},
			want: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Vec{
					Vec: &plan.LiteralVec{
						Len: 1,
						Data: func() []byte {
							mp := mpool.MustNewZeroNoFixed()
							vec := vector.NewVec(types.T_int64.ToType())
							_ = vector.AppendFixed[int64](vec, int64(1), false, mp)
							data, _ := vec.MarshalBinary()
							return data
						}(),
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertFoldExprToNormal(tt.expr)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.want != nil {
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestGetConstantFromBytes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		typ      plan.Type
		expected *plan.Literal
	}{
		{
			name: "bool type",
			data: types.EncodeBool(&[]bool{true}[0]),
			typ: plan.Type{
				Id: int32(types.T_bool),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Bval{Bval: true},
			},
		},
		{
			name: "int8 type",
			data: types.EncodeInt8(&[]int8{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_int8),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_I32Val{I32Val: 42},
			},
		},
		{
			name: "int16 type",
			data: types.EncodeInt16(&[]int16{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_int16),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_I32Val{I32Val: 42},
			},
		},
		{
			name: "int32 type",
			data: types.EncodeInt32(&[]int32{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_int32),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_I32Val{I32Val: 42},
			},
		},
		{
			name: "int64 type",
			data: types.EncodeInt64(&[]int64{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_int64),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_I64Val{I64Val: 42},
			},
		},
		{
			name: "uint8 type",
			data: types.EncodeUint8(&[]uint8{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_uint8),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_U32Val{U32Val: 42},
			},
		},
		{
			name: "uint16 type",
			data: types.EncodeUint16(&[]uint16{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_uint16),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_U32Val{U32Val: 42},
			},
		},
		{
			name: "uint32 type",
			data: types.EncodeUint32(&[]uint32{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_uint32),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_U32Val{U32Val: 42},
			},
		},
		{
			name: "uint64 type",
			data: types.EncodeUint64(&[]uint64{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_uint64),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_U64Val{U64Val: 42},
			},
		},
		{
			name: "float32 type",
			data: types.EncodeFloat32(&[]float32{42.5}[0]),
			typ: plan.Type{
				Id: int32(types.T_float32),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Fval{Fval: 42.5},
			},
		},
		{
			name: "float64 type",
			data: types.EncodeFloat64(&[]float64{42.5}[0]),
			typ: plan.Type{
				Id: int32(types.T_float64),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Dval{Dval: 42.5},
			},
		},
		{
			name: "date type",
			data: types.EncodeDate(&[]types.Date{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_date),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Dateval{Dateval: 42},
			},
		},
		{
			name: "datetime type",
			data: types.EncodeDatetime(&[]types.Datetime{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_datetime),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Datetimeval{Datetimeval: 42},
			},
		},
		{
			name: "timestamp type",
			data: types.EncodeTimestamp(&[]types.Timestamp{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_timestamp),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Timestampval{Timestampval: 42},
			},
		},
		{
			name: "decimal64 type",
			data: types.EncodeDecimal64(&[]types.Decimal64{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_decimal64),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: 42}},
			},
		},
		{
			name: "decimal128 type",
			data: types.EncodeDecimal128(&[]types.Decimal128{{B0_63: 42, B64_127: 0}}[0]),
			typ: plan.Type{
				Id: int32(types.T_decimal128),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Decimal128Val{Decimal128Val: &plan.Decimal128{A: 42, B: 0}},
			},
		},
		{
			name: "varchar type",
			data: []byte("test"),
			typ: plan.Type{
				Id: int32(types.T_varchar),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Sval{Sval: "test"},
			},
		},
		{
			name: "empty data",
			data: []byte{},
			typ: plan.Type{
				Id: int32(types.T_int64),
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := getConstantFromBytes(tt.data, tt.typ)
			if err != nil {
				t.Errorf("getConstantFromBytes() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("getConstantFromBytes() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMergeSortedSlices(t *testing.T) {
	tests := []struct {
		name   string
		slice1 []int
		slice2 []int
		want   []int
	}{
		{
			name:   "empty slices",
			slice1: []int{},
			slice2: []int{},
			want:   []int{},
		},
		{
			name:   "one empty slice",
			slice1: []int{1, 2, 3},
			slice2: []int{},
			want:   []int{1, 2, 3},
		},
		{
			name:   "no duplicates",
			slice1: []int{1, 3, 5},
			slice2: []int{2, 4, 6},
			want:   []int{1, 2, 3, 4, 5, 6},
		},
		{
			name:   "with duplicates",
			slice1: []int{1, 2, 3},
			slice2: []int{2, 3, 4},
			want:   []int{1, 2, 3, 4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeSortedSlices(tt.slice1, tt.slice2)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIntersectSortedSlices(t *testing.T) {
	tests := []struct {
		name   string
		slice1 []int
		slice2 []int
		want   []int
	}{
		{
			name:   "empty slices",
			slice1: []int{},
			slice2: []int{},
			want:   []int{},
		},
		{
			name:   "one empty slice",
			slice1: []int{1, 2, 3},
			slice2: []int{},
			want:   []int{},
		},
		{
			name:   "no intersection",
			slice1: []int{1, 3, 5},
			slice2: []int{2, 4, 6},
			want:   []int{},
		},
		{
			name:   "with intersection",
			slice1: []int{1, 2, 3},
			slice2: []int{2, 3, 4},
			want:   []int{2, 3},
		},
		{
			name:   "with duplicates",
			slice1: []int{1, 2, 2, 3},
			slice2: []int{2, 2, 3, 4},
			want:   []int{2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := intersectSortedSlices(tt.slice1, tt.slice2)
			require.Equal(t, tt.want, got)
		})
	}
}
