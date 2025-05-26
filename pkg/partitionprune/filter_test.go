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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
			// a = 0 or a = 5
			// a < 1
			// 1 <= a < 2
			// 2 <= a < 3
			name: "range filter - or condition",
			filters: []*plan.Expr{
				makeOrExpr(
					makeEqualExpr(0, 0),
					makeEqualExpr(0, 5),
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
			want:    []int{0},
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
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "=",
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
